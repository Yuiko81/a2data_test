import argparse
import asyncio
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import asyncpg


DATABASE_URL = os.environ["DATABASE_URL"]
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "7"))
RETENTION_INTERVAL_SECONDS = int(os.getenv("RETENTION_INTERVAL_SECONDS", "3600"))

IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$") # защита от logs; DROP DATABASE


@dataclass(frozen=True) # frozen=True запрещает переназначать атрибуты но листы можно изменять аппендами
class RetentionResult:
    dropped: list[str]
    skipped: list[str]


def validate_identifier(value: str) -> str: # мэтч с регуляркой
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid PostgreSQL identifier: {value}")
    return value


def quote_identifier(value: str) -> str:
    return f'"{validate_identifier(value)}"'


async def get_partitions(
    connection: asyncpg.Connection,
    *,
    schema: str,
    table: str,
) -> set[str]:
    rows = await connection.fetch(
        """
        SELECT child.relname
        FROM pg_inherits
        JOIN pg_class AS parent ON parent.oid = inhparent
        JOIN pg_namespace AS parent_namespace
          ON parent_namespace.oid = parent.relnamespace
        JOIN pg_class AS child ON child.oid = inhrelid
        WHERE parent_namespace.nspname = $1
          AND parent.relname = $2
        """,
        schema,
        table,
    )
    return {row["relname"] for row in rows}


async def drop_expired_partitions(
    connection: asyncpg.Connection,
    *,
    now: datetime | None = None,
    retention_days: int = RETENTION_DAYS,
    schema: str = "public",
    table: str = "logs",
    dry_run: bool = False, # для тестов
) -> RetentionResult:
    if retention_days < 1:
        raise ValueError("retention_days must be at least 1")

    schema = validate_identifier(schema)
    table = validate_identifier(table)
    current_time = now or datetime.now(timezone.utc)
    if current_time.tzinfo is None or current_time.utcoffset() is None:
        raise ValueError("now must include a timezone")

    cutoff_day = (
        current_time.astimezone(timezone.utc) - timedelta(days=retention_days)
    ).date()
    partition_pattern = re.compile(rf"^{re.escape(table)}_(\d{{8}})$")
    lock_name = f"{schema}.{table}.retention"
    dropped: list[str] = []
    skipped: list[str] = []

    lock_acquired = await connection.fetchval(
        "SELECT pg_try_advisory_lock(hashtext($1)::bigint)",
        lock_name,
    )
    if not lock_acquired:
        return RetentionResult(dropped=[], skipped=[])

    try:
        await connection.execute("SET lock_timeout = '2s'")
        partitions = await get_partitions(connection, schema=schema, table=table)

        for name in sorted(partitions):
            match = partition_pattern.fullmatch(name)
            if match is None:
                continue

            partition_day = datetime.strptime(match.group(1), "%Y%m%d").date()
            if partition_day >= cutoff_day:
                continue

            if dry_run: # для тестов
                dropped.append(name)
                continue

            try:
                await connection.execute(
                    f"DROP TABLE {quote_identifier(schema)}.{quote_identifier(name)}"
                )
                dropped.append(name)
            except asyncpg.LockNotAvailableError:
                skipped.append(name)
    finally:
        await connection.execute(
            "SELECT pg_advisory_unlock(hashtext($1)::bigint)",
            lock_name,
        )

    return RetentionResult(dropped=dropped, skipped=skipped)


async def run_once(*, dry_run: bool) -> RetentionResult:
    connection = await asyncpg.connect(DATABASE_URL)
    try:
        return await drop_expired_partitions(connection, dry_run=dry_run)
    finally:
        await connection.close()


async def main(*, once: bool, dry_run: bool) -> None:
    while True:
        result = await run_once(dry_run=dry_run)
        action = "Would drop" if dry_run else "Dropped"
        print(f"{action}: {result.dropped}; skipped because of locks: {result.skipped}")

        if once or dry_run:
            return
        await asyncio.sleep(RETENTION_INTERVAL_SECONDS)


# параметры запуска
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    arguments = parser.parse_args()
    asyncio.run(main(once=arguments.once, dry_run=arguments.dry_run))

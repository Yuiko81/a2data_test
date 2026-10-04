import argparse
import asyncio
import os
import re
from datetime import datetime, timedelta, timezone

import asyncpg


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://myuser:mypassword@localhost:5432/mydb",
)
PARTITIONS_AHEAD = int(os.getenv("PARTITIONS_AHEAD", "1"))
PARTITION_CREATION_INTERVAL_SECONDS = int(os.getenv("PARTITION_CREATION_INTERVAL_SECONDS", "86400")) # проверка раз в сутки

IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")


def validate_identifier(value: str) -> str:
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


async def create_missing_partitions(
    connection: asyncpg.Connection,
    *,
    now: datetime | None = None,
    days_ahead: int = PARTITIONS_AHEAD,
    schema: str = "public",
    table: str = "logs",
    dry_run: bool = False, # для тестов
) -> list[str]:
    if days_ahead < 0:
        raise ValueError("days_ahead cannot be negative")

    schema = validate_identifier(schema)
    table = validate_identifier(table)
    current_time = now or datetime.now(timezone.utc)
    if current_time.tzinfo is None or current_time.utcoffset() is None:
        raise ValueError("now must include a timezone")

    current_day = current_time.astimezone(timezone.utc).date()
    lock_name = f"{schema}.{table}.partition_creation" # защита от конфликтов при работе в бд
    created: list[str] = []

    lock_acquired = await connection.fetchval(
        "SELECT pg_try_advisory_lock(hashtext($1)::bigint)",
        lock_name,
    )
    if not lock_acquired:
        return created

    try:
        await connection.execute("SET lock_timeout = '2s'")
        partitions = await get_partitions(connection, schema=schema, table=table) # уже существующие партиции

        # days_ahead=1: проверяем партиции на сегодня и завтра.
        for day_offset in range(days_ahead + 1):
            partition_day = current_day + timedelta(days=day_offset)
            partition_name = f"{table}_{partition_day:%Y%m%d}"
            if partition_name in partitions:
                continue

            if dry_run:
                created.append(partition_name)
                continue

            next_day = partition_day + timedelta(days=1)
            await connection.execute(
                f"""
                CREATE TABLE {quote_identifier(schema)}.{quote_identifier(partition_name)}
                PARTITION OF {quote_identifier(schema)}.{quote_identifier(table)}
                FOR VALUES FROM ('{partition_day.isoformat()} 00:00:00+00')
                TO ('{next_day.isoformat()} 00:00:00+00')
                """
            )
            created.append(partition_name)
    finally:
        await connection.execute(
            "SELECT pg_advisory_unlock(hashtext($1)::bigint)",
            lock_name,
        )

    return created


async def run_once(*, dry_run: bool) -> list[str]:
    connection = await asyncpg.connect(DATABASE_URL)
    try:
        return await create_missing_partitions(connection, dry_run=dry_run)
    finally:
        await connection.close()


async def main(*, once: bool, dry_run: bool) -> None:
    while True:
        created = await run_once(dry_run=dry_run)
        action = "Would create" if dry_run else "Created"
        print(f"{action}: {created}")

        if once or dry_run:
            return
        await asyncio.sleep(PARTITION_CREATION_INTERVAL_SECONDS)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    arguments = parser.parse_args()
    asyncio.run(main(once=arguments.once, dry_run=arguments.dry_run))

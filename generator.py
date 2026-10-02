import asyncio
import os
import random
import time
from datetime import datetime, timedelta, timezone

import asyncpg


SERVICES = ("payments", "orders", "auth", "billing")
LEVELS = ("INFO", "WARNING", "ERROR")
MESSAGES = (
    "Request completed",
    "Request processing failed",
    "Connection timeout",
    "Retry scheduled",
    "Invalid request",
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://myuser:mypassword@localhost:5432/mydb",
)
TOTAL_RECORDS = int(os.getenv("SEED_TOTAL", "5000000"))
BATCH_SIZE = int(os.getenv("SEED_BATCH_SIZE", "5000"))


def generate_batch(size: int, window_end: datetime) -> list[tuple]:
    period_seconds = 7 * 24 * 60 * 60

    return [
        (
            random.choice(SERVICES),
            window_end - timedelta(seconds=random.uniform(0, period_seconds)),
            random.choice(LEVELS),
            random.choice(MESSAGES),
        )
        for _ in range(size)
    ]


async def fill_database(connection: asyncpg.Connection) -> None:
    current_count = await connection.fetchval("SELECT count(*) FROM logs")
    remaining = max(TOTAL_RECORDS - current_count, 0)

    if remaining == 0:
        print(f"Database already contains {current_count:,} log records")
        return

    inserted = current_count
    window_end = datetime.now(timezone.utc)
    started_at = time.perf_counter()

    while remaining > 0:
        current_batch_size = min(BATCH_SIZE, remaining)
        batch = generate_batch(current_batch_size, window_end)

        async with connection.transaction():
            await connection.copy_records_to_table(
                "logs",
                records=batch,
                columns=("service", "timestamp", "level", "message"),
            )

        inserted += current_batch_size
        remaining -= current_batch_size
        elapsed = time.perf_counter() - started_at
        print(
            f"Inserted {inserted:,}/{TOTAL_RECORDS:,} "
            f"records in {elapsed:.1f} seconds"
        )

    await connection.execute("ANALYZE logs")
    print("Database filling completed")


async def main() -> None:
    connection = await asyncpg.connect(DATABASE_URL)

    try:
        await fill_database(connection)
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import os
import statistics
import time
from datetime import datetime, timedelta, timezone

import asyncpg


DATABASE_URL = os.environ["DATABASE_URL"]
TOTAL_RECORDS = int(os.getenv("INSERT_BENCHMARK_RECORDS", "100000"))
BATCH_SIZE = int(os.getenv("INSERT_BENCHMARK_BATCH_SIZE", "1000"))
WARMUP_RUNS = int(os.getenv("INSERT_BENCHMARK_WARMUP_RUNS", "1"))
MEASURE_RUNS = int(os.getenv("INSERT_BENCHMARK_MEASURE_RUNS", "3"))


def validate_settings() -> None:
    settings = {
        "INSERT_BENCHMARK_RECORDS": TOTAL_RECORDS,
        "INSERT_BENCHMARK_BATCH_SIZE": BATCH_SIZE,
        "INSERT_BENCHMARK_WARMUP_RUNS": WARMUP_RUNS,
        "INSERT_BENCHMARK_MEASURE_RUNS": MEASURE_RUNS,
    }
    for name, value in settings.items():
        if value <= 0:
            raise ValueError(f"{name} must be greater than zero")


async def measure_copy(connection: asyncpg.Connection) -> float:
    timestamp = datetime.now(timezone.utc)
    batch = [
        (
            "insert-benchmark",
            timestamp + timedelta(microseconds=index),
            "INFO",
            f"benchmark message {index}",
        )
        for index in range(BATCH_SIZE)
    ]
    transaction = connection.transaction()
    await transaction.start()

    try:
        remaining = TOTAL_RECORDS
        started_at = time.perf_counter()

        while remaining > 0:
            current_size = min(BATCH_SIZE, remaining)
            await connection.copy_records_to_table(
                "logs",
                records=batch[:current_size],
                columns=("service", "timestamp", "level", "message"),
            )
            remaining -= current_size

        return time.perf_counter() - started_at
    finally:
        await transaction.rollback()


async def main() -> None:
    validate_settings()
    connection = await asyncpg.connect(DATABASE_URL)

    try:
        for run_number in range(1, WARMUP_RUNS + 1):
            elapsed = await measure_copy(connection)
            print(f"Warm-up {run_number}: {elapsed:.3f}s")

        measurements = []
        for run_number in range(1, MEASURE_RUNS + 1):
            elapsed = await measure_copy(connection)
            measurements.append(elapsed)
            print(
                f"Measurement {run_number}: {elapsed:.3f}s, "
                f"{TOTAL_RECORDS / elapsed:,.0f} rows/s"
            )

        median_seconds = statistics.median(measurements)
        print(
            f"Median: {median_seconds:.3f}s for {TOTAL_RECORDS:,} rows, "
            f"{TOTAL_RECORDS / median_seconds:,.0f} rows/s"
        )
        print("All benchmark transactions were rolled back")
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())

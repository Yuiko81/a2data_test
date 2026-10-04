import asyncio
import math
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, time as datetime_time, timedelta, timezone
from typing import Awaitable, Callable

import aiohttp
import asyncpg
import redis.asyncio as redis


API_BASE_URL = os.getenv("LOAD_API_BASE_URL", "http://api:8000").rstrip("/")
DATABASE_URL = os.environ["DATABASE_URL"]
REDIS_URL = os.environ["REDIS_URL"]
STREAM_NAME = os.getenv("REDIS_STREAM_NAME", "logs")

WARMUP_SECONDS = int(os.getenv("LOAD_WARMUP_SECONDS", "10"))
DURATION_SECONDS = int(os.getenv("LOAD_DURATION_SECONDS", "60"))
LOGS_PER_SECOND = int(os.getenv("LOAD_LOGS_PER_SECOND", "1000"))
BATCH_SIZE = int(os.getenv("LOAD_BATCH_SIZE", "100"))
SEARCH_REQUESTS_PER_SECOND = int(os.getenv("LOAD_SEARCH_RPS", "20"))
SEARCH_P95_LIMIT_MS = float(os.getenv("LOAD_SEARCH_P95_LIMIT_MS", "300"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("LOAD_REQUEST_TIMEOUT_SECONDS", "10"))
DRAIN_TIMEOUT_SECONDS = float(os.getenv("LOAD_DRAIN_TIMEOUT_SECONDS", "30"))
MIN_DATABASE_ROWS = int(os.getenv("LOAD_MIN_DATABASE_ROWS", "5000000"))
MAX_SCHEDULER_LAG_MS = float(os.getenv("LOAD_MAX_SCHEDULER_LAG_MS", "100"))


@dataclass
class RequestMetrics:
    latencies_ms: list[float] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    requests: int = 0
    error_count: int = 0
    accepted_logs: int = 0

    def record_error(self, message: str) -> None:
        self.error_count += 1
        if len(self.errors) < 5:
            self.errors.append(message)


def percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(math.ceil(len(ordered) * percent) - 1, 0)
    return ordered[index]


def validate_settings() -> None:
    positive_settings = {
        "LOAD_WARMUP_SECONDS": WARMUP_SECONDS,
        "LOAD_DURATION_SECONDS": DURATION_SECONDS,
        "LOAD_LOGS_PER_SECOND": LOGS_PER_SECOND,
        "LOAD_BATCH_SIZE": BATCH_SIZE,
        "LOAD_SEARCH_RPS": SEARCH_REQUESTS_PER_SECOND,
    }
    for name, value in positive_settings.items():
        if value <= 0:
            raise ValueError(f"{name} must be greater than zero")

    if LOGS_PER_SECOND % BATCH_SIZE != 0:
        raise ValueError("LOAD_LOGS_PER_SECOND must be divisible by LOAD_BATCH_SIZE")
    if BATCH_SIZE > 1000:
        raise ValueError("LOAD_BATCH_SIZE cannot exceed the API limit of 1000")


async def wait_for_api(session: aiohttp.ClientSession) -> None:
    deadline = asyncio.get_running_loop().time() + 60
    last_error = "API did not respond"

    while asyncio.get_running_loop().time() < deadline:
        try:
            async with session.get(f"{API_BASE_URL}/health") as response:
                if response.status == 200:
                    return
                last_error = f"health check returned HTTP {response.status}"
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            last_error = str(exc)
        await asyncio.sleep(1)

    raise RuntimeError(f"API is not ready: {last_error}")


async def send_logs(
    session: aiohttp.ClientSession,
    *,
    batch_number: int,
    service: str,
    metrics: RequestMetrics,
) -> None:
    timestamp = datetime.now(timezone.utc).isoformat()
    payload = [
        {
            "service": service,
            "timestamp": timestamp,
            "level": "INFO",
            "message": f"load test batch={batch_number} item={item_number}",
        }
        for item_number in range(BATCH_SIZE)
    ]
    started_at = time.perf_counter()
    metrics.requests += 1

    try:
        async with session.post(f"{API_BASE_URL}/logs", json=payload) as response:
            body = await response.json(content_type=None)
            elapsed_ms = (time.perf_counter() - started_at) * 1000
            metrics.latencies_ms.append(elapsed_ms)

            if response.status != 202:
                metrics.record_error(f"POST /logs: HTTP {response.status}: {body}")
                return
            if body.get("accepted") != BATCH_SIZE:
                metrics.record_error(f"POST /logs: unexpected response: {body}")
                return

            metrics.accepted_logs += body["accepted"]
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
        metrics.record_error(f"POST /logs: {type(exc).__name__}: {exc}")


async def search_logs(
    session: aiohttp.ClientSession,
    *,
    search_from: datetime,
    search_to: datetime,
    metrics: RequestMetrics,
) -> None:
    params = {
        "service": "payments",
        "level": "ERROR",
        "from": search_from.isoformat(),
        "to": search_to.isoformat(),
        "limit": "100",
    }
    started_at = time.perf_counter()
    metrics.requests += 1

    try:
        async with session.get(f"{API_BASE_URL}/logs", params=params) as response:
            body = await response.json(content_type=None)
            elapsed_ms = (time.perf_counter() - started_at) * 1000
            metrics.latencies_ms.append(elapsed_ms)

            if response.status != 200:
                metrics.record_error(f"GET /logs: HTTP {response.status}: {body}")
                return
            if not isinstance(body.get("items"), list):
                metrics.record_error(f"GET /logs: unexpected response: {body}")
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
        metrics.record_error(f"GET /logs: {type(exc).__name__}: {exc}")


async def run_at_rate(
    *,
    rate: int,
    duration_seconds: int,
    request: Callable[[int], Awaitable[None]],
) -> list[float]:
    request_count = rate * duration_seconds
    loop = asyncio.get_running_loop()
    phase_started_at = loop.time()
    scheduler_lags_ms: list[float] = []
    tasks: list[asyncio.Task[None]] = []

    for request_number in range(request_count):
        scheduled_at = phase_started_at + request_number / rate
        delay = scheduled_at - loop.time()
        if delay > 0:
            await asyncio.sleep(delay)

        scheduler_lags_ms.append(max((loop.time() - scheduled_at) * 1000, 0))
        tasks.append(asyncio.create_task(request(request_number)))

    await asyncio.gather(*tasks)
    return scheduler_lags_ms


async def run_phase(
    session: aiohttp.ClientSession,
    *,
    duration_seconds: int,
    service: str,
    search_from: datetime,
    search_to: datetime,
) -> tuple[RequestMetrics, RequestMetrics, list[float]]:
    write_metrics = RequestMetrics()
    search_metrics = RequestMetrics()
    write_requests_per_second = LOGS_PER_SECOND // BATCH_SIZE

    write_lags, search_lags = await asyncio.gather(
        run_at_rate(
            rate=write_requests_per_second,
            duration_seconds=duration_seconds,
            request=lambda number: send_logs(
                session,
                batch_number=number,
                service=service,
                metrics=write_metrics,
            ),
        ),
        run_at_rate(
            rate=SEARCH_REQUESTS_PER_SECOND,
            duration_seconds=duration_seconds,
            request=lambda _: search_logs(
                session,
                search_from=search_from,
                search_to=search_to,
                metrics=search_metrics,
            ),
        ),
    )
    return write_metrics, search_metrics, write_lags + search_lags


async def wait_for_queue_to_drain(
    redis_client: redis.Redis,
    *,
    baseline_length: int,
) -> tuple[bool, int]:
    deadline = asyncio.get_running_loop().time() + DRAIN_TIMEOUT_SECONDS
    current_length = await redis_client.xlen(STREAM_NAME)

    while current_length > baseline_length and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.2)
        current_length = await redis_client.xlen(STREAM_NAME)

    return current_length <= baseline_length, current_length


def print_metrics(
    name: str,
    metrics: RequestMetrics,
) -> None:
    print(
        f"{name}: requests={metrics.requests}, errors={metrics.error_count}, "
        f"p50={percentile(metrics.latencies_ms, 0.50):.1f} ms, "
        f"p95={percentile(metrics.latencies_ms, 0.95):.1f} ms, "
        f"max={max(metrics.latencies_ms, default=0):.1f} ms"
    )
    for error in metrics.errors:
        print(f"  {error}")


async def main() -> None:
    validate_settings()
    run_id = uuid.uuid4().hex
    load_service = f"load-test-{run_id}"
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
    connector = aiohttp.TCPConnector(limit=200)
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    database = await asyncpg.connect(DATABASE_URL)

    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            await wait_for_api(session)
            await redis_client.ping()

            database_rows_before = await database.fetchval("SELECT count(*) FROM logs")
            if database_rows_before < MIN_DATABASE_ROWS:
                raise RuntimeError(
                    f"database contains {database_rows_before:,} rows; "
                    f"at least {MIN_DATABASE_ROWS:,} are required"
                )

            latest_timestamp = await database.fetchval("SELECT max(timestamp) FROM logs")
            if latest_timestamp is None:
                raise RuntimeError("cannot select a search window from an empty database")
            search_from = datetime.combine(
                latest_timestamp.astimezone(timezone.utc).date(),
                datetime_time.min,
                tzinfo=timezone.utc,
            )
            search_to = search_from + timedelta(days=1)
            baseline_stream_length = await redis_client.xlen(STREAM_NAME)

            print(
                f"Database rows: {database_rows_before:,}; "
                f"search window: [{search_from.isoformat()}, {search_to.isoformat()})"
            )
            load_started_at = datetime.now(timezone.utc) - timedelta(seconds=1)
            print(f"Warm-up: {WARMUP_SECONDS}s")
            warmup_writes, warmup_searches, _ = await run_phase(
                session,
                duration_seconds=WARMUP_SECONDS,
                service=load_service,
                search_from=search_from,
                search_to=search_to,
            )
            if warmup_writes.error_count or warmup_searches.error_count:
                print_metrics("Warm-up writes", warmup_writes)
                print_metrics("Warm-up searches", warmup_searches)
                raise RuntimeError("warm-up failed")

            print(
                f"Measurement: {DURATION_SECONDS}s, "
                f"{LOGS_PER_SECOND} logs/s, {SEARCH_REQUESTS_PER_SECOND} searches/s"
            )
            measured_writes, measured_searches, scheduler_lags = await run_phase(
                session,
                duration_seconds=DURATION_SECONDS,
                service=load_service,
                search_from=search_from,
                search_to=search_to,
            )

            queue_drained, final_stream_length = await wait_for_queue_to_drain(
                redis_client,
                baseline_length=baseline_stream_length,
            )
            load_finished_at = datetime.now(timezone.utc) + timedelta(seconds=1)
            written_logs = await database.fetchval(
                """
                SELECT count(*)
                FROM logs
                WHERE timestamp >= $1
                  AND timestamp < $2
                  AND service = $3
                """,
                load_started_at,
                load_finished_at,
                load_service,
            )

            expected_measured_logs = LOGS_PER_SECOND * DURATION_SECONDS
            expected_total_logs = (
                LOGS_PER_SECOND * (WARMUP_SECONDS + DURATION_SECONDS)
            )
            scheduler_p95_ms = percentile(scheduler_lags, 0.95)
            search_p95_ms = percentile(measured_searches.latencies_ms, 0.95)

            print("\nResults")
            print_metrics("Writes", measured_writes)
            print_metrics("Searches", measured_searches)
            print(
                f"Accepted logs: {measured_writes.accepted_logs:,}/"
                f"{expected_measured_logs:,} "
                f"({measured_writes.accepted_logs / DURATION_SECONDS:.1f} logs/s)"
            )
            print(
                f"Scheduler p95 lag: {scheduler_p95_ms:.1f} ms; "
                f"Redis stream: {baseline_stream_length} -> {final_stream_length}; "
                f"PostgreSQL rows for this run: {written_logs:,}/"
                f"{expected_total_logs:,}"
            )

            failures: list[str] = []
            if measured_writes.error_count:
                failures.append("write requests contain errors")
            if measured_writes.accepted_logs != expected_measured_logs:
                failures.append("accepted log count is below the target")
            if measured_searches.error_count:
                failures.append("search requests contain errors")
            if search_p95_ms >= SEARCH_P95_LIMIT_MS:
                failures.append(
                    f"search p95 is {search_p95_ms:.1f} ms, "
                    f"limit is {SEARCH_P95_LIMIT_MS:.1f} ms"
                )
            if scheduler_p95_ms >= MAX_SCHEDULER_LAG_MS:
                failures.append(
                    f"load generator p95 lag is {scheduler_p95_ms:.1f} ms, "
                    f"limit is {MAX_SCHEDULER_LAG_MS:.1f} ms"
                )
            if not queue_drained:
                failures.append("Redis stream did not drain before the timeout")
            if written_logs != expected_total_logs:
                failures.append("not all accepted logs reached PostgreSQL")

            if failures:
                print("\nFAILED")
                for failure in failures:
                    print(f"- {failure}")
                raise SystemExit(1)

            print("\nPASSED")
    finally:
        await database.close()
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())

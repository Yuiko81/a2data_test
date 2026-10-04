import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest
import redis.asyncio as redis

import worker


TEST_DATABASE_URL = os.environ["TEST_DATABASE_URL"]
TEST_REDIS_URL = os.environ["TEST_REDIS_URL"]


@pytest.mark.asyncio
async def test_worker_reads_writes_and_acknowledges_stream_messages(
    monkeypatch: pytest.MonkeyPatch, 
) -> None:
    suffix = uuid.uuid4().hex
    stream_name = f"test-logs-{suffix}"
    consumer_group = f"test-workers-{suffix}"
    consumer_name = f"test-worker-{suffix}"
    service = f"integration-test-{suffix}"

    monkeypatch.setattr(worker, "STREAM_NAME", stream_name) # подмена имени для тестов
    monkeypatch.setattr(worker, "CONSUMER_GROUP", consumer_group)
    monkeypatch.setattr(worker, "CONSUMER_NAME", consumer_name)

    redis_client = redis.from_url(TEST_REDIS_URL, decode_responses=True)
    connection = await asyncpg.connect(TEST_DATABASE_URL)
    transaction = connection.transaction()
    transaction_started = False

    try:
        await redis_client.ping()
        await worker.ensure_consumer_group(redis_client)

        now = datetime.now(timezone.utc)
        payloads = [
            {
                "service": service,
                "timestamp": (now + timedelta(microseconds=index)).isoformat(),
                "level": "INFO",
                "message": f"integration message {index}",
            }
            for index in range(3)
        ]
        message_ids = [
            await redis_client.xadd(
                stream_name,
                {"payload": json.dumps(payload)},
            )
            for payload in payloads
        ]

        pending_logs = await worker.read_entries(
            redis_client,
            stream_position=">",
            count=len(payloads),
        )
        assert [message_id for message_id, _ in pending_logs] == message_ids

        pending_before_write = await redis_client.xpending(
            stream_name,
            consumer_group,
        )
        assert pending_before_write["pending"] == len(payloads)

        await transaction.start()
        transaction_started = True
        await worker.write_batch(connection, redis_client, pending_logs)

        timestamps = [
            datetime.fromisoformat(payload["timestamp"])
            for payload in payloads
        ]
        rows = await connection.fetch(
            """
            SELECT service, timestamp, level, message
            FROM logs
            WHERE timestamp = ANY($1::timestamptz[])
              AND service = $2
            ORDER BY timestamp
            """,
            timestamps,
            service,
        )
        assert [dict(row) for row in rows] == [
            {
                "service": payload["service"],
                "timestamp": datetime.fromisoformat(payload["timestamp"]),
                "level": payload["level"],
                "message": payload["message"],
            }
            for payload in payloads
        ]

        pending_after_write = await redis_client.xpending(
            stream_name,
            consumer_group,
        )
        assert pending_after_write["pending"] == 0
        assert await redis_client.xlen(stream_name) == 0
    finally:
        if transaction_started:
            await transaction.rollback()
        await redis_client.delete(stream_name)
        await connection.close()
        await redis_client.aclose()

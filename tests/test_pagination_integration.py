import os
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from main import LogLevel, decode_cursor, fetch_logs


TEST_DATABASE_URL = os.environ["TEST_DATABASE_URL"]


@pytest.mark.asyncio
async def test_cursor_pagination_does_not_skip_or_duplicate_during_insert() -> None:
    service = f"pagination-test-{uuid.uuid4().hex}"
    initial_timestamp = datetime.now(timezone.utc) - timedelta(minutes=1)
    active_insert_timestamp = initial_timestamp + timedelta(seconds=30)
    reader_pool = await asyncpg.create_pool(
        TEST_DATABASE_URL,
        min_size=1,
        max_size=2,
    )
    writer = await asyncpg.connect(TEST_DATABASE_URL)

    try:
        initial_rows = await writer.fetch(
            """
            INSERT INTO logs (service, timestamp, level, message)
            SELECT $1, $2, 'INFO', 'initial log ' || number
            FROM generate_series(1, 6) AS number
            RETURNING id
            """,
            service,
            initial_timestamp,
        )
        expected_ids = sorted(
            (row["id"] for row in initial_rows),
            reverse=True,
        )

        first_page = await fetch_logs(
            reader_pool,
            service=service,
            level=LogLevel.INFO,
            from_timestamp=initial_timestamp - timedelta(seconds=1),
            to_timestamp=active_insert_timestamp + timedelta(seconds=1),
            cursor=None,
            limit=2,
        )
        assert [item.id for item in first_page.items] == expected_ids[:2]
        assert first_page.next_cursor is not None

        active_row = await writer.fetchrow(
            """
            INSERT INTO logs (service, timestamp, level, message)
            VALUES ($1, $2, 'INFO', 'inserted during pagination')
            RETURNING id
            """,
            service,
            active_insert_timestamp,
        )

        seen_ids = [item.id for item in first_page.items]
        next_cursor = first_page.next_cursor

        while next_cursor is not None:
            page = await fetch_logs(
                reader_pool,
                service=service,
                level=LogLevel.INFO,
                from_timestamp=initial_timestamp - timedelta(seconds=1),
                to_timestamp=active_insert_timestamp + timedelta(seconds=1),
                cursor=decode_cursor(next_cursor),
                limit=2,
            )
            seen_ids.extend(item.id for item in page.items)
            next_cursor = page.next_cursor

        assert seen_ids == expected_ids
        assert len(seen_ids) == len(set(seen_ids))
        assert active_row["id"] not in seen_ids

        fresh_page = await fetch_logs(
            reader_pool,
            service=service,
            level=LogLevel.INFO,
            from_timestamp=initial_timestamp - timedelta(seconds=1),
            to_timestamp=active_insert_timestamp + timedelta(seconds=1),
            cursor=None,
            limit=1,
        )
        assert fresh_page.items[0].id == active_row["id"]
    finally:
        await writer.execute("DELETE FROM logs WHERE service = $1", service)
        await writer.close()
        await reader_pool.close()

import os
import uuid
from datetime import datetime, timezone

import asyncpg
import pytest

from partition_manager import create_missing_partitions, get_partitions


TEST_DATABASE_URL = os.environ["TEST_DATABASE_URL"]


@pytest.mark.asyncio
async def test_creates_today_and_tomorrow_only_once() -> None:
    connection = await asyncpg.connect(TEST_DATABASE_URL)
    schema = f"partition_manager_test_{uuid.uuid4().hex}"
    table = "logs"
    now = datetime(2026, 10, 2, 12, tzinfo=timezone.utc)

    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await connection.execute(
            f"""
            CREATE TABLE "{schema}"."{table}" (
                id BIGINT GENERATED ALWAYS AS IDENTITY,
                timestamp TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (timestamp, id)
            ) PARTITION BY RANGE (timestamp)
            """
        )

        first_run = await create_missing_partitions(
            connection,
            now=now,
            days_ahead=1,
            schema=schema,
            table=table,
        )
        second_run = await create_missing_partitions(
            connection,
            now=now,
            days_ahead=1,
            schema=schema,
            table=table,
        )
        partitions = await get_partitions(connection, schema=schema, table=table)

        assert first_run == ["logs_20261002", "logs_20261003"]
        assert second_run == []
        assert partitions == {"logs_20261002", "logs_20261003"}
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()

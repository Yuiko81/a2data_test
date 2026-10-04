import os
import uuid
from datetime import date, datetime, timedelta, timezone

import asyncpg
import pytest

from retention import drop_expired_partitions, get_partitions


TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql://myuser:mypassword@localhost:5432/mydb",
)


@pytest.mark.asyncio
async def test_retention_drops_only_partitions_older_than_seven_days() -> None:
    connection = await asyncpg.connect(TEST_DATABASE_URL)
    schema = f"retention_test_{uuid.uuid4().hex}" # создание отдельной схемы для тестов
    table = "logs"
    now = datetime(2026, 10, 2, 12, tzinfo=timezone.utc)
    expired_day = date(2026, 9, 24)
    boundary_day = date(2026, 9, 25)

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

        for day in (expired_day, boundary_day):
            name = f"{table}_{day:%Y%m%d}"
            next_day = day + timedelta(days=1)
            await connection.execute(
                f"""
                CREATE TABLE "{schema}"."{name}"
                PARTITION OF "{schema}"."{table}"
                FOR VALUES FROM ('{day.isoformat()}') TO ('{next_day.isoformat()}')
                """
            )

        result = await drop_expired_partitions(
            connection,
            now=now,
            retention_days=7,
            schema=schema,
            table=table,
        )
        partitions = await get_partitions(connection, schema=schema, table=table)

        assert result.dropped == ["logs_20260924"]
        assert result.skipped == []
        assert "logs_20260924" not in partitions
        assert "logs_20260925" in partitions
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()

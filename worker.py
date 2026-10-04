import asyncio
import json
import os
from datetime import datetime

import asyncpg
import redis.asyncio as redis
from redis.exceptions import ResponseError


DATABASE_URL = os.environ["DATABASE_URL"]
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_NAME = os.getenv("REDIS_STREAM_NAME", "logs") # имя потока в redis
CONSUMER_GROUP = os.getenv("REDIS_CONSUMER_GROUP", "log-workers") # имя группы воркеров
CONSUMER_NAME = os.getenv("REDIS_CONSUMER_NAME", "worker-1")
BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "1000")) # размер пачки логов
FLUSH_INTERVAL_MS = int(os.getenv("WORKER_FLUSH_INTERVAL_MS", "500")) # интервал ожидания новых логов

LogRow = tuple[str, datetime, str, str] # лог
PendingLog = tuple[str, LogRow] # id redis + лог


async def ensure_consumer_group(redis_client: redis.Redis) -> None: # создание группы воркеров
    try:
        await redis_client.xgroup_create(
            name=STREAM_NAME,
            groupname=CONSUMER_GROUP,
            id="0-0",
            mkstream=True, # создание потока если его нет
        )
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def parse_stream_entry(message_id: str, fields: dict[str, str]) -> PendingLog: # json -> словарь -> кортеж
    payload = json.loads(fields["payload"])
    row = (
        payload["service"],
        datetime.fromisoformat(payload["timestamp"]),
        payload["level"],
        payload["message"],
    )
    return message_id, row


async def read_entries( # выдает сообщение для конкретного воркера
    redis_client: redis.Redis,
    stream_position: str,
    count: int,
) -> list[PendingLog]:
    read_options = {}
    if stream_position == ">": # > новые сообщения которые еще никому не выдали, 0-0 ранее выданные, но не подтвержденные через xack
        read_options["block"] = FLUSH_INTERVAL_MS

    streams = await redis_client.xreadgroup(
        groupname=CONSUMER_GROUP,
        consumername=CONSUMER_NAME,
        streams={STREAM_NAME: stream_position},
        count=count,
        **read_options, # распаковка словаря в аргументы
    )

    pending_logs = []
    for _, messages in streams:
        for message_id, fields in messages:
            pending_logs.append(parse_stream_entry(message_id, fields))

    return pending_logs


async def write_batch(
    connection: asyncpg.Connection,
    redis_client: redis.Redis,
    pending_logs: list[PendingLog],
) -> None:
    message_ids = [message_id for message_id, _ in pending_logs] # айдишники редис
    records = [record for _, record in pending_logs] # данные логов
    # логи в постгрю айдишники в редис

    async with connection.transaction():
        await connection.copy_records_to_table(
            "logs",
            records=records,
            columns=("service", "timestamp", "level", "message"),
        ) # copy вместо тонны insert

    await redis_client.xack(STREAM_NAME, CONSUMER_GROUP, *message_ids) # убрать из списка неподтвержденных
    await redis_client.xdel(STREAM_NAME, *message_ids) # удаление из потока
    print(f"Inserted and acknowledged {len(records)} logs")


async def recover_pending(
    connection: asyncpg.Connection,
    redis_client: redis.Redis,
) -> None:
    while True:
        pending_logs = await read_entries(
            redis_client,
            stream_position="0-0",
            count=BATCH_SIZE,
        )

        if not pending_logs:
            return

        await write_batch(connection, redis_client, pending_logs)


async def run_worker() -> None:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    connection = await asyncpg.connect(DATABASE_URL)

    try:
        await redis_client.ping()
        await ensure_consumer_group(redis_client)
        await recover_pending(connection, redis_client)

        buffer: list[PendingLog] = [] # запись логов

        while True:
            remaining_capacity = max(BATCH_SIZE - len(buffer), 1)
            new_logs = await read_entries(
                redis_client,
                stream_position=">",
                count=remaining_capacity,
            )

            buffer.extend(new_logs) 

            batch_is_full = len(buffer) >= BATCH_SIZE
            flush_timeout_expired = not new_logs and bool(buffer) # true только если нет новых логов и буфер НЕ пустой, в остальных случаях false

            if batch_is_full or flush_timeout_expired: # запись происходит если (буффер полный) или (не пустой, но новых логов нет)
                await write_batch(connection, redis_client, buffer)
                buffer.clear()
    finally:
        await connection.close()
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(run_worker())

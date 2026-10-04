import base64
import binascii
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from typing import Annotated

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Query, Request, status
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError
from redis.exceptions import RedisError


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_NAME = os.getenv("REDIS_STREAM_NAME", "logs")
DATABASE_URL = os.environ["DATABASE_URL"]


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True) # запрет лишних полей + удаление пробелов

    service: str = Field(min_length=1, max_length=100)
    timestamp: AwareDatetime
    level: LogLevel
    message: str = Field(min_length=1, max_length=10_000)


LogBatch = Annotated[list[LogCreate], Field(min_length=1, max_length=1_000)] # ограничение на количество логов в одном запросе


class AcceptedResponse(BaseModel): # ответ на запрос о принятии логов
    accepted: int # количество принятых логов


class LogRead(BaseModel):
    id: int # id из postgres
    service: str
    timestamp: AwareDatetime
    level: LogLevel
    message: str


class LogsPage(BaseModel):
    items: list[LogRead]
    next_cursor: str | None


class CursorData(BaseModel):
    timestamp: AwareDatetime
    id: int = Field(ge=1) # ge = greater than or equal


def encode_cursor(log: LogRead) -> str: # создание курсора
    cursor_json = json.dumps( 
        {
            "timestamp": log.timestamp.isoformat(),
            "id": log.id,
        }, # dict -> json
        separators=(",", ":"),
    )
    return base64.urlsafe_b64encode(cursor_json.encode()).decode().rstrip("=") # кодировка json -> base64 для использования в юрл


def decode_cursor(cursor: str) -> CursorData:
    try:
        padded_cursor = cursor + "=" * (-len(cursor) % 4)
        decoded = base64.b64decode(
            padded_cursor,
            altchars=b"-_",
            validate=True,
        ).decode()
        return CursorData.model_validate_json(decoded) # проверка через пидантик
    except (binascii.Error, UnicodeDecodeError, ValidationError) as exc:
        raise ValueError("invalid cursor") from exc


async def fetch_logs(
    database: asyncpg.Pool | asyncpg.Connection,
    *, # означает что следующие параметры нужно передавать по имени типа service="auth"
    service: str | None,
    level: LogLevel | None,
    from_timestamp: datetime | None,
    to_timestamp: datetime | None,
    cursor: CursorData | None,
    limit: int,
) -> LogsPage:
    conditions: list[str] = []
    values: list[object] = []

    def add_condition(column: str, operator: str, value: object) -> None:
        values.append(value)
        conditions.append(f"{column} {operator} ${len(values)}")

    if service is not None:
        add_condition("service", "=", service)
    if level is not None:
        add_condition("level", "=", level.value)
    if from_timestamp is not None:
        add_condition("timestamp", ">=", from_timestamp)
    if to_timestamp is not None:
        add_condition("timestamp", "<", to_timestamp)

    if cursor is not None: # пагинация
        values.extend((cursor.timestamp, cursor.id))
        conditions.append(f"(timestamp, id) < (${len(values) - 1}, ${len(values)})")

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    values.append(limit + 1) # проверка наличия некст страницы
    limit_placeholder = f"${len(values)}"

    query = f"""
        SELECT id, service, timestamp, level, message
        FROM logs
        {where_clause}
        ORDER BY timestamp DESC, id DESC
        LIMIT {limit_placeholder}
    """

    rows = await database.fetch(query, *values)
    has_next_page = len(rows) > limit
    items = [LogRead.model_validate(dict(row)) for row in rows[:limit]]
    next_cursor = encode_cursor(items[-1]) if has_next_page else None # создание нового курсора если есть нест страница

    return LogsPage(items=items, next_cursor=next_cursor)


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis_client = redis.from_url(REDIS_URL, decode_responses=True) # создание клиента
    database_pool = None

    try:
        await redis_client.ping()
        database_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        app.state.redis = redis_client # сохранение клиента в состоянии приложения
        app.state.database = database_pool
        yield
    finally:
        if database_pool is not None:
            await database_pool.close()
        await redis_client.aclose()


app = FastAPI(title="Log Aggregation API", lifespan=lifespan)


@app.get("/health")
async def health(request: Request) -> dict[str, str]:
    try:
        await request.app.state.redis.ping() # достаем клиент из состояния
        await request.app.state.database.fetchval("SELECT 1")
    except (RedisError, asyncpg.PostgresError, OSError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="A dependency is unavailable",
        ) from exc

    return {"status": "ok"}


@app.post(
    "/logs",
    response_model=AcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_logs(
    payload: LogCreate | LogBatch, # union либо один лог либо пачка
    request: Request,
) -> AcceptedResponse:
    logs = payload if isinstance(payload, list) else [payload]

    try:
        #transaction=True значит что все команды в текущем пайплайне будут внутри одной multi/exec
        async with request.app.state.redis.pipeline(transaction=True) as pipeline: # создание пайплайна
            for log in logs:
                pipeline.xadd(STREAM_NAME, {"payload": log.model_dump_json()}) # сохранение логов, logcreate -> json
            await pipeline.execute()
    except RedisError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not enqueue logs",
        ) from exc

    return AcceptedResponse(accepted=len(logs))


@app.get("/logs", response_model=LogsPage)
async def get_logs(
    request: Request,
    service: Annotated[str | None, Query(min_length=1, max_length=100)] = None,
    level: LogLevel | None = None,
    from_timestamp: Annotated[AwareDatetime | None, Query(alias="from")] = None, # alias из-за того что from это петухон
    to_timestamp: Annotated[AwareDatetime | None, Query(alias="to")] = None,
    limit: Annotated[int, Query(ge=1, le=1_000)] = 100,
    cursor: str | None = None,
) -> LogsPage:
    if (
        from_timestamp is not None
        and to_timestamp is not None
        and from_timestamp >= to_timestamp
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="from must be earlier than to",
        )

    try:
        cursor_data = decode_cursor(cursor) if cursor is not None else None # декод курсора
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Invalid cursor",
        ) from exc

    return await fetch_logs( # передача фильтров
        request.app.state.database,
        service=service,
        level=level,
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        cursor=cursor_data,
        limit=limit,
    )

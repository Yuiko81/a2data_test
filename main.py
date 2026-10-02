import os
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from typing import Annotated

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field, field_validator
from redis.exceptions import RedisError


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_NAME = os.getenv("REDIS_STREAM_NAME", "logs")


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True) # запрет лишних полей + удаление пробелов

    service: str = Field(min_length=1, max_length=100)
    timestamp: datetime
    level: LogLevel
    message: str = Field(min_length=1, max_length=10_000)

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_have_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include a timezone")
        return value


LogBatch = Annotated[list[LogCreate], Field(min_length=1, max_length=1_000)] # ограничение на количество логов в одном запросе


class AcceptedResponse(BaseModel): # ответ на запрос о принятии логов
    accepted: int # количество принятых логов


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis_client = redis.from_url(REDIS_URL, decode_responses=True) # создание клиента
    await redis_client.ping()
    app.state.redis = redis_client # сохранение клиента в состоянии приложения

    try:
        yield
    finally:
        await redis_client.aclose()


app = FastAPI(title="Log Aggregation API", lifespan=lifespan)


@app.get("/health")
async def health(request: Request) -> dict[str, str]:
    try:
        await request.app.state.redis.ping() # достаем клиент из состояния
    except RedisError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis is unavailable",
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

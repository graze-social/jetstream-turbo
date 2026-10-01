import os
from typing import Final, List, Optional
import logging
from aio_statsd import TelegrafStatsdClient
from pydantic_settings import BaseSettings
from pydantic import (
    AliasChoices,
    Field,
    RedisDsn,
    computed_field,
)
from aiohttp import web, ClientSession
from redis import asyncio as redis


logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """ Settings for the app. """

    graze_api_base_url: str = "https://api.graze.social"
    stream_name: Optional[str] = os.getenv("STREAM_NAME")
    turbo_credential_secret: Optional[str] = os.getenv("TURBO_CREDENTIAL_SECRET")

    s3_bucket: Optional[str] = os.getenv("S3_BUCKET")
    s3_region: Optional[str] = os.getenv("S3_REGION")

    input_mode: Optional[str] = (
        "sqs" if os.getenv("INPUT_MODE", "").lower() == "sqs" else "websocket"
    )

    input_queue_url: Optional[str] = os.getenv("INPUT_QUEUE_URL")

    output_to_s3: Optional[bool] = bool(os.getenv("OUTPUT_TO_S3"))
    output_to_sqs: Optional[bool] = bool(os.getenv("OUTPUT_TO_SQS"))
    output_to_redis: Optional[bool] = bool(os.getenv("OUTPUT_TO_REDIS"))

    output_queue_url: Optional[str] = os.getenv("OUTPUT_QUEUE_URL")
    media_output_queue_url: Optional[str] = os.getenv("MEDIA_OUTPUT_QUEUE_URL")

    redis_url: Optional[str] = os.getenv("REDIS_URL")

    jetstream_hosts: List[str] = [
        "jetstream1.us-east.bsky.network",
        "jetstream2.us-east.bsky.network",
        "jetstream1.us-west.bsky.network",
        "jetstream2.us-west.bsky.network",
    ]
    db_dir: str = "jetstream-messages"

    metrics_port: int = int(os.getenv("METRICS_PORT", "8000"))
    http_port: int = 5100

    external_hostname: str = "localhost:5100"

    redis_dsn: RedisDsn = RedisDsn("redis://valkey:6379/1?decode_responses=True")
    worker_id: str = "worker"

    statsd_host: str = "telegraf"
    statsd_port: int = 8125
    statsd_prefix: str = "aip"

    # Comma-separated Bluesky DIDs: no ingestion/enrichment for these accounts.
    exclusion_list: str = Field(
        default="",
        validation_alias=AliasChoices("EXCLUSION_LIST", "exclusion_list"),
    )

    @computed_field
    @property
    def excluded_dids(self) -> frozenset[str]:
        if not self.exclusion_list or not str(self.exclusion_list).strip():
            return frozenset()
        parts = (p.strip() for p in str(self.exclusion_list).split(","))
        return frozenset(p for p in parts if p)


SettingsAppKey: Final = web.AppKey("settings", Settings)
SessionAppKey: Final = web.AppKey("http_session", ClientSession)
RedisPoolAppKey: Final = web.AppKey("redis_pool", redis.ConnectionPool)
RedisClientAppKey: Final = web.AppKey("redis_client", redis.Redis)
TelegrafStatsdClientAppKey: Final = web.AppKey(
    "telegraf_statsd_client", TelegrafStatsdClient
)

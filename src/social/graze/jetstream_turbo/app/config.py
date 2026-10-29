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

    redis_url: Optional[str] = os.getenv("REDIS_URL")

    jetstream_hosts: List[str] = [
        "jetstream1.us-east.bsky.network",
        "jetstream2.us-east.bsky.network",
        "jetstream1.us-west.bsky.network",
        "jetstream2.us-west.bsky.network",
    ]
    db_dir: str = "jetstream-messages"

    # Everything below is overridable by the upper-cased env var of the same name.

    # Jetstream reconnects: backoff ceiling, and how long an open but silent connection is
    # trusted before it is treated as dead. The firehose is never quiet for a minute.
    reconnect_max_backoff_seconds: float = 30.0
    jetstream_idle_timeout_seconds: float = 60.0

    # Persisted cursor. Disabled unless CURSOR_REDIS_URL is set. A cursor older than
    # `cursor_max_age_seconds` is ignored rather than replayed: jetstream retains ~36h, and
    # replaying a day by accident is its own outage. `cursor_resume_margin_seconds` is re-read
    # on every resume, so the clock skew between jetstream hosts cannot open a gap.
    cursor_redis_url: Optional[str] = None
    cursor_key: str = "jetstream_turbo:cursor"
    cursor_max_age_seconds: float = 1800.0
    cursor_flush_interval_seconds: float = 1.0
    cursor_resume_margin_seconds: float = 2.0

    # Hydration: batches hydrated at once, and attempts before a batch is egressed unhydrated.
    hydration_concurrency: int = 16
    hydration_attempts: int = 3

    # Egress: attempts per method before a batch counts as failed, and the SQS batch payload
    # ceiling (the SendMessageBatch limit is the sum of every body in the request).
    egress_attempts: int = 4
    sqs_max_batch_bytes: int = 262_144

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

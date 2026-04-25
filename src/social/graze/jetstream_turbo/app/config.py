import os
import asyncio
from typing import Final, List
import logging
from aio_statsd import TelegrafStatsdClient
from pydantic import (
    AliasChoices,
    Field,
    PostgresDsn,
    RedisDsn,
    computed_field,
)
import base64
from pydantic_settings import BaseSettings, NoDecode
from aiohttp import web
from cryptography.fernet import Fernet
from aiohttp import ClientSession
from redis import asyncio as redis


logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    graze_api_base_url: str = "https://api.graze.social"
    stream_name: str = os.getenv("STREAM_NAME")
    turbo_credential_secret: str = os.getenv("TURBO_CREDENTIAL_SECRET")

    jetstream_hosts: List[str] = [
        "jetstream1.us-east.bsky.network",
        "jetstream2.us-east.bsky.network",
        "jetstream1.us-west.bsky.network",
        "jetstream2.us-west.bsky.network",
    ]
    db_dir: str = "jetstream-messages"
    s3_bucket: str = "graze-turbo-01"
    s3_region: str = "us-east-1"
    debug: bool = False

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

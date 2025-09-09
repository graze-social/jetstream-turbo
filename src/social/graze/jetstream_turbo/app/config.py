import os
from typing import List, Optional
import logging
from pydantic_settings import BaseSettings


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

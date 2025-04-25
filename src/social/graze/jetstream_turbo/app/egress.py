import asyncio
import sqlite3
import json
from pathlib import Path
import os
import zipfile
import logging
import aioboto3
from datetime import datetime, timedelta
from typing import Any, List, Dict, Optional
import redis.asyncio as redis
from social.graze.jetstream_turbo.app.remote_storage import upload_file_to_s3

logger = logging.getLogger(__name__)


class Egress:
    """
    Handles persisting records to SQLite + S3, and pushes to Redis Stream with trimming.
    """

    REDIS_CLIENT: Optional[redis.Redis] = None
    S3_CLIENT: Any = None
    ROTATION_MINUTES = 1

    def __init__(
        self,
        db_dir: str = "data_store",
        s3_bucket: str = "graze-turbo-01",
        s3_region: str = "us-east-1",
        stream_name: str = "hydrated_jetstream",
        trim_maxlen: Optional[int] = 100,
    ):
        self.db_dir = db_dir
        os.makedirs(db_dir, exist_ok=True)

        self.s3_bucket = s3_bucket
        self.s3_region = s3_region
        self.stream_name = stream_name
        self.trim_maxlen = trim_maxlen

        self.conn: Optional[sqlite3.Connection] = None
        self.db_start_time: Optional[datetime] = None
        self.current_db_path: Optional[str] = None

        self._writer_lock = asyncio.Lock()

    async def _create_new_db(self, rotate_old_db_path: Optional[str] = None):
        if rotate_old_db_path:
            asyncio.create_task(self._compress_and_ship_old_db(rotate_old_db_path))
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self.current_db_path = os.path.join(self.db_dir, f"jetstream_{timestamp}.db")
        self.conn = sqlite3.connect(self.current_db_path, check_same_thread=False)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                at_uri TEXT CHECK(LENGTH(at_uri) <= 300),
                did TEXT CHECK(LENGTH(did) <= 100),
                time_us INTEGER,
                message TEXT CHECK(json_valid(message)),
                message_metadata TEXT CHECK(json_valid(message_metadata))
            )"""
        )
        for idx in ("at_uri", "did", "time_us"):
            self.conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_records_{idx} ON records({idx});"
            )
        self.conn.commit()
        self.db_start_time = datetime.utcnow()

    async def _compress_and_ship_old_db(self, old_db_path: str):
        if not os.path.exists(old_db_path):
            logger.warning(f"Old DB not found at {old_db_path}, skipping upload.")
            return
        # Build zip path
        zip_path = old_db_path + ".zip"
        try:
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.write(old_db_path, arcname=Path(old_db_path).name)
            await self._init_s3_client()
            await Egress.S3_CLIENT.upload_file(
                zip_path, self.s3_bucket, Path(zip_path).name
            )
            logger.info(
                "Zipped & shipped %s to s3://%s/%s",
                old_db_path,
                self.s3_bucket,
                Path(zip_path).name,
            )
            # --- CLEANUP: delete both the raw DB and its ZIP ---
            try:
                os.remove(old_db_path)
                os.remove(zip_path)
                logger.info("Deleted local files %s and %s", old_db_path, zip_path)
            except OSError as rm_err:
                logger.warning("Failed to delete old DB or zip: %s", rm_err)
        except Exception as e:
            logger.error("Failed to zip/upload %s: %s", old_db_path, e)
            if Egress.S3_CLIENT:
                await Egress.S3_CLIENT.close()
                Egress.S3_CLIENT = None

    def _parse_time_us(self, time_us: Any) -> Optional[int]:
        if time_us is None:
            return None
        try:
            return int(time_us)
        except (ValueError, TypeError):
            return None

    async def _init_s3_client(self):
        if Egress.S3_CLIENT is None:
            session = aioboto3.Session()
            Egress.S3_CLIENT = await session.client(
                "s3", region_name=self.s3_region
            ).__aenter__()
            await Egress.S3_CLIENT.head_bucket(Bucket=self.s3_bucket)

    async def _init_redis_client(self):
        if Egress.REDIS_CLIENT is None:
            url = os.getenv("REDIS_URL")
            if not url:
                raise RuntimeError("REDIS_URL not set")
            client = redis.from_url(url)
            await client.ping()
            Egress.REDIS_CLIENT = client
            logger.info("Connected to Redis at %s", url)

    async def push_batch_to_stream(self, batch: List[Dict[str, Any]]):
        if not batch:
            return
        await self._init_redis_client()
        client = Egress.REDIS_CLIENT
        pipe = client.pipeline()
        for item in batch:
            pipe.xadd(self.stream_name, {"data": json.dumps(item)})
        if self.trim_maxlen is not None:
            # single trim for the entire batch
            pipe.xtrim(self.stream_name, maxlen=self.trim_maxlen, approximate=True)
        await pipe.execute()

    async def store_records(self, enriched_records: List[Dict[str, Any]]):
        if not enriched_records:
            return
        async with self._writer_lock:
            if self.conn is None:
                await self._create_new_db()
            if (
                self.db_start_time
                and datetime.utcnow() - self.db_start_time
                >= timedelta(minutes=self.ROTATION_MINUTES)
            ):
                old = self.current_db_path
                self.conn.close()
                self.conn = None
                await self._create_new_db(rotate_old_db_path=old)
            rows = [
                (
                    r.get("at_uri", ""),
                    r.get("did", ""),
                    self._parse_time_us(r.get("time_us")),
                    json.dumps(r.get("message", {})),
                    json.dumps(r.get("hydrated_metadata", {})),
                )
                for r in enriched_records
            ]
            cur = self.conn.cursor()
            cur.executemany(
                "INSERT INTO records(at_uri,did,time_us,message,message_metadata) VALUES(?,?,?,?,?)",
                rows,
            )
            self.conn.commit()
            cur.close()
        # Trimmed stream push
        await self.push_batch_to_stream(enriched_records)

    async def close(self):
        async with self._writer_lock:
            if self.conn:
                self.conn.close()
                self.conn = None

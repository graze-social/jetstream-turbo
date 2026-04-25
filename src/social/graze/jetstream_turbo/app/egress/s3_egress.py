import asyncio
import json
import os
import sqlite3
import logging
import zipfile
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import Any, Dict, List, Optional

import aioboto3
from types_aiobotocore_s3 import S3Client

from social.graze.jetstream_turbo.app.egress.base import EgressBase

logger = logging.getLogger(__name__)


class S3Egress (EgressBase):
    """
    Handles egress to S3 by persisting records to a SQLite database and pushing them every so
    often.
    """

    def __init__(
        self,
        db_dir: str = "data_store",
        s3_bucket: str = "graze-turbo-01",
        s3_region: Optional[str] = "us-east-1",
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

        self.session: aioboto3.Session = None # type: ignore
        self.s3_client: S3Client = None # type: ignore
        self.rotation_minutes = 1

        self._writer_lock = asyncio.Lock()

    async def __aenter__(self):
        # Initialize our S3 client
        self.session = aioboto3.Session()
        self.s3_client = await self.session.client("s3", region_name=self.s3_region).__aenter__()
        await self.s3_client.head_bucket(Bucket=self.s3_bucket)

        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.s3_client:
            await self.s3_client.__aexit__(exc_type, exc, tb)
            self.s3_client = None # type: ignore

    async def _create_new_db(self, rotate_old_db_path: Optional[str] = None):
        if rotate_old_db_path:
            asyncio.create_task(self._compress_and_ship_old_db(rotate_old_db_path))
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
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
        self.db_start_time = datetime.now(UTC)

    async def _compress_and_ship_old_db(self, old_db_path: str):
        if not os.path.exists(old_db_path):
            logger.warning("Old DB not found at %s, skipping upload.", old_db_path)
            return
        # Build zip path
        zip_path = old_db_path + ".zip"
        try:
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.write(old_db_path, arcname=Path(old_db_path).name)
            await self.s3_client.upload_file(
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

    def _parse_time_us(self, time_us: Any) -> Optional[int]:
        if time_us is None:
            return None
        try:
            return int(time_us)
        except (ValueError, TypeError):
            return None

    async def store_records(self, enriched_records: List[Dict[str, Any]]):
        """
        Store records in SQLite to be uploaded.
        """

        if not enriched_records:
            return

        async with self._writer_lock:
            if self.conn is None:
                await self._create_new_db()
                assert self.conn is not None

            if (
                self.db_start_time
                and datetime.now(UTC) - self.db_start_time
                >= timedelta(minutes=self.rotation_minutes)
            ):
                old = self.current_db_path
                self.conn.close()
                self.conn = None
                await self._create_new_db(rotate_old_db_path=old)
                assert self.conn is not None

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

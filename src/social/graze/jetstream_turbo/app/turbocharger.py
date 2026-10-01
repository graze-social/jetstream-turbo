import random
import asyncio
import json
import logging
import time
from typing import Any, Iterable, List, Optional

import sqsi

from social.graze.jetstream_turbo.app.client import JetstreamClient
from social.graze.jetstream_turbo.app.hydration import Hydration
from social.graze.jetstream_turbo.app.egress import (
    EgressBase, MultipleEgress, RedisEgress, S3Egress, SQSEgress
)
from social.graze.jetstream_turbo.app.config import Settings
from social.graze.jetstream_turbo.app.graze_api import GrazeAPI
from social.graze.jetstream_turbo.app.bluesky_api import BlueskyAPI
from social.graze.jetstream_turbo.app.utility import ConfigurationException
from social.graze.jetstream_turbo.app.metrics import posts_processed, batch_processing_time

logger = logging.getLogger(__name__)

BATCH_SIZE = 10  # Number of records per hydration batch


class TurboCharger:
    """
    Coordinates parallel hydration of jetstream data in bulk (100 records at a time)
    and yields enriched records as they arrive.
    """

    def __init__(
        self,
        egress: EgressBase,
        session_strings: List[str],
        endpoint: str,
        *,
        modulo: Optional[int] = None,
        shard: Optional[int] = None,
        excluded_dids: frozenset[str] = frozenset(),
    ):
        self.session_strings = session_strings
        self.endpoint = endpoint
        self.modulo = modulo
        self.shard = shard
        self.excluded_dids = excluded_dids

        self.client = JetstreamClient(endpoint)
        self.buffer = []
        self.semaphore = asyncio.Semaphore(100)
        self.egress = egress

        self.bluesky_clients: List[BlueskyAPI] = []

    async def load_clients(self):
        """
        Load Bluesky API clients from sessions.
        """

        random.shuffle(self.session_strings)
        self.bluesky_clients = await BlueskyAPI.load_sessions(self.session_strings)

    async def run_from_jetstream(self):
        """
        Reads raw messages from the jetstream, buffers them in groups of 100,
        hydrates them in parallel, and yields the enriched records.
        """
        async for record in self.client.run_stream():
            shard_ok = (not self.modulo and not self.shard) or (
                record.get("time_us") % self.modulo == self.shard
            )
            if not shard_ok:
                continue
            if self.excluded_dids and record.get("did") in self.excluded_dids:
                continue
            self.buffer.append(record)
            if len(self.buffer) >= BATCH_SIZE:
                batch = self.buffer[:BATCH_SIZE]
                self.buffer = self.buffer[BATCH_SIZE:]
                await self._process_batch(batch)

        # Process any leftover records if the stream ends
        if self.buffer:
            batch = self.buffer[:BATCH_SIZE]
            self.buffer = self.buffer[BATCH_SIZE:]
            await self._process_batch(batch)

    async def run_from_sqs(self, queue_url: str):
        """
        Read raw messages from SQS, buffers them in groups of `BATCH_SIZE`, hydrates them in
        parallel, and yields the enriched results.
        """

        async with sqsi.QueueIterator(
            queue_url,
            buffer_size=10,
            wait_time=1,
            deletion_mode="handle",
            transformer=json.loads,
            transformer_exceptions="skip-delete",
            transformer_exception_logger=logger.info,
        ) as queue:
            async for chunk in queue.chunks(size=BATCH_SIZE, timeout=1): # type: ignore
                await self._process_batch(message for message, _ in chunk)
                await queue.mark_complete(*(handle for _, handle in chunk))

    async def _process_batch(self, batch: Iterable[Any]):
        """
        Processes a batch of up to 100 records by hydrating them in bulk.
        """

        await self.semaphore.acquire()
        try:
            await asyncio.create_task(self._hydrate_and_release(list(batch), self.semaphore))
        except Exception as ex:
            logger.error("Exception in hydration: %s", ex, exc_info=True)

    async def _hydrate_and_release(
        self, records: List[dict], semaphore: asyncio.Semaphore
    ):
        """
        Hydrates a batch of records and emits the enriched data.
        """
        start_time = time.time()
        try:
            enriched = await Hydration.hydrate_bulk(
                records, self.bluesky_clients, excluded_dids=self.excluded_dids
            )
            posts_processed.inc(len(records))
#            logger.debug(f"Enriched {len(records)} records, storing...")
            await self.egress.store_records(enriched)
#            logger.debug(f"Stored {len(records)} records.")
            batch_processing_time.observe(time.time() - start_time)
        finally:
            semaphore.release()


async def start_turbo_charger(
    settings: Optional[Settings] = None,
    *,
    modulo: Optional[int] = None,
    shard: Optional[int] = None,
):
    """
    Starts the TurboCharger with configured settings.
    """
    if settings is None:
        settings = Settings()

    async with MultipleEgress(*get_egress_methods(settings)) as egress:
        session_strings = await GrazeAPI.fetch_session_strings(settings)

        if excl := settings.excluded_dids:
            logger.info("EXCLUSION_LIST is active with %d entries", len(excl))

        turbo_charger = TurboCharger(
            egress=egress,
            session_strings=session_strings,
            endpoint=random.choice(settings.jetstream_hosts),
            modulo=modulo,
            shard=shard,
            excluded_dids=excl,
        )

        await turbo_charger.load_clients()
        if settings.input_mode == "sqs":
            if not settings.input_queue_url:
                raise ConfigurationException("Mode is SQS but `INPUT_QUEUE_URL` is not set.")
            logger.info("Input from SQS queue: %s", settings.input_queue_url)
            await turbo_charger.run_from_sqs(settings.input_queue_url)
        else:
            logger.info("Input from Websocket")
            await turbo_charger.run_from_jetstream()

def get_egress_methods(settings: Settings):
    """ Get an iterator of egress methods. """

    if settings.output_to_s3:
        if not settings.s3_bucket:
            raise ConfigurationException("settings.s3_bucket must be set to egress to S3.")

        logger.info("Egress to S3 bucket %s", settings.s3_bucket)

        yield S3Egress(
            db_dir=settings.db_dir,
            s3_bucket=settings.s3_bucket,
            s3_region=settings.s3_region,
        )

    if settings.output_to_sqs:
        if not settings.output_queue_url:
            raise ConfigurationException("settings.output_queue_url must be set to egress to SQS.")

        logger.info("Egress to SQS queue %s", settings.output_queue_url)

        yield SQSEgress(
            queue_url=settings.output_queue_url,
            media_queue_url=settings.media_output_queue_url,
        )

    if settings.output_to_redis:
        if not settings.redis_url:
            raise ConfigurationException("settings.redis_url must be set to egress to Redis.")
        if not settings.stream_name:
            raise ConfigurationException("settings.stream_name must be set to egress to Redis.")

        logger.info("Egress to Redis stream %s", settings.stream_name)

        yield RedisEgress(
            redis_url=settings.redis_url,
            stream_name=settings.stream_name,
        )

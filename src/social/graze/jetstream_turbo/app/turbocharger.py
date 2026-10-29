import random
import asyncio
import contextlib
import json
import logging
import time
from typing import Any, Iterable, List, Optional, Sequence, Set

import sqsi

from social.graze.jetstream_turbo.app.client import JetstreamClient
from social.graze.jetstream_turbo.app.cursor import CursorStore, CursorTracker
from social.graze.jetstream_turbo.app.hydration import Hydration
from social.graze.jetstream_turbo.app.egress import (
    EgressBase, MultipleEgress, RedisEgress, S3Egress, SQSEgress
)
from social.graze.jetstream_turbo.app.config import Settings
from social.graze.jetstream_turbo.app.graze_api import GrazeAPI
from social.graze.jetstream_turbo.app.bluesky_api import BlueskyAPI
from social.graze.jetstream_turbo.app.utility import ConfigurationException, backoff_delay
from social.graze.jetstream_turbo.app.metrics import (
    batch_processing_time, cursor_lag_seconds, hydration_retries, posts_processed, unhydrated_records,
)

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
        endpoints: Sequence[str] | str,
        *,
        modulo: Optional[int] = None,
        shard: Optional[int] = None,
        excluded_dids: frozenset[str] = frozenset(),
        settings: Optional[Settings] = None,
        start_cursor: Optional[int] = None,
        cursor_store: Optional[CursorStore] = None,
    ):
        if settings is None:
            settings = Settings()

        self.session_strings = session_strings
        self.endpoints = endpoints
        self.modulo = modulo
        self.shard = shard
        self.excluded_dids = excluded_dids

        self.client = JetstreamClient(
            endpoints,
            cursor=start_cursor,
            resume_margin_us=int(settings.cursor_resume_margin_seconds * 1_000_000),
            max_backoff=settings.reconnect_max_backoff_seconds,
            idle_timeout=settings.jetstream_idle_timeout_seconds,
        )
        self.buffer = []
        self.semaphore = asyncio.Semaphore(settings.hydration_concurrency)
        self.hydration_attempts = settings.hydration_attempts
        self.egress = egress

        # Batches hydrate concurrently. Keep a reference to each task so it is not garbage
        # collected mid-flight, and so the stream ending can wait for all of them.
        self._in_flight: Set[asyncio.Task] = set()
        self.cursor_tracker = CursorTracker()
        self.cursor_store = cursor_store
        self.cursor_flush_interval = settings.cursor_flush_interval_seconds
        self._saved_cursor: Optional[int] = None

        self.bluesky_clients: List[BlueskyAPI] = []

    async def load_clients(self):
        """
        Load Bluesky API clients from sessions.
        """

        random.shuffle(self.session_strings)
        self.bluesky_clients = await BlueskyAPI.load_sessions(self.session_strings)

    async def run_from_jetstream(self):
        """
        Reads raw messages from the jetstream, buffers them in groups of `BATCH_SIZE`,
        hydrates them in parallel, and yields the enriched records.
        """
        persister = asyncio.create_task(self._persist_cursor_forever())
        try:
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

            await self.drain()
        finally:
            persister.cancel()
            await self.flush_cursor()

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
                # Serial, as before: a message is only marked complete once it is egressed,
                # so one whose egress failed is redelivered by SQS rather than lost.
                task = await self._process_batch([message for message, _ in chunk])
                if await task:
                    await queue.mark_complete(*(handle for _, handle in chunk))

    async def _process_batch(self, batch: Iterable[Any]) -> "asyncio.Task[bool]":
        """
        Starts hydrating and egressing a batch of records, once fewer than
        `hydration_concurrency` batches are in flight. Returns without waiting for it.
        """

        records = list(batch)
        await self.semaphore.acquire()
        token = self.cursor_tracker.begin(records)
        task = asyncio.create_task(self._hydrate_and_release(records, token))
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)
        return task

    async def drain(self):
        """ Wait for every in-flight batch to finish. """

        while self._in_flight:
            await asyncio.gather(*self._in_flight, return_exceptions=True)

    async def _hydrate_and_release(self, records: List[dict], token: Optional[int]) -> bool:
        """
        Hydrates a batch of records and emits the enriched data. Returns whether it was egressed.
        """
        start_time = time.time()
        try:
            enriched = await self._hydrate(records)
            posts_processed.inc(len(records))
            await self.egress.store_records(enriched)
        except Exception as ex:  # pylint: disable=broad-exception-caught
            # The egress has already retried and logged the detail. Hold the cursor here so a
            # restart replays this batch instead of skipping it.
            logger.error(
                "Batch of %d records was NOT egressed; holding the cursor at or before %s: %s",
                len(records), self.cursor_tracker.low_water(), ex,
            )
            self.cursor_tracker.fail(token)
            return False
        finally:
            self.semaphore.release()

        self.cursor_tracker.finish(token)
        batch_processing_time.observe(time.time() - start_time)
        return True

    async def _hydrate(self, records: List[dict]) -> List[dict]:
        """
        Hydrates a batch, retrying with backoff. If every attempt fails the records are still
        returned, unhydrated: a post missing its hydrated metadata is better than no post.
        """

        for attempt in range(1, self.hydration_attempts + 1):
            try:
                return await Hydration.hydrate_bulk(
                    records, self.bluesky_clients, excluded_dids=self.excluded_dids
                )
            except Exception as ex:  # pylint: disable=broad-exception-caught
                if attempt == self.hydration_attempts:
                    logger.error(
                        "Hydration failed %d times for a batch of %d records; egressing them "
                        "unhydrated: %s",
                        attempt, len(records), ex, exc_info=True,
                    )
                    break
                hydration_retries.inc()
                delay = backoff_delay(attempt, base=0.5, cap=5.0)
                logger.warning(
                    "Hydration failed for a batch of %d records (%r); retrying in %.1fs.",
                    len(records), ex, delay,
                )
                await asyncio.sleep(delay)

        enriched = Hydration.unhydrated_bulk(records, excluded_dids=self.excluded_dids)
        unhydrated_records.inc(len(enriched))
        return enriched

    async def flush_cursor(self):
        """
        Persist the low-water cursor if it has moved, and publish how far behind it is.
        """

        cursor = self.cursor_tracker.low_water()
        if cursor is None:
            return
        cursor_lag_seconds.set(max(0.0, time.time() - cursor / 1_000_000))
        if self.cursor_store is None or cursor == self._saved_cursor:
            return
        if await self.cursor_store.save(cursor):
            self._saved_cursor = cursor

    async def _persist_cursor_forever(self):
        while True:
            await asyncio.sleep(self.cursor_flush_interval)
            try:
                await self.flush_cursor()
            except Exception as ex:  # pylint: disable=broad-exception-caught
                logger.warning("Failed to flush the jetstream cursor: %s", ex)


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

    egress_methods = get_egress_methods(settings)
    async with (
        MultipleEgress(*egress_methods, max_attempts=settings.egress_attempts) as egress,
        contextlib.AsyncExitStack() as stack,
    ):
        session_strings = await GrazeAPI.fetch_session_strings(settings)

        if excl := settings.excluded_dids:
            logger.info("EXCLUSION_LIST is active with %d entries", len(excl))

        cursor_store: Optional[CursorStore] = None
        start_cursor: Optional[int] = None
        if settings.input_mode != "sqs":
            if settings.cursor_redis_url:
                # Each shard reads its own slice of the stream, so each needs its own cursor.
                key = settings.cursor_key + (f":{shard}/{modulo}" if modulo else "")
                cursor_store = await stack.enter_async_context(
                    CursorStore(settings.cursor_redis_url, key)
                )
                start_cursor = await cursor_store.load(settings.cursor_max_age_seconds)
            else:
                logger.warning(
                    "CURSOR_REDIS_URL is not set: the jetstream cursor is not persisted, and a "
                    "restart will resume from the live tail."
                )

        turbo_charger = TurboCharger(
            egress=egress,
            session_strings=session_strings,
            endpoints=settings.jetstream_hosts,
            modulo=modulo,
            shard=shard,
            excluded_dids=excl,
            settings=settings,
            start_cursor=start_cursor,
            cursor_store=cursor_store,
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
            max_batch_bytes=settings.sqs_max_batch_bytes,
            max_attempts=settings.egress_attempts,
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
            trim_maxlen=settings.redis_trim_maxlen or None,
        )

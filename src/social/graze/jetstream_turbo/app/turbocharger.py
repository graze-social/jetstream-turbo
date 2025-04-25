import json
import random
import asyncio
import logging
from typing import AsyncGenerator, List, Optional

from social.graze.jetstream_turbo.app.client import JetstreamClient
from social.graze.jetstream_turbo.app.hydration import Hydration
from social.graze.jetstream_turbo.app.egress import Egress
from social.graze.jetstream_turbo.app.config import Settings
from social.graze.jetstream_turbo.app.graze_api import GrazeAPI
from social.graze.jetstream_turbo.app.bluesky_api import BlueskyAPI

logger = logging.getLogger(__name__)

BATCH_SIZE = 10  # Number of records per hydration batch


class TurboCharger:
    """
    Coordinates parallel hydration of jetstream data in bulk (100 records at a time)
    and yields enriched records as they arrive.
    """

    def __init__(
        self,
        storage_instance: Egress,
        session_strings: List[str],
        endpoint: str,
        *,
        modulo: int = None,
        shard: int = None,
    ):
        self.session_strings = session_strings
        self.endpoint = endpoint
        self.modulo = modulo
        self.shard = shard

        self.client = JetstreamClient(endpoint)
        self.buffer = []
        self.semaphore = asyncio.Semaphore(100)
        self.storage = storage_instance

    async def load_clients(self):
        random.shuffle(self.session_strings)
        self.bluesky_clients = await BlueskyAPI.load_sessions(self.session_strings)

    async def run(self) -> AsyncGenerator[List[dict], None]:
        """
        Reads raw messages from the jetstream, buffers them in groups of 100,
        hydrates them in parallel, and yields the enriched records.
        """
        async for record in self.client.run_stream():
            if (not self.modulo and not self.shard) or (
                record.get("time_us") % self.modulo == self.shard
            ):
                self.buffer.append(record)
            if len(self.buffer) >= BATCH_SIZE:
                await self._process_batch()

        # Process any leftover records if the stream ends
        if self.buffer:
            await self._process_batch()

    async def _process_batch(self):
        """
        Processes a batch of up to 100 records by hydrating them in bulk.
        """
        batch = self.buffer[:BATCH_SIZE]
        self.buffer = self.buffer[BATCH_SIZE:]

        await self.semaphore.acquire()
        asyncio.create_task(self._hydrate_and_release(batch, self.semaphore))

    async def _hydrate_and_release(
        self, records: List[dict], semaphore: asyncio.Semaphore
    ):
        """
        Hydrates a batch of records and emits the enriched data.
        """
        try:
            logger.debug(f"Got {len(records)} records, enriching...")
            enriched = await Hydration.hydrate_bulk(records, self.bluesky_clients)
            logger.debug(f"Enriched {len(records)} records, storing...")
            await self.storage.store_records(enriched)
            logger.debug(f"Stored {len(records)} records.")
        finally:
            semaphore.release()


async def start_turbo_charger(
    settings: Optional[Settings] = None,
    *,
    modulo: int = None,
    shard: int = None,
):
    """
    Starts the TurboCharger with configured settings.
    """
    if settings is None:
        settings = Settings()  # type: ignore

    storage_instance = Egress(
        db_dir=settings.db_dir,
        s3_bucket=settings.s3_bucket,
        s3_region=settings.s3_region,
        stream_name=settings.stream_name,
    )

    session_strings = await GrazeAPI.fetch_session_strings(settings)

    turbo_charger = TurboCharger(
        storage_instance=storage_instance,
        session_strings=session_strings,
        endpoint=random.choice(settings.jetstream_hosts),
        modulo=modulo,
        shard=shard,
    )

    await turbo_charger.load_clients()
    await turbo_charger.run()

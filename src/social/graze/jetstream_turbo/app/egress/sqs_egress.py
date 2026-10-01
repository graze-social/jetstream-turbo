import asyncio
import logging
import json
from typing import Any, Dict, Iterable, List

from aiobotocore.session import AioSession
from types_aiobotocore_sqs import SQSClient

from social.graze.jetstream_turbo.app.egress.base import EgressBase
from social.graze.jetstream_turbo.app.utility import chunks
from social.graze.jetstream_turbo.content_extractor import ContentExtractor

logger = logging.getLogger(__name__)


class SQSEgress (EgressBase):
    """
    Output turbostream-enriched results to SQS.
    """

    def __init__(self, queue_url: str, media_queue_url: str | None = None):
        self.queue_url = queue_url
        self.media_queue_url = media_queue_url
        
        self._session = AioSession()
        self._sqs_client: SQSClient = None # type: ignore
        self._content_extractor = ContentExtractor()

    async def __aenter__(self):
        self._sqs_client = await self._session.create_client("sqs").__aenter__()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._sqs_client:
            await self._sqs_client.__aexit__(exc_type, exc_val, exc_tb)

    async def store_records(self, enriched_records: List[Dict[str, Any]]):
        """
        Store records in SQS.
        """

        if self.media_queue_url:
            split_records = [(self._has_media(r), r) for r in enriched_records]
            await asyncio.gather(*[
                self._send_to_queue(self.media_queue_url, (r for m, r in split_records if m)),
                self._send_to_queue(self.queue_url, (r for m, r in split_records if not m))
            ])
        else:
            await self._send_to_queue(self.queue_url, enriched_records)

    async def _send_to_queue(self, queue_url: str, records: Iterable[Dict[str, Any]]):
        for chunk in chunks(records, 10):
            await self._sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=[
                    {
                        "Id": str(index),
                        "MessageBody": json.dumps(message)
                    }
                    for index, message in enumerate(chunk)
                ]
            )

    def _has_media(self, record: Dict[str, Any]):
        """ Determine if a record has images or videos in it. """
        return bool(self._content_extractor.extract_videos(record))

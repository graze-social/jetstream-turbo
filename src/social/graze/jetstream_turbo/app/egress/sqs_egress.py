import logging
import json
from typing import Any, Dict, List

from aiobotocore.session import AioSession
from types_aiobotocore_sqs import SQSClient

from social.graze.jetstream_turbo.app.egress.base import EgressBase
from social.graze.jetstream_turbo.app.utility import chunks

logger = logging.getLogger(__name__)


class SQSEgress (EgressBase):
    """
    Output turbostream-enriched results to SQS.
    """

    def __init__(self, queue_url: str):
        self.queue_url = queue_url
        self._session = AioSession()
        self._sqs_client: SQSClient = None # type: ignore

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

        for chunk in chunks(enriched_records, 10):
            await self._sqs_client.send_message_batch(
                QueueUrl=self.queue_url,
                Entries=[
                    {
                        "Id": str(index),
                        "MessageBody": json.dumps(message)
                    }
                    for index, message in enumerate(chunk)
                ]
            )

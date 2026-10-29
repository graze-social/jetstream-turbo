import asyncio
import logging
import json
from typing import Any, Dict, List, Optional

from aiobotocore.session import AioSession
from botocore.exceptions import ClientError
from types_aiobotocore_sqs import SQSClient

from social.graze.jetstream_turbo.app.egress.base import EgressBase, EgressError
from social.graze.jetstream_turbo.app.hydration import UNHYDRATED_METADATA
from social.graze.jetstream_turbo.app.metrics import egress_failures, egress_retries
from social.graze.jetstream_turbo.app.utility import backoff_delay

logger = logging.getLogger(__name__)

# SendMessageBatch takes at most 10 entries per request.
MAX_BATCH_ENTRIES = 10

BATCH_TOO_LONG_CODES = frozenset({
    "AWS.SimpleQueueService.BatchRequestTooLong",
    "BatchRequestTooLong",
})


class SQSEgress (EgressBase):
    """
    Output turbostream-enriched results to SQS.

    SendMessageBatch can fail per entry while the call as a whole succeeds, so every `Failed`
    entry is retried (with backoff, a bounded number of times) and `EgressError` is raised if
    any are still failing. Entries SQS rejects as the sender's fault are not retried: they
    would fail identically forever, so they are logged and counted as `rejected` instead.

    Batches are packed to stay under `max_batch_bytes` in total. A single hydrated record can
    be large on its own -- it carries the author's, every mention's and the parent, root and
    quoted posts' full objects -- so one that cannot fit even alone is sent unhydrated.
    """

    name = "sqs"

    def __init__(
        self,
        queue_url: str,
        *,
        max_batch_bytes: int = 262_144,
        max_attempts: int = 4,
    ):
        self.queue_url = queue_url
        self.max_batch_bytes = max_batch_bytes
        self.max_attempts = max_attempts
        self._session = AioSession()
        self._sqs_client: SQSClient = None # type: ignore
        self._sleep = asyncio.sleep  # Swapped out by tests.

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

        bodies = [body for body in map(self._encode, enriched_records) if body is not None]
        for batch in self._pack(bodies):
            await self._send_batch(batch)

    def _encode(self, record: Dict[str, Any]) -> Optional[str]:
        body = json.dumps(record)
        if len(body.encode()) <= self.max_batch_bytes:
            return body

        unhydrated = json.dumps({**record, "hydrated_metadata": dict(UNHYDRATED_METADATA)})
        if len(unhydrated.encode()) <= self.max_batch_bytes:
            logger.warning(
                "Record %s is %d bytes hydrated, over the %d-byte SQS limit; sending it unhydrated.",
                record.get("at_uri"), len(body.encode()), self.max_batch_bytes,
            )
            return unhydrated

        logger.error(
            "Record %s is %d bytes even unhydrated, over the %d-byte SQS limit; it cannot be sent.",
            record.get("at_uri"), len(unhydrated.encode()), self.max_batch_bytes,
        )
        egress_failures.labels(egress=self.name, reason="rejected").inc()
        return None

    def _pack(self, bodies: List[str]) -> List[List[str]]:
        """ Group bodies into requests of at most 10 entries and `max_batch_bytes` in total. """

        batches: List[List[str]] = []
        current: List[str] = []
        size = 0
        for body in bodies:
            length = len(body.encode())
            if current and (len(current) >= MAX_BATCH_ENTRIES or size + length > self.max_batch_bytes):
                batches.append(current)
                current, size = [], 0
            current.append(body)
            size += length
        if current:
            batches.append(current)
        return batches

    async def _send_batch(self, bodies: List[str]):
        # Entry Id -> body for everything not yet accepted. Ids stay stable across retries.
        pending = {str(index): body for index, body in enumerate(bodies)}
        error: Any = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                response = await self._sqs_client.send_message_batch(
                    QueueUrl=self.queue_url,
                    Entries=[
                        {"Id": entry_id, "MessageBody": body} for entry_id, body in pending.items()
                    ],
                )
            except ClientError as ex:
                if ex.response.get("Error", {}).get("Code") in BATCH_TOO_LONG_CODES:
                    await self._split_too_long(list(pending.values()))
                    return
                error = ex
            except Exception as ex:  # pylint: disable=broad-exception-caught
                error = ex
            else:
                failed = response.get("Failed") or []
                for entry in failed:
                    if entry.get("SenderFault"):
                        logger.error(
                            "SQS rejected a message (%s: %s); not retrying.",
                            entry.get("Code"), entry.get("Message"),
                        )
                        egress_failures.labels(egress=self.name, reason="rejected").inc()
                retry_ids = {entry["Id"] for entry in failed if not entry.get("SenderFault")}
                pending = {entry_id: pending[entry_id] for entry_id in pending if entry_id in retry_ids}
                if not pending:
                    return
                error = sorted({entry.get("Code") for entry in failed if not entry.get("SenderFault")})

            if attempt < self.max_attempts:
                egress_retries.labels(egress=self.name).inc()
                delay = backoff_delay(attempt, base=0.5, cap=10.0)
                logger.warning(
                    "%d SQS messages failed (%s); retrying in %.1fs (attempt %d/%d).",
                    len(pending), error, delay, attempt, self.max_attempts,
                )
                await self._sleep(delay)

        raise EgressError(
            f"{len(pending)} SQS messages still failing after {self.max_attempts} attempts: {error}",
            count=len(pending),
        )

    async def _split_too_long(self, bodies: List[str]):
        """ SQS measured the request differently than we did; halve it until it fits. """

        if len(bodies) == 1:
            logger.error("SQS says a single %d-byte message is too long; it cannot be sent.",
                         len(bodies[0].encode()))
            egress_failures.labels(egress=self.name, reason="rejected").inc()
            return
        half = len(bodies) // 2
        logger.warning("SQS batch of %d messages was too long; splitting it.", len(bodies))
        await self._send_batch(bodies[:half])
        await self._send_batch(bodies[half:])

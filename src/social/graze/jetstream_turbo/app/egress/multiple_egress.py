import asyncio
import contextlib
import logging
from typing import Any, Dict, List
from social.graze.jetstream_turbo.app.egress.base import EgressBase, EgressError
from social.graze.jetstream_turbo.app.metrics import egress_failures, egress_retries
from social.graze.jetstream_turbo.app.utility import backoff_delay


logger = logging.getLogger(__name__)

class MultipleEgress (EgressBase):
    """
    Handles sending egress to multiple places.

    A method that fails is retried on its own (the ones that succeeded are not re-sent), with
    backoff, up to `max_attempts` times. If any method is still failing after that,
    `EgressError` is raised: the batch did not get everywhere it was meant to go, and must not
    be reported as delivered.
    """

    def __init__(self, *methods: EgressBase, max_attempts: int = 4):
        self._exit_stack = contextlib.AsyncExitStack()
        self._methods: List[EgressBase] = list(methods)
        self.max_attempts = max_attempts
        self._sleep = asyncio.sleep  # Swapped out by tests.
        if len(self._methods) == 0:
            logger.warning("No egress methods configured.")

    async def __aenter__(self):
        await self._exit_stack.__aenter__()

        for method in self._methods:
            await self._exit_stack.enter_async_context(method)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    async def store_records(self, enriched_records: List[Dict[str, Any]]):
        """
        A method to store records in whatever means of egress.
        """

        pending = list(self._methods)
        failed: List[tuple[EgressBase, BaseException]] = []

        for attempt in range(1, self.max_attempts + 1):
            results = await asyncio.gather(
                *(method.store_records(enriched_records) for method in pending),
                return_exceptions=True,
            )
            failed = [
                (method, result)
                for method, result in zip(pending, results)
                if isinstance(result, BaseException)
            ]
            if not failed:
                return
            if attempt == self.max_attempts:
                break

            delay = backoff_delay(attempt, base=0.5, cap=10.0)
            for method, result in failed:
                egress_retries.labels(egress=method.name).inc()
                logger.warning(
                    "Egress to %s failed (%r); retrying in %.1fs (attempt %d/%d).",
                    method.name, result, delay, attempt, self.max_attempts,
                )
            await self._sleep(delay)
            pending = [method for method, _ in failed]

        for method, result in failed:
            count = getattr(result, "count", None) or len(enriched_records)
            egress_failures.labels(egress=method.name, reason="exhausted").inc(count)
            logger.error(
                "Egress to %s failed for %d of %d records after %d attempts: %r",
                method.name, count, len(enriched_records), self.max_attempts, result,
                exc_info=result,
            )
        raise EgressError(
            "Egress failed to " + ", ".join(method.name for method, _ in failed)
        )

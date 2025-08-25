import asyncio
import contextlib
import logging
from typing import Any, Dict, List
from social.graze.jetstream_turbo.app.egress.base import EgressBase


logger = logging.getLogger(__name__)

class MultipleEgress (EgressBase):
    """
    Handles sending egress to multiple places.
    """

    def __init__(self, *methods: EgressBase):
        self._exit_stack = contextlib.AsyncExitStack()
        self._methods: List[EgressBase] = list(methods)
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

        results = await asyncio.gather(
            *(method.store_records(enriched_records) for method in self._methods),
            return_exceptions=True,
        )

        for result in results:
            if isinstance(result, Exception):
                logger.error("Exception in egress: %s", result, exc_info=True)

import json
from typing import Any, Dict, List, Optional

import redis.asyncio as redis

from social.graze.jetstream_turbo.app.egress.base import EgressBase


class RedisEgress (EgressBase):
    """
    Output turbostream-enriched results to a Redis stream.
    """

    def __init__(self, redis_url: str, stream_name: str, trim_maxlen: Optional[int] = None):
        self.redis_url = redis_url
        self.stream_name = stream_name
        self.trim_maxlen = trim_maxlen

        self._redis_client: redis.Redis = None # type: ignore

    async def __aenter__(self):
        self._redis_client = await redis.from_url(self.redis_url).__aenter__()
        await self._redis_client.ping()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._redis_client:
            await self._redis_client.__aexit__(exc_type, exc_val, exc_tb)
            self._redis_client = None # type: ignore

    async def store_records(self, enriched_records: List[Dict[str, Any]]):
        """
        Output results to a Redis queue.
        """

        async with self._redis_client.pipeline() as pipe:

            for item in enriched_records:
                pipe.xadd(self.stream_name, { "data": json.dumps(item) })

            if self.trim_maxlen is not None:
                pipe.xtrim(self.stream_name, maxlen=self.trim_maxlen, approximate=True)

            await pipe.execute()

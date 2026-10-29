import logging
import time
from typing import Any, Dict, Iterable, Optional

import redis.asyncio as redis

logger = logging.getLogger(__name__)


def record_time_us(record: Any) -> Optional[int]:
    """ The jetstream receipt time on a raw record's envelope, or None. """

    try:
        return int(record.get("time_us"))
    except (AttributeError, TypeError, ValueError):
        return None


class CursorTracker:
    """
    Tracks which records have been egressed, so the cursor we persist is one a restart can
    resume from without skipping anything.

    Batches hydrate and egress concurrently and finish out of order, so "the newest record
    egressed" is not safe: an older batch may still be in flight behind it. The safe cursor is
    the low-water mark -- the oldest `time_us` of any batch not yet egressed, or, when nothing
    is outstanding, the newest `time_us` that was. A batch whose egress failed outright holds
    the mark at its start for the life of the process, so the cursor never passes data that
    did not get out.
    """

    def __init__(self):
        self._next_token = 0
        self._in_flight: Dict[int, tuple[int, int]] = {}  # token -> (min time_us, max time_us)
        self._held: Optional[int] = None
        self._max_done: Optional[int] = None

    def begin(self, records: Iterable[Any]) -> Optional[int]:
        """ Register a batch as in flight. Returns a token for `finish`/`fail`. """

        times = [t for t in map(record_time_us, records) if t is not None]
        if not times:
            return None
        token = self._next_token
        self._next_token += 1
        self._in_flight[token] = (min(times), max(times))
        return token

    def finish(self, token: Optional[int]):
        """ The batch was egressed. """

        if token is None or token not in self._in_flight:
            return
        _, newest = self._in_flight.pop(token)
        if self._max_done is None or newest > self._max_done:
            self._max_done = newest

    def fail(self, token: Optional[int]):
        """ The batch could not be egressed; never let the cursor move past it. """

        if token is None or token not in self._in_flight:
            return
        oldest, _ = self._in_flight.pop(token)
        if self._held is None or oldest < self._held:
            self._held = oldest

    @property
    def in_flight(self) -> int:
        return len(self._in_flight)

    def low_water(self) -> Optional[int]:
        """ The `time_us` a restart should resume from, or None if nothing is known yet. """

        pending = [oldest for oldest, _ in self._in_flight.values()]
        if self._held is not None:
            pending.append(self._held)
        if pending:
            return min(pending)
        return self._max_done


class CursorStore:
    """
    Persists the jetstream cursor in Redis so a restarted process resumes where the last one
    stopped egressing, instead of at the live tail.

    Redis being unavailable is never fatal here: the worst case is the old behaviour, a
    restart that live-tails.
    """

    def __init__(self, redis_url: str, key: str):
        self.redis_url = redis_url
        self.key = key
        self._redis_client: redis.Redis = None  # type: ignore

    async def __aenter__(self):
        self._redis_client = redis.from_url(self.redis_url)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._redis_client:
            await self._redis_client.aclose()
            self._redis_client = None  # type: ignore

    async def load(self, max_age_seconds: float) -> Optional[int]:
        """ The persisted cursor, if there is one and it is recent enough to replay from. """

        try:
            raw = await self._redis_client.get(self.key)
        except Exception as ex:  # pylint: disable=broad-exception-caught
            logger.error(
                "Could not read the jetstream cursor from %s; STARTING FROM THE LIVE TAIL, "
                "anything posted while we were down is lost: %s", self.key, ex,
            )
            return None

        if raw is None:
            logger.warning(
                "No persisted jetstream cursor at %s; STARTING FROM THE LIVE TAIL.", self.key
            )
            return None

        try:
            cursor = int(raw)
        except (TypeError, ValueError):
            logger.error("Persisted jetstream cursor %r is not an integer; ignoring it.", raw)
            return None

        age = time.time() - cursor / 1_000_000
        if age > max_age_seconds:
            logger.error(
                "Persisted jetstream cursor %d is %.0fs old (max %.0fs); STARTING FROM THE LIVE "
                "TAIL, the %.0fs in between will not be ingested.",
                cursor, age, max_age_seconds, age,
            )
            return None

        logger.info("Resuming jetstream from persisted cursor %d (%.1fs old).", cursor, age)
        return cursor

    async def save(self, cursor: int) -> bool:
        try:
            await self._redis_client.set(self.key, str(cursor))
            return True
        except Exception as ex:  # pylint: disable=broad-exception-caught
            logger.warning("Could not persist jetstream cursor %d to %s: %s", cursor, self.key, ex)
            return False

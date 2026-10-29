import asyncio
import json
import logging
import random
from typing import Optional, Sequence

import websockets
import websockets.exceptions

from social.graze.jetstream_turbo.app.metrics import reconnects
from social.graze.jetstream_turbo.app.utility import backoff_delay

logger = logging.getLogger(__name__)


class IdleTimeout(Exception):
    """ The connection stayed open but delivered nothing for `idle_timeout` seconds. """


def reconnect_reason(ex: BaseException) -> str:
    """ Classify why a connection ended, for the `reason` label on the reconnect counter. """

    if isinstance(ex, IdleTimeout):
        return "idle"
    if isinstance(ex, websockets.exceptions.ConnectionClosedOK):
        return "closed_ok"
    if isinstance(ex, websockets.exceptions.ConnectionClosedError):
        return "closed_error"
    # InvalidStatus (a 503/429 from the host) and InvalidMessage both subclass this.
    if isinstance(ex, websockets.exceptions.InvalidHandshake):
        return "handshake"
    # Before OSError: TimeoutError is a subclass of it.
    if isinstance(ex, TimeoutError):
        return "timeout"
    if isinstance(ex, OSError):
        return "os_error"
    return "other"


class JetstreamClient:
    """
    Subscribes to a Bluesky jetstream (firehose) and yields parsed messages.

    The stream never ends on its own. Every way a connection can fail -- a refused handshake,
    a socket error, a clean close, a host that goes silent -- reconnects, with backoff, to the
    next host in `endpoints`, resuming from the last `time_us` seen minus `resume_margin_us`.
    Re-reading a couple of seconds is harmless (the matcher and ClickHouse dedup); skipping
    them is not, and jetstream hosts do not stamp `time_us` from one shared clock.
    """

    def __init__(
        self,
        endpoints: Sequence[str] | str,
        wanted_collections: str = "app.bsky.feed.post",
        *,
        cursor: Optional[int] = None,
        resume_margin_us: int = 2_000_000,
        max_backoff: float = 30.0,
        idle_timeout: Optional[float] = 60.0,
    ):
        self.endpoints = [endpoints] if isinstance(endpoints, str) else list(endpoints)
        if not self.endpoints:
            raise ValueError("JetstreamClient needs at least one endpoint.")
        self.wanted_collections = wanted_collections
        self.resume_margin_us = resume_margin_us
        self.max_backoff = max_backoff
        self.idle_timeout = idle_timeout

        # The newest `time_us` read off the socket, or the persisted cursor to start from.
        self.cursor = cursor

        self._host_index = random.randrange(len(self.endpoints))
        self._sleep = asyncio.sleep  # Swapped out by tests.

    @property
    def endpoint(self) -> str:
        return self.endpoints[self._host_index]

    def jetstream_url(self, time_us: int | None = None):
        url = f"wss://{self.endpoint}/subscribe?wantedCollections={self.wanted_collections}"
        if time_us:
            url += f"&cursor={time_us}"
        return url

    def resume_cursor(self) -> Optional[int]:
        """ The cursor to (re)connect with: the last one seen, less the safety margin. """

        if self.cursor is None:
            return None
        return max(self.cursor - self.resume_margin_us, 0) or None

    def _connect(self, url: str):
        return websockets.connect(url)

    async def _recv(self, ws):
        if self.idle_timeout is None:
            return await ws.recv()
        try:
            async with asyncio.timeout(self.idle_timeout):
                return await ws.recv()
        except TimeoutError as ex:
            raise IdleTimeout(f"no message in {self.idle_timeout:.0f}s") from ex

    async def run_stream(self):
        """ Run a Jetstream iterator. """

        failures = 0

        while True:
            url = self.jetstream_url(self.resume_cursor())
            received = False
            try:
                async with self._connect(url) as ws:
                    logger.info("Connected to jetstream: %s", url)
                    while True:
                        raw = await self._recv(ws)
                        try:
                            message = json.loads(raw)
                        except (json.JSONDecodeError, TypeError):
                            continue
                        if not isinstance(message, dict):
                            continue
                        try:
                            self.cursor = int(message.get("time_us"))
                        except (TypeError, ValueError):
                            pass
                        received = True
                        yield message
            except Exception as ex:  # pylint: disable=broad-exception-caught
                # Not CancelledError or GeneratorExit: those are BaseExceptions and still stop us.
                reason = reconnect_reason(ex)
                reconnects.labels(reason=reason).inc()

                # A connection that delivered anything was healthy; start the backoff over.
                failures = 1 if received else failures + 1
                delay = backoff_delay(failures, base=0.5, cap=self.max_backoff)
                self._host_index = (self._host_index + 1) % len(self.endpoints)

                logger.warning(
                    "Jetstream connection ended (%s: %r); reconnecting to %s in %.1fs from cursor %s "
                    "(%d consecutive failures).",
                    reason, ex, self.endpoint, delay, self.resume_cursor(), failures,
                    exc_info=reason == "other",
                )
                await self._sleep(delay)

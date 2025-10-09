import asyncio
import json
import logging
import websockets
import websockets.exceptions

logger = logging.getLogger(__name__)

class JetstreamClient:
    """
    Subscribes to a Bluesky jetstream (firehose) and yields parsed messages.
    """

    def __init__(
        self,
        endpoint: str,
        wanted_collections: str = "app.bsky.feed.post",
        max_retries: int = 5,
        retry_window: float = 300
    ):
        self.endpoint = endpoint
        self.wanted_collections = wanted_collections
        self.max_retries = max_retries
        self.retry_window = retry_window

    def jetstream_url(self, time_us: int | None = None):
        url = f"wss://{self.endpoint}/subscribe?wantedCollections={self.wanted_collections}"
        if time_us:
            url += f"&cursor={time_us}"
        return url

    async def run_stream(self):
        """ Run a Jetstream iterator. """

        retry_times: list[float] = []
        time_us: int | None = None

        while True:
            try:
                async with websockets.connect(self.jetstream_url(time_us)) as ws:
                    async for message in ws:
                        try:
                            message = json.loads(message)
                            try:
                                time_us = int(message.get("time_us"))
                            except (TypeError, ValueError, AttributeError):
                                pass
                            yield message
                        except (json.JSONDecodeError, KeyError):
                            continue
            except websockets.exceptions.ConnectionClosedError as e:
                logger.error("Exception when reading from jetstream: %s", e, exc_info=True)

                now = asyncio.get_event_loop().time()
                retry_times = [t for t in retry_times if now - t < self.retry_window]
                retry_times.append(now)
                if len(retry_times) > self.max_retries:
                    logger.error(
                        "Ending `run_stream()` iterator due to hitting max retries (%d) in window (%.1f s).",
                        self.max_retries,
                        self.retry_window,
                    )

                logger.error(
                    "Continuing processing (%d/%d retries in the last %.1f s).",
                    len(retry_times),
                    self.max_retries,
                    self.retry_window,
                )
                continue

            # If we're not terminating due to a known exception, break and die.
            break

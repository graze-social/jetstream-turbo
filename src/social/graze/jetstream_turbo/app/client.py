import asyncio
import json
import websockets


class JetstreamClient:
    """
    Subscribes to a Bluesky jetstream (firehose) and yields parsed messages.
    """

    def __init__(self, endpoint: str, wanted_collections: str = "app.bsky.feed.post"):
        self.endpoint = endpoint
        self.wanted_collections = wanted_collections

    def jetstream_url(self):
        return f"wss://{self.endpoint}/subscribe?wantedCollections={self.wanted_collections}"

    async def run_stream(self):
        async with websockets.connect(self.jetstream_url()) as ws:
            async for message in ws:
                try:
                    data = json.loads(message)
                    yield data
                except (json.JSONDecodeError, KeyError):
                    continue

import asyncio
import random
from typing import List, Dict, TypeVar, Callable, Awaitable

from atproto import AsyncClient, models
from atproto_client.exceptions import (
    BadRequestError,
    RequestException,
    InvokeTimeoutError,
)

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")
CLIENT_BANDWIDTH = 10


class BlueskyAPI:
    """
    Minimal Bluesky client that logs in with session strings and
    exposes methods to fetch user data and hydrate post records.
    """

    def __init__(self, client: AsyncClient):
        self._client = client

    @classmethod
    async def load_sessions(cls, session_strings: List[str]) -> List["BlueskyAPI"]:
        """
        For each session_string in the list, split on ':::' to get its domain,
        login an AsyncClient at that domain, and return a list of BlueskyAPI
        instances wrapping those clients.
        """
        apis: List[BlueskyAPI] = []
        bads: List[str] = []
        for ss in session_strings:
            if len(apis) < CLIENT_BANDWIDTH:
                try:
                    domain = ss.split(":::")[-1]
                    client = AsyncClient(domain)
                    try:
                        await client.login(session_string=ss)
                    except (BadRequestError, RequestException, ValueError) as e:
                        raise RuntimeError(f"Login failed for '{ss[:8]}…': {e}")
                    apis.append(cls(client))
                except:
                    bads.append(ss)
        return apis

    async def _login_client(self, client: AsyncClient, session_string: str):
        """Logs a given AsyncClient in with a particular session string."""
        try:
            await client.login(session_string=session_string)
            await client.user_timeline()
        except (BadRequestError, RequestException, ValueError) as e:
            raise RuntimeError(f"Failed to login with session string: {e}")

    async def _chunked_map(
        self,
        items: List[T],
        chunk_size: int,
        fetcher: Callable[[List[T]], Awaitable[Dict[K, V]]],
    ) -> Dict[K, V]:
        """Split `items` into chunks, call `fetcher` on each, and merge the dicts."""
        if not items:
            return {}
        tasks = [
            fetcher(items[i : i + chunk_size]) for i in range(0, len(items), chunk_size)
        ]
        chunks = await asyncio.gather(*tasks)
        merged: Dict[K, V] = {}
        for d in chunks:
            merged.update(d)
        return merged

    async def get_user_data_for_dids(
        self, dids: List[str]
    ) -> Dict[str, models.AppBskyActorDefs.ProfileViewDetailed]:
        """Fetch profiles for many DIDs, in pages of 25."""

        async def fetch_profiles(sub: List[str]):
            resp = await self._client.app.bsky.actor.get_profiles({"actors": sub})
            return {p.did: p for p in resp.profiles}

        return await self._chunked_map(dids, 25, fetch_profiles)

    async def hydrate_records_for_uris(
        self, uris: List[str]
    ) -> Dict[str, models.AppBskyFeedDefs.PostView]:
        """Fetch posts for many URIs, in pages of 25."""

        async def fetch_posts(sub: List[str]):
            resp = await self._client.app.bsky.feed.get_posts({"uris": sub})
            return {p.uri: p for p in resp.posts}

        return await self._chunked_map(uris, 25, fetch_posts)

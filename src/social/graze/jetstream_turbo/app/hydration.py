import asyncio
import random
from typing import List, Dict, Any, Set, Optional
from collections import OrderedDict

import aiorwlock

from social.graze.jetstream_turbo.app.bluesky_api import BlueskyAPI


def repo_did_from_at_uri(uri: str) -> Optional[str]:
    """Return the repository DID for an at:// URI, or None if not parseable."""
    if not uri or not isinstance(uri, str) or not uri.startswith("at://"):
        return None
    rest = uri[5:]
    if not rest:
        return None
    return rest.split("/", 1)[0]


def _author_did_from_post_dict(post: dict) -> Optional[str]:
    if not post or not isinstance(post, dict):
        return None
    author = post.get("author")
    if isinstance(author, dict):
        d = author.get("did")
        if isinstance(d, str):
            return d
    return None


def _post_if_not_excluded(
    post: Optional[dict], excluded_dids: frozenset[str]
) -> Optional[dict]:
    if not post or not excluded_dids:
        return post
    aid = _author_did_from_post_dict(post)
    if aid and aid in excluded_dids:
        return None
    return post


class LRUCache:
    """
    A simple LRU (Least Recently Used) cache based on OrderedDict.
    Not thread-safe or async-safe by itself—use it behind a lock.
    """

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.cache = OrderedDict()  # key -> value

    def get(self, key: str):
        """
        Return the cached value or None if not present.
        Move the accessed key to the end (most recently used).
        """
        if key not in self.cache:
            return None
        value = self.cache.pop(key)
        self.cache[key] = value  # re-insert to mark as most recently used
        return value

    def set(self, key: str, value: Any):
        """
        Insert or update a key-value pair, evicting if needed.
        """
        # If key exists, pop it so we can re-insert to update order
        if key in self.cache:
            self.cache.pop(key)
        elif len(self.cache) >= self.max_size:
            # Evict least recently used item
            self.cache.popitem(last=False)  # pop from the front
        self.cache[key] = value


class Hydration:
    """
    Handles bulk hydration of raw Bluesky records using the BlueskyAPI.
    Produces a final list of dicts with fields:
      - at_uri
      - did
      - created_at
      - message (the original jetstream record)
      - hydrated_metadata: {
          user: the user profile object of the posting user,
          mentions: dict of { mentionDid: userProfileObject },
          parent_post: resolved data for parent post,
          reply_post: resolved data for root post
        }
    """

    # Class-level caches (shared by all calls).
    user_cache_size = 20000
    post_cache_size = 20000

    _user_cache = LRUCache(max_size=user_cache_size)
    _post_cache = LRUCache(max_size=post_cache_size)

    # Single RWLock guarding both caches:
    _cache_lock = aiorwlock.RWLock()

    @classmethod
    def configure_cache(cls, user_cache_size: int, post_cache_size: int):
        """
        Optionally call this at startup to tune the cache sizes.
        """
        cls.user_cache_size = user_cache_size
        cls.post_cache_size = post_cache_size
        cls._user_cache = LRUCache(max_size=user_cache_size)
        cls._post_cache = LRUCache(max_size=post_cache_size)

    @staticmethod
    async def hydrate_bulk(
        records: List[dict],
        api_clients: List[BlueskyAPI],
        *,
        excluded_dids: frozenset[str] = frozenset(),
    ) -> List[dict]:
        """
        Process up to 100 raw Jetstream records:
          1) Collect all unique DIDs (including mentions) and URIs.
          2) Bulk fetch user profiles and post data from Bluesky (skipping any in cache).
          3) Build a final enriched object for each record.
        """
        # 1) Collect all unique DIDs & URIs we need to hydrate
        record_mentions_map: Dict[int, Set[str]] = {}
        record_embed_map: Dict[int, str] = {}
        all_dids: Set[str] = set()
        all_uris: Set[str] = set()
        mention_dids_global: Set[str] = set()

        for idx, rec in enumerate(records):
            rec_dids = set()

            did = rec.get("did")
            if did and (not excluded_dids or did not in excluded_dids):
                all_dids.add(did)

            commit = rec.get("commit", {})
            c_record = commit.get("record", {})

            # —— QUOTE PATCH —— detect quote embed URIs
            embed = c_record.get("embed", {})
            if embed.get("$type") == "app.bsky.embed.record":
                quote_uri = embed.get("record", {}).get("uri")
                if quote_uri:
                    qdid = repo_did_from_at_uri(quote_uri)
                    skip = (
                        excluded_dids
                        and qdid is not None
                        and qdid in excluded_dids
                    )
                    if not skip:
                        all_uris.add(quote_uri)
                        record_embed_map[idx] = quote_uri

            # Collect mention DID(s) from facets
            facets = c_record.get("facets", [])
            for facet in facets:
                features = facet.get("features", [])
                for feature in features:
                    if feature.get("$type") == "app.bsky.richtext.facet#mention":
                        mention_did = feature.get("did")
                        if mention_did and (
                            not excluded_dids or mention_did not in excluded_dids
                        ):
                            rec_dids.add(mention_did)
                            mention_dids_global.add(mention_did)

            reply = c_record.get("reply", {})
            parent_uri = reply.get("parent", {}).get("uri")
            if parent_uri:
                p_did = repo_did_from_at_uri(parent_uri)
                skip = (
                    excluded_dids
                    and p_did is not None
                    and p_did in excluded_dids
                )
                if not skip:
                    all_uris.add(parent_uri)
            root_uri = reply.get("root", {}).get("uri")
            if root_uri:
                r_did = repo_did_from_at_uri(root_uri)
                skip = (
                    excluded_dids
                    and r_did is not None
                    and r_did in excluded_dids
                )
                if not skip:
                    all_uris.add(root_uri)

            record_mentions_map[idx] = rec_dids

        # Combine posting user DIDs + mention DIDs
        all_dids.update(mention_dids_global)

        # Filter out the DIDs & URIs that are already cached
        # We'll do a read-lock while we check the caches.

        async with Hydration._cache_lock.reader_lock:
            missing_dids = [d for d in all_dids if Hydration._user_cache.get(d) is None]
            missing_uris = [u for u in all_uris if Hydration._post_cache.get(u) is None]

        # 2) Get a single BlueskyAPI object
        api = random.choice(api_clients)

        # 3) Bulk fetch only missing user data and post data in parallel
        #    Then populate the cache in a single (writer) lock.
        async def fetch_missing_users():
            if not missing_dids:
                return {}
            # We'll get a list of [ (did, profile) ... ] or something similar
            profiles = await api.get_user_data_for_dids(missing_dids)
            return profiles

        async def fetch_missing_posts():
            if not missing_uris:
                return {}
            # This returns List[(uri, data)]
            posts = await api.hydrate_records_for_uris(missing_uris)
            return posts

        fetched_users, fetched_posts = await asyncio.gather(
            fetch_missing_users(), fetch_missing_posts()
        )

        # 4) Put fetched items into the cache
        async with Hydration._cache_lock.writer_lock:
            # Add user profiles to cache
            for did, profile in fetched_users.items():
                # 'profile' might be an object with `.did`
                if hasattr(profile, "did"):
                    Hydration._user_cache.set(profile.did, profile.model_dump())
            # Add post data to cache
            for uri, post_data in fetched_posts.items():
                Hydration._post_cache.set(
                    uri,
                    post_data.model_dump()
                    if hasattr(post_data, "model_dump")
                    else post_data,
                )

            # Now read from cache with updated contents
            did_to_profile = {}
            for d in all_dids:
                val = Hydration._user_cache.get(d)
                if val is not None:
                    # Move it to the end (most recently used)
                    Hydration._user_cache.set(d, val)
                did_to_profile[d] = val

            uri_to_post = {}
            for u in all_uris:
                val = Hydration._post_cache.get(u)
                if val is not None:
                    Hydration._post_cache.set(u, val)
                uri_to_post[u] = val

        # 5) Final pass: build a new list of records with the desired shape
        enriched_list = []

        for idx, rec in enumerate(records):
            commit = rec.get("commit", {})
            c_record = commit.get("record", {})

            # Construct an at_uri if possible from commit data
            did = rec.get("did", "")
            if excluded_dids and did in excluded_dids:
                continue

            collection = commit.get("collection", "")
            rkey = commit.get("rkey", "")
            if did and collection and rkey:
                at_uri = f"at://{did}/{collection}/{rkey}"
            else:
                at_uri = ""

            # `time_us` is jetstream's own receipt time and it lives on the RECORD
            # ENVELOPE, next to `did` and `commit` -- never inside `commit.record`, which
            # holds only what the posting client wrote. Reading it from `c_record` meant
            # this was `None` on every record ever enriched: it is why the archives'
            # `time_us` column is uniformly NULL, and why the envelope copy could not be
            # used as the post's arrival time even though the value was right there.
            #
            # Consumers read `message.time_us` first for exactly this reason, so they work
            # on the backlog as well as on records enriched after this fix. Do not remove
            # that fallback on the strength of this line.
            time_us = rec.get("time_us", None)

            # The user is the 'did' who posted
            user_profile = did_to_profile.get(did, None)

            # Mentions: a dict of { mention_did -> user_profile }
            mention_dict = {}
            for mention_did in record_mentions_map[idx]:
                if excluded_dids and mention_did in excluded_dids:
                    continue
                mention_dict[mention_did] = did_to_profile.get(mention_did)

            # Parent & root post data
            reply = c_record.get("reply", {})
            parent_uri = reply.get("parent", {}).get("uri")
            parent_post = uri_to_post.get(parent_uri) if parent_uri else None
            parent_post = _post_if_not_excluded(parent_post, excluded_dids)

            root_uri = reply.get("root", {}).get("uri")
            root_post = uri_to_post.get(root_uri) if root_uri else None
            root_post = _post_if_not_excluded(root_post, excluded_dids)

            quote_uri = record_embed_map.get(idx)
            quote_post = uri_to_post.get(quote_uri) if quote_uri else None
            quote_post = _post_if_not_excluded(quote_post, excluded_dids)

            hydrated_metadata = {
                "user": user_profile,
                "mentions": mention_dict,
                "parent_post": parent_post,
                "reply_post": root_post,
                "quote_post": quote_post,
            }

            enriched_obj = {
                "at_uri": at_uri,
                "did": did,
                "time_us": time_us,
                "message": rec,  # the entire raw record
                "hydrated_metadata": hydrated_metadata,
            }
            if overrides := rec.get("override_algorithms"):
                enriched_obj["override_algorithms"] = overrides

            enriched_list.append(enriched_obj)
        return enriched_list

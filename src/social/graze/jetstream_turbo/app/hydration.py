import asyncio
import random
import time
from typing import List, Dict, Any, Tuple, Set
import aiorwlock
from collections import OrderedDict

from social.graze.jetstream_turbo.app.bluesky_api import BlueskyAPI


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
        records: List[dict], api_clients: List[BlueskyAPI]
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
            if did:
                all_dids.add(did)

            commit = rec.get("commit", {})
            c_record = commit.get("record", {})

            # —— QUOTE PATCH —— detect quote embed URIs
            embed = c_record.get("embed", {})
            if embed.get("$type") == "app.bsky.embed.record":
                quote_uri = embed.get("record", {}).get("uri")
                if quote_uri:
                    all_uris.add(quote_uri)
                    record_embed_map[idx] = quote_uri

            # Collect mention DID(s) from facets
            facets = c_record.get("facets", [])
            for facet in facets:
                features = facet.get("features", [])
                for feature in features:
                    if feature.get("$type") == "app.bsky.richtext.facet#mention":
                        mention_did = feature.get("did")
                        if mention_did:
                            rec_dids.add(mention_did)
                            mention_dids_global.add(mention_did)

            reply = c_record.get("reply", {})
            parent_uri = reply.get("parent", {}).get("uri")
            if parent_uri:
                all_uris.add(parent_uri)
            root_uri = reply.get("root", {}).get("uri")
            if root_uri:
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
            collection = commit.get("collection", "")
            rkey = commit.get("rkey", "")
            if did and collection and rkey:
                at_uri = f"at://{did}/{collection}/{rkey}"
            else:
                at_uri = ""

            time_us = c_record.get("time_us", None)

            # The user is the 'did' who posted
            user_profile = did_to_profile.get(did, None)

            # Mentions: a dict of { mention_did -> user_profile }
            mention_dict = {}
            for mention_did in record_mentions_map[idx]:
                mention_dict[mention_did] = did_to_profile.get(mention_did)

            # Parent & root post data
            reply = c_record.get("reply", {})
            parent_uri = reply.get("parent", {}).get("uri")
            parent_post = uri_to_post.get(parent_uri) if parent_uri else None

            root_uri = reply.get("root", {}).get("uri")
            root_post = uri_to_post.get(root_uri) if root_uri else None
            quote_uri = record_embed_map.get(idx)
            quote_post = uri_to_post.get(quote_uri) if quote_uri else None

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
            enriched_list.append(enriched_obj)
        return enriched_list

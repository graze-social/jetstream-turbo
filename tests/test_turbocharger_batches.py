"""Batches must hydrate concurrently, must never be dropped, and must hold the cursor back
until they are egressed.

Before: `await asyncio.create_task(...)` made hydration fully serial (the Semaphore(100) never
had more than one holder), and any hydration exception dropped the whole batch after logging.
"""
import asyncio
import sys
from pathlib import Path

import aiorwlock
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from social.graze.jetstream_turbo.app import turbocharger as turbocharger_module  # noqa: E402
from social.graze.jetstream_turbo.app.config import Settings  # noqa: E402
from social.graze.jetstream_turbo.app.egress import EgressBase, MultipleEgress  # noqa: E402
from social.graze.jetstream_turbo.app.hydration import Hydration  # noqa: E402
from social.graze.jetstream_turbo.app.turbocharger import TurboCharger  # noqa: E402

DID = "did:plc:4zt6zth77miqy5w2ym3upvde"
MENTIONED = "did:plc:mentionedmentionedmention"


def record(time_us):
    return {
        "did": DID,
        "time_us": time_us,
        "kind": "commit",
        "commit": {
            "collection": "app.bsky.feed.post",
            "rkey": f"rkey{time_us}",
            "record": {
                "$type": "app.bsky.feed.post",
                "text": "hi @someone",
                "facets": [{"features": [
                    {"$type": "app.bsky.richtext.facet#mention", "did": MENTIONED},
                ]}],
                "reply": {
                    "parent": {"uri": f"at://{DID}/app.bsky.feed.post/parent"},
                    "root": {"uri": f"at://{DID}/app.bsky.feed.post/root"},
                },
            },
        },
    }


class RecordingEgress(EgressBase):
    name = "recording"

    def __init__(self):
        self.stored = []

    async def store_records(self, enriched_records):
        self.stored.extend(enriched_records)


class FailingEgress(EgressBase):
    name = "failing"

    async def store_records(self, enriched_records):
        raise ConnectionError("egress down")


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch):
    monkeypatch.setattr(turbocharger_module, "backoff_delay", lambda *a, **k: 0)


def charger(egress, **settings):
    settings.setdefault("hydration_concurrency", 4)
    settings.setdefault("hydration_attempts", 3)
    return TurboCharger(egress, [], ["host.example"], settings=Settings(**settings))


def test_hydration_that_keeps_failing_egresses_unhydrated_instead_of_dropping(monkeypatch):
    calls = []

    async def broken(records, clients, *, excluded_dids=frozenset()):
        calls.append(len(records))
        raise RuntimeError("appview 502")

    monkeypatch.setattr(Hydration, "hydrate_bulk", broken)
    egress = RecordingEgress()
    tc = charger(egress)

    async def run():
        task = await tc._process_batch([record(100), record(101)])
        return await task

    assert asyncio.run(run()) is True
    assert calls == [2, 2, 2]  # Retried, then gave up on hydration -- not on the posts.
    assert [r["time_us"] for r in egress.stored] == [100, 101]
    first = egress.stored[0]
    assert first["at_uri"] == f"at://{DID}/app.bsky.feed.post/rkey100"
    assert first["message"] == record(100)
    assert first["hydrated_metadata"] == {
        "user": None,
        "mentions": {MENTIONED: None},
        "parent_post": None,
        "reply_post": None,
        "quote_post": None,
    }
    assert tc.cursor_tracker.low_water() == 101


def test_unhydrated_is_exactly_what_hydration_yields_when_every_lookup_is_empty(monkeypatch):
    """Downstream must not be able to tell a fallback record from an unlucky hydrated one."""

    class EmptyAPI:
        async def get_user_data_for_dids(self, dids):
            return {}

        async def hydrate_records_for_uris(self, uris):
            return {}

    # The class-level lock binds to the first event loop that takes it; give this test its own.
    monkeypatch.setattr(Hydration, "_cache_lock", aiorwlock.RWLock())
    records = [record(1), record(2)]
    hydrated = asyncio.run(Hydration.hydrate_bulk(records, [EmptyAPI()]))
    assert Hydration.unhydrated_bulk(records) == hydrated


def test_a_transient_hydration_failure_is_retried(monkeypatch):
    attempts = []

    async def flaky(records, clients, *, excluded_dids=frozenset()):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("timeout")
        return [{"time_us": r["time_us"], "hydrated_metadata": {"user": {"did": DID}}} for r in records]

    monkeypatch.setattr(Hydration, "hydrate_bulk", flaky)
    egress = RecordingEgress()
    tc = charger(egress)

    async def run():
        return await (await tc._process_batch([record(1)]))

    assert asyncio.run(run()) is True
    assert len(attempts) == 2
    assert egress.stored[0]["hydrated_metadata"]["user"] == {"did": DID}


def test_batches_run_concurrently_and_the_cursor_is_the_low_water_mark(monkeypatch):
    gates = {}
    running = []
    peak = []

    async def gated(records, clients, *, excluded_dids=frozenset()):
        first = records[0]["time_us"]
        running.append(first)
        peak.append(len(running))
        await gates[first].wait()
        running.remove(first)
        return [{"time_us": r["time_us"]} for r in records]

    monkeypatch.setattr(Hydration, "hydrate_bulk", gated)
    egress = RecordingEgress()
    tc = charger(egress)

    async def run():
        for start in (100, 200, 300):
            gates[start] = asyncio.Event()
        await tc._process_batch([record(100), record(110)])
        await tc._process_batch([record(200), record(210)])
        await tc._process_batch([record(300), record(310)])
        await asyncio.sleep(0)
        assert max(peak) == 3  # All three hydrating at once.

        gates[300].set()
        await asyncio.sleep(0.01)
        assert tc.cursor_tracker.low_water() == 100  # 300 is done; 100 and 200 are not.

        gates[100].set()
        await asyncio.sleep(0.01)
        assert tc.cursor_tracker.low_water() == 200

        gates[200].set()
        await tc.drain()
        assert tc.cursor_tracker.low_water() == 310

    asyncio.run(run())
    assert sorted(r["time_us"] for r in egress.stored) == [100, 110, 200, 210, 300, 310]


def test_concurrency_is_bounded(monkeypatch):
    running = []
    peak = []

    async def slow(records, clients, *, excluded_dids=frozenset()):
        running.append(1)
        peak.append(len(running))
        await asyncio.sleep(0.01)
        running.pop()
        return [{"time_us": r["time_us"]} for r in records]

    monkeypatch.setattr(Hydration, "hydrate_bulk", slow)
    tc = charger(RecordingEgress(), hydration_concurrency=2)

    async def run():
        for t in range(10):
            await tc._process_batch([record(t)])
        await tc.drain()

    asyncio.run(run())
    assert max(peak) == 2


def test_a_batch_that_cannot_be_egressed_holds_the_persisted_cursor(monkeypatch):
    async def ok(records, clients, *, excluded_dids=frozenset()):
        return [{"time_us": r["time_us"]} for r in records]

    monkeypatch.setattr(Hydration, "hydrate_bulk", ok)
    egress = MultipleEgress(FailingEgress(), max_attempts=2)

    async def no_sleep(_):
        return None

    egress._sleep = no_sleep
    tc = charger(egress)

    class Store:
        saved = []

        async def save(self, cursor):
            self.saved.append(cursor)
            return True

    tc.cursor_store = Store()

    async def run():
        failed = await (await tc._process_batch([record(100), record(110)]))
        tc.egress = RecordingEgress()
        later = await (await tc._process_batch([record(200), record(210)]))
        await tc.flush_cursor()
        return failed, later

    assert asyncio.run(run()) == (False, True)
    assert tc.cursor_store.saved == [100]

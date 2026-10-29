"""The persisted cursor may only ever point at data that has been egressed.

Batches hydrate concurrently and finish out of order, so the cursor is a low-water mark: the
oldest `time_us` still in flight, never the newest one finished.
"""
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from social.graze.jetstream_turbo.app.cursor import CursorStore, CursorTracker  # noqa: E402


def batch(*times):
    return [{"did": "did:plc:x", "time_us": t} for t in times]


def test_out_of_order_completion_never_passes_an_unfinished_batch():
    tracker = CursorTracker()
    a = tracker.begin(batch(100, 105, 110))
    b = tracker.begin(batch(111, 120))
    c = tracker.begin(batch(121, 130))
    assert tracker.low_water() == 100

    tracker.finish(c)
    assert tracker.low_water() == 100  # C is done, but A and B are not.

    tracker.finish(a)
    assert tracker.low_water() == 111  # B is the oldest thing still outstanding.

    tracker.finish(b)
    assert tracker.low_water() == 130  # Nothing outstanding: the newest egressed record.


def test_a_failed_batch_holds_the_cursor_for_good():
    tracker = CursorTracker()
    a = tracker.begin(batch(100, 110))
    b = tracker.begin(batch(111, 120))
    tracker.fail(a)
    tracker.finish(b)
    assert tracker.low_water() == 100

    later = tracker.begin(batch(500, 510))
    tracker.finish(later)
    assert tracker.low_water() == 100


def test_nothing_known_means_no_cursor():
    tracker = CursorTracker()
    assert tracker.low_water() is None
    assert tracker.begin([{"did": "did:plc:x"}]) is None  # No time_us to track.
    assert tracker.low_water() is None


class FakeRedis:
    def __init__(self, value=None, fail=False):
        self.value = value
        self.fail = fail

    async def get(self, key):
        if self.fail:
            raise ConnectionError("down")
        return self.value

    async def set(self, key, value):
        if self.fail:
            raise ConnectionError("down")
        self.value = value.encode()


def store_with(redis):
    store = CursorStore("redis://unused", "jetstream_turbo:cursor")
    store._redis_client = redis
    return store


def now_us(offset_seconds=0.0):
    return int((time.time() + offset_seconds) * 1_000_000)


def test_a_recent_cursor_is_resumed_from():
    fresh = now_us(-60)
    assert asyncio.run(store_with(FakeRedis(str(fresh).encode())).load(1800)) == fresh


def test_a_stale_cursor_is_ignored_rather_than_replaying_hours():
    stale = now_us(-3600)
    assert asyncio.run(store_with(FakeRedis(str(stale).encode())).load(1800)) is None


def test_missing_garbage_or_unreachable_means_live_tail_not_a_crash():
    assert asyncio.run(store_with(FakeRedis(None)).load(1800)) is None
    assert asyncio.run(store_with(FakeRedis(b"nope")).load(1800)) is None
    assert asyncio.run(store_with(FakeRedis(fail=True)).load(1800)) is None


def test_save_round_trips_and_survives_redis_being_down():
    redis = FakeRedis()
    store = store_with(redis)
    assert asyncio.run(store.save(123)) is True
    assert redis.value == b"123"
    redis.fail = True
    assert asyncio.run(store.save(456)) is False

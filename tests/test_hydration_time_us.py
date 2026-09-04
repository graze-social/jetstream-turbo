"""The enriched envelope's `time_us` must carry jetstream's receipt time.

It never did. `hydrate_bulk` read it from `commit.record`, where it has never lived --
`time_us` sits on the record envelope next to `did` and `commit`, because it is stamped by
the firehose consumer, not written by the posting client. So the field was `None` on every
record this pipeline has ever produced, which is why the mega archives' `time_us` column is
uniformly NULL.

It matters beyond the archives: this is the only faithful "when did the post reach the
network" signal Graze has. `createdAt` is author-supplied and validated by nothing upstream,
and for bridged accounts it is routinely hours off -- 40.9% of `*.brid.gy` posts sit more
than ten minutes from their arrival, against 0.70% of everything else.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from social.graze.jetstream_turbo.app.hydration import Hydration  # noqa: E402

# 2026-09-04T12:48:37.381258Z, taken from a real archive record.
TIME_US = 1_788_526_117_381_258
DID = "did:plc:4zt6zth77miqy5w2ym3upvde"


class StubAPI:
    """Hydration is not what is under test here; the envelope it builds is."""

    async def get_user_data_for_dids(self, dids):
        return {}

    async def hydrate_records_for_uris(self, uris):
        return {}


def jetstream_record(time_us=TIME_US, record=None):
    """A raw jetstream record: `time_us` on the envelope, never inside `commit.record`."""
    rec = {
        "did": DID,
        "kind": "commit",
        "commit": {
            "collection": "app.bsky.feed.post",
            "rkey": "3munohxiqvrq2",
            "record": record if record is not None else {
                "$type": "app.bsky.feed.post",
                "text": "hi",
                "createdAt": "2026-09-03T09:59:03.000Z",
            },
        },
    }
    if time_us is not None:
        rec["time_us"] = time_us
    return rec


# ONE event loop for the whole module. `Hydration._cache_lock` is an `aiorwlock` created at
# class definition time, and aiorwlock binds itself to the first loop that takes it -- so a
# fresh `asyncio.run` per test makes every test after the first raise "bound to a different
# event loop". That is a property of the production singleton, not of these tests.
_LOOP = asyncio.new_event_loop()


def hydrate(records):
    return _LOOP.run_until_complete(Hydration.hydrate_bulk(records, [StubAPI()]))


def test_the_envelope_carries_the_jetstream_receipt_time():
    """🔴 The regression. This was `None` on every record before the fix."""
    enriched = hydrate([jetstream_record()])
    assert len(enriched) == 1
    assert enriched[0]["time_us"] == TIME_US


def test_the_envelope_copy_agrees_with_the_inner_message():
    """Consumers prefer `message.time_us` so they also work on the pre-fix backlog. The two
    must never disagree, or `arrived_at` would mean different things by record age."""
    enriched = hydrate([jetstream_record()])[0]
    assert enriched["time_us"] == enriched["message"]["time_us"] == TIME_US


def test_a_record_field_named_time_us_is_not_mistaken_for_it():
    """What the old code actually read. A client is free to write this key; it is not the
    receipt time and must not be copied up as though it were."""
    enriched = hydrate([
        jetstream_record(record={
            "$type": "app.bsky.feed.post",
            "text": "hi",
            "createdAt": "2026-09-03T09:59:03.000Z",
            "time_us": 1,
        })
    ])[0]
    assert enriched["time_us"] == TIME_US


def test_a_record_with_no_receipt_time_stays_none():
    """0.19% of live records, all malformed or non-commit events. Absent stays absent rather
    than becoming 0, which downstream would read as an instant in 1970."""
    enriched = hydrate([jetstream_record(time_us=None)])[0]
    assert enriched["time_us"] is None

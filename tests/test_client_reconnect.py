"""The jetstream client must survive every way a connection ends, and resume without a gap.

In production (10/21-10/29) the single pod exited 79 times: only `ConnectionClosedError` was
caught, so a refused handshake (503/429), a socket error or a timeout escaped `run_stream`,
and a clean close (`ConnectionClosedOK`) simply ended the iterator. Each exit restarted from
the live tail and lost the backlog.
"""
import asyncio
import json
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from prometheus_client import REGISTRY
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK, InvalidStatus
from websockets.frames import Close
from websockets.http11 import Response

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from social.graze.jetstream_turbo.app.client import JetstreamClient  # noqa: E402
from social.graze.jetstream_turbo.app.metrics import RECONNECT_REASONS  # noqa: E402

HOSTS = ["a.example", "b.example", "c.example"]
MARGIN = 2_000_000


def handshake_503():
    return InvalidStatus(Response(503, "Service Unavailable", Headers()))


def closed_ok():
    return ConnectionClosedOK(Close(1000, "bye"), Close(1000, "bye"), True)


def closed_error():
    return ConnectionClosedError(Close(1011, "oops"), None)


def message(time_us):
    return json.dumps({"did": "did:plc:x", "kind": "commit", "time_us": time_us})


class FakeSocket:
    """Delivers `messages`, then ends the way a real connection would: by raising `end`."""

    def __init__(self, messages, end):
        self.messages = list(messages)
        self.end = end

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def recv(self):
        if self.messages:
            return self.messages.pop(0)
        if self.end is None:
            await asyncio.Event().wait()  # Silent forever.
        raise self.end


class RefusedConnection:
    """What `websockets.connect` does when the handshake or the socket fails."""

    def __init__(self, error):
        self.error = error

    async def __aenter__(self):
        raise self.error

    async def __aexit__(self, *exc):
        return False


def scripted_client(script, **kwargs):
    client = JetstreamClient(HOSTS, **kwargs)
    client._host_index = 0
    urls = []

    def connect(url):
        urls.append(url)
        step = script.pop(0)
        return RefusedConnection(step) if isinstance(step, BaseException) else step

    async def no_sleep(_delay):
        return None

    client._connect = connect
    client._sleep = no_sleep
    return client, urls


async def take(client, n):
    out = []
    stream = client.run_stream()
    async for msg in stream:
        out.append(msg)
        if len(out) == n:
            break
    await stream.aclose()
    return out


def host(url):
    return urlparse(url).hostname


def cursor(url):
    values = parse_qs(urlparse(url).query).get("cursor")
    return int(values[0]) if values else None


def reconnects(reason):
    return REGISTRY.get_sample_value("jetstream_turbo_reconnects_total", {"reason": reason})


def test_every_reconnect_reason_is_exported_before_it_happens():
    for reason in RECONNECT_REASONS:
        assert reconnects(reason) is not None, reason


def test_a_refused_handshake_reconnects_to_another_host():
    """A 503 used to escape `run_stream` and exit the process."""
    before = reconnects("handshake")
    client, urls = scripted_client([
        handshake_503(),
        FakeSocket([message(10_000_000)], None),
    ])

    got = asyncio.run(take(client, 1))

    assert [m["time_us"] for m in got] == [10_000_000]
    assert [host(u) for u in urls] == ["a.example", "b.example"]
    assert cursor(urls[1]) is None  # Nothing seen yet: nothing to resume from.
    assert reconnects("handshake") == before + 1


def test_a_clean_close_reconnects_from_the_last_cursor_less_the_margin():
    """`async for` ends quietly on ConnectionClosedOK, which used to end the stream."""
    before = reconnects("closed_ok")
    client, urls = scripted_client([
        FakeSocket([message(10_000_000), message(11_000_000)], closed_ok()),
        FakeSocket([message(10_500_000), message(12_000_000)], closed_error()),
        FakeSocket([message(13_000_000)], None),
    ])

    got = asyncio.run(take(client, 5))

    assert [m["time_us"] for m in got] == [
        10_000_000, 11_000_000, 10_500_000, 12_000_000, 13_000_000,
    ]
    assert [host(u) for u in urls] == ["a.example", "b.example", "c.example"]
    assert cursor(urls[1]) == 11_000_000 - MARGIN
    assert cursor(urls[2]) == 12_000_000 - MARGIN
    assert reconnects("closed_ok") == before + 1


@pytest.mark.parametrize("error, reason", [
    (OSError("connection refused"), "os_error"),
    (TimeoutError(), "timeout"),
    (RuntimeError("anything else"), "other"),
])
def test_socket_errors_and_timeouts_reconnect_too(error, reason):
    before = reconnects(reason)
    client, urls = scripted_client([error, error, FakeSocket([message(5_000_000)], None)])

    got = asyncio.run(take(client, 1))

    assert len(got) == 1
    assert [host(u) for u in urls] == ["a.example", "b.example", "c.example"]
    assert reconnects(reason) == before + 2


def test_a_silent_connection_is_abandoned():
    """Open, answering pings, delivering nothing: treated as dead after `idle_timeout`."""
    before = reconnects("idle")
    client, urls = scripted_client(
        [FakeSocket([message(7_000_000)], None), FakeSocket([message(8_000_000)], None)],
        idle_timeout=0.01,
    )

    got = asyncio.run(take(client, 2))

    assert [m["time_us"] for m in got] == [7_000_000, 8_000_000]
    assert cursor(urls[1]) == 7_000_000 - MARGIN
    assert reconnects("idle") == before + 1


def test_hosts_rotate_round_robin_and_wrap():
    client, urls = scripted_client([OSError()] * 4 + [FakeSocket([message(1)], None)])
    asyncio.run(take(client, 1))
    assert [host(u) for u in urls] == [
        "a.example", "b.example", "c.example", "a.example", "b.example",
    ]


def test_a_persisted_start_cursor_is_used_with_the_margin():
    client, urls = scripted_client(
        [FakeSocket([message(50_000_000)], None)], cursor=40_000_000,
    )
    asyncio.run(take(client, 1))
    assert cursor(urls[0]) == 40_000_000 - MARGIN


def test_garbage_frames_are_skipped_not_fatal():
    client, _ = scripted_client([
        FakeSocket(["not json", "[1, 2]", message(9_000_000)], None),
    ])
    got = asyncio.run(take(client, 1))
    assert got[0]["time_us"] == 9_000_000

"""Egress must not report success for records it did not deliver.

`SendMessageBatch` can fail some entries while the call itself succeeds; the `Failed` list
was ignored. `MultipleEgress` logged and swallowed every exception. Either way the records
were gone and nothing upstream could know.
"""
import asyncio
import json
import sys
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from social.graze.jetstream_turbo.app.egress import (  # noqa: E402
    EgressBase, EgressError, MultipleEgress, SQSEgress,
)


async def no_sleep(_):
    return None


class FakeSQS:
    """`respond(entries)` returns a response dict or raises, per call."""

    def __init__(self, respond):
        self.respond = respond
        self.calls = []

    async def send_message_batch(self, QueueUrl, Entries):
        self.calls.append(Entries)
        return self.respond(Entries)


def sqs_with(respond, **kwargs):
    egress = SQSEgress("https://sqs.example/queue", **kwargs)
    egress._sqs_client = FakeSQS(respond)
    egress._sleep = no_sleep
    return egress


def records(n, pad=0):
    return [
        {"at_uri": f"at://x/{i}", "time_us": i, "hydrated_metadata": {"user": {"bio": "x" * pad}}}
        for i in range(n)
    ]


def ok(entries):
    return {"Successful": [{"Id": e["Id"]} for e in entries], "Failed": []}


def test_failed_entries_are_retried_and_only_they_are():
    attempts = []

    def respond(entries):
        attempts.append(1)
        if len(attempts) == 1:
            return {
                "Successful": [{"Id": e["Id"]} for e in entries if e["Id"] not in ("1", "3")],
                "Failed": [
                    {"Id": "1", "SenderFault": False, "Code": "InternalError"},
                    {"Id": "3", "SenderFault": False, "Code": "ServiceUnavailable"},
                ],
            }
        return ok(entries)

    egress = sqs_with(respond)
    asyncio.run(egress.store_records(records(5)))

    calls = egress._sqs_client.calls
    assert len(calls) == 2
    assert [e["Id"] for e in calls[1]] == ["1", "3"]
    assert [json.loads(e["MessageBody"])["time_us"] for e in calls[1]] == [1, 3]


def test_entries_still_failing_after_every_attempt_raise():
    def respond(entries):
        return {"Successful": [], "Failed": [
            {"Id": e["Id"], "SenderFault": False, "Code": "InternalError"} for e in entries
        ]}

    egress = sqs_with(respond, max_attempts=3)
    with pytest.raises(EgressError) as raised:
        asyncio.run(egress.store_records(records(4)))
    assert len(egress._sqs_client.calls) == 3
    assert raised.value.count == 4


def test_a_whole_call_failing_is_retried():
    attempts = []

    def respond(entries):
        attempts.append(1)
        if len(attempts) < 3:
            raise ConnectionError("reset by peer")
        return ok(entries)

    egress = sqs_with(respond)
    asyncio.run(egress.store_records(records(2)))
    assert len(attempts) == 3


def test_sender_fault_entries_are_not_retried_or_raised():
    """Retrying a message SQS rejects as malformed would fail identically forever."""
    def respond(entries):
        return {"Successful": [], "Failed": [
            {"Id": entries[0]["Id"], "SenderFault": True, "Code": "InvalidMessageContents"},
        ]} if len(entries) == 1 else ok(entries)

    egress = sqs_with(respond)
    asyncio.run(egress.store_records(records(1)))
    assert len(egress._sqs_client.calls) == 1


def test_batches_are_split_to_fit_the_payload_limit():
    """Ten hydrated records routinely exceed the batch payload limit between them."""
    egress = sqs_with(ok, max_batch_bytes=262_144)
    asyncio.run(egress.store_records(records(5, pad=100_000)))

    sizes = [len(call) for call in egress._sqs_client.calls]
    assert sizes == [2, 2, 1]
    for call in egress._sqs_client.calls:
        assert sum(len(e["MessageBody"].encode()) for e in call) <= 262_144
    delivered = [json.loads(e["MessageBody"])["time_us"] for call in egress._sqs_client.calls for e in call]
    assert delivered == [0, 1, 2, 3, 4]


def test_never_more_than_ten_entries_per_call():
    egress = sqs_with(ok)
    asyncio.run(egress.store_records(records(23)))
    assert [len(call) for call in egress._sqs_client.calls] == [10, 10, 3]


def test_a_record_too_large_even_alone_is_sent_unhydrated():
    egress = sqs_with(ok, max_batch_bytes=10_000)
    asyncio.run(egress.store_records(records(1, pad=50_000)))

    body = json.loads(egress._sqs_client.calls[0][0]["MessageBody"])
    assert body["at_uri"] == "at://x/0"
    assert body["hydrated_metadata"]["user"] is None


def test_batch_request_too_long_from_sqs_splits_the_batch():
    """Our size estimate and SQS's can disagree; the backstop is to halve and resend."""
    def respond(entries):
        if len(entries) > 2:
            raise ClientError(
                {"Error": {"Code": "AWS.SimpleQueueService.BatchRequestTooLong", "Message": "x"}},
                "SendMessageBatch",
            )
        return ok(entries)

    egress = sqs_with(respond)
    asyncio.run(egress.store_records(records(8)))

    accepted = [call for call in egress._sqs_client.calls if len(call) <= 2]
    delivered = sorted(
        json.loads(e["MessageBody"])["time_us"] for call in accepted for e in call
    )
    assert delivered == list(range(8))


class FlakyEgress(EgressBase):
    def __init__(self, name, failures):
        self.name = name
        self.failures = failures
        self.calls = 0

    async def store_records(self, enriched_records):
        self.calls += 1
        if self.calls <= self.failures:
            raise ConnectionError(f"{self.name} down")


def multiple(*methods, attempts=3):
    egress = MultipleEgress(*methods, max_attempts=attempts)
    egress._sleep = no_sleep
    return egress


def test_multiple_egress_retries_only_the_method_that_failed():
    healthy = FlakyEgress("s3", failures=0)
    flaky = FlakyEgress("sqs", failures=2)
    asyncio.run(multiple(healthy, flaky).store_records(records(3)))
    assert (healthy.calls, flaky.calls) == (1, 3)


def test_multiple_egress_raises_instead_of_swallowing():
    healthy = FlakyEgress("s3", failures=0)
    dead = FlakyEgress("redis", failures=99)
    with pytest.raises(EgressError):
        asyncio.run(multiple(healthy, dead, attempts=3).store_records(records(3)))
    assert dead.calls == 3

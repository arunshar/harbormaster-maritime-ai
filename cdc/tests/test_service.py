"""Gate C6: consumer-loop sequencing with an injected Kafka consumer. No Kafka,
no AWS: the loop's contract is poll -> parse -> apply -> commit, batch by batch."""

from __future__ import annotations

import json
import threading

import pytest

from cdc.connector.config import table_topics
from cdc.consumer.applier import Applier
from cdc.consumer.service import ConsumerConfig, ConsumerLoop, build_kafka_config
from cdc.fixtures.loader import load_envelope_messages
from cdc.sinks.base import MemoryAudit, MemorySink


class FakeMessage:
    def __init__(self, topic: str, key: str | None, value: str | None, error=None) -> None:
        self._topic, self._key, self._value, self._error = topic, key, value, error

    def topic(self) -> str:
        return self._topic

    def key(self):
        return None if self._key is None else self._key.encode()

    def value(self):
        return None if self._value is None else self._value.encode()

    def error(self):
        return self._error


class FakeConsumer:
    """Scripted poll() sequence; records subscribe/commit/close ordering."""

    def __init__(self, messages: list[FakeMessage | None]) -> None:
        self._script = list(messages)
        self.log: list[str] = []
        self.commits = 0

    def subscribe(self, topics: list[str]) -> None:
        self.log.append(f"subscribe:{','.join(sorted(topics))}")

    def poll(self, timeout: float):
        if not self._script:
            return None
        msg = self._script.pop(0)
        if msg is not None:
            self.log.append("poll:msg")
        return msg

    def commit(self, asynchronous: bool = True):
        assert asynchronous is False, "offsets must commit synchronously after sink ack"
        self.commits += 1
        self.log.append("commit")

    def close(self) -> None:
        self.log.append("close")


def _fixture_messages() -> list[FakeMessage]:
    return [FakeMessage(t, k, v) for t, k, v in load_envelope_messages()]


def _loop(consumer, store=None, audit=None, **kwargs) -> ConsumerLoop:
    return ConsumerLoop(
        consumer=consumer,
        applier=Applier(store=store or MemorySink(), audit=audit or MemoryAudit()),
        **kwargs,
    )


def test_run_once_drains_applies_then_commits_exactly_once():
    consumer = FakeConsumer([*_fixture_messages(), None])
    store = MemorySink()
    result = _loop(consumer, store=store).run_once()
    assert result is not None and result.events == 8 and result.applied == 7
    assert consumer.commits == 1
    assert consumer.log[-1] == "commit"  # commit strictly after every apply
    assert store.final_state()  # state landed


def test_run_once_returns_none_on_an_empty_poll_without_committing():
    consumer = FakeConsumer([None])
    assert _loop(consumer).run_once() is None
    assert consumer.commits == 0


def test_subscribes_to_the_table_topics_from_the_shared_config():
    consumer = FakeConsumer([None])
    _loop(consumer).run_once()
    assert consumer.log[0] == "subscribe:" + ",".join(sorted(table_topics("hm")))


def test_batch_max_bounds_one_batch():
    consumer = FakeConsumer([*_fixture_messages(), None])
    result = _loop(consumer, batch_max=3).run_once()
    assert result is not None
    # 3 messages drained: 2 data events + 1 heartbeat skip in fixture order
    assert result.events + result.tombstones + sum(result.skips.values()) == 3


def test_malformed_message_is_counted_skipped_and_the_batch_still_commits():
    bad = FakeMessage("hm.public.watchlist", json.dumps({"mmsi": 1}), "{not json")
    good = _fixture_messages()
    consumer = FakeConsumer([bad, *good, None])
    result = _loop(consumer).run_once()
    assert result is not None and result.events == 8  # the bad message vanished, counted
    assert consumer.commits == 1


def test_kafka_transport_error_messages_are_skipped():
    err = FakeMessage("hm.public.watchlist", None, None, error="broker went away")
    consumer = FakeConsumer([err, *_fixture_messages(), None])
    result = _loop(consumer).run_once()
    assert result is not None and result.events == 8


def test_sink_failure_propagates_and_never_commits():
    class FailingStore(MemorySink):
        def flush(self) -> None:
            raise RuntimeError("dynamo outage")

    consumer = FakeConsumer([*_fixture_messages(), None])
    loop = _loop(consumer, store=FailingStore())
    with pytest.raises(Exception, match="offsets not committed"):
        loop.run_once()
    assert consumer.commits == 0


def test_run_forever_drains_then_closes_on_stop():
    consumer = FakeConsumer([*_fixture_messages(), None])
    loop = _loop(consumer)
    stop = threading.Event()

    # run_once consumes everything on the first pass; stop after it
    orig_run_once = loop.run_once

    def run_once_then_stop():
        result = orig_run_once()
        stop.set()
        return result

    loop.run_once = run_once_then_stop  # type: ignore[method-assign]
    loop.run_forever(stop)
    assert consumer.log[-1] == "close"
    assert consumer.commits == 1


# ------------------------------------------------------------------- config


def test_config_from_env_requires_bootstrap_and_parses_types():
    with pytest.raises(ValueError, match="HM_KAFKA_BOOTSTRAP"):
        ConsumerConfig.from_env({})
    cfg = ConsumerConfig.from_env(
        {
            "HM_KAFKA_BOOTSTRAP": "b:9098",
            "HM_KAFKA_MSK_IAM": "true",
            "HM_ONLINE_TABLE": "t",
            "HM_BATCH_MAX": "50",
            "HM_POLL_TIMEOUT_S": "0.25",
            "AWS_REGION": "us-east-1",
        }
    )
    assert cfg.msk_iam is True and cfg.batch_max == 50 and cfg.poll_timeout_s == 0.25


def test_kafka_config_disables_auto_commit_and_wires_msk_iam():
    plain = build_kafka_config(ConsumerConfig(kafka_bootstrap="b:9092"))
    assert plain["enable.auto.commit"] is False
    assert plain["auto.offset.reset"] == "earliest"
    assert "security.protocol" not in plain

    iam = build_kafka_config(ConsumerConfig(kafka_bootstrap="b:9098", msk_iam=True))
    assert iam["security.protocol"] == "SASL_SSL"
    assert iam["sasl.mechanisms"] == "OAUTHBEARER"

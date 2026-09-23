"""Gate C3: connector config generation golden-matches and the validator holds the line."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from cdc.connector.config import (
    HEARTBEAT_ACTION_QUERY,
    build_connector_config,
    heartbeat_topic,
    table_topics,
    topic_for,
    validate_connector_config,
)
from cdc.schema.ddl import CDC_TABLES, HEARTBEAT_TABLE

EXPECTED_PATH = Path(__file__).parent.parent / "fixtures" / "connector_config.expected.json"
EXPECTATIONS_PATH = Path(__file__).parent.parent / "fixtures" / "expectations.json"


def _build() -> dict:
    return build_connector_config(db_host="postgres.local", db_port=5432)


def test_generated_config_passes_validation():
    validate_connector_config(_build())


def test_generated_config_golden_matches_committed_expectation():
    expected = json.loads(EXPECTED_PATH.read_text())
    assert _build() == expected, (
        "the generated connector config changed; if intentional, regenerate "
        "cdc/fixtures/connector_config.expected.json AND update PHASE_2.md in the same commit"
    )


def test_canonical_config_sha256_matches_pinned_expectation():
    actual = hashlib.sha256(
        json.dumps(_build(), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    expected = json.loads(EXPECTATIONS_PATH.read_text())["connector_config_sha256"]
    assert actual == expected, (
        "the canonical connector config changed; if intentional, update "
        "cdc/fixtures/expectations.json and the golden fixture together"
    )


def test_topics_follow_the_prefix_schema_table_convention():
    assert topic_for("watchlist") == "hm.public.watchlist"
    assert table_topics() == tuple(f"hm.public.{t}" for t in CDC_TABLES)
    assert heartbeat_topic() == "__debezium-heartbeat.hm"


def test_heartbeat_action_uses_the_private_published_table_only():
    cfg = _build()["config"]
    assert cfg["heartbeat.action.query"] == HEARTBEAT_ACTION_QUERY
    assert HEARTBEAT_TABLE in HEARTBEAT_ACTION_QUERY
    assert HEARTBEAT_TABLE not in cfg["table.include.list"]


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (lambda c: c.__setitem__("publication.autocreate.mode", "filtered"), "autocreate"),
        (lambda c: c.__setitem__("plugin.name", "decoderbufs"), "pgoutput"),
        (lambda c: c.__setitem__("table.include.list", "public.vessels"), "schema surface"),
        (lambda c: c.__setitem__("tombstones.on.delete", "false"), "tombstones"),
        (lambda c: c.__delitem__("slot.name"), "missing required"),
        (lambda c: c.__setitem__("slot.name", "other"), "slot.name"),
        (lambda c: c.__setitem__("publication.name", "other"), "publication.name"),
        (lambda c: c.__setitem__("heartbeat.interval.ms", "0"), "heartbeat"),
        (lambda c: c.__setitem__("heartbeat.action.query", "SELECT 1"), "heartbeat.action.query"),
    ],
)
def test_validator_rejects_drift(mutate, match):
    body = copy.deepcopy(_build())
    mutate(body["config"])
    with pytest.raises(ValueError, match=match):
        validate_connector_config(body)


def test_validator_rejects_missing_config_object():
    with pytest.raises(ValueError, match="no config"):
        validate_connector_config({"name": "x"})

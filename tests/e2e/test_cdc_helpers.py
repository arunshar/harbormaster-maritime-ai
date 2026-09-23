"""Unit tests for the Phase 2 e2e helpers (run unguarded, no stack needed)."""

from __future__ import annotations

from e2e.cdc_helpers import (
    DEFAULT_TENANT_ID,
    has_reason,
    item_is_online,
    missing_online,
    online_state_hash,
    score_payload,
)


def _wl_item(mmsi: int, deleted: bool = False) -> dict:
    return {
        "entity_id": {"S": f"{DEFAULT_TENANT_ID}:{mmsi}"},
        "feature_name": {"S": "watchlist"},
        "deleted": {"BOOL": deleted},
        "last_applied_lsn": {"N": "10"},
    }


def test_has_reason_reads_score_responses():
    resp = {"reasons": [{"code": "watchlist_hit", "severity": 0.9}]}
    assert has_reason(resp, "watchlist_hit") is True
    assert has_reason(resp, "sanctions_hit") is False
    assert has_reason({}, "watchlist_hit") is False


def test_item_is_online_respects_soft_delete():
    assert item_is_online(_wl_item(1)) is True
    assert item_is_online(_wl_item(1, deleted=True)) is False
    assert item_is_online(None) is False
    assert item_is_online({}) is False


def test_online_state_hash_is_order_insensitive_and_content_sensitive():
    a, b = _wl_item(1), _wl_item(2)
    assert online_state_hash([a, b]) == online_state_hash([b, a])
    assert online_state_hash([a, b]) != online_state_hash([a, _wl_item(2, deleted=True)])


def test_missing_online_flags_only_absent_or_deleted():
    items = [_wl_item(1), _wl_item(2, deleted=True)]
    assert missing_online([1, 2, 3], items) == [2, 3]


def test_score_payload_shape():
    p = score_payload(200000003)
    assert p["mmsi"] == 200000003 and p["history"] == []
    assert set(p["fix"]) == {"lat", "lon", "t", "sog"}

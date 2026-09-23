"""Pure helpers for the Phase 2 CDC e2e acceptance (gate C9). Unit-tested in
test_cdc_helpers.py; test_phase2.py uses them against a live stack."""

from __future__ import annotations

import hashlib
import json
from typing import Any

DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000000"


def has_reason(score_response: dict, code: str) -> bool:
    """True if a /v1/score-ais response carries the given reason code."""
    return code in [r.get("code") for r in score_response.get("reasons", [])]


def item_is_online(item: dict[str, Any] | None) -> bool:
    """A typed DynamoDB item counts as online unless absent or soft-deleted."""
    if not item:
        return False
    return not item.get("deleted", {}).get("BOOL", False)


def online_state_hash(items: list[dict[str, Any]]) -> str:
    """Canonical hash of a full online-table scan; replay must not change it."""
    canon = json.dumps(sorted(items, key=lambda i: json.dumps(i, sort_keys=True)), sort_keys=True)
    return hashlib.sha256(canon.encode()).hexdigest()


def missing_online(
    written_mmsis: list[int],
    items: list[dict[str, Any]],
    tenant_id: str = DEFAULT_TENANT_ID,
) -> list[int]:
    """Which written watchlist MMSIs have not (yet) reached the online store."""
    online = {
        i.get("entity_id", {}).get("S")
        for i in items
        if i.get("feature_name", {}).get("S") == "watchlist" and item_is_online(i)
    }
    return [m for m in written_mmsis if f"{tenant_id}:{m}" not in online]


def score_payload(mmsi: int, lat: float = 37.7, lon: float = -122.5) -> dict:
    """A minimal, kinematically boring /v1/score-ais request for one vessel."""
    return {
        "mmsi": mmsi,
        "fix": {"lat": lat, "lon": lon, "t": "2026-07-03T12:00:00Z", "sog": 10.0},
        "history": [],
    }

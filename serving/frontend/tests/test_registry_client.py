"""Pure registry-client helper tests (Phase 2, gate C1). No server, no Streamlit."""

from __future__ import annotations

import pytest

from frontend.registry_client import (
    format_watchlist_row,
    sanction_payload,
    vessel_payload,
    watchlist_payload,
)


def test_watchlist_payload_shape_and_trimming():
    p = watchlist_payload("  dark rendezvous ", 0.8, added_by="arun")
    assert p == {"reason": "dark rendezvous", "severity": 0.8, "added_by": "arun"}


def test_watchlist_payload_rejects_empty_reason_and_bad_severity():
    with pytest.raises(ValueError):
        watchlist_payload("   ")
    with pytest.raises(ValueError):
        watchlist_payload("x", 1.5)


def test_sanction_payload_requires_regime():
    assert sanction_payload("OFAC", "SDN-EXAMPLE-0001") == {
        "regime": "OFAC",
        "reference": "SDN-EXAMPLE-0001",
    }
    with pytest.raises(ValueError):
        sanction_payload("  ")


def test_vessel_payload_defaults():
    assert vessel_payload() == {"name": "", "flag_state": "", "vessel_type": ""}


def test_format_watchlist_row_flattens_and_rounds():
    row = {
        "mmsi": 200000003,
        "reason": "dark rendezvous",
        "severity": 0.85001,
        "added_by": "arun",
        "updated_at": "2026-07-03T12:00:00+00:00",
        "created_at": "ignored",
    }
    out = format_watchlist_row(row)
    assert out == {
        "mmsi": 200000003,
        "reason": "dark rendezvous",
        "severity": 0.85,
        "added_by": "arun",
        "updated_at": "2026-07-03T12:00:00+00:00",
    }

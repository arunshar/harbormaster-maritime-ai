"""Unit tests for the HITL console's pure helpers (gate G6). No server, no Streamlit."""

from __future__ import annotations

import pytest

from frontend.hitl_client import (
    LABELS,
    feedback_payload,
    format_row,
    positions_from_rows,
    reason_codes,
)

ROW = {
    "trace_id": "t-1",
    "mmsi": 200000001,
    "ts": "2024-06-01T05:00:00Z",
    "score": 0.812345,
    "confidence": 0.4,
    "reasons": [
        {
            "code": "abnormal_gap",
            "severity": 0.9,
            "detail": "gap",
            "evidence": {"lat": 40.4, "lon": -74.0},
        },
        {"code": "off_corridor", "severity": 0.5, "detail": "off", "evidence": {}},
    ],
}


def test_feedback_payload_valid_and_notes():
    assert feedback_payload("t-1", "correct", "arun") == {
        "trace_id": "t-1",
        "label": "correct",
        "reviewer": "arun",
    }
    assert feedback_payload("t-1", "ambiguous", "arun", "unsure")["notes"] == "unsure"


def test_feedback_payload_rejects_bad_label():
    with pytest.raises(ValueError):
        feedback_payload("t-1", "bogus", "arun")


def test_all_labels_are_accepted():
    for lab in LABELS:
        assert feedback_payload("t-1", lab, "arun")["label"] == lab


def test_reason_codes_and_format_row():
    assert reason_codes(ROW) == ["abnormal_gap", "off_corridor"]
    row = format_row(ROW)
    assert row["mmsi"] == 200000001
    assert row["score"] == 0.812  # rounded
    assert row["reasons"] == "abnormal_gap, off_corridor"


def test_positions_from_rows_pulls_first_evidence_coords():
    pts = positions_from_rows([ROW])
    assert pts == [{"mmsi": 200000001, "lat": 40.4, "lon": -74.0}]


def test_positions_empty_when_no_coords():
    row = {"mmsi": 1, "reasons": [{"code": "x", "evidence": {}}]}
    assert positions_from_rows([row]) == []

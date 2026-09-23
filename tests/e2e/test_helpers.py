"""Unit tests for the e2e helpers (gate G8). These run in the normal suite."""

from __future__ import annotations

from e2e.helpers import anomaly_in_pending, athena_count_query, reconciles, within_slo


def test_anomaly_in_pending_found_and_missing():
    rows = [{"mmsi": 1}, {"mmsi": 200000001, "trace_id": "t"}]
    assert anomaly_in_pending(rows, 200000001)["trace_id"] == "t"
    assert anomaly_in_pending(rows, 999) is None


def test_anomaly_in_pending_tolerates_bad_mmsi():
    assert anomaly_in_pending([{"mmsi": None}, {"mmsi": "x"}], 1) is None


def test_within_slo():
    assert within_slo(9.9, 10.0)
    assert not within_slo(10.1, 10.0)


def test_athena_count_query():
    assert athena_count_query("hm", "ais_raw") == 'SELECT count(*) AS n FROM "hm"."ais_raw"'


def test_reconciles_with_gate_drops():
    assert reconciles(2709, 2700, gate_dropped=9)
    assert not reconciles(2709, 2709, gate_dropped=9)

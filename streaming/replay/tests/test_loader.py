"""Fixture loader + checksum tests (Phase 1.1, gate G1)."""

from __future__ import annotations

import pytest

from replay.loader import (
    AisRecord,
    load_expectations,
    load_fixture,
    recorded_sha256,
    sha256_of,
    verify_fixture,
)


def test_fixture_parses_and_schema_validates_every_record():
    records = load_fixture()
    assert len(records) > 2_000
    assert all(isinstance(r, AisRecord) for r in records)


def test_record_count_matches_expectations():
    records = load_fixture()
    exp = load_expectations()
    assert len(records) == exp["n_records"]


def test_recorded_sha256_matches():
    assert sha256_of() == recorded_sha256()
    assert load_expectations()["sha256"] == recorded_sha256()
    assert verify_fixture() is True


def test_planted_anomaly_mmsis_present_in_fixture():
    mmsis = {r.mmsi for r in load_fixture()}
    for a in load_expectations()["anomalies"]:
        assert a["mmsi"] in mmsis


def test_malformed_line_raises_with_line_number(tmp_path):
    bad = tmp_path / "bad.jsonl"
    bad.write_text(
        '{"mmsi":1,"lat":40.0,"lon":-73.0,"t":"2024-06-01T00:00:00Z"}\n'
        '{"mmsi":2,"lat":999.0,"lon":-73.0,"t":"2024-06-01T00:01:00Z"}\n'  # lat out of range
    )
    with pytest.raises(ValueError, match="line 2"):
        load_fixture(bad)


def test_fixture_mmsis_use_the_unallocated_mid_200():
    # MID 200 is not allocated to any country in the ITU MID table, so the
    # fixture fleet is fictional. See the provenance note in expectations.json.
    mmsis = {r.mmsi for r in load_fixture()}
    assert mmsis
    assert all(len(str(m)) == 9 and str(m).startswith("200") for m in mmsis)
    assert "MID 200" in load_expectations()["provenance"]

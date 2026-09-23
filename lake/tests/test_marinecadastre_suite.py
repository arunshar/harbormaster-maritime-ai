from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from lake.quality.marinecadastre_suite import (
    MMSI_MAX,
    MMSI_MIN,
    expectation_config,
    expectation_config_sha256,
    validate_marinecadastre_batch,
)

EXPECTATIONS = Path(__file__).parent.parent / "fixtures" / "expectations.json"

GOOD_ROWS = [
    {
        "mmsi": 200000001,
        "t": "2024-06-01T00:00:00Z",
        "lat": 40.40,
        "lon": -74.09,
        "sog": 9.7,
        "cog": 49.6,
    },
    {
        "mmsi": 200000001,
        "t": "2024-06-01T00:01:00Z",
        "lat": 40.41,
        "lon": -74.08,
        "sog": 9.7,
        "cog": 49.6,
    },
    {
        "mmsi": 200000002,
        "t": "2024-06-01T00:00:30Z",
        "lat": 40.30,
        "lon": -74.10,
        "sog": 8.2,
        "cog": 12.0,
    },
]


def _df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_good_fixture_passes():
    result = validate_marinecadastre_batch(_df(GOOD_ROWS), min_rows=1)
    assert result.passed, result.failures
    assert result.row_count == 3


def test_missing_column_fails():
    rows = [{k: v for k, v in r.items() if k != "mmsi"} for r in GOOD_ROWS]
    result = validate_marinecadastre_batch(_df(rows), min_rows=1)
    assert not result.passed
    assert any(f.name == "column_exists" and "mmsi" in f.detail for f in result.failures)


def test_out_of_range_lat_fails():
    rows = [dict(GOOD_ROWS[0], lat=91.0)] + GOOD_ROWS[1:]
    result = validate_marinecadastre_batch(_df(rows), min_rows=1)
    assert not result.passed
    assert any(f.name == "lat_range" for f in result.failures)


def test_out_of_range_lon_fails():
    rows = [dict(GOOD_ROWS[0], lon=200.0)] + GOOD_ROWS[1:]
    result = validate_marinecadastre_batch(_df(rows), min_rows=1)
    assert not result.passed
    assert any(f.name == "lon_range" for f in result.failures)


def test_out_of_range_mmsi_fails():
    rows = [dict(GOOD_ROWS[0], mmsi=42)] + GOOD_ROWS[1:]
    result = validate_marinecadastre_batch(_df(rows), min_rows=1)
    assert not result.passed
    assert any(f.name == "mmsi_range" for f in result.failures)


def test_null_mmsi_fails():
    # GE's between-expectation excludes nulls by default (vacuously true), so
    # a null mmsi is caught specifically by the not-null expectation, not by
    # the range check.
    rows = [dict(GOOD_ROWS[0], mmsi=None)] + GOOD_ROWS[1:]
    result = validate_marinecadastre_batch(_df(rows), min_rows=1)
    assert not result.passed
    assert any(f.name == "mmsi_not_null" for f in result.failures)


def test_non_monotonic_timestamp_within_mmsi_fails():
    rows = [
        {
            "mmsi": 200000001,
            "t": "2024-06-01T00:01:00Z",
            "lat": 40.41,
            "lon": -74.08,
            "sog": 9.7,
            "cog": 49.6,
        },
        {
            "mmsi": 200000001,
            "t": "2024-06-01T00:00:00Z",
            "lat": 40.40,
            "lon": -74.09,
            "sog": 9.7,
            "cog": 49.6,
        },
    ]
    result = validate_marinecadastre_batch(_df(rows), min_rows=1)
    assert not result.passed
    assert any(f.name == "per_mmsi_timestamp_monotonic" for f in result.failures)


def test_unparseable_timestamp_fails():
    rows = [dict(GOOD_ROWS[0], t="not-a-timestamp")] + GOOD_ROWS[1:]
    result = validate_marinecadastre_batch(_df(rows), min_rows=1)
    assert not result.passed
    assert any(f.name == "per_mmsi_timestamp_monotonic" for f in result.failures)


def test_empty_batch_fails_row_count_floor():
    result = validate_marinecadastre_batch(_df([]), min_rows=1)
    assert not result.passed
    assert any(f.name == "row_count_floor" for f in result.failures)
    assert result.row_count == 0


def test_row_count_floor_is_configurable():
    result = validate_marinecadastre_batch(_df(GOOD_ROWS), min_rows=10)
    assert not result.passed
    assert any(f.name == "row_count_floor" for f in result.failures)


@pytest.mark.parametrize("bad_mmsi", [MMSI_MIN - 1, MMSI_MAX + 1])
def test_mmsi_range_boundaries(bad_mmsi):
    rows = [dict(GOOD_ROWS[0], mmsi=bad_mmsi)] + GOOD_ROWS[1:]
    result = validate_marinecadastre_batch(_df(rows), min_rows=1)
    assert not result.passed
    assert any(f.name == "mmsi_range" for f in result.failures)


def test_mmsi_range_inclusive_boundaries_pass():
    rows = [dict(GOOD_ROWS[0], mmsi=MMSI_MIN)] + [dict(GOOD_ROWS[1], mmsi=MMSI_MAX)]
    result = validate_marinecadastre_batch(_df(rows), min_rows=1)
    assert result.passed, result.failures


def test_expectation_config_is_deterministic():
    assert expectation_config(min_rows=1) == expectation_config(min_rows=1)
    assert expectation_config_sha256(min_rows=1) == expectation_config_sha256(min_rows=1)


def test_expectation_config_changes_with_min_rows():
    assert expectation_config(min_rows=1) != expectation_config(min_rows=100)


def test_expectation_config_matches_the_pinned_expectation():
    pinned = json.loads(EXPECTATIONS.read_text())["marinecadastre_suite"]
    assert (
        expectation_config_sha256(min_rows=1) == pinned["expectation_config_sha256_min_rows_1"]
    ), (
        "the suite's expectation config changed; if intentional, update "
        "lake/fixtures/expectations.json AND docs/phases/PHASE_3.md in the same commit"
    )
    assert len(expectation_config(min_rows=1)) == pinned["expectation_count"]


def test_smoke_fixture_is_the_first_ten_replay_rows():
    # The provenance note says the smoke fixture is derived, so check the
    # derivation. A regenerated replay fixture must be copied here too.
    repo = Path(__file__).resolve().parents[2]
    sample = (repo / "lake" / "fixtures" / "marinecadastre_sample.jsonl").read_text()
    replay = (repo / "streaming" / "fixtures" / "ais_recorded.jsonl").read_text()
    assert sample.splitlines() == replay.splitlines()[:10]
    assert "MID 200" in json.loads(EXPECTATIONS.read_text())["smoke_fixture"]["provenance"]

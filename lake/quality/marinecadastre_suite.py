"""MarineCadastre raw-extract data-quality gate (Phase 3, gate 3.1).

Runs as the first Spark step of the EMR backfill (gate 3.2): a failing suite
halts the job before anything reaches Iceberg, so bad data blocks training
rather than landing silently. The checks are deliberately pandas-native (a
real `great_expectations` PandasDataset, no DataContext/Checkpoint project
scaffolding) so the unit suite needs no Spark and no AWS; the EMR job calls
the same pure function against a pandas-converted micro-batch or a bounded
sample of the raw extract.

Field names match the existing AIS fixture convention (streaming/fixtures/
ais_recorded.jsonl): mmsi, t, lat, lon, sog, cog.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

REQUIRED_COLUMNS: tuple[str, ...] = ("mmsi", "t", "lat", "lon", "sog", "cog")

# MMSI is a 9-digit maritime identifier; the full valid range spans MID-coded
# blocks, but a demo-scale batch check only needs the outer bound, not per-MID
# validation (that belongs to a real registry lookup, out of scope here).
MMSI_MIN = 100_000_000
MMSI_MAX = 999_999_999


@dataclass(frozen=True)
class ExpectationFailure:
    name: str
    detail: str


@dataclass(frozen=True)
class SuiteResult:
    passed: bool
    row_count: int
    failures: list[ExpectationFailure] = field(default_factory=list)


def expectation_config(*, min_rows: int) -> list[dict[str, Any]]:
    """The deterministic, checksum-able list of expectations this suite runs.

    A plain list of dicts rather than a live GE object, so it can be
    JSON-serialized and SHA256-pinned without depending on great_expectations'
    own (evolving) suite-serialization format.
    """
    config: list[dict[str, Any]] = [
        {"expectation_type": "expect_column_to_exist", "kwargs": {"column": c}}
        for c in REQUIRED_COLUMNS
    ]
    config.append(
        {
            "expectation_type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": min_rows},
        }
    )
    config.extend(
        [
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "lat", "min_value": -90, "max_value": 90},
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "lon", "min_value": -180, "max_value": 180},
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "mmsi", "min_value": MMSI_MIN, "max_value": MMSI_MAX},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "mmsi"},
            },
            {
                "expectation_type": "custom_per_mmsi_timestamp_monotonic",
                "kwargs": {"timestamp_column": "t", "group_by": "mmsi"},
            },
        ]
    )
    return config


def expectation_config_sha256(*, min_rows: int) -> str:
    payload = json.dumps(expectation_config(min_rows=min_rows), sort_keys=True).encode()
    return hashlib.sha256(payload).hexdigest()


def _check_per_mmsi_timestamp_monotonic(
    df: pd.DataFrame, *, mmsi_col: str = "mmsi", ts_col: str = "t"
) -> ExpectationFailure | None:
    """Per-vessel timestamps must be non-decreasing within the batch.

    Not a great_expectations builtin (GE has no native per-group ordering
    check), so this is plain pandas: group by MMSI, parse timestamps, and
    assert no negative diff. Unparseable timestamps count as a violation
    rather than being silently dropped.
    """
    ts = pd.to_datetime(df[ts_col], utc=True, errors="coerce", format="ISO8601")
    bad_mmsis: list[Any] = []
    for mmsi, idx in df.groupby(mmsi_col, sort=False).groups.items():
        group_ts = ts.loc[idx]
        if group_ts.isna().any() or (group_ts.diff().dropna() < pd.Timedelta(0)).any():
            bad_mmsis.append(mmsi)
    if bad_mmsis:
        return ExpectationFailure(
            name="per_mmsi_timestamp_monotonic",
            detail=f"non-monotonic or unparseable '{ts_col}' for mmsi(s): {sorted(set(bad_mmsis))}",
        )
    return None


def validate_marinecadastre_batch(df: pd.DataFrame, *, min_rows: int = 1) -> SuiteResult:
    """Run the full suite against a pandas batch. Pure: no I/O, no network.

    Column-existence failures short-circuit the corresponding value checks
    (a missing column would otherwise raise a confusing KeyError instead of
    reporting the real problem: the column is missing).
    """
    from great_expectations.dataset import PandasDataset

    failures: list[ExpectationFailure] = []
    row_count = len(df)

    missing = {c for c in REQUIRED_COLUMNS if c not in df.columns}
    for col in missing:
        failures.append(ExpectationFailure("column_exists", f"missing required column: {col}"))

    if row_count < min_rows:
        failures.append(
            ExpectationFailure("row_count_floor", f"{row_count} rows below the floor of {min_rows}")
        )

    if row_count > 0 and not missing:
        ds = PandasDataset(df)

        r = ds.expect_column_values_to_be_between(
            "lat", min_value=-90, max_value=90, result_format="SUMMARY"
        )
        if not r.success:
            failures.append(
                ExpectationFailure(
                    "lat_range", f"{r.result['unexpected_count']} rows outside [-90, 90]"
                )
            )

        r = ds.expect_column_values_to_be_between(
            "lon", min_value=-180, max_value=180, result_format="SUMMARY"
        )
        if not r.success:
            failures.append(
                ExpectationFailure(
                    "lon_range", f"{r.result['unexpected_count']} rows outside [-180, 180]"
                )
            )

        r = ds.expect_column_values_to_be_between(
            "mmsi", min_value=MMSI_MIN, max_value=MMSI_MAX, result_format="SUMMARY"
        )
        if not r.success:
            failures.append(
                ExpectationFailure(
                    "mmsi_range", f"{r.result['unexpected_count']} rows outside the MMSI range"
                )
            )

        r = ds.expect_column_values_to_not_be_null("mmsi", result_format="SUMMARY")
        if not r.success:
            failures.append(
                ExpectationFailure(
                    "mmsi_not_null", f"{r.result['unexpected_count']} null mmsi values"
                )
            )

        mono_failure = _check_per_mmsi_timestamp_monotonic(df)
        if mono_failure:
            failures.append(mono_failure)

    return SuiteResult(passed=not failures, row_count=row_count, failures=failures)

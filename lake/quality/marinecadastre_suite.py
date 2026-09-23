"""MarineCadastre raw-extract data-quality gate (Phase 3, gate 3.1).

Runs as the first Spark step of the EMR backfill (gate 3.2). A failing suite
halts the job before anything reaches Iceberg, so bad data blocks training
instead of landing silently. The checks build a fresh, in-memory
great_expectations ephemeral context for each call, and they never touch a
project directory, a Checkpoint, or any file on disk. The unit suite needs no
Spark and no AWS because of this, and the EMR job calls the same pure
function against a pandas-converted micro-batch or a bounded sample of the
raw extract.

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

    Column-existence failures short-circuit the corresponding value checks.
    A missing column would otherwise raise a confusing KeyError instead of
    reporting the real problem, which is that the column is missing.
    """
    import great_expectations as gx
    from great_expectations.core import ExpectationSuite
    from great_expectations.expectations import (
        ExpectColumnValuesToBeBetween,
        ExpectColumnValuesToNotBeNull,
    )

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
        # An ephemeral context lives only in memory. It writes no project
        # directory, no Checkpoint, and no file to disk, and a fresh one for
        # each call means no state or name ever carries over between calls.
        ctx = gx.get_context(mode="ephemeral")
        # great_expectations 1.x resolves the metric graph with a tqdm
        # progress bar on by default, and it passes disable=False on its own,
        # so TQDM_DISABLE has no effect. This module runs as the first Spark
        # step of the EMR backfill on every micro-batch, so an unsuppressed
        # bar would add real log volume to production CloudWatch output.
        # Turning it off here keeps stderr silent, verified empirically: a
        # single-row batch produces 0 bytes of stderr with this line in
        # place, versus about 3.8 KB without it.
        ctx.variables.progress_bars = {"globally": False}
        data_source = ctx.data_sources.add_pandas(name="marinecadastre")
        asset = data_source.add_dataframe_asset(name="batch")
        batch_definition = asset.add_batch_definition_whole_dataframe("whole_batch")
        batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

        # One suite with all four expectations, validated in a single call.
        # Each separate batch.validate() call used to make great_expectations
        # resolve the metric graph from scratch, so four calls meant four
        # resolutions against the same batch on every micro-batch gate run.
        # A null value is vacuously true for expect_column_values_to_be_between
        # in this version of great_expectations, confirmed by running it
        # directly against a batch with a null mmsi: the between check reports
        # success and the not-null check catches the null, the same split the
        # tests require.
        suite = ExpectationSuite(name="marinecadastre_batch")
        suite.add_expectation(
            ExpectColumnValuesToBeBetween(column="lat", min_value=-90, max_value=90)
        )
        suite.add_expectation(
            ExpectColumnValuesToBeBetween(column="lon", min_value=-180, max_value=180)
        )
        suite.add_expectation(
            ExpectColumnValuesToBeBetween(column="mmsi", min_value=MMSI_MIN, max_value=MMSI_MAX)
        )
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="mmsi"))

        suite_result = batch.validate(suite, result_format="SUMMARY")

        range_checks = {
            "lat": ("lat_range", "[-90, 90]"),
            "lon": ("lon_range", "[-180, 180]"),
            "mmsi": ("mmsi_range", "the MMSI range"),
        }
        for expectation_result in suite_result.results:
            if expectation_result.success:
                continue
            exp_type = expectation_result.expectation_config.type
            column = expectation_result.expectation_config.kwargs.get("column")
            unexpected = expectation_result.result["unexpected_count"]
            if exp_type == "expect_column_values_to_not_be_null":
                failures.append(
                    ExpectationFailure("mmsi_not_null", f"{unexpected} null mmsi values")
                )
            else:
                name, range_desc = range_checks[column]
                failures.append(ExpectationFailure(name, f"{unexpected} rows outside {range_desc}"))

        mono_failure = _check_per_mmsi_timestamp_monotonic(df)
        if mono_failure:
            failures.append(mono_failure)

    return SuiteResult(passed=not failures, row_count=row_count, failures=failures)

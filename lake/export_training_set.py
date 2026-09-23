"""Point-in-time-correct training-set export (Phase 3, gate 3.3).

The locally-testable half of the Feast/Athena story: `lake/offline_store.py`
declares the real Feast AthenaOfflineStore config for the AWS showcase (not
exercised here, Athena needs live AWS), but the actual point-in-time-join
correctness property lives here as a pure function, independent of Feast's
own join implementation, so it is unit-testable with zero AWS and zero Feast.

The join itself is `pandas.merge_asof(direction="backward")`, the standard
tool for "as of" joins: for each entity request at time t, attach the most
recent feature row at or before t (never a row from t' > t, the classic
training-serving-skew source this gate's unit tests guard against).
"""

from __future__ import annotations

import hashlib
import json

import pandas as pd


def point_in_time_join(
    entity_requests: pd.DataFrame,
    feature_events: pd.DataFrame,
    *,
    entity_col: str = "mmsi",
    request_ts_col: str = "event_timestamp",
    feature_ts_col: str = "t",
) -> pd.DataFrame:
    """For each (entity_col, request_ts_col) row, attach the feature_events
    columns from the most recent row of the same entity with
    feature_ts_col <= request_ts_col. An entity request before any feature
    history for that entity gets nulls (correctly: there is nothing to know
    yet), never a future row's values.
    """
    left = entity_requests.sort_values(request_ts_col).reset_index(drop=True)
    right = feature_events.sort_values(feature_ts_col).reset_index(drop=True)
    return pd.merge_asof(
        left,
        right,
        left_on=request_ts_col,
        right_on=feature_ts_col,
        by=entity_col,
        direction="backward",
    )


def data_fingerprint(df: pd.DataFrame, *, timestamp_col: str = "event_timestamp") -> str:
    """SHA256 over the canonical (sorted, JSON-serialized) row content plus
    row count and time range, so an identical export is byte-reproducible and
    any real change (rows, values, or coverage window) changes the hash."""
    ordered = df.sort_values(list(df.columns)).reset_index(drop=True)
    rows = json.loads(ordered.to_json(orient="records", date_format="iso"))
    time_range = (
        [str(df[timestamp_col].min()), str(df[timestamp_col].max())]
        if len(df) and timestamp_col in df.columns
        else [None, None]
    )
    payload = {"rows": rows, "row_count": len(df), "time_range": time_range}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def export_training_set(
    entity_requests: pd.DataFrame,
    feature_events: pd.DataFrame,
    *,
    entity_col: str = "mmsi",
    request_ts_col: str = "event_timestamp",
    feature_ts_col: str = "t",
) -> tuple[pd.DataFrame, str]:
    """point_in_time_join + data_fingerprint in one call: the training-set
    export the training cluster pulls, plus the fingerprint recorded in its checkpoint
    manifest (gate 3.4) as the data-lineage anchor."""
    exported = point_in_time_join(
        entity_requests,
        feature_events,
        entity_col=entity_col,
        request_ts_col=request_ts_col,
        feature_ts_col=feature_ts_col,
    )
    fingerprint = data_fingerprint(exported, timestamp_col=request_ts_col)
    return exported, fingerprint

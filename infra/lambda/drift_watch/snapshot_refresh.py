"""Validated, provenance-stamped AIS snapshot refresh for the drift watch.

The scheduled drift Lambda reads a bounded, current window from the existing
MarineCadastre raw prefix.  This module deliberately owns only the refresh
transaction:

1. inspect newest raw Parquet candidates first;
2. reject a candidate unless it matches the reference schema and its numeric
   drift features are finite;
3. publish the validated bounded window to a content-addressed immutable key;
4. publish its provenance record; and only then
5. atomically advance ``drift/current.parquet`` by copying that immutable
   object.

It never writes ``drift/reference.parquet``.  Terraform grants the Lambda
write permission only for the current pointer and ``drift/windows/``.
"""

from __future__ import annotations

import hashlib
import io
import json
from dataclasses import dataclass
from datetime import UTC
from typing import Any

import numpy as np
import pandas as pd

PRODUCER_NAME = "harbormaster.phase4.ais_snapshot_refresh"
PROVENANCE_SCHEMA_VERSION = 1


class SnapshotRefreshError(RuntimeError):
    """A raw candidate or publication state cannot safely advance current."""


@dataclass(frozen=True)
class SnapshotRefreshResult:
    """The successful bounded-window publication state returned to handler."""

    reference: pd.DataFrame
    current: pd.DataFrame
    source_key: str
    source_version_id: str | None
    window_key: str
    provenance_key: str
    window_sha256: str
    window_rows: int
    rejected_candidates: tuple[dict[str, str], ...]
    current_advanced: bool


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_json(payload: dict[str, Any]) -> bytes:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return (serialized + "\n").encode("ascii")


def _is_not_found(error: BaseException) -> bool:
    response = getattr(error, "response", None)
    code = str((response or {}).get("Error", {}).get("Code", ""))
    return code in {"404", "NoSuchKey", "NotFound"} or error.__class__.__name__ in {
        "NoSuchKey",
        "NotFound",
    }


def _is_precondition_conflict(error: BaseException) -> bool:
    response = getattr(error, "response", None)
    code = str((response or {}).get("Error", {}).get("Code", ""))
    return code in {"412", "PreconditionFailed", "ConditionalRequestConflict"}


def _head_or_none(s3_client, *, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        return s3_client.head_object(Bucket=bucket, Key=key)
    except Exception as error:  # S3 client errors vary between Lambda and fakes.
        if _is_not_found(error):
            return None
        raise


def _read_parquet_object(
    s3_client, *, bucket: str, key: str
) -> tuple[pd.DataFrame, dict[str, Any], bytes]:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    return pd.read_parquet(io.BytesIO(body)), response, body


def _list_parquet_candidates(s3_client, *, bucket: str, prefix: str) -> list[dict[str, Any]]:
    """List only Parquet source candidates, newest first, without recursion assumptions."""
    if hasattr(s3_client, "get_paginator"):
        pages = s3_client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    else:  # Simple fakes used by unit tests retain the same response shape.
        pages = [s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)]
    candidates = [
        item
        for page in pages
        for item in page.get("Contents", [])
        if str(item.get("Key", "")).endswith(".parquet")
    ]
    return sorted(
        candidates,
        key=lambda item: (item.get("LastModified"), item["Key"]),
        reverse=True,
    )


def _reference_contract(reference: pd.DataFrame) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if reference.empty:
        raise SnapshotRefreshError("reference snapshot is empty")
    columns = tuple(str(column) for column in reference.columns)
    if "t" not in columns:
        raise SnapshotRefreshError("reference schema has no timestamp column 't'")
    numeric = tuple(
        str(column)
        for column in reference.columns
        if pd.api.types.is_numeric_dtype(reference[column])
    )
    if not numeric:
        raise SnapshotRefreshError("reference schema has no numeric drift features")
    return columns, numeric


def _validated_window(
    candidate: pd.DataFrame,
    *,
    expected_columns: tuple[str, ...],
    numeric_columns: tuple[str, ...],
    min_rows: int,
    max_rows: int,
) -> pd.DataFrame:
    """Return a stable bounded time-ordered window or fail before publication."""
    if tuple(str(column) for column in candidate.columns) != expected_columns:
        raise SnapshotRefreshError(
            "schema mismatch: expected columns "
            f"{list(expected_columns)!r}, got {list(candidate.columns)!r}"
        )
    if len(candidate) < min_rows:
        raise SnapshotRefreshError(f"row count {len(candidate)} is below minimum {min_rows}")
    if max_rows < min_rows or max_rows < 1:
        raise SnapshotRefreshError("window row bounds are invalid")

    for column in numeric_columns:
        series = candidate[column]
        if not pd.api.types.is_numeric_dtype(series):
            raise SnapshotRefreshError(f"numeric feature '{column}' is not numeric")
        values = series.to_numpy(dtype=float, na_value=np.nan)
        if not np.isfinite(values).all():
            raise SnapshotRefreshError(
                f"numeric feature '{column}' contains null or non-finite values"
            )

    timestamps = pd.to_datetime(candidate["t"], utc=True, errors="coerce")
    if timestamps.isna().any():
        raise SnapshotRefreshError("timestamp column 't' contains an unparseable value")

    ordered = candidate.copy()
    ordered["t"] = timestamps
    # mmsi exists in the expected AIS schema, but timestamp alone is enough
    # for a stable source that does not include it.
    sort_columns = ["t"] + (["mmsi"] if "mmsi" in ordered.columns else [])
    return ordered.sort_values(sort_columns, kind="mergesort").tail(max_rows).reset_index(drop=True)


def _put_immutable(
    s3_client,
    *,
    bucket: str,
    key: str,
    body: bytes,
    digest: str,
    content_type: str,
    kind: str,
) -> None:
    existing = _head_or_none(s3_client, bucket=bucket, key=key)
    if existing is not None:
        metadata = {
            str(name).lower(): str(value) for name, value in existing.get("Metadata", {}).items()
        }
        if existing.get("ContentLength") != len(body) or metadata.get("sha256") != digest:
            raise SnapshotRefreshError(f"immutable key already exists with different bytes: {key}")
        return
    try:
        # The precondition closes the check-then-write race. On an overlap the
        # winning content-addressed payload is re-read and must be exact.
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
            Metadata={"sha256": digest, "producer": PRODUCER_NAME, "kind": kind},
            IfNoneMatch="*",
        )
    except Exception as error:
        if not _is_precondition_conflict(error):
            raise
        existing = _head_or_none(s3_client, bucket=bucket, key=key)
        if existing is None:
            raise SnapshotRefreshError(
                f"immutable write conflicted but key is absent: {key}"
            ) from error
        metadata = {
            str(name).lower(): str(value) for name, value in existing.get("Metadata", {}).items()
        }
        if existing.get("ContentLength") != len(body) or metadata.get("sha256") != digest:
            raise SnapshotRefreshError(
                f"immutable key raced with different bytes: {key}"
            ) from error


def _current_matches_window(s3_client, *, bucket: str, key: str, digest: str, size: int) -> bool:
    current = _head_or_none(s3_client, bucket=bucket, key=key)
    if current is None:
        return False
    metadata = {
        str(name).lower(): str(value) for name, value in current.get("Metadata", {}).items()
    }
    return current.get("ContentLength") == size and metadata.get("sha256") == digest


def refresh_current_snapshot(
    s3_client,
    *,
    bucket: str,
    reference_key: str,
    current_key: str,
    source_prefix: str,
    window_prefix: str,
    min_rows: int,
    max_rows: int,
) -> SnapshotRefreshResult:
    """Validate and publish the newest acceptable bounded AIS source window.

    The current pointer is changed only after the exact immutable Parquet
    window and its immutable JSON provenance have both been present or
    successfully published.  This function contains no reference write path.
    """
    if reference_key == current_key:
        raise SnapshotRefreshError("reference and current snapshot keys must differ")
    if not source_prefix.endswith("/") or not window_prefix.endswith("/"):
        raise SnapshotRefreshError("source and window prefixes must end with '/'")
    if not source_prefix or not window_prefix:
        raise SnapshotRefreshError("source and window prefixes are required")

    reference, _, _ = _read_parquet_object(s3_client, bucket=bucket, key=reference_key)
    expected_columns, numeric_columns = _reference_contract(reference)
    rejected: list[dict[str, str]] = []
    chosen: tuple[dict[str, Any], dict[str, Any], bytes, pd.DataFrame] | None = None

    for candidate in _list_parquet_candidates(s3_client, bucket=bucket, prefix=source_prefix):
        key = str(candidate["Key"])
        try:
            frame, object_response, body = _read_parquet_object(s3_client, bucket=bucket, key=key)
            window = _validated_window(
                frame,
                expected_columns=expected_columns,
                numeric_columns=numeric_columns,
                min_rows=min_rows,
                max_rows=max_rows,
            )
            chosen = candidate, object_response, body, window
            break
        except (SnapshotRefreshError, ValueError, OSError, TypeError) as error:
            rejected.append({"key": key, "reason": str(error)[:240]})

    if chosen is None:
        raise SnapshotRefreshError(
            "no valid Parquet source candidate under "
            f"{source_prefix!r}; rejected={json.dumps(rejected, sort_keys=True)}"
        )

    candidate, source_response, source_bytes, window = chosen
    window_buffer = io.BytesIO()
    window.to_parquet(window_buffer, index=False)
    window_bytes = window_buffer.getvalue()
    window_sha256 = _sha256(window_bytes)
    window_key = f"{window_prefix}{window_sha256}.parquet"
    provenance_key = f"{window_prefix}{window_sha256}.provenance.json"

    source_version_id = source_response.get("VersionId") or candidate.get("VersionId")
    source_etag = source_response.get("ETag") or candidate.get("ETag")
    provenance = {
        "producer": PRODUCER_NAME,
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        # S3 records immutable publication time in the object metadata.  The
        # payload deliberately contains only source-derived fields, so the
        # same validated source/version produces the same immutable record on
        # an idempotent retry rather than a conflicting second provenance body.
        "source": {
            "bucket": bucket,
            "key": candidate["Key"],
            "version_id": source_version_id,
            "etag": source_etag,
            "last_modified": candidate.get("LastModified")
            .astimezone(UTC)
            .isoformat()
            .replace("+00:00", "Z"),
            "size": len(source_bytes),
            "sha256": _sha256(source_bytes),
        },
        "window": {
            "key": window_key,
            "sha256": window_sha256,
            "size": len(window_bytes),
            "rows": len(window),
            "time_start": window["t"].iloc[0].isoformat(),
            "time_end": window["t"].iloc[-1].isoformat(),
            "columns": list(expected_columns),
            "numeric_features": list(numeric_columns),
            "dtypes": {str(column): str(window[column].dtype) for column in window.columns},
        },
        "rejected_newer_candidates": rejected,
    }
    provenance_bytes = _canonical_json(provenance)
    provenance_sha256 = _sha256(provenance_bytes)

    _put_immutable(
        s3_client,
        bucket=bucket,
        key=window_key,
        body=window_bytes,
        digest=window_sha256,
        content_type="application/vnd.apache.parquet",
        kind="validated-ais-window",
    )
    _put_immutable(
        s3_client,
        bucket=bucket,
        key=provenance_key,
        body=provenance_bytes,
        digest=provenance_sha256,
        content_type="application/json",
        kind="validated-ais-window-provenance",
    )

    advanced = not _current_matches_window(
        s3_client, bucket=bucket, key=current_key, digest=window_sha256, size=len(window_bytes)
    )
    if advanced:
        # S3's per-key replacement is atomic. The copy source is the immutable
        # content-addressed object that was validated and provenance-stamped.
        s3_client.copy_object(
            Bucket=bucket,
            Key=current_key,
            CopySource={"Bucket": bucket, "Key": window_key},
            MetadataDirective="COPY",
        )

    return SnapshotRefreshResult(
        reference=reference,
        current=window,
        source_key=str(candidate["Key"]),
        source_version_id=source_version_id,
        window_key=window_key,
        provenance_key=provenance_key,
        window_sha256=window_sha256,
        window_rows=len(window),
        rejected_candidates=tuple(rejected),
        current_advanced=advanced,
    )

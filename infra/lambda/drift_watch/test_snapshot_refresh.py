"""Focused unit coverage for the drift-watch snapshot refresh transaction.

These tests use an in-memory S3-shaped fake.  They exercise no AWS client and
prove the ordering that matters for the live role: validated immutable window,
then immutable provenance, then the mutable current pointer.  The reference
object is never a write target.
"""

from __future__ import annotations

import io
import os
import sys
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from snapshot_refresh import SnapshotRefreshError, refresh_current_snapshot  # noqa: E402


class _NoSuchKey(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class _Body:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def read(self) -> bytes:
        return self.body


class _FakePaginator:
    def __init__(self, client: _FakeS3) -> None:
        self.client = client

    def paginate(self, **kwargs):
        return [self.client.list_objects_v2(**kwargs)]


class _FakeS3:
    def __init__(self, *, entries: dict[str, dict], raw_candidates: list[dict]) -> None:
        self.entries = entries
        self.raw_candidates = raw_candidates
        self.operations: list[tuple[str, str]] = []

    def get_paginator(self, operation: str) -> _FakePaginator:
        assert operation == "list_objects_v2"
        return _FakePaginator(self)

    def list_objects_v2(self, *, Bucket: str, Prefix: str) -> dict:
        items = [item for item in self.raw_candidates if item["Key"].startswith(Prefix)]
        return {"Contents": items}

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        entry = self.entries[Key]
        return {
            "Body": _Body(entry["Body"]),
            "VersionId": entry.get("VersionId"),
            "ETag": entry.get("ETag"),
        }

    def head_object(self, *, Bucket: str, Key: str) -> dict:
        if Key not in self.entries:
            raise _NoSuchKey()
        entry = self.entries[Key]
        return {"ContentLength": len(entry["Body"]), "Metadata": dict(entry.get("Metadata", {}))}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, Metadata: dict, **kwargs) -> None:
        self.operations.append(("put", Key))
        self.entries[Key] = {"Body": Body, "Metadata": dict(Metadata)}

    def copy_object(self, *, Bucket: str, Key: str, CopySource: dict, **kwargs) -> None:
        self.operations.append(("copy", Key))
        source = self.entries[CopySource["Key"]]
        self.entries[Key] = {"Body": source["Body"], "Metadata": dict(source["Metadata"])}


def _parquet(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    df.to_parquet(output, index=False)
    return output.getvalue()


def _ais_frame(*, rows: int = 12, lat_offset: float = 0.0) -> pd.DataFrame:
    start = pd.Timestamp("2026-08-14T00:00:00Z")
    return pd.DataFrame(
        {
            "cog": [float(index * 3) for index in range(rows)],
            "heading": [float((index * 7) % 360) for index in range(rows)],
            "lat": [44.0 + lat_offset + index / 100 for index in range(rows)],
            "lon": [-93.0 - index / 100 for index in range(rows)],
            "mmsi": [200000001 + index % 2 for index in range(rows)],
            "sog": [4.0 + index / 10 for index in range(rows)],
            "t": [(start + pd.Timedelta(minutes=index)).isoformat() for index in range(rows)],
        }
    )


def _candidate(key: str, modified: datetime, *, version: str) -> dict:
    return {"Key": key, "LastModified": modified, "ETag": f'"{version}"', "VersionId": version}


def _client_with_candidates(*, valid: pd.DataFrame, invalid: pd.DataFrame | None = None) -> _FakeS3:
    now = datetime(2026, 8, 14, tzinfo=UTC)
    entries = {
        "drift/reference.parquet": {"Body": _parquet(_ais_frame()), "VersionId": "reference-v1"},
        "drift/current.parquet": {"Body": b"previous-current"},
        "raw/marinecadastre/valid.parquet": {"Body": _parquet(valid), "VersionId": "valid-v1"},
    }
    candidates = [_candidate("raw/marinecadastre/valid.parquet", now, version="valid-v1")]
    if invalid is not None:
        entries["raw/marinecadastre/newer-invalid.parquet"] = {
            "Body": _parquet(invalid),
            "VersionId": "invalid-v1",
        }
        candidates.append(
            _candidate(
                "raw/marinecadastre/newer-invalid.parquet",
                now + timedelta(minutes=1),
                version="invalid-v1",
            )
        )
    return _FakeS3(entries=entries, raw_candidates=candidates)


def _refresh(s3: _FakeS3, *, max_rows: int = 5):
    return refresh_current_snapshot(
        s3,
        bucket="hm-lake",
        reference_key="drift/reference.parquet",
        current_key="drift/current.parquet",
        source_prefix="raw/marinecadastre/",
        window_prefix="drift/windows/",
        min_rows=1,
        max_rows=max_rows,
    )


def test_refresh_rejects_newest_invalid_candidate_and_promotes_bounded_valid_window():
    invalid = _ais_frame().drop(columns=["lat"])
    s3 = _client_with_candidates(valid=_ais_frame(lat_offset=0.02), invalid=invalid)

    result = _refresh(s3)

    assert result.source_key == "raw/marinecadastre/valid.parquet"
    assert result.window_rows == 5
    assert result.current_advanced is True
    assert [operation for operation, _ in s3.operations] == ["put", "put", "copy"]
    assert s3.operations[0][1] == result.window_key
    assert s3.operations[1][1] == result.provenance_key
    assert s3.operations[2] == ("copy", "drift/current.parquet")
    assert all(key != "drift/reference.parquet" for _, key in s3.operations)
    assert s3.entries["drift/current.parquet"]["Body"] == s3.entries[result.window_key]["Body"]
    assert result.rejected_candidates[0]["key"] == "raw/marinecadastre/newer-invalid.parquet"


def test_refresh_does_not_advance_current_when_every_candidate_is_invalid():
    s3 = _client_with_candidates(valid=_ais_frame().assign(sog=float("nan")))
    previous = s3.entries["drift/current.parquet"]["Body"]

    with pytest.raises(SnapshotRefreshError, match="no valid Parquet source candidate"):
        _refresh(s3)

    assert s3.entries["drift/current.parquet"]["Body"] == previous
    assert s3.operations == []


def test_refresh_is_idempotent_for_the_same_validated_window():
    s3 = _client_with_candidates(valid=_ais_frame())

    first = _refresh(s3)
    second = _refresh(s3)

    assert first.window_key == second.window_key
    assert second.current_advanced is False
    assert [operation for operation, _ in s3.operations] == ["put", "put", "copy"]


def test_refresh_rejects_non_numeric_required_drift_feature_before_writing():
    bad = _ais_frame().astype({"sog": "string"})
    s3 = _client_with_candidates(valid=bad)

    with pytest.raises(SnapshotRefreshError, match="no valid Parquet source candidate"):
        _refresh(s3)

    assert s3.operations == []

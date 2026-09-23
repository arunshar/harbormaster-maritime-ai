"""Gate 4.6: infra/lambda/drift_watch/handler.py.

Kept out of pyproject.toml's testpaths, matching infra/lambda/teardown's own
convention (Lambda-runtime code, run explicitly rather than as part of the
default `pytest -q`): `python -m pytest infra/lambda/drift_watch/test_handler.py`.

Only summarize_drift (the pure decision path) and handler() with fully
stubbed boto3 clients are tested; the module is never applied to real AWS
this sprint (docs/phases/PHASE_4.md gate 4.6), so there is nothing live to
verify against.
"""

from __future__ import annotations

import io
import os
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
for p in (HERE, REPO_ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

import handler  # noqa: E402
from snapshot_refresh import SnapshotRefreshError  # noqa: E402

from mlops.drift import DriftResult  # noqa: E402

STABLE = DriftResult(feature="a", psi=0.0, ks=0.0, ks_pvalue=1.0, drifted=False)
DRIFTED = DriftResult(feature="b", psi=0.5, ks=0.4, ks_pvalue=0.001, drifted=True)


def test_summarize_drift_returns_false_when_nothing_drifted():
    should_alert, message = handler.summarize_drift([STABLE])
    assert should_alert is False
    assert "no input drift" in message


def test_summarize_drift_returns_true_and_names_the_feature_when_drifted():
    should_alert, message = handler.summarize_drift([STABLE, DRIFTED])
    assert should_alert is True
    lines = message.split("\n")[1:]  # drop the header line
    assert any(line.startswith("b:") for line in lines)
    assert not any(line.startswith("a:") for line in lines)  # only the drifted feature is listed


class _FakeBody:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeS3:
    def __init__(self, snapshots: dict[str, bytes]) -> None:
        self._snapshots = snapshots

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        return {"Body": _FakeBody(self._snapshots[Key])}


class _FakeSNS:
    def __init__(self) -> None:
        self.published: list[dict] = []

    def publish(self, **kwargs) -> None:
        self.published.append(kwargs)


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf)
    return buf.getvalue()


def _refresh_result(reference: pd.DataFrame, current: pd.DataFrame) -> SimpleNamespace:
    """Keep handler alert tests focused on alert wiring, not S3 refresh I/O."""
    return SimpleNamespace(
        reference=reference,
        current=current,
        source_key="raw/marinecadastre/validated.parquet",
        source_version_id="source-v1",
        window_key="drift/windows/window.parquet",
        provenance_key="drift/windows/window.provenance.json",
        window_sha256="a" * 64,
        window_rows=len(current),
        rejected_candidates=(),
        current_advanced=True,
    )


def test_handler_publishes_to_sns_when_drift_detected(monkeypatch):
    import numpy as np

    reference = pd.DataFrame({"x": np.linspace(0, 10, 50)})
    current = pd.DataFrame({"x": np.linspace(20, 30, 50)})  # clear mean shift

    fake_s3 = _FakeS3(
        {"ref.parquet": _parquet_bytes(reference), "cur.parquet": _parquet_bytes(current)}
    )
    fake_sns = _FakeSNS()
    monkeypatch.setattr(
        handler.boto3, "client", lambda name: {"s3": fake_s3, "sns": fake_sns}[name]
    )
    monkeypatch.setattr(
        handler,
        "refresh_current_snapshot",
        lambda *args, **kwargs: _refresh_result(reference, current),
    )
    monkeypatch.setenv("LAKE_BUCKET", "hm-lake")
    monkeypatch.setenv("REFERENCE_SNAPSHOT_KEY", "ref.parquet")
    monkeypatch.setenv("CURRENT_SNAPSHOT_KEY", "cur.parquet")
    monkeypatch.setenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:000000000000:hm-alerts")

    result = handler.handler({}, None)

    assert result["alerted"] is True
    assert "x" in result["drifted_features"]
    assert len(fake_sns.published) == 1
    assert fake_sns.published[0]["TopicArn"] == "arn:aws:sns:us-east-1:000000000000:hm-alerts"


def test_handler_does_not_publish_when_no_drift(monkeypatch):
    reference = pd.DataFrame({"x": [1.0, 2.0, 3.0] * 10})
    current = pd.DataFrame({"x": [1.0, 2.0, 3.0] * 10})

    fake_s3 = _FakeS3(
        {"ref.parquet": _parquet_bytes(reference), "cur.parquet": _parquet_bytes(current)}
    )
    fake_sns = _FakeSNS()
    monkeypatch.setattr(
        handler.boto3, "client", lambda name: {"s3": fake_s3, "sns": fake_sns}[name]
    )
    monkeypatch.setattr(
        handler,
        "refresh_current_snapshot",
        lambda *args, **kwargs: _refresh_result(reference, current),
    )
    monkeypatch.setenv("LAKE_BUCKET", "hm-lake")
    monkeypatch.setenv("REFERENCE_SNAPSHOT_KEY", "ref.parquet")
    monkeypatch.setenv("CURRENT_SNAPSHOT_KEY", "cur.parquet")
    monkeypatch.setenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:000000000000:hm-alerts")

    result = handler.handler({}, None)

    assert result["alerted"] is False
    assert fake_sns.published == []


def test_handler_skips_publish_when_no_topic_configured_even_if_drifted(monkeypatch):
    reference = pd.DataFrame({"x": [1.0, 2.0, 3.0] * 10})
    current = pd.DataFrame({"x": [100.0, 200.0, 300.0] * 10})

    fake_s3 = _FakeS3(
        {"ref.parquet": _parquet_bytes(reference), "cur.parquet": _parquet_bytes(current)}
    )
    fake_sns = _FakeSNS()
    monkeypatch.setattr(
        handler.boto3, "client", lambda name: {"s3": fake_s3, "sns": fake_sns}[name]
    )
    monkeypatch.setattr(
        handler,
        "refresh_current_snapshot",
        lambda *args, **kwargs: _refresh_result(reference, current),
    )
    monkeypatch.setenv("LAKE_BUCKET", "hm-lake")
    monkeypatch.setenv("REFERENCE_SNAPSHOT_KEY", "ref.parquet")
    monkeypatch.setenv("CURRENT_SNAPSHOT_KEY", "cur.parquet")
    monkeypatch.delenv("SNS_TOPIC_ARN", raising=False)

    result = handler.handler({}, None)

    assert result["alerted"] is False
    assert fake_sns.published == []


def test_handler_publishes_snapshot_failure_before_propagating_it(monkeypatch):
    reference = pd.DataFrame({"x": [1.0, 2.0, 3.0]})
    fake_s3 = _FakeS3({"ref.parquet": _parquet_bytes(reference)})
    fake_sns = _FakeSNS()
    monkeypatch.setattr(
        handler.boto3, "client", lambda name: {"s3": fake_s3, "sns": fake_sns}[name]
    )
    monkeypatch.setattr(
        handler,
        "refresh_current_snapshot",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            SnapshotRefreshError("all candidates invalid")
        ),
    )
    monkeypatch.setenv("LAKE_BUCKET", "hm-lake")
    monkeypatch.setenv("REFERENCE_SNAPSHOT_KEY", "ref.parquet")
    monkeypatch.setenv("CURRENT_SNAPSHOT_KEY", "cur.parquet")
    monkeypatch.setenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:000000000000:hm-alerts")

    with pytest.raises(SnapshotRefreshError, match="all candidates invalid"):
        handler.handler({}, None)

    assert fake_sns.published == [
        {
            "TopicArn": "arn:aws:sns:us-east-1:000000000000:hm-alerts",
            "Subject": "Harbormaster: AIS snapshot refresh failed",
            "Message": "Harbormaster Phase 4 AIS snapshot refresh failed: all candidates invalid",
        }
    ]

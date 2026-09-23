"""Tests for the Phase 3 AWS-showcase demo stand-in container (NOT Pi-DPM).

Real Flask test client, no Docker: verifies the /ping + /invocations
contract and the pure score_trajectory function before this ever goes into
an image, matching the project's "verify by running" convention.
"""

from __future__ import annotations

import json

import pytest

from mlops.pidpm_container.demo.server import app, score_trajectory


def test_score_trajectory_is_deterministic_and_bounded():
    traj = [[40.30, -74.15], [40.32, -74.14]]
    score = score_trajectory(traj, feature_mean=900.0, feature_std=400.0)
    assert 0.0 <= score <= 1.0
    assert score == score_trajectory(traj, feature_mean=900.0, feature_std=400.0)


def test_score_trajectory_larger_spread_scores_higher():
    small = [[40.0, -74.0], [40.001, -74.001]]
    large = [[40.0, -74.0], [41.0, -75.0]]
    s_small = score_trajectory(small, feature_mean=900.0, feature_std=400.0)
    s_large = score_trajectory(large, feature_mean=900.0, feature_std=400.0)
    assert s_large > s_small


@pytest.fixture
def client(tmp_path, monkeypatch):
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text(json.dumps({"feature_mean": 900.0, "feature_std": 400.0}))
    monkeypatch.setattr("mlops.pidpm_container.demo.server.CHECKPOINT_PATH", str(checkpoint_path))
    monkeypatch.setattr("mlops.pidpm_container.demo.server._checkpoint", None)
    with app.test_client() as c:
        yield c


def test_ping_returns_200_when_checkpoint_present(client):
    resp = client.get("/ping")
    assert resp.status_code == 200


def test_ping_returns_503_when_checkpoint_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "mlops.pidpm_container.demo.server.CHECKPOINT_PATH", str(tmp_path / "missing.json")
    )
    monkeypatch.setattr("mlops.pidpm_container.demo.server._checkpoint", None)
    with app.test_client() as c:
        resp = c.get("/ping")
        assert resp.status_code == 503


def test_invocations_returns_a_score_and_labels_itself_as_the_demo_standin(client):
    resp = client.post(
        "/invocations",
        data=json.dumps({"trajectory": [[40.30, -74.15], [40.32, -74.14]]}),
        content_type="application/json",
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert "score" in body
    assert 0.0 <= body["score"] <= 1.0
    assert body["model"] == "phase3-demo-standin"

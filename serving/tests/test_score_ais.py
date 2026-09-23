"""/v1/score-ais endpoint + scoring-decision tests (Phase 1.2)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from app.agents.corridor_detector import CorridorFinding
from app.config import Settings
from app.main import app
from app.models import AisFix, AisScoreIn
from app.orchestrator import Orchestrator
from replay.loader import load_expectations, load_fixture

from ._helpers import build_score_in


def _by_mmsi() -> dict:
    by: dict[int, list] = {}
    for r in load_fixture():
        by.setdefault(r.mmsi, []).append(r)
    for m in by:
        by[m].sort(key=lambda r: r.t)
    return by


def test_health_score_happy_path_and_metrics():
    by = _by_mmsi()
    jump = next(a for a in load_expectations()["anomalies"] if a["kind"] == "implausible_jump")
    payload = build_score_in(by, jump["mmsi"], jump["t"]).model_dump(mode="json")
    with TestClient(app) as c:
        assert c.get("/healthz").json()["status"] == "ok"
        r = c.post("/v1/score-ais", json=payload)
        assert r.status_code == 200
        body = r.json()
        assert "implausible_speed" in [x["code"] for x in body["reasons"]]
        assert body["hitl_required"] is True
        assert 0.0 <= body["score"] <= 1.0
        assert body["latency_ms"] >= 0.0
        assert body["trace_id"]
        assert "hm_scores_total" in c.get("/metrics").text


def test_corrupt_teleport_returns_422():
    corrupt = {
        "mmsi": 200999999,
        "fix": {"lat": 42.0, "lon": -70.0, "t": "2024-06-01T00:01:00Z"},
        "history": [{"lat": 40.5, "lon": -74.0, "t": "2024-06-01T00:00:00Z"}],
    }
    with TestClient(app) as c:
        r = c.post("/v1/score-ais", json=corrupt)
        assert r.status_code == 422
        assert r.json()["code"] == "harbormaster.corrupt_input"


def test_feedback_roundtrip_clears_pending():
    by = _by_mmsi()
    off = next(a for a in load_expectations()["anomalies"] if a["kind"] == "off_corridor")
    payload = build_score_in(by, off["mmsi"], off["t"]).model_dump(mode="json")
    with TestClient(app) as c:
        body = c.post("/v1/score-ais", json=payload).json()
        assert body["hitl_required"] is True
        before = len(c.get("/v1/hitl/pending").json())
        assert before >= 1
        fb = {"trace_id": body["trace_id"], "label": "correct", "reviewer": "arun"}
        assert c.post("/v1/feedback", json=fb).json()["accepted"] is True
        after = len(c.get("/v1/hitl/pending").json())
        assert after == before - 1


def test_feedback_unknown_trace_returns_404():
    with TestClient(app) as c:
        r = c.post(
            "/v1/feedback",
            json={"trace_id": "does-not-exist", "label": "correct", "reviewer": "arun"},
        )
        assert r.status_code == 404
        assert r.json()["code"] == "harbormaster.hitl_trace_not_found"


class _BenignCorridor:
    async def detect(self, track):
        return CorridorFinding(False, 0.0, "", False, 0.0, 1e9)


async def test_confidence_below_threshold_sets_hitl():
    """An ambiguous mid-band score (no decisive reason) routes to HITL via confidence."""

    o = await Orchestrator.bootstrap(Settings())
    o.corridor = _BenignCorridor()  # isolate the speed signal from geography
    try:
        t0 = datetime(2024, 1, 1, tzinfo=UTC)
        dlat = 0.013186  # ~1466 m of latitude -> ~1.9x v_max over 60 s
        payload = AisScoreIn(
            mmsi=1,
            fix=AisFix(lat=40.0 + dlat, lon=-74.0, t=t0 + timedelta(seconds=60)),
            history=[AisFix(lat=40.0, lon=-74.0, t=t0)],
        )
        out = await o.score(payload)
        assert "implausible_speed" in {r.code.value for r in out.reasons}
        assert out.score < o.settings.anomaly_hitl_threshold  # not score-driven
        assert out.confidence < o.settings.hitl_confidence_threshold
        assert out.hitl_required is True  # confidence-driven
    finally:
        await o.shutdown()

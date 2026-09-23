"""Golden checksum gate (Phase 1.2).

The documented known-anomaly events in expectations.json must produce their
expected reason and HITL verdict; documented normal events must produce neither.
"""

from __future__ import annotations

from ._helpers import build_score_in


async def test_golden_anomalies_score_as_expected(orch, fixture_by_mmsi, expectations):
    for a in expectations["anomalies"]:
        out = await orch.score(build_score_in(fixture_by_mmsi, a["mmsi"], a["t"]))
        codes = {r.code.value for r in out.reasons}
        assert a["expect_reason"] in codes, f"{a['kind']}: got {codes}"
        assert out.hitl_required == a["expect_hitl"], f"{a['kind']}: hitl {out.hitl_required}"


async def test_golden_normals_score_clean(orch, fixture_by_mmsi, expectations):
    for ns in expectations["normal_samples"]:
        out = await orch.score(build_score_in(fixture_by_mmsi, ns["mmsi"], ns["t"]))
        assert out.reasons == [], f"{ns['mmsi']}: unexpected {[r.code.value for r in out.reasons]}"
        assert out.hitl_required is False


async def test_score_latency_under_slo(orch, fixture_by_mmsi, expectations):
    a = expectations["anomalies"][0]
    out = await orch.score(build_score_in(fixture_by_mmsi, a["mmsi"], a["t"]))
    assert out.latency_ms < 200.0  # the 1.2 smoke checksum (< 200 ms kernel path)


async def test_anomalies_enqueue_to_hitl(orch, fixture_by_mmsi, expectations):
    for a in expectations["anomalies"]:
        await orch.score(build_score_in(fixture_by_mmsi, a["mmsi"], a["t"]))
    pending = await orch.hitl.pending()
    enqueued_mmsis = {r["mmsi"] for r in pending}
    for a in expectations["anomalies"]:
        assert a["mmsi"] in enqueued_mmsis

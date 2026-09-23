"""Unit tests for the Phase 1 SLO evaluator (gate G7)."""

from __future__ import annotations

import math

from app.burn_rate import Bucket, BurnStatus
from app.slo import (
    PHASE1_SLOS,
    all_ok,
    evaluate,
    evaluate_success_burn_rate,
    success_burn_status,
)


def test_all_slos_pass():
    results = evaluate(
        {"score_success_ratio": 1.0, "score_kernel_p95_ms": 120.0, "replay_to_hitl_p95_s": 4.0}
    )
    assert all_ok(results)
    assert {r.slo.name for r in results} == {s.name for s in PHASE1_SLOS}


def test_latency_breach_flags_only_that_slo():
    results = evaluate(
        {"score_success_ratio": 1.0, "score_kernel_p95_ms": 500.0, "replay_to_hitl_p95_s": 4.0}
    )
    assert not all_ok(results)
    breached = [r.slo.name for r in results if not r.ok]
    assert breached == ["score_kernel_p95_ms"]


def test_success_ratio_below_target_breaches():
    results = evaluate(
        {"score_success_ratio": 0.99, "score_kernel_p95_ms": 100.0, "replay_to_hitl_p95_s": 2.0}
    )
    assert [r.slo.name for r in results if not r.ok] == ["score_success_ratio"]


def test_missing_measurement_breaches_as_nan():
    results = evaluate({"score_success_ratio": 1.0})
    missing = [r for r in results if r.slo.name == "score_kernel_p95_ms"][0]
    assert missing.ok is False
    assert math.isnan(missing.measured)
    assert not all_ok(results)


# --- Windowed burn-rate view of the success SLO (DR-13) --------------------


def _flat(n: int, total: int, bad: int) -> list[Bucket]:
    return [Bucket(timestamp=float(i * 60), total=total, bad=bad) for i in range(n)]


def test_success_burn_rate_healthy_series_is_ok():
    series = _flat(1440, total=1000, bad=0)
    assert success_burn_status(series, bucket_seconds=60.0) is BurnStatus.OK


def test_success_burn_rate_fast_outage_pages_and_recommends_rollback():
    # 1h+ healthy then a 5-minute total outage: fast tier fires.
    series = _flat(120, total=1000, bad=0) + _flat(5, total=1000, bad=1000)
    result = evaluate_success_burn_rate(series, bucket_seconds=60.0)
    assert result.status is BurnStatus.PAGE
    assert result.should_rollback is True


def test_success_burn_rate_uses_the_999_target_by_default():
    # 0.008 failure ratio -> burn 8x at the default 99.9% target -> slow PAGE.
    series = _flat(400, total=1000, bad=8)
    result = evaluate_success_burn_rate(series, bucket_seconds=60.0)
    assert result.target == 0.999
    assert math.isclose(result.error_budget, 0.001, rel_tol=0, abs_tol=1e-12)
    assert "slow" in [t.tier.name for t in result.tiers if t.fired]

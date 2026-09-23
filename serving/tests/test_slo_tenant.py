"""Per-tenant SLO tier tests (Phase 5, gate 5.4).

Covers tier selection, per-tenant partitioning, the golden pins in
mlops/fixtures/expectations.json, and the per-tenant make_burn_check wiring
driven through a real run_promotion call. The calculator's own boundary and
formula tests live in test_burn_rate.py and are deliberately not duplicated.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.burn_rate import Bucket, evaluate_burn_rate
from app.slo import PHASE1_SLOS
from app.slo_tenant import (
    TIER_SLOS,
    PerTenantSlo,
    TenantTier,
    make_tenant_burn_check,
    tenant_series_provider,
    tier_success_target,
)

EXPECTATIONS = Path(__file__).parent.parent.parent / "mlops" / "fixtures" / "expectations.json"


def _flat(n: int, total: int, bad: int) -> list[Bucket]:
    return [Bucket(timestamp=float(i * 60), total=total, bad=bad) for i in range(n)]


# One sustained 3%-bad series; three verdicts, one per tier (the pinned
# boundary case: page for real-time, warning for near-real-time, ok for batch).
BOUNDARY_SERIES = _flat(1440, total=1000, bad=30)


# ---- tier tables ------------------------------------------------------------


def test_every_tier_carries_the_three_phase1_slo_names():
    phase1_names = [s.name for s in PHASE1_SLOS]
    for tier in TenantTier:
        assert [s.name for s in TIER_SLOS[tier]] == phase1_names


def test_real_time_tier_is_the_phase1_table_verbatim():
    assert TIER_SLOS[TenantTier.REAL_TIME] is PHASE1_SLOS


def test_tiers_relax_monotonically_from_real_time_to_batch():
    rt, nrt, batch = (
        {s.name: s.target for s in TIER_SLOS[t]}
        for t in (TenantTier.REAL_TIME, TenantTier.NEAR_REAL_TIME, TenantTier.BATCH)
    )
    assert rt["score_success_ratio"] > nrt["score_success_ratio"] > batch["score_success_ratio"]
    assert rt["score_kernel_p95_ms"] < nrt["score_kernel_p95_ms"] < batch["score_kernel_p95_ms"]
    assert rt["replay_to_hitl_p95_s"] < nrt["replay_to_hitl_p95_s"] < batch["replay_to_hitl_p95_s"]


def test_tier_success_target_returns_each_tiers_ratio():
    assert tier_success_target(TenantTier.REAL_TIME) == 0.999
    assert tier_success_target(TenantTier.NEAR_REAL_TIME) == 0.995
    assert tier_success_target(TenantTier.BATCH) == 0.99


def test_tier_success_target_raises_on_a_tier_table_missing_the_success_slo(monkeypatch):
    monkeypatch.setitem(TIER_SLOS, TenantTier.BATCH, ())
    with pytest.raises(KeyError):
        tier_success_target(TenantTier.BATCH)


def test_tier_targets_match_the_pinned_goldens():
    pinned = json.loads(EXPECTATIONS.read_text())["tenant_slo_tiers"]["targets"]
    live = {tier.value: {s.name: s.target for s in TIER_SLOS[tier]} for tier in TenantTier}
    assert live == pinned, (
        "a tier target changed; if intentional, update "
        "mlops/fixtures/expectations.json AND docs/phases/PHASE_5.md in the same commit"
    )


# ---- PerTenantSlo: the existing evaluate(), once per tenant -----------------


def test_per_tenant_evaluate_applies_the_tenants_own_tier():
    measurements = {
        "score_success_ratio": 0.996,
        "score_kernel_p95_ms": 500.0,
        "replay_to_hitl_p95_s": 30.0,
    }
    rt = PerTenantSlo(tenant_id="tenant-a", tier=TenantTier.REAL_TIME)
    nrt = PerTenantSlo(tenant_id="tenant-b", tier=TenantTier.NEAR_REAL_TIME)
    # the same window breaches every real-time SLO and passes every NRT one
    assert [r.ok for r in rt.evaluate(measurements)] == [False, False, False]
    assert [r.ok for r in nrt.evaluate(measurements)] == [True, True, True]


def test_per_tenant_evaluate_breaches_on_a_missing_measurement():
    tenant = PerTenantSlo(tenant_id="tenant-a", tier=TenantTier.BATCH)
    results = tenant.evaluate({})
    assert all(not r.ok for r in results)  # evaluate()'s contract, unchanged


def test_success_target_comes_from_the_tenants_tier():
    assert PerTenantSlo("t", TenantTier.NEAR_REAL_TIME).success_target() == 0.995
    assert PerTenantSlo("t", TenantTier.NEAR_REAL_TIME).slos is TIER_SLOS[TenantTier.NEAR_REAL_TIME]


# ---- the pinned one-series-three-verdicts boundary case ----------------------


def test_boundary_series_verdicts_match_the_pinned_goldens_per_tier():
    pinned = json.loads(EXPECTATIONS.read_text())["tenant_slo_tiers"]["boundary_case"]
    for tier in TenantTier:
        result = evaluate_burn_rate(
            BOUNDARY_SERIES, target=tier_success_target(tier), bucket_seconds=60.0
        )
        assert result.status.value == pinned[f"{tier.value}_status"]
        assert result.should_rollback == pinned[f"{tier.value}_should_rollback"]


# ---- per-tenant partitioning + make_burn_check wiring ------------------------


def test_tenant_series_provider_partitions_by_tenant():
    calls: list[tuple[str, int]] = []

    def provider(tenant_id: str, weight: int) -> list[Bucket]:
        calls.append((tenant_id, weight))
        return _flat(5, 100, 0)

    for_a = tenant_series_provider(provider, "tenant-a")
    for_b = tenant_series_provider(provider, "tenant-b")
    for_a(5)
    for_b(25)
    assert calls == [("tenant-a", 5), ("tenant-b", 25)]


def test_one_tenants_page_rolls_back_while_the_other_tenants_same_window_promotes():
    # The gate's acceptance shape: the SAME provider, the SAME measurement
    # window; tenant-a's series crosses a PAGE threshold at weight 25 and
    # tenant-b's does not. run_promotion must revert exactly for tenant-a.
    from mlops.holdout_gate import HoldoutGateResult
    from mlops.promote import CANARY_WEIGHTS, run_promotion
    from mlops.shadow_diff import ShadowDiffResult

    passing_gate = HoldoutGateResult(
        auc=0.95, crps=0.2, calibration_ratio=1.0, passed=True, failures=[]
    )
    clean_shadow = ShadowDiffResult(
        mean_abs_diff=0.01, max_abs_diff=0.02, n_samples=100, passed=True
    )

    def provider(tenant_id: str, weight: int) -> list[Bucket]:
        if tenant_id == "tenant-a" and weight >= 25:
            return _flat(120, 1000, 0) + _flat(5, 1000, 1000)  # 5-minute total outage
        return _flat(1440, 1000, 0)

    outcomes: dict[str, tuple[str, bool]] = {}
    for tenant_id in ("tenant-a", "tenant-b"):
        tenant = PerTenantSlo(tenant_id=tenant_id, tier=TenantTier.REAL_TIME)
        burn_check = make_tenant_burn_check(provider, tenant=tenant, bucket_seconds=60.0)
        reverted = False

        def revert() -> None:
            nonlocal reverted
            reverted = True

        result = run_promotion(
            holdout_result=passing_gate,
            shadow_result=clean_shadow,
            burn_check=burn_check,
            set_canary_weight=lambda w: None,
            revert_to_champion=revert,
        )
        outcomes[tenant_id] = (result.final_status, reverted)
        if tenant_id == "tenant-a":
            assert result.steps[-1].stage == "canary_25"
            assert result.steps[-1].action == "revert"

    assert outcomes["tenant-a"] == ("rolled_back", True)
    assert outcomes["tenant-b"] == ("promoted", False)
    assert CANARY_WEIGHTS == (5, 25, 50, 100)  # wiring assumes the locked ramp


def test_tenant_burn_check_uses_the_tiers_own_target():
    # The boundary series pages for a real-time tenant and stays quiet for a
    # batch tenant: same buckets, only the tier target differs.
    def provider(tenant_id: str, weight: int) -> list[Bucket]:
        return BOUNDARY_SERIES

    rt_check = make_tenant_burn_check(
        provider, tenant=PerTenantSlo("t-rt", TenantTier.REAL_TIME), bucket_seconds=60.0
    )
    batch_check = make_tenant_burn_check(
        provider, tenant=PerTenantSlo("t-b", TenantTier.BATCH), bucket_seconds=60.0
    )
    assert rt_check(5) is True
    assert batch_check(5) is False

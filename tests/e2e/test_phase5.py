"""Phase 5 acceptance (gate 5.9, the phase gate).

The phase gate stays OPEN until the W4 demo window closes the three LIVE legs
that cannot be honestly asserted without a cluster and a clock: (a) measured
KEDA cold-start, (b) a real Flink-backpressure postmortem, and (f) an actual
teardown-guard force-destroy. This file asserts everything that CAN be verified
locally and hermetically, and marks each live leg's measured portion as
deferred, never faked:

  (a) KEDA scale-to-zero front door is authored (minReplicaCount 0 + lag
      trigger); the MEASURED cold-start number is a W4 leg.
  (b) the backpressure load SHAPE spikes then recovers (pure rate function);
      the real Flink backpressure + postmortem is a W4 leg.
  (c) per-tenant drift catches a single-tenant shift a same-fixture global
      pool averages away  [fully local].
  (d) every Bedrock explanation is provably built from reason codes + score
      only; raw trajectory vocabulary cannot enter the prompt  [fully local].
  (e) RLS DDL is fail-closed by construction (structural, always) and rejects
      a real cross-tenant read (live, gated on HM_TEST_PG_DSN; also run by
      `make phase5-tenant-smoke` and the W4 M-tenant-leak drill).
  (f) the teardown-guard decision fires at the age boundary and the guard is
      armed by default (pure + structural); the live force-destroy is a W4 leg.
  (g) the PPO stretch's reward/feasibility are correct on the pinned fixture
      and it never appears in the core promotion pipeline  [fully local].
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from app.bedrock_explainer import build_prompt
from cdc.schema import tenancy
from e2e.phase5_helpers import (
    GUARD_MODULE_PATH,
    guard_is_armed_by_default,
    guard_window_default_hours,
)
from mlops.drift import check_input_drift
from mlops.route_optimizer.feasibility import feasible_action_mask, v_max_mps_for
from mlops.route_optimizer.graph import tiny_synthetic_graph
from mlops.route_optimizer.reward import coverage_minus_fuel
from mlops.tenant_drift import check_tenant_drift, drifted_tenants, pooled_windows

REPO = Path(__file__).resolve().parents[2]
EXPECTATIONS = REPO / "mlops" / "fixtures" / "expectations.json"


def _load(name: str, rel: str):
    """Load a non-package module (a script / lambda handler) by file path.

    Registered in sys.modules before exec so dataclasses defined in the loaded
    module can resolve their own module namespace (BurstProfile would otherwise
    raise on instantiation)."""
    import sys

    spec = importlib.util.spec_from_file_location(name, REPO / rel)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# --- (a) KEDA scale-to-zero front door: authored; measured cold-start is W4 ---


def test_a_keda_scale_to_zero_authored_measured_coldstart_deferred_to_w4():
    manifests = list((REPO / "deploy" / "k8s" / "serving").rglob("*.yaml"))
    assert manifests, "no deploy/k8s/serving manifests authored for the EKS/KEDA front door"
    blob = "\n".join(m.read_text() for m in manifests)
    assert "ScaledObject" in blob, "no KEDA ScaledObject authored"
    assert "minReplicaCount: 0" in blob, "the front door is not authored to scale to zero"
    # The MEASURED 0 -> N -> 0 cold-start latency (criterion a) is a live W4 leg;
    # nothing here claims a number.


# --- (b) backpressure load shape spikes then recovers; live postmortem is W4 ---


def test_b_backpressure_load_shape_spikes_then_recovers():
    lt = _load("loadtest_kinesis_backpressure", "scripts/loadtest_kinesis_backpressure.py")
    profile = lt.BurstProfile(
        steady_rps=50.0, burst_rps=500.0, burst_start_s=10.0, burst_end_s=30.0, ramp_s=5.0
    )
    base = lt.rate_at(0.0, profile)  # before the burst: steady state
    peak = lt.rate_at(20.0, profile)  # mid-plateau: full burst
    tail = lt.rate_at(50.0, profile)  # after the down-ramp: recovered
    assert base == 50.0
    assert peak == 500.0, "the burst does not reach the configured plateau"
    assert tail == 50.0, "the load never returns to steady state"
    # The real Flink backpressure + postmortem (criterion b) is a live W4 leg.


# --- (c) per-tenant drift catches a shift the global pool averages away --------


def _stable() -> pd.DataFrame:
    return pd.DataFrame({"feature_x": np.linspace(0, 10, 60)})


def _shifted() -> pd.DataFrame:
    return pd.DataFrame({"feature_x": np.linspace(7, 17, 60)})


def test_c_per_tenant_drift_catches_single_tenant_shift_global_pool_misses():
    windows = {"tenantA": (_stable(), _shifted())}
    for i in range(9):  # nine stable tenants dilute A's shift in the global pool
        windows[f"t{i:02d}"] = (_stable(), _stable())

    per_tenant = check_tenant_drift(windows)
    assert drifted_tenants(per_tenant) == ["tenantA"]
    a_result = {r.feature: r for r in per_tenant["tenantA"]}["feature_x"]
    assert a_result.drifted is True

    ref, cur = pooled_windows(windows)
    pooled = {r.feature: r for r in check_input_drift(ref, cur)}["feature_x"]
    assert pooled.drifted is False, (
        "the global pool alerted; the P4 contrast requires it to average the shift away"
    )
    assert a_result.psi > pooled.psi  # the tenant sees the shift, the pool does not


# --- (d) Bedrock explanation is provably reason-code + score only -------------


def test_d_bedrock_prompt_is_reasons_and_score_only_no_trajectory_leak():
    reasons = ["implausible_speed", "off_corridor", "watchlist_hit"]  # real ReasonCode values
    prompt = build_prompt(reasons, 0.873)
    for code in reasons:
        assert code in prompt
    assert "0.873" in prompt
    # a raw coordinate / trajectory field cannot physically enter the prompt
    for leak in ["37.7749,-122.4194", "lat", "prism", "fix_id", "Not A Code"]:
        with pytest.raises(ValueError):
            build_prompt([*reasons, leak], 0.873)
    with pytest.raises(ValueError):
        build_prompt(reasons, 1.5)  # out-of-range score


# --- (e) RLS: fail-closed by construction (structural) + live cross-tenant ----


def test_e_rls_ddl_is_fail_closed_by_construction():
    pinned = json.loads(EXPECTATIONS.read_text())["tenant_rls"]
    assert tenancy.ddl_sha256() == pinned["ddl_sha256"], (
        "RLS DDL drifted from the pinned sha256; update the fixture AND PHASE_5.md together"
    )
    ddl = "\n".join(tenancy.statements())
    predicate = pinned["policy_predicate"]
    # the fail-closed predicate must be the pinned one (unset app.tenant_id ->
    # NULLIF -> NULL -> matches no row), and it must be the policy's USING
    # clause, not merely present somewhere (a column DEFAULT also mentions
    # current_setting). A fail-OPEN `USING (true)` must never appear.
    assert predicate == "tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid"
    assert "USING (true)" not in ddl and "USING(true)" not in ddl
    # every tenant table is RLS-enabled, FORCEd (owner is not exempt), and its
    # isolation policy USES exactly the fail-closed predicate: isolation is
    # structural, not application-layer, and cannot silently become fail-open.
    for table in tenancy.TENANT_TABLES:
        assert f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;" in ddl
        assert f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;" in ddl
        assert f"CREATE POLICY {table}_tenant_isolation ON {table}" in ddl
        assert f"USING ({predicate})" in ddl


def test_e_live_rls_rejects_cross_tenant_read_when_postgres_available():
    dsn = os.environ.get("HM_TEST_PG_DSN")
    if not dsn:
        pytest.skip(
            "no HM_TEST_PG_DSN; the live cross-tenant-rejection proof runs via "
            "`make phase5-tenant-smoke` and the W4 M-tenant-leak drill"
        )
    smoke = _load("phase5_tenant_smoke", "scripts/phase5_tenant_smoke.py")
    log = asyncio.run(smoke.rls_fail_closed_check(dsn))
    assert any("cross-tenant blocked by RLS" in line for line in log)
    assert any("FAIL-CLOSED CONFIRMED" in line for line in log)


# --- (f) teardown-guard decision + armed-by-default; live force-destroy is W4 -


def test_f_teardown_guard_fires_at_boundary_and_is_armed_by_default():
    import datetime as dt

    handler = _load("eks_teardown_handler", "infra/lambda/eks_teardown/handler.py")
    created = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    # before the age window: no teardown
    assert (
        handler.should_teardown(created, None, created + dt.timedelta(hours=1), max_age_hours=4)
        is False
    )
    # at the boundary (inclusive): fires
    assert (
        handler.should_teardown(created, None, created + dt.timedelta(hours=4), max_age_hours=4)
        is True
    )
    # a future keep-alive tag holds the guard off regardless of age
    held = handler.should_teardown(
        created, created + dt.timedelta(hours=10), created + dt.timedelta(hours=5), max_age_hours=4
    )
    assert held is False

    guard_vars = (GUARD_MODULE_PATH / "variables.tf").read_text()
    assert guard_is_armed_by_default(guard_vars) is True  # dry_run defaults false = armed
    assert guard_window_default_hours(guard_vars) is not None
    # The LIVE force-destroy (criterion f) is demonstrated once in the W4 window.


# --- (g) PPO stretch reward/feasibility correct; never in the core pipeline ---


def test_g_ppo_stretch_reward_and_feasibility_correct_and_isolated():
    pins = json.loads(EXPECTATIONS.read_text())["ppo_stretch_expectations"]
    graph = tiny_synthetic_graph()
    reward = coverage_minus_fuel(pins["canonical_route"], graph)
    assert reward == pins["reward"]  # pinned, criterion g

    v_max = v_max_mps_for("vessel")
    assert feasible_action_mask(graph, 0, dt_s=3600.0, v_max_mps=v_max).all()
    assert not feasible_action_mask(graph, 0, dt_s=1.0, v_max_mps=v_max).any()

    # isolation: the core pipeline must not import the stretch (criterion g)
    for core in ("mlops/promote.py", "serving/app/orchestrator.py"):
        text = (REPO / core).read_text()
        assert "route_optimizer" not in text

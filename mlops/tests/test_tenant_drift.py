"""Per-tenant drift tests (Phase 5, gate 5.5).

The load-bearing test is the P4 acceptance shape: one tenant's population
genuinely shifts while the pooled/global distribution across all tenants
stays within the alert thresholds, and BOTH verdicts are computed in the same
test from the same fixture (the per-tenant check alerts, the tenant-blind
pooled baseline does not). Fixture magnitudes were verified against the real
gate 4.1 statistics before pinning the assertions, not assumed.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from mlops.drift import DriftConfig, DriftResult, check_input_drift
from mlops.tenant_drift import check_tenant_drift, drifted_tenants, pooled_windows

SHIFTED_TENANT = "tenant-d"


@pytest.fixture(scope="module")
def tenant_windows():
    """Four tenants, two features. tenant-d (10% of the pool) shifts its
    speed_kts population by +3 sigma between windows; every other window is
    drawn from the unchanged distributions. Seeded, deterministic."""
    rng = np.random.default_rng(42)
    windows = {}
    for tenant in ("tenant-a", "tenant-b", "tenant-c"):
        windows[tenant] = (
            pd.DataFrame({"speed_kts": rng.normal(10, 2, 600), "gap_s": rng.normal(300, 60, 600)}),
            pd.DataFrame({"speed_kts": rng.normal(10, 2, 600), "gap_s": rng.normal(300, 60, 600)}),
        )
    windows[SHIFTED_TENANT] = (
        pd.DataFrame({"speed_kts": rng.normal(10, 2, 200), "gap_s": rng.normal(300, 60, 200)}),
        pd.DataFrame({"speed_kts": rng.normal(16, 2, 200), "gap_s": rng.normal(300, 60, 200)}),
    )
    return windows


def test_per_tenant_check_catches_the_shift_a_global_pool_baseline_misses(tenant_windows):
    # The exact Phase 5 acceptance criterion, both halves from
    # the SAME fixture in the SAME test.
    per_tenant = check_tenant_drift(tenant_windows)
    shifted = {r.feature: r for r in per_tenant[SHIFTED_TENANT]}
    assert shifted["speed_kts"].drifted, "the shifted tenant's own partition must alert"
    assert shifted["speed_kts"].psi > DriftConfig().psi_alert  # a real shift, not a warn-band graze

    pooled_ref, pooled_cur = pooled_windows(tenant_windows)
    pooled = check_input_drift(pooled_ref, pooled_cur)
    assert pooled, "the pooled baseline must actually compute, not vacuously pass"
    assert not any(r.drifted for r in pooled), (
        "the tenant-blind global pool flagged drift; the fixture no longer "
        "demonstrates the P4 averaging-away failure mode"
    )
    # and the dilution is real: the pooled PSI sits under even the warn band
    pooled_speed = next(r for r in pooled if r.feature == "speed_kts")
    assert pooled_speed.psi < DriftConfig().psi_warn


def test_unshifted_tenants_and_features_stay_quiet(tenant_windows):
    results = check_tenant_drift(tenant_windows)
    for tenant, tenant_results in results.items():
        for r in tenant_results:
            if tenant == SHIFTED_TENANT and r.feature == "speed_kts":
                continue
            assert not r.drifted, f"false alarm: {tenant}/{r.feature} psi={r.psi:.4f}"


def test_results_are_keyed_per_tenant_and_sorted(tenant_windows):
    results = check_tenant_drift(tenant_windows)
    assert list(results) == sorted(tenant_windows)
    for tenant_results in results.values():
        assert [r.feature for r in tenant_results] == ["speed_kts", "gap_s"]


def test_result_keys_sort_reverse_insertion_order(tenant_windows):
    reverse_inserted = dict(reversed(list(tenant_windows.items())))
    assert list(reverse_inserted) != sorted(reverse_inserted)

    results = check_tenant_drift(reverse_inserted)

    assert list(results) == sorted(reverse_inserted)


def test_drifted_tenants_lists_exactly_the_shifted_tenant(tenant_windows):
    assert drifted_tenants(check_tenant_drift(tenant_windows)) == [SHIFTED_TENANT]
    assert drifted_tenants({}) == []


def test_drifted_tenant_fanout_sorts_reverse_insertion_order():
    drifted = DriftResult(feature="speed_kts", psi=0.5, ks=0.4, ks_pvalue=0.001, drifted=True)
    quiet = DriftResult(feature="speed_kts", psi=0.0, ks=0.0, ks_pvalue=1.0, drifted=False)
    reverse_inserted = {
        "tenant-z": [drifted],
        "tenant-a": [drifted],
        "tenant-m": [quiet],
    }

    assert drifted_tenants(reverse_inserted) == ["tenant-a", "tenant-z"]


def test_config_passes_through_to_every_partition(tenant_windows):
    # A paranoid config (alert on any nonzero PSI) must flag every tenant:
    # proof the wrapper forwards config instead of re-defaulting per call.
    paranoid = DriftConfig(psi_warn=0.0, psi_alert=0.0)
    results = check_tenant_drift(tenant_windows, paranoid)
    assert drifted_tenants(results) == sorted(tenant_windows)


def test_pooled_windows_concatenates_every_tenants_rows(tenant_windows):
    pooled_ref, pooled_cur = pooled_windows(tenant_windows)
    assert len(pooled_ref) == sum(len(ref) for ref, _ in tenant_windows.values())
    assert len(pooled_cur) == sum(len(cur) for _, cur in tenant_windows.values())
    assert list(pooled_ref.columns) == ["speed_kts", "gap_s"]


def test_empty_input_is_a_clean_no_op():
    assert check_tenant_drift({}) == {}
    pooled_ref, pooled_cur = pooled_windows({})
    assert pooled_ref.empty and pooled_cur.empty
    assert check_input_drift(pooled_ref, pooled_cur) == []

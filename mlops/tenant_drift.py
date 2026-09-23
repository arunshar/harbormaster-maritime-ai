"""Phase 5 gate 5.5: per-tenant drift monitoring.

A thin partitioning wrapper over ``mlops/drift.py``'s ``check_input_drift``
(Phase 4, gate 4.1), which is called once per ``tenant_id`` partition of the
reference/current feature windows. NOT a new drift algorithm: the PSI/KS
statistics, thresholds, and the DriftConfig semantics are the gate 4.1
mechanism verbatim; this module adds only the partitioning, so a single
tenant's population shift shows up in that tenant's own PSI/KS result instead
of being averaged into a global distribution that dilutes it below the alert
threshold (war story P35, the stated Phase 5 acceptance criterion).

Partitioning the windows is the caller's job (a per-tenant WHERE on the
feature snapshots), matching the injected-I/O convention drift.py itself
states: this module only computes, it never fetches. ``pooled_windows`` is
the deliberately tenant-blind baseline the acceptance test and drill
M-drift-hidden contrast against; it exists so the "a global monitor would
have missed this" claim is computed from the same fixture, not asserted.
"""

from __future__ import annotations

import pandas as pd

from mlops.drift import DriftConfig, DriftResult, check_input_drift

__all__ = [
    "TenantWindows",
    "check_tenant_drift",
    "drifted_tenants",
    "pooled_windows",
]

# tenant_id -> (reference window, current window), both feature-column frames
# with the gate 3.3 training-set export's columns.
TenantWindows = dict[str, tuple[pd.DataFrame, pd.DataFrame]]


def check_tenant_drift(
    tenant_windows: TenantWindows,
    config: DriftConfig | None = None,
) -> dict[str, list[DriftResult]]:
    """The gate 4.1 check, once per tenant partition.

    Returns one ``check_input_drift`` result list per tenant, keyed by
    ``tenant_id`` (iteration order sorted for deterministic output). The
    config, defaulting exactly as gate 4.1 defaults, applies to every tenant:
    per-tenant thresholds are deliberately NOT a feature (a tenant-specific
    alert bar would be a new drift policy, out of this gate's scope).
    """
    return {
        tenant_id: check_input_drift(reference, current, config)
        for tenant_id, (reference, current) in sorted(tenant_windows.items())
    }


def pooled_windows(tenant_windows: TenantWindows) -> tuple[pd.DataFrame, pd.DataFrame]:
    """The tenant-blind global pool: every tenant's windows concatenated.

    This is what a single global drift monitor sees, and it is the baseline
    the P4 contrast is computed against. Empty input yields empty frames (no
    tenants, nothing pooled), so the caller's ``check_input_drift`` on the
    result is a clean no-op rather than a concat error.
    """
    if not tenant_windows:
        return pd.DataFrame(), pd.DataFrame()
    ordered = [tenant_windows[t] for t in sorted(tenant_windows)]
    reference = pd.concat([ref for ref, _ in ordered], ignore_index=True)
    current = pd.concat([cur for _, cur in ordered], ignore_index=True)
    return reference, current


def drifted_tenants(results: dict[str, list[DriftResult]]) -> list[str]:
    """Tenants with at least one drifted feature, sorted; the alert fan-out
    list (which tenants get a drift ticket, per incident P4's per-tenant
    alerting requirement)."""
    return sorted(
        tenant_id
        for tenant_id, tenant_results in results.items()
        if any(r.drifted for r in tenant_results)
    )

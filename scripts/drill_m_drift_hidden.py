"""Drill M-drift-hidden: per-tenant drift catches a single-tenant population
shift that a global-pool monitor averages away (Phase 5, gate 5.9; war story
P35 in docs/WAR_STORIES.md).

Runs the REAL Phase 5 code (mlops.tenant_drift over mlops.drift, gate 4.1's
PSI/KS), not a reimplementation, matching every prior drill's convention. One
tenant's feature genuinely shifts; nine stable tenants dilute it in the pooled
distribution. The per-tenant check alerts on exactly that tenant; the
same-fixture global pool, computed here so the contrast is measured and not
asserted, stays below the alert threshold. This is acceptance criterion (c) and
the grounding for incident P4.

Exit 0 only if the tenant alerts AND the global pool does not. Transcript to
docs/drills/M_drift_hidden.md.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from mlops.drift import check_input_drift  # noqa: E402
from mlops.tenant_drift import check_tenant_drift, drifted_tenants, pooled_windows  # noqa: E402

TRANSCRIPT = REPO_ROOT / "docs" / "drills" / "M_drift_hidden.md"
N_STABLE = 9


def _stable() -> pd.DataFrame:
    return pd.DataFrame({"feature_x": np.linspace(0, 10, 60)})


def _shifted() -> pd.DataFrame:
    return pd.DataFrame({"feature_x": np.linspace(7, 17, 60)})


def main() -> int:
    windows = {"tenant_a": (_stable(), _shifted())}
    for i in range(N_STABLE):
        windows[f"tenant_{i:02d}"] = (_stable(), _stable())

    per_tenant = check_tenant_drift(windows)
    alerted = drifted_tenants(per_tenant)
    a_feat = {r.feature: r for r in per_tenant["tenant_a"]}["feature_x"]

    ref, cur = pooled_windows(windows)
    pooled = {r.feature: r for r in check_input_drift(ref, cur)}["feature_x"]

    tenant_ok = alerted == ["tenant_a"] and a_feat.drifted
    pool_ok = not pooled.drifted
    all_ok = tenant_ok and pool_ok

    lines = [
        "# Drill M-drift-hidden transcript: per-tenant vs global-pool drift "
        f"({datetime.now(UTC).isoformat()})",
        "",
        f"Fixture: 1 tenant with a real feature shift + {N_STABLE} stable tenants "
        "(the same windows feed both code paths).",
        "",
        "## 1. Per-tenant check (gate 5.5) alerts on exactly the shifted tenant",
        f"PASSED: {tenant_ok}",
        f"alerted_tenants={alerted} tenant_a psi={a_feat.psi:.4f} "
        f"ks={a_feat.ks:.4f} drifted={a_feat.drifted}",
        "",
        "## 2. Same-fixture global pool averages the shift away (incident P4)",
        f"PASSED: {pool_ok}",
        f"pooled psi={pooled.psi:.4f} ks={pooled.ks:.4f} drifted={pooled.drifted} "
        f"(alert threshold psi>=0.25); the single tenant's shift is diluted below the bar",
        "",
        "VERDICT: "
        + (
            "PASS (per-tenant partitioning catches what the global average hides)"
            if all_ok
            else "FAIL"
        ),
    ]
    TRANSCRIPT.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

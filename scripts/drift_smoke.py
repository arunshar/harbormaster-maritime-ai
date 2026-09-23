"""Gate 4.1 smoke: run check_input_drift against the committed fixture pair.

Usage: .venv/bin/python scripts/drift_smoke.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mlops.drift import check_input_drift

REFERENCE = pd.DataFrame({"feature_a": np.linspace(0, 10, 50), "feature_b": np.linspace(0, 10, 50)})
CURRENT_DRIFTED = pd.DataFrame(
    {"feature_a": np.linspace(0, 10, 50), "feature_b": np.linspace(5, 15, 50)}
)


def main() -> int:
    results = check_input_drift(REFERENCE, CURRENT_DRIFTED)
    ok = True
    for r in results:
        tag = "DRIFTED" if r.drifted else "stable"
        print(f"  {r.feature}: psi={r.psi:.4f} ks={r.ks:.4f} pvalue={r.ks_pvalue:.2e} [{tag}]")
    by_name = {r.feature: r for r in results}
    if by_name["feature_a"].drifted or not by_name["feature_b"].drifted:
        ok = False
    print("[PASS]" if ok else "[FAIL]", "feature_a stable, feature_b flagged, as expected")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

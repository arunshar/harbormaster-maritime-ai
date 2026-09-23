"""Phase 4 gate 4.1: input-drift detection.

Ported (vendored, not cross-repo-imported, per this repo's external-reuse
convention) from serving/monitoring/drift.py in the separate MIRROR project: Population
Stability Index plus a two-sample Kolmogorov-Smirnov test with its asymptotic
p-value, both pure NumPy. population_stability_index and ks_statistic are
copied verbatim. The asymptotic p-value (ks_pvalue and _kolmogorov_sf) is a
new implementation written from the mathematical definition of the
Kolmogorov distribution. check_input_drift is adapted to accept a pandas
DataFrame directly (Harbormaster's feature frames are the gate 3.3
training-set export's columns, a DataFrame, not a bare array), deriving
feature names from the DataFrame's own columns rather than a
separately-supplied config.feature_names list.

Reference window (locked by gate 4.1): the feature
columns of the most recent accepted gate 3.3 training-set export. Supplying
that snapshot is the caller's job (gate 4.6's scheduled Lambda does it for
real; the drills and unit tests pass a fixture directly), matching the
injected-I/O convention this repo uses everywhere (WatchlistLookup,
PiDpmClient): this module only computes, it never fetches.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

__all__ = [
    "DriftConfig",
    "DriftResult",
    "population_stability_index",
    "ks_statistic",
    "ks_pvalue",
    "check_input_drift",
]


@dataclass
class DriftConfig:
    """Thresholds for the input-drift check, identical to MIRROR's defaults.

    PSI convention (industry standard): < 0.1 stable, 0.1-0.25 moderate
    shift, > 0.25 significant shift. KS only corroborates once PSI is
    already at least in the warn band (a raw p-value < 0.05 fires on
    statistically-identical samples at large n, so significance alone is
    never treated as drift).
    """

    psi_warn: float = 0.1
    psi_alert: float = 0.25
    ks_pvalue_alert: float = 0.05
    n_bins: int = 10


@dataclass
class DriftResult:
    feature: str
    psi: float
    ks: float
    ks_pvalue: float
    drifted: bool


def population_stability_index(reference, current, n_bins: int = 10, eps: float = 1e-6) -> float:
    """Population Stability Index between a reference and a current sample.

    Bins are equal-frequency quantile edges of the reference, with the outer
    edges opened to +/- inf so the current sample is always covered.
    """
    ref = np.asarray(reference, dtype=float).ravel()
    cur = np.asarray(current, dtype=float).ravel()
    if ref.size == 0 or cur.size == 0:
        return 0.0
    quantiles = np.linspace(0.0, 1.0, n_bins + 1)
    edges = np.unique(np.quantile(ref, quantiles)).astype(float)
    if edges.size < 2:
        return 0.0
    edges[0], edges[-1] = -np.inf, np.inf
    ref_hist, _ = np.histogram(ref, bins=edges)
    cur_hist, _ = np.histogram(cur, bins=edges)
    ref_frac = np.clip(ref_hist / ref.size, eps, None)
    cur_frac = np.clip(cur_hist / cur.size, eps, None)
    return float(np.sum((cur_frac - ref_frac) * np.log(cur_frac / ref_frac)))


def ks_statistic(reference, current) -> float:
    """Two-sample Kolmogorov-Smirnov statistic (max gap between empirical CDFs)."""
    ref = np.sort(np.asarray(reference, dtype=float).ravel())
    cur = np.sort(np.asarray(current, dtype=float).ravel())
    if ref.size == 0 or cur.size == 0:
        return 0.0
    grid = np.concatenate([ref, cur])
    cdf_ref = np.searchsorted(ref, grid, side="right") / ref.size
    cdf_cur = np.searchsorted(cur, grid, side="right") / cur.size
    return float(np.max(np.abs(cdf_ref - cdf_cur)))


def _kolmogorov_sf(lam: float) -> float:
    """Survival function Q(lam) = P(K > lam) of the Kolmogorov distribution.

    The distribution (Kolmogorov, 1933) has two standard series forms, and
    this function evaluates whichever one converges quickly for ``lam``:

    - for large ``lam``: Q = 2 * sum_{j>=1} (-1)**(j-1) * exp(-2 * j**2 * lam**2)
    - for small ``lam``: Q = 1 - sqrt(2*pi) / lam
      * sum_{j>=1} exp(-(2*j - 1)**2 * pi**2 / (8 * lam**2))

    Both sums use a fixed number of terms, far more than double precision
    needs over the range where each form is used. Returns a value in [0, 1].
    """
    if lam <= 0.0:
        return 1.0
    if lam < 0.1:
        # P(K <= 0.1) is below 1e-100, so Q rounds to exactly 1.0 in doubles.
        return 1.0
    if lam < 1.0:
        k = np.arange(1, 21, dtype=float)
        odd_sq = (2.0 * k - 1.0) ** 2
        cdf = np.sqrt(2.0 * np.pi) / lam * np.sum(np.exp(-odd_sq * np.pi**2 / (8.0 * lam * lam)))
        q = 1.0 - cdf
    else:
        k = np.arange(1, 101, dtype=float)
        signs = np.where(k % 2.0 == 1.0, 1.0, -1.0)
        q = 2.0 * np.sum(signs * np.exp(-2.0 * k * k * lam * lam))
    return float(min(max(q, 0.0), 1.0))


def ks_pvalue(d: float, n: int, m: int) -> float:
    """Asymptotic p-value for the two-sample KS statistic ``d``.

    Evaluates the Kolmogorov survival function at the effective sample size
    with the small-sample correction of Stephens (1970),
    lam = (sqrt(ne) + 0.12 + 0.11 / sqrt(ne)) * d with ne = n*m / (n + m).
    Returns a value in [0, 1].
    """
    n, m = int(n), int(m)
    if n == 0 or m == 0:
        return 1.0
    en = np.sqrt(n * m / (n + m))
    lam = (en + 0.12 + 0.11 / en) * float(d)
    return _kolmogorov_sf(lam)


def check_input_drift(
    reference: pd.DataFrame,
    current: pd.DataFrame,
    config: DriftConfig | None = None,
) -> list[DriftResult]:
    """Per-feature input drift between two feature-column DataFrames.

    Only columns present in both frames are checked (a column dropped or
    added between snapshots is a schema change, not a drift signal this
    check reports on); numeric columns are compared as-is, non-numeric
    columns are skipped (drift on categorical corridor/quality-flag columns
    is out of this gate's scope).
    """
    config = config or DriftConfig()
    shared = [c for c in reference.columns if c in current.columns]
    results: list[DriftResult] = []
    for name in shared:
        rcol = reference[name]
        ccol = current[name]
        if not (pd.api.types.is_numeric_dtype(rcol) and pd.api.types.is_numeric_dtype(ccol)):
            continue
        psi = population_stability_index(rcol, ccol, n_bins=config.n_bins)
        ks = ks_statistic(rcol, ccol)
        pval = ks_pvalue(ks, rcol.size, ccol.size)
        drifted = bool(
            psi >= config.psi_alert or (psi >= config.psi_warn and pval < config.ks_pvalue_alert)
        )
        results.append(DriftResult(feature=name, psi=psi, ks=ks, ks_pvalue=pval, drifted=drifted))
    return results

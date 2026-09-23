"""Phase 4 gate 4.2: calibration-drift watch.

Re-calls mlops.holdout_gate.calibration_ratio directly against a rolling
window of HITL-labeled outcomes; it does not re-port the formula. The reason,
stated in docs/phases/PHASE_4_SKETCH.md and worth repeating here: a model
whose serving-time calibration has drifted outside the same [0.8, 1.2] band
its own promotion gate (gate 3.7) required is, by definition, no longer the
model that gate approved, so the check has to be the identical function, not
a lookalike.

`labeled_outcomes` is an injected iterable of (error, variance) pairs; the
derivation of those pairs from real hitl_queue rows is the caller's job (a
pure function over Postgres rows, kept separate so this module has no I/O),
matching the WatchlistLookup/PiDpmClient convention used throughout this repo.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from mlops.holdout_gate import calibration_ratio

DEFAULT_BAND: tuple[float, float] = (0.8, 1.2)
DEFAULT_MIN_LABELED = 20


@dataclass(frozen=True)
class CalibrationWatchResult:
    ratio: float
    band: tuple[float, float]
    in_band: bool
    n_labeled: int
    response: Literal["none", "supervised_retrain"]
    reason: str


def watch_calibration(
    labeled_outcomes,
    *,
    band: tuple[float, float] = DEFAULT_BAND,
    min_labeled: int = DEFAULT_MIN_LABELED,
) -> CalibrationWatchResult:
    """labeled_outcomes: iterable of (error, variance) pairs from HITL-labeled
    traces. Fewer than min_labeled pairs is an insufficient-data guard: the
    decision-table's ~90% "plain supervised retrain" response is only made
    once the ratio is estimated on enough data to trust, never on a handful
    of labels."""
    pairs = list(labeled_outcomes)
    n = len(pairs)
    if n < min_labeled:
        return CalibrationWatchResult(
            ratio=float("nan"),
            band=band,
            in_band=True,
            n_labeled=n,
            response="none",
            reason=f"only {n} labeled outcomes, need >= {min_labeled} before trusting a ratio",
        )

    errors = [e for e, _ in pairs]
    variances = [v for _, v in pairs]
    ratio = calibration_ratio(errors, variances)
    lo, hi = band
    in_band = lo <= ratio <= hi
    if in_band:
        return CalibrationWatchResult(
            ratio=ratio,
            band=band,
            in_band=True,
            n_labeled=n,
            response="none",
            reason=f"calibration_ratio {ratio:.4f} within band {band}",
        )
    return CalibrationWatchResult(
        ratio=ratio,
        band=band,
        in_band=False,
        n_labeled=n,
        response="supervised_retrain",
        reason=f"calibration_ratio {ratio:.4f} outside band {band}",
    )

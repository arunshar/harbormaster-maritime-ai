"""Phase 1 SLOs (gate G7).

The service-level objectives the Phase 1 slice is held to, plus a pure evaluator
that turns measured numbers into pass/breach verdicts. The e2e acceptance test
(1.8) and the dashboard narrative use these; targets mirror PHASE_1.md 1.7.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from app.burn_rate import (
    Bucket,
    BurnRateResult,
    BurnStatus,
    evaluate_burn_rate,
)


class Compare(StrEnum):
    LTE = "<="  # measured must be <= target
    GTE = ">="  # measured must be >= target


@dataclass(frozen=True)
class Slo:
    name: str
    target: float
    compare: Compare
    unit: str


PHASE1_SLOS: tuple[Slo, ...] = (
    Slo("score_success_ratio", 0.999, Compare.GTE, "ratio"),
    Slo("score_kernel_p95_ms", 300.0, Compare.LTE, "ms"),
    Slo("replay_to_hitl_p95_s", 10.0, Compare.LTE, "s"),
)


@dataclass(frozen=True)
class SloResult:
    slo: Slo
    measured: float
    ok: bool


def evaluate(
    measurements: dict[str, float], slos: tuple[Slo, ...] = PHASE1_SLOS
) -> list[SloResult]:
    """Turn measured values into pass/breach results. A missing measurement breaches."""
    out: list[SloResult] = []
    for slo in slos:
        if slo.name not in measurements:
            out.append(SloResult(slo, float("nan"), ok=False))
            continue
        m = measurements[slo.name]
        ok = m <= slo.target if slo.compare is Compare.LTE else m >= slo.target
        out.append(SloResult(slo, m, ok=ok))
    return out


def all_ok(results: list[SloResult]) -> bool:
    return all(r.ok for r in results)


# --- Windowed burn-rate view of the success SLO (DR-13) --------------------
#
# The static `score_success_ratio` SLO above is a single point-in-time verdict:
# it says whether the measured success ratio cleared the target in one snapshot.
# The burn-rate view answers the operational question the static check cannot:
# is the error budget being consumed fast enough to page and roll back? Both run
# off the same 99.9% target; they are complementary, not redundant. This is the
# real signal `mlops/promote.py`'s burn_check consults, replacing the hardcoded
# False that DR-13 admitted stood in for it through Phase 3.

SCORE_SUCCESS_TARGET: float = 0.999


def evaluate_success_burn_rate(
    buckets: Sequence[Bucket],
    *,
    bucket_seconds: float,
    target: float = SCORE_SUCCESS_TARGET,
) -> BurnRateResult:
    """Multi-window burn rate for the score-success SLO over an injected series.

    ``buckets`` is an aggregated ``(timestamp, total, bad)`` series where a
    "bad" event is a failed score. Returns the full BurnRateResult, whose
    ``status`` (ok/warning/page) and ``should_rollback`` drive both the alarm
    narrative and the promotion state machine.
    """
    return evaluate_burn_rate(buckets, target=target, bucket_seconds=bucket_seconds)


def success_burn_status(
    buckets: Sequence[Bucket],
    *,
    bucket_seconds: float,
    target: float = SCORE_SUCCESS_TARGET,
) -> BurnStatus:
    """Convenience: just the overall BurnStatus for the score-success SLO."""
    return evaluate_success_burn_rate(buckets, bucket_seconds=bucket_seconds, target=target).status

"""Multi-window multi-burn-rate error-budget calculator (DR-13).

The real signal source behind ``mlops/promote.py``'s ``burn_check`` and behind
``serving/app/slo.py``'s ``score_success_ratio`` SLO, implementing the Google
SRE multi-window multi-burn-rate alerting pattern (SRE Workbook, ch. 5) that
DR-13 named from Phase 3 but did not build until now. Pure Python, no AWS: the
caller injects an aggregated ``(timestamp, total, bad)`` time series, so the
math is exercised identically by a local test and by the AWS demo window (a
thin CloudWatch adapter can feed the same series without touching this module).

Definitions used throughout:
- The SLO target is a success ratio, e.g. 0.999 (99.9%).
- The error budget is ``1 - target``, e.g. 0.001. This is the fraction of
  requests that are *allowed* to be bad over the SLO window.
- The burn rate over a window is the observed bad-event ratio in that window
  divided by the error budget. A burn rate of 1 exhausts the whole budget in
  exactly one SLO window; 14.4 exhausts it in 1/14.4 of the window.

Multi-window: a real outage is confirmed only when a long *and* a short window
both exceed the same burn-rate threshold. The short window makes the alert
reset quickly once the outage stops; the long window keeps a single bad minute
from paging. Fast and slow tiers cover both sudden outages and slow bleeds.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum

# The SLO window the budget is defined over. Google's canonical table is stated
# for a 30-day window; the burn-rate multipliers below are dimensionless (they
# are ratios of observed-bad to budget), so they hold for any window length.
# Kept explicit so the alert-exhaustion arithmetic in the docstrings is checkable.
SLO_WINDOW_DAYS: float = 30.0


class BurnStatus(StrEnum):
    OK = "ok"
    WARNING = "warning"
    PAGE = "page"


@dataclass(frozen=True)
class Bucket:
    """One aggregated observation: ``bad`` of ``total`` events were failures.

    ``timestamp`` is epoch seconds and only orders the series; the windows are
    counted in buckets, so buckets are assumed evenly spaced (one per
    ``bucket_seconds``). ``total == 0`` is a legal idle bucket (no traffic, no
    burn) and contributes nothing to either sum.
    """

    timestamp: float
    total: int
    bad: int


@dataclass(frozen=True)
class WindowBurn:
    """The burn rate observed over one window (``long`` or ``short`` of a tier)."""

    label: str
    window_seconds: float
    total: int
    bad: int
    bad_ratio: float
    burn_rate: float
    exceeds: bool


@dataclass(frozen=True)
class BurnAlertTier:
    """One row of the multi-window table: a threshold checked over two windows."""

    name: str  # "fast" | "slow" | "warn"
    burn_rate_threshold: float
    long_window_seconds: float
    short_window_seconds: float
    status: BurnStatus  # the status this tier raises when it fires


# --- The exact SRE thresholds this platform uses ---------------------------
#
# Google SRE Workbook multi-window multi-burn-rate table, stated for a 30-day
# SLO window. A tier fires only when BOTH its long and short windows exceed the
# threshold. Budget-consumed at fire time and time-to-exhaustion (for a 99.9%
# SLO / 0.1% budget over 30 days) are shown to make the choice auditable:
#
#   tier   burn  long / short   budget consumed at fire   exhausts budget in
#   fast   14.4x   1h / 5m       ~2%  of 30d budget        ~2.08 h   -> PAGE
#   slow    6x     6h / 30m      ~5%  of 30d budget        ~5    d   -> PAGE
#   warn    3x     1d / 2h       ~10% of 30d budget        ~10   d   -> WARNING
#
# Fast and slow both PAGE (a real, actionable outage that auto-rollback should
# catch); the 3x tier only WARNS (ticket-grade, not worth a rollback). A 1x/72h
# "info" row exists in the canonical table but is log-only, so it is omitted
# here rather than added as dead, never-actioned config.
_HOUR = 3600.0
DEFAULT_TIERS: tuple[BurnAlertTier, ...] = (
    BurnAlertTier(
        name="fast",
        burn_rate_threshold=14.4,
        long_window_seconds=1 * _HOUR,
        short_window_seconds=5 * 60.0,
        status=BurnStatus.PAGE,
    ),
    BurnAlertTier(
        name="slow",
        burn_rate_threshold=6.0,
        long_window_seconds=6 * _HOUR,
        short_window_seconds=30 * 60.0,
        status=BurnStatus.PAGE,
    ),
    BurnAlertTier(
        name="warn",
        burn_rate_threshold=3.0,
        long_window_seconds=24 * _HOUR,
        short_window_seconds=2 * _HOUR,
        status=BurnStatus.WARNING,
    ),
)


@dataclass(frozen=True)
class TierResult:
    tier: BurnAlertTier
    long_window: WindowBurn
    short_window: WindowBurn
    fired: bool  # both windows exceeded the threshold


@dataclass(frozen=True)
class BurnRateResult:
    target: float
    error_budget: float
    status: BurnStatus
    tiers: list[TierResult]

    @property
    def should_rollback(self) -> bool:
        """A promotion should revert when any PAGE-severity tier fires.

        WARNING never rolls back (ticket-grade); OK obviously never does. This
        is the single fact ``mlops/promote.py`` needs from the calculator.
        """
        return self.status is BurnStatus.PAGE

    def fired_tiers(self) -> list[str]:
        return [t.tier.name for t in self.tiers if t.fired]


def _window_burn(
    buckets: Sequence[Bucket],
    *,
    label: str,
    window_seconds: float,
    bucket_seconds: float,
    error_budget: float,
    threshold: float,
) -> WindowBurn:
    """Aggregate the most recent ``window_seconds`` of buckets into one burn rate.

    Buckets are assumed evenly spaced at ``bucket_seconds``; the window covers
    the last ``ceil(window_seconds / bucket_seconds)`` buckets. A window with no
    traffic has a bad ratio of 0 and therefore a burn rate of 0 (idle is not a
    breach), which is exactly why ``treat_missing_data`` on the real CloudWatch
    alarm is ``notBreaching``.
    """
    n = max(1, _ceil_div(window_seconds, bucket_seconds))
    tail = buckets[-n:]
    total = sum(b.total for b in tail)
    bad = sum(b.bad for b in tail)
    bad_ratio = (bad / total) if total > 0 else 0.0
    # error_budget > 0 is guaranteed by the caller (target < 1.0 is validated).
    burn_rate = bad_ratio / error_budget
    return WindowBurn(
        label=label,
        window_seconds=window_seconds,
        total=total,
        bad=bad,
        bad_ratio=bad_ratio,
        burn_rate=burn_rate,
        exceeds=burn_rate > threshold,
    )


def _ceil_div(numerator: float, denominator: float) -> int:
    if denominator <= 0:
        raise ValueError("bucket_seconds must be positive")
    return int(-(-numerator // denominator))


def evaluate_burn_rate(
    buckets: Sequence[Bucket],
    *,
    target: float,
    bucket_seconds: float,
    tiers: tuple[BurnAlertTier, ...] = DEFAULT_TIERS,
) -> BurnRateResult:
    """Compute per-tier multi-window burn rates and an overall BurnStatus.

    ``target`` is a success ratio in (0, 1), e.g. 0.999. The error budget is
    ``1 - target``. Each tier fires only if BOTH its long and short window burn
    rates strictly exceed the tier threshold. The overall status is the most
    severe status among fired tiers (PAGE > WARNING > OK).
    """
    if not 0.0 < target < 1.0:
        raise ValueError(f"target must be in (0, 1), got {target}")
    if bucket_seconds <= 0:
        raise ValueError(f"bucket_seconds must be positive, got {bucket_seconds}")
    error_budget = 1.0 - target

    tier_results: list[TierResult] = []
    status = BurnStatus.OK
    for tier in tiers:
        long_w = _window_burn(
            buckets,
            label=f"{tier.name}_long",
            window_seconds=tier.long_window_seconds,
            bucket_seconds=bucket_seconds,
            error_budget=error_budget,
            threshold=tier.burn_rate_threshold,
        )
        short_w = _window_burn(
            buckets,
            label=f"{tier.name}_short",
            window_seconds=tier.short_window_seconds,
            bucket_seconds=bucket_seconds,
            error_budget=error_budget,
            threshold=tier.burn_rate_threshold,
        )
        fired = long_w.exceeds and short_w.exceeds
        tier_results.append(
            TierResult(tier=tier, long_window=long_w, short_window=short_w, fired=fired)
        )
        if fired:
            status = _max_status(status, tier.status)

    return BurnRateResult(
        target=target,
        error_budget=error_budget,
        status=status,
        tiers=tier_results,
    )


_STATUS_RANK: dict[BurnStatus, int] = {
    BurnStatus.OK: 0,
    BurnStatus.WARNING: 1,
    BurnStatus.PAGE: 2,
}


def _max_status(a: BurnStatus, b: BurnStatus) -> BurnStatus:
    return a if _STATUS_RANK[a] >= _STATUS_RANK[b] else b


# --- Metrics-adapter seam --------------------------------------------------
#
# The calculator takes a pre-aggregated series so tests inject it directly. In
# the AWS demo window the same series comes from CloudWatch: a caller wires a
# provider matching this signature (e.g. a GetMetricData call bucketing the
# serving success/failure EMF metrics) and passes its output straight in. No
# live-CloudWatch dependency is hard-required here; this is only the type seam.
SeriesProvider = Callable[[], Sequence[Bucket]]

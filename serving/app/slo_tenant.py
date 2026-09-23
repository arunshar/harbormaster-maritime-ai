"""Per-tenant SLO tiers over the existing Slo and evaluate shapes.

This module wraps the existing SLO code and does not replace it. The three
tiers below are tuples shaped like PHASE1_SLOS, keyed by tenant tier. The pure
pass-or-breach signature of ``slo.evaluate`` is unchanged, and this module
calls it once per tenant per measurement window. The burn-rate calculator
(serving/app/burn_rate.py) and ``make_burn_check`` in ``mlops/promote.py`` are
reused as they are. This module adds only the tenant dimension, the tier
tables, and the per-tenant wiring. The calculator's own boundary and formula
tests live in serving/tests/test_burn_rate.py and are not duplicated here.

The real-time tier copies the Phase 1 SLO table exactly, because those targets
were the Phase 1 acceptance bar. The near-real-time and batch tiers are design
targets and not measured figures. No tier has a live measurement, and nothing
in this module claims one.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum

from app.burn_rate import Bucket
from app.slo import PHASE1_SLOS, Compare, Slo, SloResult, evaluate

_SUCCESS_SLO_NAME = "score_success_ratio"


class TenantTier(StrEnum):
    REAL_TIME = "real_time"
    NEAR_REAL_TIME = "near_real_time"
    BATCH = "batch"


# The three tier tables: real-time, near-real-time, and batch.
# Golden copies of the targets are pinned in mlops/fixtures/expectations.json
# ("tenant_slo_tiers"); a test fails if the two drift.
TIER_SLOS: dict[TenantTier, tuple[Slo, ...]] = {
    # Real-time IS Phase 1's table: the platform's existing bar, unchanged.
    TenantTier.REAL_TIME: PHASE1_SLOS,
    TenantTier.NEAR_REAL_TIME: (
        Slo("score_success_ratio", 0.995, Compare.GTE, "ratio"),
        Slo("score_kernel_p95_ms", 1_000.0, Compare.LTE, "ms"),
        Slo("replay_to_hitl_p95_s", 60.0, Compare.LTE, "s"),
    ),
    TenantTier.BATCH: (
        Slo("score_success_ratio", 0.99, Compare.GTE, "ratio"),
        Slo("score_kernel_p95_ms", 5_000.0, Compare.LTE, "ms"),
        Slo("replay_to_hitl_p95_s", 900.0, Compare.LTE, "s"),
    ),
}


def tier_success_target(tier: TenantTier) -> float:
    """The success-ratio target a tier feeds to ``evaluate_burn_rate``.

    This is the ONE number that changes per tier in the burn-rate wiring; the
    calculator, thresholds, and windows stay the Google-SRE defaults.
    """
    for slo in TIER_SLOS[tier]:
        if slo.name == _SUCCESS_SLO_NAME:
            return slo.target
    raise KeyError(f"tier {tier} has no {_SUCCESS_SLO_NAME} SLO")


@dataclass(frozen=True)
class PerTenantSlo:
    """One tenant's SLO surface: tier table selection plus the existing pure
    ``evaluate`` called with that table. Nothing else changes shape."""

    tenant_id: str
    tier: TenantTier

    @property
    def slos(self) -> tuple[Slo, ...]:
        return TIER_SLOS[self.tier]

    def evaluate(self, measurements: dict[str, float]) -> list[SloResult]:
        """The existing evaluate(), once per tenant per measurement window."""
        return evaluate(measurements, self.slos)

    def success_target(self) -> float:
        return tier_success_target(self.tier)


# (tenant_id, canary_weight) -> that tenant's aggregated (timestamp, total, bad)
# series. The per-tenant analogue of burn_rate.SeriesProvider: partitioning by
# tenant happens in the provider (the caller queries metrics WHERE tenant_id =
# ...), never inside the calculator.
TenantSeriesProvider = Callable[[str, int], Sequence[Bucket]]


def tenant_series_provider(
    provider: TenantSeriesProvider, tenant_id: str
) -> Callable[[int], Sequence[Bucket]]:
    """Fix the tenant dimension, yielding the (weight) -> series shape that the
    existing ``mlops.promote.make_burn_check`` takes unchanged."""

    def series_for_weight(weight: int) -> Sequence[Bucket]:
        return provider(tenant_id, weight)

    return series_for_weight


def make_tenant_burn_check(
    provider: TenantSeriesProvider,
    *,
    tenant: PerTenantSlo,
    bucket_seconds: float,
) -> Callable[[int], bool]:
    """Per-tenant wiring of the EXISTING ``make_burn_check``: one burn check per
    tenant, fed that tenant's own partitioned series and that tenant's tier
    target. The calculator is untouched; only the two arguments vary.
    """
    # Lazy import, deliberately: the serving wheel ships without the mlops
    # package (pyproject packages = serving/app + streaming/features), and only
    # promotion-time callers, which run where mlops is installed, reach this
    # function. Mirrors registry.py's lazy-cdc-on-the-PG-path convention.
    from mlops.promote import make_burn_check

    return make_burn_check(
        tenant_series_provider(provider, tenant.tenant_id),
        bucket_seconds=bucket_seconds,
        target=tenant.success_target(),
    )

"""Unit tests for the multi-window multi-burn-rate calculator (DR-13).

Hermetic, seeded (deterministic, no randomness), no network, no AWS. The whole
point of the module is that the series is injected, so these tests feed closed-
form series and check exact math and exact status transitions. A reverted
calculator (one that always returned OK, or that dropped the multi-window AND)
fails these.
"""

from __future__ import annotations

import math

from app.burn_rate import (
    DEFAULT_TIERS,
    Bucket,
    BurnStatus,
    evaluate_burn_rate,
)

# One bucket per minute keeps the arithmetic legible: a 1h window is 60 buckets,
# a 5m window is 5 buckets, 6h is 360, 30m is 30, 24h is 1440, 2h is 120.
BUCKET_SECONDS = 60.0
TARGET = 0.999  # 99.9% -> error budget 0.001


def _steady(n: int, total_per_bucket: int, bad_per_bucket: int) -> list[Bucket]:
    """A flat series: n buckets each with the same total/bad counts."""
    return [
        Bucket(timestamp=float(i * 60), total=total_per_bucket, bad=bad_per_bucket)
        for i in range(n)
    ]


def test_healthy_series_does_not_page_or_warn():
    # 1000 scores/min, 0 bad, for a full day. Every window sees a 0 bad ratio.
    series = _steady(1440, total_per_bucket=1000, bad_per_bucket=0)
    result = evaluate_burn_rate(series, target=TARGET, bucket_seconds=BUCKET_SECONDS)
    assert result.status is BurnStatus.OK
    assert result.should_rollback is False
    assert result.fired_tiers() == []


def test_error_budget_is_exact():
    series = _steady(60, total_per_bucket=1000, bad_per_bucket=0)
    result = evaluate_burn_rate(series, target=0.999, bucket_seconds=BUCKET_SECONDS)
    assert math.isclose(result.error_budget, 0.001, rel_tol=0, abs_tol=1e-12)
    # target 0.995 -> budget 0.005
    result2 = evaluate_burn_rate(series, target=0.995, bucket_seconds=BUCKET_SECONDS)
    assert math.isclose(result2.error_budget, 0.005, rel_tol=0, abs_tol=1e-12)


def test_burn_rate_math_is_exact_on_closed_form_input():
    # Every bucket: 1000 total, 20 bad -> observed bad ratio 0.02 in every
    # window. Budget 0.001 -> burn rate 0.02 / 0.001 = 20.0 exactly, in all
    # windows. 20 > 14.4 (fast) and > 6 (slow), so both windows of both PAGE
    # tiers exceed; warn (3x) too.
    series = _steady(1440, total_per_bucket=1000, bad_per_bucket=20)
    result = evaluate_burn_rate(series, target=TARGET, bucket_seconds=BUCKET_SECONDS)
    for tier in result.tiers:
        assert math.isclose(tier.long_window.burn_rate, 20.0, abs_tol=1e-9)
        assert math.isclose(tier.short_window.burn_rate, 20.0, abs_tol=1e-9)
        assert tier.long_window.bad_ratio == 0.02
    assert result.status is BurnStatus.PAGE
    assert result.should_rollback is True


def test_fast_burn_outage_pages_and_recommends_rollback():
    # Long healthy history, then a sharp 5-minute outage at 100% failure.
    # Fast tier: 1h long window and 5m short window.
    #   - 5m short window: all 5 recent buckets are 100% bad -> ratio 1.0 ->
    #     burn 1000x, far over 14.4.
    #   - 1h long window: 55 healthy + 5 fully-bad buckets. bad = 5*1000 = 5000,
    #     total = 60*1000 = 60000 -> ratio 0.0833 -> burn 83.3x > 14.4.
    # Both fast windows exceed -> fast tier fires -> PAGE + rollback.
    healthy = _steady(120, total_per_bucket=1000, bad_per_bucket=0)
    outage = _steady(5, total_per_bucket=1000, bad_per_bucket=1000)
    # re-timestamp the outage to follow the healthy run
    outage = [Bucket(timestamp=float((120 + i) * 60), total=1000, bad=1000) for i in range(5)]
    series = healthy + outage

    result = evaluate_burn_rate(series, target=TARGET, bucket_seconds=BUCKET_SECONDS)
    assert result.status is BurnStatus.PAGE
    assert result.should_rollback is True
    assert "fast" in result.fired_tiers()


def test_a_single_bad_minute_does_not_page():
    # The multi-window guard: one 100%-bad minute after a long healthy history.
    # The 5m short fast window still catches it (1 of 5 buckets bad -> ratio 0.2
    # -> burn 200x > 14.4), BUT the 1h long fast window sees 1 bad of 60 buckets
    # -> ratio ~0.0167 -> burn ~16.7x. That is over 14.4, so fast would fire on
    # a single bad minute at this volume. Use a lower-volume blip so the long
    # window stays under threshold and the AND correctly suppresses the page.
    #
    # 60 healthy buckets of 1000, then ONE bucket with 5 bad of 1000.
    # 5m short fast window: last 5 buckets = 4 healthy + 1 blip -> 5 bad /
    #   5000 = 0.001 ratio -> burn 1.0x, under 14.4. AND fails -> no fast page.
    healthy = _steady(60, total_per_bucket=1000, bad_per_bucket=0)
    blip = [Bucket(timestamp=float(60 * 60), total=1000, bad=5)]
    series = healthy + blip
    result = evaluate_burn_rate(series, target=TARGET, bucket_seconds=BUCKET_SECONDS)
    assert result.status is BurnStatus.OK
    assert result.should_rollback is False


def test_slow_burn_bleed_pages_at_the_slow_tier_not_the_fast_tier():
    # A steady low-grade bleed: 8 bad per 1000 (0.008 ratio) for 6+ hours.
    # burn = 0.008 / 0.001 = 8.0x, in every window.
    #   - Fast tier threshold 14.4: 8.0 < 14.4 -> fast does NOT fire.
    #   - Slow tier threshold 6.0: 8.0 > 6.0 in both 6h and 30m windows -> slow
    #     fires -> PAGE.
    #   - Warn tier threshold 3.0: 8.0 > 3.0 -> also fires (WARNING), but PAGE
    #     dominates.
    series = _steady(400, total_per_bucket=1000, bad_per_bucket=8)  # ~6.6h of minutes
    result = evaluate_burn_rate(series, target=TARGET, bucket_seconds=BUCKET_SECONDS)
    fired = result.fired_tiers()
    assert "slow" in fired
    assert "fast" not in fired
    assert result.status is BurnStatus.PAGE
    assert result.should_rollback is True


def test_warning_tier_bleed_warns_but_does_not_rollback():
    # A 4x bleed: 4 bad per 1000 (0.004 ratio) sustained over a day.
    # burn = 4.0x. Fast (14.4) no, Slow (6.0) no, Warn (3.0) yes in both its
    # 24h and 2h windows -> WARNING, and crucially NOT a rollback.
    series = _steady(1440, total_per_bucket=1000, bad_per_bucket=4)
    result = evaluate_burn_rate(series, target=TARGET, bucket_seconds=BUCKET_SECONDS)
    assert result.fired_tiers() == ["warn"]
    assert result.status is BurnStatus.WARNING
    assert result.should_rollback is False


def test_idle_window_with_no_traffic_never_breaches():
    # Zero traffic everywhere: total 0, bad 0. bad_ratio must be 0, burn 0, OK.
    series = [Bucket(timestamp=float(i * 60), total=0, bad=0) for i in range(1440)]
    result = evaluate_burn_rate(series, target=TARGET, bucket_seconds=BUCKET_SECONDS)
    assert result.status is BurnStatus.OK
    for tier in result.tiers:
        assert tier.long_window.bad_ratio == 0.0
        assert tier.long_window.burn_rate == 0.0


def test_window_bucket_counts_match_the_tier_definitions():
    # 1 bad in a series long enough to fill every window; check each window
    # aggregated the right number of buckets (window_seconds / 60).
    series = _steady(2000, total_per_bucket=100, bad_per_bucket=1)
    result = evaluate_burn_rate(series, target=TARGET, bucket_seconds=BUCKET_SECONDS)
    by_name = {t.tier.name: t for t in result.tiers}
    # fast long = 1h = 60 buckets * 100 = 6000 total
    assert by_name["fast"].long_window.total == 6000
    assert by_name["fast"].short_window.total == 500  # 5m = 5 buckets
    assert by_name["slow"].long_window.total == 36000  # 6h = 360 buckets
    assert by_name["slow"].short_window.total == 3000  # 30m = 30 buckets
    assert by_name["warn"].long_window.total == 144000  # 24h = 1440 buckets
    assert by_name["warn"].short_window.total == 12000  # 2h = 120 buckets


def test_invalid_target_is_rejected():
    series = _steady(10, 100, 0)
    for bad_target in (0.0, 1.0, -0.1, 1.5):
        try:
            evaluate_burn_rate(series, target=bad_target, bucket_seconds=BUCKET_SECONDS)
        except ValueError:
            continue
        raise AssertionError(f"target {bad_target} should have raised ValueError")


def test_default_tiers_are_the_documented_sre_thresholds():
    by_name = {t.name: t for t in DEFAULT_TIERS}
    assert by_name["fast"].burn_rate_threshold == 14.4
    assert by_name["fast"].long_window_seconds == 3600
    assert by_name["fast"].short_window_seconds == 300
    assert by_name["fast"].status is BurnStatus.PAGE
    assert by_name["slow"].burn_rate_threshold == 6.0
    assert by_name["slow"].long_window_seconds == 21600
    assert by_name["slow"].short_window_seconds == 1800
    assert by_name["slow"].status is BurnStatus.PAGE
    assert by_name["warn"].burn_rate_threshold == 3.0
    assert by_name["warn"].status is BurnStatus.WARNING

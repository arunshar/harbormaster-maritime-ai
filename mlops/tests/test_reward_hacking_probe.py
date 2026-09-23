"""Gate 4.5: mlops/reward_hacking_probe.py."""

from __future__ import annotations

from mlops.preference_builder import RewardBreakdown, RewardWeights
from mlops.reward_hacking_probe import run_reward_hacking_probe


def _rewards(total: float, structural: float, n: int) -> list[RewardBreakdown]:
    return [
        RewardBreakdown(total=total, structural=structural, shaping=0.0, data=0.0, pref=0.0)
        for _ in range(n)
    ]


def test_reward_up_and_violations_up_is_blocked():
    baseline = _rewards(total=5.0, structural=0.5, n=10)
    # higher reward, negative (violating) structural term
    candidate = _rewards(total=8.0, structural=-1.0, n=10)
    result = run_reward_hacking_probe(baseline, candidate)
    assert result.blocked is True
    assert result.reason is not None


def test_reward_up_with_violations_flat_passes():
    baseline = _rewards(total=5.0, structural=0.5, n=10)
    candidate = _rewards(total=8.0, structural=0.5, n=10)
    result = run_reward_hacking_probe(baseline, candidate)
    assert result.blocked is False


def test_reward_up_with_violations_down_passes():
    baseline = _rewards(total=5.0, structural=-0.5, n=10)
    candidate = _rewards(total=8.0, structural=0.5, n=10)
    result = run_reward_hacking_probe(baseline, candidate)
    assert result.blocked is False


def test_reward_down_passes_regardless_of_violations():
    baseline = _rewards(total=8.0, structural=0.5, n=10)
    candidate = _rewards(total=5.0, structural=-1.0, n=10)  # worse reward AND more violations
    result = run_reward_hacking_probe(baseline, candidate)
    assert result.blocked is False


def test_empty_streams_do_not_crash_and_never_block():
    result = run_reward_hacking_probe([], [])
    assert result.blocked is False
    assert result.baseline_mean_reward == 0.0
    assert result.candidate_mean_reward == 0.0


def test_custom_structural_violation_threshold_is_respected():
    baseline = _rewards(total=5.0, structural=0.05, n=10)
    candidate = _rewards(total=8.0, structural=0.02, n=10)
    # Reward rises (8.0 > 5.0) while the structural term slips (0.02 < 0.05):
    # the magnitude check (added by the rate-matched-severity fix below)
    # blocks this regardless of structural_violation_threshold, since
    # candidate_mean_structural < baseline_mean_structural on its own. This
    # is correct, not a regression. A reward-up candidate whose physics
    # margin degraded is exactly what the probe exists to catch, and the
    # rate-only comparison used to miss it whenever both sides landed on the
    # same side of the threshold.
    assert (
        run_reward_hacking_probe(baseline, candidate, structural_violation_threshold=0.0).blocked
        is True
    )
    assert (
        run_reward_hacking_probe(baseline, candidate, structural_violation_threshold=0.1).blocked
        is True
    )


# ---- adversarial: a rate-matched severity increase -------------------------
#
# Every test above (and drill L4, and the phase-4 e2e) builds a candidate the
# same way: a uniform per-sample shift in `structural` alone, which pushes the
# candidate's violation rate strictly above the baseline's. This test builds a
# candidate that raises the total reward and keeps the violation rate exactly
# equal to the baseline's, while its structural term still gets worse on
# average, to check that comparing the mean structural value catches what the
# rate comparison alone would miss:
#
#   1. On 18 of 20 samples its structural term sits exactly at the violation
#      threshold (0.0). The counter uses a strict `<`, so zero physics margin
#      is never counted as a violation, even though the baseline held a
#      healthy +0.5 margin on those same samples.
#   2. Its 2 genuinely violating samples are 20x more severe than the
#      baseline's (-4.0 vs -0.2), but there are exactly as many of them, so
#      candidate rate == baseline rate == 0.10 and the strict `>` rate
#      comparison never fires.
#   3. Every point of its mean-total win comes from the shaping/data/pref
#      terms (non-uniform per sample, so this is not the uniform shift
#      pattern), and its weighted structural contribution actually collapses.
#
# Totals are computed from the real RewardWeights (structural 5.0, others
# 1.0), not hand-set like the fixtures above, so the total genuinely is won
# through the shaping terms.

_W = RewardWeights()


def _weighted(structural: float, shaping: float, data: float, pref: float) -> RewardBreakdown:
    total = _W.structural * structural + _W.shaping * shaping + _W.data * data + _W.pref * pref
    return RewardBreakdown(
        total=total, structural=structural, shaping=shaping, data=data, pref=pref
    )


def _honest_baseline() -> list[RewardBreakdown]:
    # 18 healthy samples (+0.5 structural margin) and 2 mild violations
    # (-0.2): mean total 5.15, structural-violation rate 0.10, mean
    # structural +0.43.
    healthy = [_weighted(structural=0.5, shaping=1.0, data=1.0, pref=1.0) for _ in range(18)]
    mild = [_weighted(structural=-0.2, shaping=1.0, data=1.0, pref=1.0) for _ in range(2)]
    return healthy + mild


def _rate_matched_severity_shift() -> list[RewardBreakdown]:
    # 18 samples ride the threshold at structural == 0.0 exactly (never
    # counted by the strict `<`), with per-sample-varying inflated
    # shaping/data/pref; the 2 violations are severe (-4.0) but no more
    # NUMEROUS than baseline's: mean total ~6.17 (> 5.15), rate 0.10
    # (== baseline), mean structural -0.40.
    riders = [
        _weighted(
            structural=0.0,
            shaping=2.5 + 0.05 * (i % 5),
            data=2.9 - 0.05 * (i % 4),
            pref=2.6 + 0.05 * (i % 3),
        )
        for i in range(18)
    ]
    severe = [_weighted(structural=-4.0, shaping=3.0, data=3.0, pref=3.0) for _ in range(2)]
    return riders + severe


def test_rate_matched_severity_shift_preconditions_hold():
    # Guards the fixture so the test below stays meaningful: the candidate
    # really does win on mean total, really does match the baseline's
    # violation rate exactly, and really does collapse the structural term.
    baseline = _honest_baseline()
    candidate = _rate_matched_severity_shift()
    result = run_reward_hacking_probe(baseline, candidate)

    assert result.candidate_mean_reward > result.baseline_mean_reward
    assert (
        result.candidate_structural_violation_rate
        == result.baseline_structural_violation_rate
        == 0.10
    )
    baseline_mean_structural = sum(r.structural for r in baseline) / len(baseline)
    candidate_mean_structural = sum(r.structural for r in candidate) / len(candidate)
    assert baseline_mean_structural > 0.0
    # the structural term collapsed, rewards rose anyway
    assert candidate_mean_structural < 0.0


def test_probe_blocks_a_rate_matched_severity_increase():
    # Closed (adversarial-review addendum, fix applied 2026-07-04): the rate
    # comparison alone is magnitude-blind to a candidate that sits exactly at
    # the structural threshold on most samples while keeping its far-more-
    # severe violations at exactly the baseline's rate. The added
    # mean-structural-degradation check catches it. This candidate's mean
    # structural term collapses (see
    # test_rate_matched_severity_shift_preconditions_hold) even though its
    # violation RATE never moves.
    result = run_reward_hacking_probe(_honest_baseline(), _rate_matched_severity_shift())
    assert result.blocked is True
    assert result.reason is not None

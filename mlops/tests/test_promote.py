from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import pytest

from mlops.holdout_gate import HoldoutGateResult
from mlops.promote import CANARY_WEIGHTS, PromotionResult, run_promotion
from mlops.reward_hacking_probe import RewardHackingProbeResult
from mlops.shadow_diff import ShadowDiffResult

EXPECTATIONS = Path(__file__).parent.parent / "fixtures" / "expectations.json"

PASSING_GATE = HoldoutGateResult(
    auc=0.95, crps=0.2, calibration_ratio=1.0, passed=True, failures=[]
)
FAILING_GATE = HoldoutGateResult(
    auc=0.5, crps=0.2, calibration_ratio=1.0, passed=False, failures=["auc too low"]
)
CLEAN_SHADOW = ShadowDiffResult(mean_abs_diff=0.01, max_abs_diff=0.02, n_samples=100, passed=True)
DIRTY_SHADOW = ShadowDiffResult(mean_abs_diff=0.5, max_abs_diff=0.9, n_samples=100, passed=False)


class _Recorder:
    def __init__(self) -> None:
        self.weights_set: list[int] = []
        self.reverted = False

    def set_canary_weight(self, weight: int) -> None:
        self.weights_set.append(weight)

    def revert_to_champion(self) -> None:
        self.reverted = True


def test_a_failing_holdout_gate_never_reaches_shadow_or_canary():
    rec = _Recorder()
    result = run_promotion(
        holdout_result=FAILING_GATE,
        shadow_result=None,
        burn_check=lambda w: False,
        set_canary_weight=rec.set_canary_weight,
        revert_to_champion=rec.revert_to_champion,
    )
    assert result.final_status == "rejected_gate"
    assert [s.stage for s in result.steps] == ["gate"]
    assert rec.weights_set == []
    assert rec.reverted is False


def test_missing_shadow_result_never_reaches_canary():
    rec = _Recorder()
    result = run_promotion(
        holdout_result=PASSING_GATE,
        shadow_result=None,
        burn_check=lambda w: False,
        set_canary_weight=rec.set_canary_weight,
        revert_to_champion=rec.revert_to_champion,
    )
    assert result.final_status == "rejected_shadow"
    assert rec.weights_set == []


def test_a_dirty_shadow_diff_never_reaches_canary():
    rec = _Recorder()
    result = run_promotion(
        holdout_result=PASSING_GATE,
        shadow_result=DIRTY_SHADOW,
        burn_check=lambda w: False,
        set_canary_weight=rec.set_canary_weight,
        revert_to_champion=rec.revert_to_champion,
    )
    assert result.final_status == "rejected_shadow"
    assert rec.weights_set == []


def test_a_clean_candidate_ramps_through_every_canary_weight():
    rec = _Recorder()
    result = run_promotion(
        holdout_result=PASSING_GATE,
        shadow_result=CLEAN_SHADOW,
        burn_check=lambda w: False,  # never burning
        set_canary_weight=rec.set_canary_weight,
        revert_to_champion=rec.revert_to_champion,
    )
    assert result.final_status == "promoted"
    assert rec.weights_set == list(CANARY_WEIGHTS)
    assert rec.reverted is False


@pytest.mark.parametrize("burn_at_weight", CANARY_WEIGHTS)
def test_a_burn_at_any_weight_triggers_an_immediate_full_revert(burn_at_weight):
    rec = _Recorder()
    result = run_promotion(
        holdout_result=PASSING_GATE,
        shadow_result=CLEAN_SHADOW,
        burn_check=lambda w: w == burn_at_weight,
        set_canary_weight=rec.set_canary_weight,
        revert_to_champion=rec.revert_to_champion,
    )
    assert result.final_status == "rolled_back"
    assert rec.reverted is True
    # weight only ever set up to and including the burning step, never beyond
    expected_weights = [w for w in CANARY_WEIGHTS if w <= burn_at_weight]
    assert rec.weights_set == expected_weights
    assert result.steps[-1].stage == f"canary_{burn_at_weight}"
    assert result.steps[-1].action == "revert"


def test_promotion_result_transition_sequence_matches_the_pinned_clean_run():
    result = run_promotion(
        holdout_result=PASSING_GATE,
        shadow_result=CLEAN_SHADOW,
        burn_check=lambda w: False,
        set_canary_weight=lambda w: None,
        revert_to_champion=lambda: None,
    )
    pinned = json.loads(EXPECTATIONS.read_text())["promotion_state_machine"]["clean_run"]
    live = [asdict(s) for s in result.steps]
    assert live == pinned, (
        "the clean-run transition sequence changed; if intentional, update "
        "mlops/fixtures/expectations.json AND docs/phases/PHASE_3.md in the same commit"
    )
    assert result.final_status == "promoted"


def test_promotion_result_transition_sequence_matches_the_pinned_rollback_run():
    result = run_promotion(
        holdout_result=PASSING_GATE,
        shadow_result=CLEAN_SHADOW,
        burn_check=lambda w: w == 25,  # burns at the second canary step
        set_canary_weight=lambda w: None,
        revert_to_champion=lambda: None,
    )
    pinned = json.loads(EXPECTATIONS.read_text())["promotion_state_machine"][
        "rollback_run_burns_at_25"
    ]
    live = [asdict(s) for s in result.steps]
    assert live == pinned, (
        "the rollback-run transition sequence changed; if intentional, update "
        "mlops/fixtures/expectations.json AND docs/phases/PHASE_3.md in the same commit"
    )
    assert result.final_status == "rolled_back"


def test_promotion_result_defaults_are_sane():
    empty = PromotionResult()
    assert empty.steps == []
    assert empty.final_status == "rejected_gate"


# ---- Phase 4 gate 4.5: reward_hacking_result wiring ------------------------

PASSING_PROBE = RewardHackingProbeResult(
    baseline_mean_reward=5.0,
    candidate_mean_reward=8.0,
    baseline_hard_violation_rate=0.1,
    candidate_hard_violation_rate=0.1,
    blocked=False,
    reason=None,
)
BLOCKED_PROBE = RewardHackingProbeResult(
    baseline_mean_reward=5.0,
    candidate_mean_reward=8.0,
    baseline_hard_violation_rate=0.1,
    candidate_hard_violation_rate=0.3,
    blocked=True,
    reason="gamed",
)


def test_none_reward_hacking_result_reproduces_the_pinned_clean_sequence_unchanged():
    # supervised-retrain candidates pass reward_hacking_result=None; the
    # sequence must be byte-identical to the pre-Phase-4 pinned clean run.
    result = run_promotion(
        holdout_result=PASSING_GATE,
        shadow_result=CLEAN_SHADOW,
        burn_check=lambda w: False,
        set_canary_weight=lambda w: None,
        revert_to_champion=lambda: None,
        reward_hacking_result=None,
    )
    pinned = json.loads(EXPECTATIONS.read_text())["promotion_state_machine"]["clean_run"]
    assert [asdict(s) for s in result.steps] == pinned
    assert result.final_status == "promoted"


def test_a_blocked_reward_hacking_probe_halts_before_shadow():
    rec = _Recorder()
    result = run_promotion(
        holdout_result=PASSING_GATE,
        shadow_result=CLEAN_SHADOW,
        burn_check=lambda w: False,
        set_canary_weight=rec.set_canary_weight,
        revert_to_champion=rec.revert_to_champion,
        reward_hacking_result=BLOCKED_PROBE,
    )
    assert result.final_status == "rejected_reward_probe"
    assert [s.stage for s in result.steps] == ["gate", "reward_probe"]
    assert result.steps[-1].action == "fail"
    assert rec.weights_set == []  # never reached canary


def test_a_passing_reward_hacking_probe_proceeds_to_shadow_and_promotes():
    result = run_promotion(
        holdout_result=PASSING_GATE,
        shadow_result=CLEAN_SHADOW,
        burn_check=lambda w: False,
        set_canary_weight=lambda w: None,
        revert_to_champion=lambda: None,
        reward_hacking_result=PASSING_PROBE,
    )
    assert [s.stage for s in result.steps][:2] == ["gate", "reward_probe"]
    assert result.steps[1].action == "pass"
    assert result.final_status == "promoted"


def test_a_blocked_probe_never_reached_when_holdout_gate_already_failed():
    result = run_promotion(
        holdout_result=FAILING_GATE,
        shadow_result=None,
        burn_check=lambda w: False,
        set_canary_weight=lambda w: None,
        revert_to_champion=lambda: None,
        reward_hacking_result=BLOCKED_PROBE,
    )
    # the gate is checked first; a failed gate never even looks at the probe
    assert [s.stage for s in result.steps] == ["gate"]
    assert result.final_status == "rejected_gate"


# ---- DR-13: the real burn_check, fed by the burn-rate calculator -----------

from app.burn_rate import Bucket  # noqa: E402
from mlops.promote import make_burn_check  # noqa: E402


def _flat(n: int, total: int, bad: int) -> list[Bucket]:
    return [Bucket(timestamp=float(i * 60), total=total, bad=bad) for i in range(n)]


def test_make_burn_check_is_healthy_on_a_clean_series_at_every_weight():
    # A clean series at every canary weight: the real burn_check returns False
    # (healthy) everywhere, so a clean candidate ramps through and promotes.
    def clean_series(weight: int):
        return _flat(1440, total=1000, bad=0)

    burn_check = make_burn_check(clean_series, bucket_seconds=60.0)
    rec = _Recorder()
    result = run_promotion(
        holdout_result=PASSING_GATE,
        shadow_result=CLEAN_SHADOW,
        burn_check=burn_check,
        set_canary_weight=rec.set_canary_weight,
        revert_to_champion=rec.revert_to_champion,
    )
    assert result.final_status == "promoted"
    assert rec.weights_set == list(CANARY_WEIGHTS)
    assert rec.reverted is False


def test_make_burn_check_pages_and_rolls_back_on_a_fast_burn_outage_at_a_weight():
    # A fast-burn outage that only surfaces once traffic reaches 25%: the real
    # calculator returns PAGE for weight 25, so run_promotion reverts there and
    # never sets 50 or 100. This is the drill-L2 scenario with the REAL signal
    # source instead of an injected boolean.
    def series_for_weight(weight: int):
        if weight >= 25:
            return _flat(120, 1000, 0) + _flat(5, 1000, 1000)  # 5m total outage
        return _flat(1440, 1000, 0)

    burn_check = make_burn_check(series_for_weight, bucket_seconds=60.0)
    rec = _Recorder()
    result = run_promotion(
        holdout_result=PASSING_GATE,
        shadow_result=CLEAN_SHADOW,
        burn_check=burn_check,
        set_canary_weight=rec.set_canary_weight,
        revert_to_champion=rec.revert_to_champion,
    )
    assert result.final_status == "rolled_back"
    assert result.steps[-1].stage == "canary_25"
    assert result.steps[-1].action == "revert"
    assert rec.reverted is True
    assert rec.weights_set == [5, 25]  # never advanced past the burning weight


def test_make_burn_check_does_not_roll_back_on_a_ticket_grade_warning_bleed():
    # A 4x bleed is WARNING, not PAGE: the real burn_check returns False, so the
    # candidate is NOT rolled back for a ticket-grade slow bleed.
    def warn_series(weight: int):
        return _flat(1440, total=1000, bad=4)  # burn 4x -> warn tier only

    burn_check = make_burn_check(warn_series, bucket_seconds=60.0)
    assert burn_check(5) is False
    rec = _Recorder()
    result = run_promotion(
        holdout_result=PASSING_GATE,
        shadow_result=CLEAN_SHADOW,
        burn_check=burn_check,
        set_canary_weight=rec.set_canary_weight,
        revert_to_champion=rec.revert_to_champion,
    )
    assert result.final_status == "promoted"
    assert rec.reverted is False

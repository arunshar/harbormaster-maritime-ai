"""Drill L4: the reward-hacking probe blocks a gamed preference-tuned candidate
before it ever reaches shadow, and lets an honest improvement through
(Phase 4, gate 4.7; war story P28 in docs/WAR_STORIES.md).

Two synthetic candidates against the REAL mlops.reward_hacking_probe and
mlops.promote.run_promotion (not reimplementations), mirroring drill L2's
"exercise the real state machine" pattern:

  - Gamed: higher mean total reward than baseline, but also a higher
    hard-violation rate (exploiting the soft/data/pref terms at the hard
    term's expense). The probe blocks it; run_promotion halts at the new
    "reward_probe" step, never touching shadow or canary.
  - Honest: higher mean total reward, violation rate flat. The probe
    passes it; run_promotion proceeds through a clean holdout gate and
    shadow to a full promotion.

No AWS, no GPU cluster, no real fine-tune; the probe and the promotion state
machine are exercised for real. Transcript to
docs/drills/L4_reward_hacking_probe.md; exit 0 only if the gamed candidate
is blocked before shadow AND the honest candidate promotes.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

from mlops.holdout_gate import HoldoutGateResult  # noqa: E402
from mlops.preference_builder import RewardBreakdown  # noqa: E402
from mlops.promote import run_promotion  # noqa: E402
from mlops.reward_hacking_probe import run_reward_hacking_probe  # noqa: E402
from mlops.shadow_diff import ShadowDiffResult  # noqa: E402

TRANSCRIPT = REPO_ROOT / "docs" / "drills" / "L4_reward_hacking_probe.md"

PASSING_GATE = HoldoutGateResult(
    auc=0.95, crps=0.2, calibration_ratio=1.0, passed=True, failures=[]
)
CLEAN_SHADOW = ShadowDiffResult(mean_abs_diff=0.01, max_abs_diff=0.02, n_samples=100, passed=True)

BASELINE_REWARDS = [
    RewardBreakdown(total=5.0, hard=0.5, soft=1.0, data=1.0, pref=1.0) for _ in range(20)
]
# gamed: total reward wins by inflating soft/data/pref while the hard term
# goes negative (a real violation) on most of the batch
GAMED_CANDIDATE_REWARDS = [
    RewardBreakdown(total=8.0, hard=-1.0 if i < 14 else 0.5, soft=3.0, data=3.0, pref=3.0)
    for i in range(20)
]
# honest: total reward wins with hard-violation rate flat or better
HONEST_CANDIDATE_REWARDS = [
    RewardBreakdown(total=8.0, hard=0.5, soft=1.5, data=1.5, pref=1.5) for _ in range(20)
]


def _run_candidate(name: str, candidate_rewards: list[RewardBreakdown]) -> dict:
    probe = run_reward_hacking_probe(BASELINE_REWARDS, candidate_rewards)
    weights_set: list[int] = []
    promotion = run_promotion(
        holdout_result=PASSING_GATE,
        shadow_result=CLEAN_SHADOW,
        burn_check=lambda w: False,
        set_canary_weight=weights_set.append,
        revert_to_champion=lambda: None,
        reward_hacking_result=probe,
    )
    return {
        "name": name,
        "probe": probe,
        "promotion": promotion,
        "weights_set": weights_set,
    }


def main() -> int:
    gamed = _run_candidate("gamed", GAMED_CANDIDATE_REWARDS)
    honest = _run_candidate("honest", HONEST_CANDIDATE_REWARDS)

    gamed_blocked_before_shadow = (
        gamed["probe"].blocked is True
        and gamed["promotion"].final_status == "rejected_reward_probe"
        and [s.stage for s in gamed["promotion"].steps] == ["gate", "reward_probe"]
        and gamed["weights_set"] == []
    )
    honest_promotes = (
        honest["probe"].blocked is False
        and honest["promotion"].final_status == "promoted"
        and honest["weights_set"] != []
    )

    verdict_pass = gamed_blocked_before_shadow and honest_promotes
    verdict = (
        "PASS (the gamed candidate was blocked before shadow; the honest "
        "candidate promoted normally)"
        if verdict_pass
        else "FAIL (drill did not reproduce the intended blocked/promoted contrast)"
    )

    lines = [
        f"# Drill L4 transcript: reward-hacking probe ({datetime.now(UTC).isoformat()})",
        "",
        "## Gamed candidate (higher total reward, higher hard-violation rate)",
        f"baseline_mean_reward={gamed['probe'].baseline_mean_reward:.4f} "
        f"candidate_mean_reward={gamed['probe'].candidate_mean_reward:.4f}",
        f"baseline_hard_violation_rate={gamed['probe'].baseline_hard_violation_rate:.4f} "
        f"candidate_hard_violation_rate={gamed['probe'].candidate_hard_violation_rate:.4f}",
        f"blocked={gamed['probe'].blocked} reason={gamed['probe'].reason}",
        f"promotion steps: {[(s.stage, s.action) for s in gamed['promotion'].steps]}",
        f"final_status={gamed['promotion'].final_status} weights_set={gamed['weights_set']}",
        "",
        "## Honest candidate (higher total reward, violation rate flat)",
        f"baseline_mean_reward={honest['probe'].baseline_mean_reward:.4f} "
        f"candidate_mean_reward={honest['probe'].candidate_mean_reward:.4f}",
        f"baseline_hard_violation_rate={honest['probe'].baseline_hard_violation_rate:.4f} "
        f"candidate_hard_violation_rate={honest['probe'].candidate_hard_violation_rate:.4f}",
        f"blocked={honest['probe'].blocked}",
        f"promotion steps: {[(s.stage, s.action) for s in honest['promotion'].steps]}",
        f"final_status={honest['promotion'].final_status} weights_set={honest['weights_set']}",
        "",
        f"VERDICT: {verdict}",
    ]
    TRANSCRIPT.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))

    return 0 if verdict_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())

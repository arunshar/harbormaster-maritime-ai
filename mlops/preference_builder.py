"""Phase 4 gate 4.4: preference-triple builder + storage.

Ports (vendors, does not cross-repo-import) the preference-triple logic
from the earlier RL repository's reward module: the same filter over
{"correct","incorrect"} rows, and the same argmax-reward,
margin-gated, at-most-K-1-pairs shape for the synthesized triples. It
also ports that module's `RewardWeights`/`RewardBreakdown` dataclasses,
matching this repo's established external-reuse pattern: vendor the
shape, do not import the module.

No `hitl_queue` schema migration (a deliberate gate 4.4 design decision,
recorded in docs/phases/PHASE_4.md): the chosen/rejected framing for a HITL
disagreement is fully derivable from existing columns. The model's implied
verdict is `score >= hitl_threshold` (every row that legitimately reaches
the queue was flagged anomalous); the operator's verdict is the `label`
column; a disagreement (`label == "incorrect"`) yields
`chosen = the operator's side` (not anomalous), `rejected = the model's side`
(anomalous). "correct" rows have no chosen/rejected difference to extract
(agreement, not a preference signal) and are not turned into triples;
"ambiguous"/unlabeled rows are excluded exactly as the earlier RL
repository's own preference-triple filter excludes ambiguous rows.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

# ---- vendored verbatim from the earlier RL repository's reward module ----


@dataclass(frozen=True)
class RewardWeights:
    hard: float = 5.0
    soft: float = 1.0
    data: float = 1.0
    pref: float = 1.0


@dataclass(frozen=True)
class RewardBreakdown:
    total: float
    hard: float
    soft: float
    data: float
    pref: float

    def to_panel(self) -> dict[str, float]:
        return {
            "reward/total": self.total,
            "reward/hard": self.hard,
            "reward/soft": self.soft,
            "reward/data": self.data,
            "reward/pref": self.pref,
        }


ZERO_REWARD = RewardBreakdown(total=0.0, hard=0.0, soft=0.0, data=0.0, pref=0.0)

# ---- the Harbormaster-extended preference-triple schema (docs/phases/ ----
# ---- PHASE_4_SKETCH.md's locked JSON shape) --------------------------------


@dataclass(frozen=True)
class PreferenceArm:
    source: Literal["hitl_operator", "reward_synthesized"]
    verdict_or_trajectory: str
    reward: RewardBreakdown


@dataclass(frozen=True)
class PreferenceTriple:
    trace_id: str
    mmsi: int
    context: Mapping[str, Any]
    chosen: PreferenceArm
    rejected: PreferenceArm
    preference_source: Literal["hitl_verdict", "reward_synthesized"]
    hitl_operator: str | None
    hard_violation_in_either_arm: bool
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _default_now() -> str:
    return datetime.now(UTC).isoformat()


def _hard_violation(*rewards: RewardBreakdown, threshold: float) -> bool:
    return any(r.hard < threshold for r in rewards)


def build_from_hitl(
    rows: Iterable[Mapping[str, Any]],
    contexts: Mapping[str, Mapping[str, Any]],
    *,
    hitl_threshold: float,
    hard_violation_threshold: float = 0.0,
    now: Callable[[], str] = _default_now,
) -> list[PreferenceTriple]:
    """rows: hitl_queue-shaped mappings (trace_id, mmsi, score, label,
    reviewer). Optional per-row "chosen_reward"/"rejected_reward"
    RewardBreakdown values (a caller that has already computed a physics
    reward for the reviewed trajectory may supply them); default to
    ZERO_REWARD when omitted, an honest "no reward signal computed for this
    HITL-derived pair" rather than a fabricated claim.

    contexts: trace_id -> {"anchors":..., "reasons":...} (the orchestrator's
    own run_plan/ScoreReason output; assembling it is the caller's job, this
    module has no I/O of its own, matching WatchlistLookup/PiDpmClient's
    injected-everything convention)."""
    triples: list[PreferenceTriple] = []
    for row in rows:
        if row.get("label") != "incorrect":
            continue  # "correct" = agreement (no preference signal); ambiguous/None excluded
        if row.get("score", 0.0) < hitl_threshold:
            continue  # model-implied verdict inconsistency guard, per the module docstring

        chosen_reward = row.get("chosen_reward", ZERO_REWARD)
        rejected_reward = row.get("rejected_reward", ZERO_REWARD)
        chosen = PreferenceArm(
            source="hitl_operator", verdict_or_trajectory="not_anomalous", reward=chosen_reward
        )
        rejected = PreferenceArm(
            source="hitl_operator", verdict_or_trajectory="anomalous", reward=rejected_reward
        )
        triples.append(
            PreferenceTriple(
                trace_id=row["trace_id"],
                mmsi=row["mmsi"],
                context=contexts.get(row["trace_id"], {}),
                chosen=chosen,
                rejected=rejected,
                preference_source="hitl_verdict",
                hitl_operator=row.get("reviewer"),
                hard_violation_in_either_arm=_hard_violation(
                    chosen_reward, rejected_reward, threshold=hard_violation_threshold
                ),
                created_at=now(),
            )
        )
    return triples


def synthesize_from_reward(
    candidates: Iterable[tuple[str, int, list[str], list[RewardBreakdown]]],
    *,
    margin_min: float = 0.5,
    hard_violation_threshold: float = 0.0,
    contexts: Mapping[str, Mapping[str, Any]] | None = None,
    now: Callable[[], str] = _default_now,
) -> list[PreferenceTriple]:
    """candidates: (trace_id, mmsi, verdict_options, rewards) tuples, one per
    RL-eligible trace with K candidate verdicts/trajectories and their
    RewardBreakdowns. The earlier RL repository's real synthesize_from_reward semantics,
    unchanged: chosen = argmax(reward.total), rejected = each other
    candidate whose margin to chosen is >= margin_min, at most K-1 pairs per
    trace. hard_violation_in_either_arm audits the locked invariant (the
    unbounded, 5.0-weighted hard term should make a violating arm lose on
    total; this field lets the reward-hacking probe check that claim rather
    than assume it)."""
    contexts = contexts or {}
    triples: list[PreferenceTriple] = []
    for trace_id, mmsi, verdicts, rewards in candidates:
        if not verdicts or len(verdicts) != len(rewards):
            continue
        idxs = sorted(range(len(rewards)), key=lambda i: -rewards[i].total)
        top = idxs[0]
        for j in idxs[1:]:
            margin = rewards[top].total - rewards[j].total
            if margin < margin_min:
                continue
            chosen = PreferenceArm(
                source="reward_synthesized",
                verdict_or_trajectory=verdicts[top],
                reward=rewards[top],
            )
            rejected = PreferenceArm(
                source="reward_synthesized", verdict_or_trajectory=verdicts[j], reward=rewards[j]
            )
            triples.append(
                PreferenceTriple(
                    trace_id=trace_id,
                    mmsi=mmsi,
                    context=contexts.get(trace_id, {}),
                    chosen=chosen,
                    rejected=rejected,
                    preference_source="reward_synthesized",
                    hitl_operator=None,
                    hard_violation_in_either_arm=_hard_violation(
                        rewards[top], rewards[j], threshold=hard_violation_threshold
                    ),
                    created_at=now(),
                )
            )
    return triples


def write_triples(
    directory: Path, triples: Iterable[PreferenceTriple], *, filename: str = "preferences.jsonl"
) -> Path:
    """Append-only JSONL under directory (mlops/preference_data/ at the real
    call site), mirroring mlops/manifest.py's file-based convention.
    Preference triples are training data, not operational state, so they
    live where cluster training reads from, not in Postgres."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / filename
    with path.open("a") as f:
        for triple in triples:
            f.write(json.dumps(triple.to_dict(), sort_keys=True) + "\n")
    return path


__all__ = [
    "RewardWeights",
    "RewardBreakdown",
    "ZERO_REWARD",
    "PreferenceArm",
    "PreferenceTriple",
    "build_from_hitl",
    "synthesize_from_reward",
    "write_triples",
]

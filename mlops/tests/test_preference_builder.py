"""Gate 4.4: mlops/preference_builder.py."""

from __future__ import annotations

import json

from mlops.preference_builder import (
    RewardBreakdown,
    build_from_hitl,
    synthesize_from_reward,
    write_triples,
)

HITL_THRESHOLD = 0.6


def _fixed_now() -> str:
    return "2026-07-04T00:00:00+00:00"


def test_build_from_hitl_produces_a_triple_only_for_disagreements():
    rows = [
        {"trace_id": "t1", "mmsi": 1, "score": 0.9, "label": "correct", "reviewer": "alice"},
        {"trace_id": "t2", "mmsi": 2, "score": 0.9, "label": "incorrect", "reviewer": "bob"},
    ]
    triples = build_from_hitl(rows, contexts={}, hitl_threshold=HITL_THRESHOLD, now=_fixed_now)
    assert len(triples) == 1
    assert triples[0].trace_id == "t2"
    assert triples[0].preference_source == "hitl_verdict"
    assert triples[0].hitl_operator == "bob"


def test_build_from_hitl_drops_ambiguous_and_unlabeled_rows():
    rows = [
        {"trace_id": "t1", "mmsi": 1, "score": 0.9, "label": "ambiguous", "reviewer": "alice"},
        {"trace_id": "t2", "mmsi": 2, "score": 0.9, "label": None, "reviewer": "bob"},
    ]
    triples = build_from_hitl(rows, contexts={}, hitl_threshold=HITL_THRESHOLD, now=_fixed_now)
    assert triples == []


def test_build_from_hitl_drops_rows_below_the_model_implied_threshold():
    rows = [{"trace_id": "t1", "mmsi": 1, "score": 0.1, "label": "incorrect", "reviewer": "bob"}]
    triples = build_from_hitl(rows, contexts={}, hitl_threshold=HITL_THRESHOLD, now=_fixed_now)
    assert triples == []


def test_build_from_hitl_defaults_to_zero_reward_and_no_violation():
    rows = [{"trace_id": "t1", "mmsi": 1, "score": 0.9, "label": "incorrect", "reviewer": "bob"}]
    triples = build_from_hitl(rows, contexts={}, hitl_threshold=HITL_THRESHOLD, now=_fixed_now)
    assert triples[0].hard_violation_in_either_arm is False
    assert triples[0].chosen.reward.total == 0.0


def test_build_from_hitl_attaches_the_supplied_context():
    rows = [{"trace_id": "t1", "mmsi": 1, "score": 0.9, "label": "incorrect", "reviewer": "bob"}]
    contexts = {"t1": {"anchors": [1, 2, 3], "reasons": ["off_corridor"]}}
    triples = build_from_hitl(
        rows, contexts=contexts, hitl_threshold=HITL_THRESHOLD, now=_fixed_now
    )
    assert triples[0].context == contexts["t1"]


def test_synthesize_from_reward_chooses_argmax_and_gates_on_margin():
    rewards = [
        RewardBreakdown(total=10.0, hard=0.0, soft=0.0, data=0.0, pref=0.0),
        RewardBreakdown(total=9.6, hard=0.0, soft=0.0, data=0.0, pref=0.0),  # margin 0.4 < 0.5
        RewardBreakdown(total=5.0, hard=0.0, soft=0.0, data=0.0, pref=0.0),  # margin 5.0 >= 0.5
    ]
    candidates = [("t1", 1, ["a", "b", "c"], rewards)]
    triples = synthesize_from_reward(candidates, margin_min=0.5, now=_fixed_now)
    assert len(triples) == 1  # only the K-1 pair meeting margin_min survives
    assert triples[0].chosen.verdict_or_trajectory == "a"
    assert triples[0].rejected.verdict_or_trajectory == "c"


def test_synthesize_from_reward_flags_a_kinematically_violating_winner():
    # a candidate that wins on total reward despite a hard violation: exactly
    # what hard_violation_in_either_arm exists to let the probe audit.
    rewards = [
        RewardBreakdown(
            total=10.0, hard=-1.0, soft=5.0, data=5.0, pref=5.0
        ),  # violates, still wins
        RewardBreakdown(total=1.0, hard=0.5, soft=0.5, data=0.0, pref=0.0),
    ]
    candidates = [("t1", 1, ["gamed", "honest"], rewards)]
    triples = synthesize_from_reward(candidates, margin_min=0.5, now=_fixed_now)
    assert triples[0].hard_violation_in_either_arm is True


def test_synthesize_from_reward_skips_mismatched_lengths():
    candidates = [("t1", 1, ["a", "b"], [RewardBreakdown(1, 0, 0, 0, 0)])]
    assert synthesize_from_reward(candidates) == []


def test_write_triples_appends_valid_json_lines(tmp_path):
    rows = [{"trace_id": "t1", "mmsi": 1, "score": 0.9, "label": "incorrect", "reviewer": "bob"}]
    triples = build_from_hitl(rows, contexts={}, hitl_threshold=HITL_THRESHOLD, now=_fixed_now)

    path = write_triples(tmp_path, triples)
    lines = path.read_text().splitlines()
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed["trace_id"] == "t1"
    assert parsed["preference_source"] == "hitl_verdict"

    # append-only: a second call adds a line, never overwrites
    write_triples(tmp_path, triples)
    assert len(path.read_text().splitlines()) == 2

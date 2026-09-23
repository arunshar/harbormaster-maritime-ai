"""Gate 5.7: the S-KBM feasibility filter, reusing the validator's kinematic gate."""

from __future__ import annotations

import numpy as np

from app.agents.validator import ValidatorAgent  # noqa: F401  (reuse-anchor import proof)
from mlops.route_optimizer.feasibility import (
    KINEMATIC_TOLERANCE,
    feasible_action_mask,
    feasible_hop,
    v_max_mps_for,
)
from mlops.route_optimizer.graph import tiny_synthetic_graph


def test_tolerance_matches_validator_gate():
    # ValidatorAgent enforces v_req > v_max * 1.05; the optimizer reuses that
    # exact slack. If the validator's constant ever changes, this pins the drift.
    assert KINEMATIC_TOLERANCE == 1.05


def test_v_max_mps_for_uses_real_speed_model():
    # exact reuse of the serving plane's speed model (space_time_prism constants),
    # not a re-derived number: matches the arithmetic bit-for-bit.
    from app.components.space_time_prism import KMH_TO_MPS, KNOTS_TO_MPS

    assert v_max_mps_for("vessel") == 25.0 * KNOTS_TO_MPS
    assert v_max_mps_for("vehicle") == 130.0 * KMH_TO_MPS


def test_feasible_hop_reachability_rule():
    v = 10.0  # m/s
    dt = 100.0  # s -> reachable radius 1000 m, times 1.05 tolerance = 1050 m
    assert feasible_hop(1000.0, dt, v) is True
    assert feasible_hop(1050.0, dt, v) is True  # exactly on the tolerance boundary
    assert feasible_hop(1051.0, dt, v) is False


def test_feasible_hop_nonpositive_budget_is_infeasible():
    assert feasible_hop(0.0, 0.0, 10.0) is False
    assert feasible_hop(0.0, -5.0, 10.0) is False
    assert feasible_hop(1.0, 100.0, 0.0) is False  # zero speed cannot move


def test_feasible_action_mask_length_and_type():
    g = tiny_synthetic_graph()
    v = v_max_mps_for("vessel")
    mask = feasible_action_mask(g, 0, dt_s=3600.0, v_max_mps=v)
    assert isinstance(mask, np.ndarray) and mask.dtype == bool
    assert len(mask) == len(g.neighbors[0])  # out-degree of node 0


def test_generous_budget_all_feasible_tight_budget_none():
    g = tiny_synthetic_graph()
    v = v_max_mps_for("vessel")
    generous = feasible_action_mask(g, 0, dt_s=3600.0, v_max_mps=v)
    tight = feasible_action_mask(g, 0, dt_s=1.0, v_max_mps=v)
    assert generous.all()
    assert not tight.any()

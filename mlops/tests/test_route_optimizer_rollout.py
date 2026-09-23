"""Gate 5.7: rollout sampling, batch construction, and feasibility-in-the-loop.

Covers build_batch directly (its discounting / baseline / normalization were
previously only exercised transitively through train_optimizer), the S-KBM
feasibility gate's edge-exclusion inside real rollouts, and the empty-batch
(infeasible-start) path that must not crash training.
"""

from __future__ import annotations

import numpy as np
import pytest

from mlops.route_optimizer.feasibility import v_max_mps_for
from mlops.route_optimizer.graph import tiny_synthetic_graph
from mlops.route_optimizer.ppo import PpoConfig, TabularPolicy, ValueTable
from mlops.route_optimizer.rollout import (
    Rollout,
    build_batch,
    greedy_route,
    run_training,
    sample_route,
)

V_MAX = v_max_mps_for("vessel")


def _rollout(node_ids, states, actions, reward, K=2):
    masks = [np.array([True] * K) for _ in states]
    logp = [float(np.log(1.0 / K)) for _ in states]
    return Rollout(
        node_ids=node_ids,
        states=states,
        actions=actions,
        masks=masks,
        behavior_logp=logp,
        reward=reward,
    )


def test_build_batch_discounts_terminal_reward_over_the_walk():
    cfg = PpoConfig(gamma=0.9)
    # one 3-decision rollout, terminal reward 2.0
    r = _rollout(["n0", "n1", "n3", "n2"], [0, 1, 3], [0, 0, 0], reward=2.0)
    vt = ValueTable(4)  # zero baseline
    ref = TabularPolicy(4, 2)
    batch = build_batch([r], vt, ref, cfg)
    # return[i] = gamma^(T-1-i) * reward, T=3
    assert batch.returns == pytest.approx([0.9**2 * 2.0, 0.9**1 * 2.0, 0.9**0 * 2.0])


def test_build_batch_normalizes_advantages_when_variance_present():
    cfg = PpoConfig(gamma=0.9)
    r = _rollout(["n0", "n1", "n3", "n2"], [0, 1, 3], [0, 0, 0], reward=2.0)
    batch = build_batch([r], ValueTable(4), TabularPolicy(4, 2), cfg)
    # with distinct returns and a zero baseline, advantages are standardized
    assert batch.advantages.mean() == pytest.approx(0.0, abs=1e-6)
    assert batch.advantages.std() == pytest.approx(1.0, abs=1e-3)


def test_build_batch_ref_logp_comes_from_the_frozen_ref_policy():
    cfg = PpoConfig()
    r = _rollout(["n0", "n1"], [0], [1], reward=1.0)
    ref = TabularPolicy(4, 2, logits=np.array([[2.0, 0.0], [0, 0], [0, 0], [0, 0]]))
    batch = build_batch([r], ValueTable(4), ref, cfg)
    expected, _, _ = ref.forward(batch.states, batch.actions, batch.masks)
    assert batch.ref_logp == pytest.approx(expected)
    # action 1 under logits [2,0] has the smaller probability
    assert batch.ref_logp[0] == pytest.approx(np.log(np.exp(0) / (np.exp(2) + np.exp(0))))


def test_build_batch_empty_rollouts_is_shape_consistent():
    r = _rollout([], [], [], reward=0.0)  # a start-only rollout, no decisions
    ref = TabularPolicy(4, 3)
    batch = build_batch([r], ValueTable(4), ref, PpoConfig())
    assert batch.states.shape == (0,)
    assert batch.masks.shape == (0, ref.max_out_degree)  # not (0, n_nodes)


def test_sample_route_excludes_infeasible_edges():
    g = tiny_synthetic_graph()
    pol = TabularPolicy(g.n_nodes, g.max_out_degree)
    rng = np.random.default_rng(0)
    # tight budget: every ~5.6 km edge is infeasible, so the walk is start-only
    tight = sample_route(pol, g, rng, start_idx=0, horizon=6, dt_s=1.0, v_max_mps=V_MAX)
    assert tight.node_ids == ["n0"]
    assert tight.states == []
    # generous budget: the walk actually takes hops
    loose = sample_route(pol, g, rng, start_idx=0, horizon=6, dt_s=3600.0, v_max_mps=V_MAX)
    assert len(loose.node_ids) > 1
    assert len(loose.states) >= 1


def test_greedy_route_excludes_infeasible_edges():
    g = tiny_synthetic_graph()
    pol = TabularPolicy(g.n_nodes, g.max_out_degree)
    tight = greedy_route(pol, g, start_idx=0, horizon=6, dt_s=1.0, v_max_mps=V_MAX)
    assert tight.node_ids == ["n0"]  # no feasible edge to take
    loose = greedy_route(pol, g, start_idx=0, horizon=6, dt_s=3600.0, v_max_mps=V_MAX)
    assert len(loose.node_ids) > 1


def test_training_on_an_all_infeasible_graph_does_not_crash():
    g = tiny_synthetic_graph()
    cfg = PpoConfig(hop_seconds=1.0)  # every edge infeasible -> every rollout empty
    hist = run_training(g, cfg, seed=0, n_steps=3, n_rollouts=8, horizon=6)
    # the empty-batch guard returns finite zero-metrics instead of dividing by n=0
    assert all(np.isfinite(h["loss"]) for h in hist)
    assert all(h["mean_reward"] >= 0.0 for h in hist)  # start-node coverage only


def test_greedy_route_picks_the_most_probable_feasible_edge():
    """Wave 3 finding [29]: pin greedy's argmax direction so a mutation to
    argsort(+p) (least-probable first) is caught."""
    g = tiny_synthetic_graph()
    # node 0 has edges to slots [n1, n2]; bias the policy hard toward slot 1 (n2)
    logits = np.zeros((g.n_nodes, g.max_out_degree))
    logits[0, 1] = 10.0  # n2 is by far the most probable first hop
    pol = TabularPolicy(g.n_nodes, g.max_out_degree, logits=logits)
    r = greedy_route(pol, g, start_idx=0, horizon=1, dt_s=3600.0, v_max_mps=V_MAX)
    assert r.node_ids[1] == "n2"  # took the highest-probability feasible edge, not the lowest


def test_greedy_route_does_not_reuse_an_edge():
    """Wave 3 finding [15]: pin the visited-edges dedup so greedy cannot spin in
    a 2-cycle; every consecutive hop is a distinct directed edge."""
    g = tiny_synthetic_graph()
    pol = TabularPolicy(g.n_nodes, g.max_out_degree)
    r = greedy_route(pol, g, start_idx=0, horizon=20, dt_s=3600.0, v_max_mps=V_MAX)
    edges = list(zip(r.node_ids, r.node_ids[1:], strict=False))
    assert len(edges) == len(set(edges)), f"greedy reused an edge: {edges}"

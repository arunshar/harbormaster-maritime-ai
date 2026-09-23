"""Rollouts on the corridor graph and the one-CPU-step training driver
(gate 5.7). Zero GPU, zero AWS; ``make ppo-stretch-smoke`` calls
``run_training`` with ``n_steps=1``.

A rollout walks the corridor edge set under the current policy, with the S-KBM
feasibility gate masking every non-reachable hop out of the action
distribution, so a sampled route is a feasible walk BY CONSTRUCTION (the reward
in ``reward.py`` still raises on a non-edge hop, a second, redundant guard). The
terminal reward is ``coverage_minus_fuel``; returns are discounted back over the
walk and the value table is the baseline, the standard actor-critic reduction
the ported ``PpoTrainer`` consumes.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from mlops.route_optimizer.feasibility import feasible_action_mask, v_max_mps_for
from mlops.route_optimizer.graph import CorridorGraph
from mlops.route_optimizer.ppo import (
    PpoConfig,
    PpoTrainer,
    RouteBatch,
    TabularPolicy,
    ValueTable,
)
from mlops.route_optimizer.reward import coverage_minus_fuel

__all__ = [
    "Rollout",
    "sample_route",
    "greedy_route",
    "build_batch",
    "train_optimizer",
    "run_training",
]


@dataclass
class Rollout:
    node_ids: list[str]  # the visited node-id sequence, start included
    states: list[int]  # decision states (node indices), one per action taken
    actions: list[int]  # action-slot indices taken at each state
    masks: list[np.ndarray]  # (K,) feasibility mask per decision
    behavior_logp: list[float]  # log-prob of each action under the sampling policy
    reward: float  # coverage_minus_fuel of the full route


def _padded_mask(
    graph: CorridorGraph, node_idx: int, *, dt_s: float, v_max_mps: float
) -> np.ndarray:
    """A (max_out_degree,) bool mask: feasible edges True, padding slots False."""
    feas = feasible_action_mask(graph, node_idx, dt_s=dt_s, v_max_mps=v_max_mps)
    out = np.zeros(graph.max_out_degree, dtype=bool)
    out[: len(feas)] = feas
    return out


def sample_route(
    policy: TabularPolicy,
    graph: CorridorGraph,
    rng: np.random.Generator,
    *,
    start_idx: int,
    horizon: int,
    dt_s: float,
    v_max_mps: float,
    coverage_weight: float = 1.0,
    fuel_weight: float = 1.0,
) -> Rollout:
    """Sample one feasible walk of up to ``horizon`` hops from ``start_idx``."""
    current = start_idx
    node_ids = [graph.node_ids[current]]
    states: list[int] = []
    actions: list[int] = []
    masks: list[np.ndarray] = []
    behavior_logp: list[float] = []

    for _ in range(horizon):
        mask = _padded_mask(graph, current, dt_s=dt_s, v_max_mps=v_max_mps)
        if not mask.any():
            break  # no feasible corridor out of here: terminal, never a teleport
        p = policy.probs(np.array([current]), mask[None, :])[0]
        slot = int(rng.choice(len(p), p=p))
        states.append(current)
        actions.append(slot)
        masks.append(mask)
        behavior_logp.append(float(np.log(max(p[slot], 1e-12))))
        current = graph.neighbors[current][slot]
        node_ids.append(graph.node_ids[current])

    reward = coverage_minus_fuel(
        node_ids, graph, coverage_weight=coverage_weight, fuel_weight=fuel_weight
    )
    return Rollout(
        node_ids=node_ids,
        states=states,
        actions=actions,
        masks=masks,
        behavior_logp=behavior_logp,
        reward=reward,
    )


def greedy_route(
    policy: TabularPolicy,
    graph: CorridorGraph,
    *,
    start_idx: int,
    horizon: int,
    dt_s: float,
    v_max_mps: float,
    coverage_weight: float = 1.0,
    fuel_weight: float = 1.0,
) -> Rollout:
    """The deterministic argmax walk the service returns: no sampling, the most
    probable feasible edge at each step, with revisits stopped so the greedy
    walk cannot spin in a 2-cycle forever (a sampled rollout self-limits via the
    horizon, but argmax would loop deterministically)."""
    current = start_idx
    node_ids = [graph.node_ids[current]]
    visited_edges: set[tuple[int, int]] = set()
    for _ in range(horizon):
        mask = _padded_mask(graph, current, dt_s=dt_s, v_max_mps=v_max_mps)
        if not mask.any():
            break
        p = policy.probs(np.array([current]), mask[None, :])[0]
        order = np.argsort(-p)
        slot = next(
            (
                int(s)
                for s in order
                if mask[s] and (current, graph.neighbors[current][int(s)]) not in visited_edges
            ),
            None,
        )
        if slot is None:
            break  # every feasible edge already used: stop rather than loop
        visited_edges.add((current, graph.neighbors[current][slot]))
        current = graph.neighbors[current][slot]
        node_ids.append(graph.node_ids[current])
    reward = coverage_minus_fuel(
        node_ids, graph, coverage_weight=coverage_weight, fuel_weight=fuel_weight
    )
    return Rollout(
        node_ids=node_ids, states=[], actions=[], masks=[], behavior_logp=[], reward=reward
    )


def build_batch(
    rollouts: list[Rollout],
    value_table: ValueTable,
    ref_policy: TabularPolicy,
    cfg: PpoConfig,
) -> RouteBatch:
    """Flatten rollouts into a ``RouteBatch``: discounted terminal returns, a
    value-baseline advantage, and reference log-probs from the frozen ref."""
    states: list[int] = []
    actions: list[int] = []
    masks: list[np.ndarray] = []
    action_logp: list[float] = []
    returns: list[float] = []
    for r in rollouts:
        t = len(r.states)
        for i in range(t):
            # only-terminal reward, discounted from step i to the walk's end
            returns.append(cfg.gamma ** (t - 1 - i) * r.reward)
        states.extend(r.states)
        actions.extend(r.actions)
        masks.extend(r.masks)
        action_logp.extend(r.behavior_logp)

    states_a = np.array(states, dtype=np.int64)
    actions_a = np.array(actions, dtype=np.int64)
    # empty fallback keeps the action-slot column count (max_out_degree), not
    # n_nodes, so a zero-decision batch has a shape-consistent (0, K) mask.
    masks_a = np.stack(masks) if masks else np.zeros((0, ref_policy.max_out_degree), dtype=bool)
    returns_a = np.array(returns, dtype=np.float64)
    baseline = value_table.values[states_a] if len(states_a) else np.zeros(0)
    adv = returns_a - baseline
    if adv.size > 1 and adv.std() > 0:
        adv = (adv - adv.mean()) / (adv.std() + 1e-8)
    ref_logp, _, _ = ref_policy.forward(states_a, actions_a, masks_a)
    return RouteBatch(
        states=states_a,
        actions=actions_a,
        masks=masks_a,
        action_logp=np.array(action_logp, dtype=np.float64),
        returns=returns_a,
        advantages=adv,
        ref_logp=ref_logp,
    )


def train_optimizer(
    graph: CorridorGraph,
    cfg: PpoConfig,
    *,
    seed: int,
    n_steps: int,
    n_rollouts: int,
    horizon: int,
    start_idx: int = 0,
    domain: str = "vessel",
    coverage_weight: float = 1.0,
    fuel_weight: float = 1.0,
) -> tuple[TabularPolicy, list[dict[str, float]]]:
    """Drive ``n_steps`` PPO updates on ``graph`` and return the TRAINED policy
    plus one metrics dict per step (each augmented with ``mean_reward``, the
    sampled rollouts' mean reward at that step, so a caller can assert
    improvement). The frozen reference for the KL penalty is the policy snapshot
    taken before any update, so ``kl`` starts near zero and grows as the policy
    moves off the reference, exactly the ported trainer's contract. The reward
    weights are threaded into training so the policy optimizes the SAME reward
    the caller later reads out greedily (not the default-weighted one).
    """
    rng = np.random.default_rng(seed)
    policy = TabularPolicy(graph.n_nodes, graph.max_out_degree)
    ref = policy.copy()  # frozen pre-training reference for the KL penalty
    value_table = ValueTable(graph.n_nodes)
    trainer = PpoTrainer(policy=policy, ref_policy=ref, value_head=value_table, cfg=cfg)
    v_max = v_max_mps_for(domain)

    history: list[dict[str, float]] = []
    for _ in range(n_steps):
        rollouts = [
            sample_route(
                policy,
                graph,
                rng,
                start_idx=start_idx,
                horizon=horizon,
                dt_s=cfg.hop_seconds,
                v_max_mps=v_max,
                coverage_weight=coverage_weight,
                fuel_weight=fuel_weight,
            )
            for _ in range(n_rollouts)
        ]
        batch = build_batch(rollouts, value_table, ref, cfg)
        metrics = trainer.step_update(batch)
        metrics["mean_reward"] = float(np.mean([r.reward for r in rollouts]))
        history.append(metrics)
    return policy, history


def run_training(
    graph: CorridorGraph,
    cfg: PpoConfig,
    *,
    seed: int,
    n_steps: int,
    n_rollouts: int,
    horizon: int,
    start_idx: int = 0,
    domain: str = "vessel",
) -> list[dict[str, float]]:
    """Metrics-only wrapper over ``train_optimizer`` (the trained policy is
    discarded). ``make ppo-stretch-smoke`` runs this with ``n_steps=1`` for a
    single end-to-end CPU step; the improvement test runs it for many."""
    _, history = train_optimizer(
        graph,
        cfg,
        seed=seed,
        n_steps=n_steps,
        n_rollouts=n_rollouts,
        horizon=horizon,
        start_idx=start_idx,
        domain=domain,
    )
    return history

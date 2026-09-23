"""Gate 5.7: the ported PPO trainer over the tabular corridor policy.

Covers the ML gates the engineering standard requires for a training path:
shape/finiteness, a seed-determinism check, and an overfit/improvement smoke
(a policy that cannot improve reward on a tiny fixture is a bug).
"""

from __future__ import annotations

import numpy as np
import pytest

from mlops.route_optimizer.graph import tiny_synthetic_graph
from mlops.route_optimizer.ppo import (
    PpoConfig,
    PpoTrainer,
    RouteBatch,
    TabularPolicy,
    ValueTable,
    global_grad_norm,
)
from mlops.route_optimizer.rollout import run_training, train_optimizer


def test_masked_softmax_sums_to_one_over_feasible_slots():
    pol = TabularPolicy(2, 3, logits=np.array([[1.0, 2.0, 3.0], [0.0, 0.0, 0.0]]))
    mask = np.array([[True, True, False]])  # slot 2 masked out
    p = pol.probs(np.array([0]), mask)
    assert p[0, 2] == 0.0  # masked slot has zero probability
    assert p[0].sum() == pytest.approx(1.0)
    # relative weighting of the two live slots follows their logits
    assert p[0, 1] > p[0, 0]


def test_fully_masked_row_is_uniform_not_nan():
    pol = TabularPolicy(1, 3)
    p = pol.probs(np.array([0]), np.array([[False, False, False]]))
    assert np.all(np.isfinite(p))
    assert p[0].sum() == pytest.approx(1.0)
    assert np.allclose(p[0], 1 / 3)


def test_forward_logp_and_entropy():
    pol = TabularPolicy(1, 2, logits=np.array([[0.0, 0.0]]))
    mask = np.array([[True, True]])
    logp, ent, probs = pol.forward(np.array([0]), np.array([0]), mask)
    assert logp[0] == pytest.approx(np.log(0.5))
    assert ent[0] == pytest.approx(np.log(2))  # uniform-over-2 entropy
    assert probs[0].sum() == pytest.approx(1.0)


def test_global_grad_norm():
    a = np.array([3.0, 0.0])
    b = np.array([4.0])
    assert global_grad_norm(a, b) == pytest.approx(5.0)


# warmup_steps=0 so a single step has nonzero LR (the ported cosine schedule
# warms up from lr=0, which is faithful but hides one-step movement in unit tests).
_NOW = PpoConfig(warmup_steps=0)


def _one_batch(policy: TabularPolicy, *, returns=(1.0, -1.0), advantages=(1.0, -1.0)) -> RouteBatch:
    states = np.array([0, 0])
    actions = np.array([0, 1])
    masks = np.array([[True, True], [True, True]])
    logp, _, _ = policy.forward(states, actions, masks)
    return RouteBatch(
        states=states,
        actions=actions,
        masks=masks,
        action_logp=logp,
        returns=np.array(returns),
        advantages=np.array(advantages),
        ref_logp=logp.copy(),
    )


def test_step_update_returns_finite_metrics():
    pol = TabularPolicy(2, 2)
    trainer = PpoTrainer(
        policy=pol, ref_policy=pol.copy(), value_head=ValueTable(2), cfg=PpoConfig()
    )
    m = trainer.step_update(_one_batch(pol))
    for k in ("loss", "pg_loss", "vf_loss", "kl", "kl_coef", "grad_norm", "lr", "entropy"):
        assert np.isfinite(m[k])
    assert trainer.step == 1


@pytest.mark.parametrize(
    ("ratio", "advantage", "expected_pg_loss"),
    [
        (1.5, 2.0, -2.4),  # positive A, above upper clip: min(3.0, 2.4)
        (0.5, 2.0, -1.0),  # positive A, below lower clip: min(1.0, 1.6)
        (1.5, -2.0, 3.0),  # negative A, above upper clip: min(-3.0, -2.4)
        (0.5, -2.0, 1.6),  # negative A, below lower clip: min(-1.0, -1.6)
    ],
)
def test_clipped_surrogate_matches_hand_computed_sign_cases(ratio, advantage, expected_pg_loss):
    policy = TabularPolicy(1, 2)
    states = np.array([0])
    actions = np.array([0])
    masks = np.array([[True, True]])
    new_logp, _, _ = policy.forward(states, actions, masks)
    batch = RouteBatch(
        states=states,
        actions=actions,
        masks=masks,
        action_logp=new_logp - np.log(ratio),
        returns=np.array([0.0]),
        advantages=np.array([advantage]),
        ref_logp=new_logp.copy(),
    )
    trainer = PpoTrainer(
        policy=policy,
        ref_policy=policy.copy(),
        value_head=ValueTable(1),
        cfg=PpoConfig(warmup_steps=0, vf_coef=0.0, ent_coef=0.0),
    )

    metrics = trainer.step_update(batch)

    assert metrics["pg_loss"] == pytest.approx(expected_pg_loss)


def test_warmup_step_zero_has_zero_lr():
    # the ported cosine schedule warms up from 0; at step 0 with warmup>0 nothing moves
    pol = TabularPolicy(2, 2)
    trainer = PpoTrainer(
        policy=pol, ref_policy=pol.copy(), value_head=ValueTable(2), cfg=PpoConfig()
    )
    before = pol.logits.copy()
    m = trainer.step_update(_one_batch(pol))
    assert m["lr"] == 0.0
    assert np.array_equal(pol.logits, before)  # zero-LR step is a no-op on params


def test_positive_advantage_action_gains_probability():
    pol = TabularPolicy(2, 2)
    trainer = PpoTrainer(policy=pol, ref_policy=pol.copy(), value_head=ValueTable(2), cfg=_NOW)
    before = pol.probs(np.array([0]), np.array([[True, True]]))[0, 0]
    trainer.step_update(_one_batch(pol))  # action 0 has advantage +1, action 1 has -1
    after = pol.probs(np.array([0]), np.array([[True, True]]))[0, 0]
    assert after > before  # the advantaged action's probability rose


def test_value_head_moves_toward_returns():
    pol = TabularPolicy(2, 2)
    vt = ValueTable(2)
    trainer = PpoTrainer(policy=pol, ref_policy=pol.copy(), value_head=vt, cfg=_NOW)
    # state 0 sees returns {+2, +1} (mean 1.5, not self-cancelling); baseline rises
    trainer.step_update(_one_batch(pol, returns=(2.0, 1.0)))
    assert vt.values[0] > 0.0


def test_grad_clipping_bounds_the_update():
    pol = TabularPolicy(2, 2)
    cfg = PpoConfig(warmup_steps=0, grad_clip=1e-6)  # a very tight clip, nonzero LR
    trainer = PpoTrainer(policy=pol, ref_policy=pol.copy(), value_head=ValueTable(2), cfg=cfg)
    before = pol.logits.copy()
    trainer.step_update(_one_batch(pol))
    # Adam normalizes the step to ~lr per element, so the tight clip on the raw
    # gradient still bounds movement to well under the lr; the point is it stays
    # finite and small, not that the clip is a hard displacement cap.
    moved = np.max(np.abs(pol.logits - before))
    assert 0.0 < moved < cfg.lr * 2


def test_grad_clip_actually_clips_the_gradient_norm():
    """Capture the gradient the trainer feeds its optimizer and assert the
    global-norm clip bounds it. (The displacement test above cannot see this:
    Adam renormalizes the step to ~lr regardless of the raw-gradient norm, so
    removing the clip would still pass a displacement bound.)"""

    def _captured_grad_norm(grad_clip: float) -> float:
        pol = TabularPolicy(2, 2)
        cfg = PpoConfig(warmup_steps=0, grad_clip=grad_clip)
        trainer = PpoTrainer(policy=pol, ref_policy=pol.copy(), value_head=ValueTable(2), cfg=cfg)
        seen: dict[str, float] = {}

        def _fake_step(grad, lr):
            seen["norm"] = float(np.linalg.norm(grad))
            return np.zeros_like(grad)

        trainer.opt.step = _fake_step
        # large advantages -> a raw gradient norm well above a tight clip
        trainer.step_update(_one_batch(pol, returns=(5.0, -5.0), advantages=(9.0, -9.0)))
        return seen["norm"]

    tight = _captured_grad_norm(0.01)
    loose = _captured_grad_norm(1e9)
    assert tight <= 0.01 + 1e-9, "tight grad_clip did not bound the gradient norm"
    assert loose > 0.01, (
        "the unclipped gradient norm should exceed the tight clip (else the test is vacuous)"
    )


def test_seed_determinism():
    g = tiny_synthetic_graph()
    cfg = PpoConfig()
    h1 = run_training(g, cfg, seed=7, n_steps=5, n_rollouts=8, horizon=6)
    h2 = run_training(g, cfg, seed=7, n_steps=5, n_rollouts=8, horizon=6)
    assert [m["loss"] for m in h1] == [m["loss"] for m in h2]
    assert [m["mean_reward"] for m in h1] == [m["mean_reward"] for m in h2]


def _improvement(cfg, seed):
    g = tiny_synthetic_graph()
    _, hist = train_optimizer(g, cfg, seed=seed, n_steps=60, n_rollouts=24, horizon=6)
    early = np.mean([h["mean_reward"] for h in hist[:5]])
    late = np.mean([h["mean_reward"] for h in hist[-5:]])
    assert all(np.isfinite(h["loss"]) for h in hist)  # never diverges to NaN
    return late - early


def test_training_improves_mean_reward():
    """Overfit-the-fixture smoke: the policy must LEARN a higher-reward routing
    distribution on the tiny graph, by a margin RNG alone cannot produce.

    A bare ``late > early`` at one seed passes even with the reward-driven
    gradient removed (the mean_reward series is stochastic), so this asserts a
    real margin across several seeds: the learner's mean improvement is ~0.08
    with a per-seed floor near 0.07, whereas a learning-disabled policy
    (advantages zeroed) sits near 0.01 and dips negative on some seeds. The
    thresholds below sit well inside that gap."""
    cfg = PpoConfig()
    gains = [_improvement(cfg, seed) for seed in range(4)]
    assert np.mean(gains) > 0.04, f"mean improvement {np.mean(gains):.4f} too small to be learning"
    assert min(gains) > 0.03, f"a seed failed to improve ({min(gains):.4f}); learning is not robust"


def test_zeroing_the_reward_signal_kills_the_improvement():
    """The companion guard proving the margin above is a LEARNING signal, not
    RNG: with the advantage (the only reward-driven gradient) zeroed, the same
    margin assertion must fail. If this ever passes, the improvement test is a
    tautology."""
    import mlops.route_optimizer.rollout as rollout_mod

    cfg = PpoConfig()
    original = rollout_mod.build_batch

    def _zeroed(rollouts, value_table, ref_policy, cfg):
        batch = original(rollouts, value_table, ref_policy, cfg)
        batch.advantages[:] = 0.0
        return batch

    rollout_mod.build_batch = _zeroed
    try:
        gains = [_improvement(cfg, seed) for seed in range(4)]
    finally:
        rollout_mod.build_batch = original
    # with no reward signal the learner cannot clear the learning margin
    assert np.mean(gains) < 0.04

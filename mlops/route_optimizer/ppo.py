"""The PPO trainer SHAPE from an earlier reinforcement-learning repository of
mine, retargeted to a tabular numpy policy over the corridor graph (gate 5.7).
Zero torch, zero GPU, zero AWS.

PPO is the clipped-surrogate policy-gradient method of Schulman et al. (2017),
arXiv:1707.06347.

Provenance: the ``PpoConfig`` FIELD SET and the ``PpoTrainer.step_update`` loss
structure are ported from the earlier RL repository's
``app/trainers/ppo_trainer.py``: the clipped surrogate
``-min(ratio*A, clip(ratio)*A)``, the value MSE scaled by
``vf_coef``, the entropy bonus scaled by ``ent_coef``, the adaptive-KL penalty,
global-norm gradient clipping, and a cosine LR schedule, in that exact
composition. What is retargeted, not copied: the policy is a
masked-softmax LOOKUP TABLE over corridor edges (not an LM), the value function
is a per-node table (not a torch head), and the gradients are the closed-form
softmax/PPO gradients computed in numpy (the earlier RL repository backprops
through torch).
``AdaptiveKLController`` and ``cosine_lr`` come from the byte-for-byte
``vendored_kl`` module; only the torch-bound ``clip_grad_norm`` is reimplemented
here, in numpy, as the source file's header states.

Deliberate departures from the source, all because the target is a tiny tabular
MDP rather than LM fine-tuning:
- The optimizer is a minimal in-file Adam with weight decay OFF (torch's
  ``AdamW`` defaults to ``1e-2``; decaying a corridor lookup table toward zero
  has no meaning here), and advantages are Monte-Carlo returns minus the value
  baseline (not GAE-lambda).
- The DEFAULT VALUES of the learning-rate and schedule fields are RETUNED for
  the tiny MDP, not carried over from the source: ``lr`` 1e-2 (source 1e-6),
  ``lr_min`` 1e-4 (source 1e-7), ``warmup_steps`` 5 (source 100),
  ``total_steps`` 200 (source 5000). The source's 1e-6 LM-fine-tuning rate would
  barely move a lookup table over a 200-step smoke; only the field set and the
  clip/vf/ent/target-KL coefficients are the source's.
None of this changes the update math this gate is checked on. Hand-computed
clipped-surrogate cases and trainer behavior tests pin the ported composition;
all departures are disclosed so the "ported shape" claim stays honest.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from mlops.route_optimizer.vendored_kl import AdaptiveKLController, cosine_lr

__all__ = [
    "PpoConfig",
    "TabularPolicy",
    "ValueTable",
    "RouteBatch",
    "PpoTrainer",
    "global_grad_norm",
]


@dataclass
class PpoConfig:
    """Ported field-for-field from the source ``PpoConfig`` so the shared shape is
    obvious. The clip/vf/ent/target-KL/minibatch/rollout coefficients keep the
    source's values; the learning-rate and schedule defaults (``lr``,
    ``lr_min``, ``warmup_steps``, ``total_steps``) are RETUNED for this tiny
    tabular MDP (see the module docstring's departures list). Plus two fields
    the corridor MDP needs that an LM run does not: ``gamma`` (return discount)
    and ``hop_seconds`` (the per-hop time budget the S-KBM feasibility gate
    measures against)."""

    lr: float = 1e-2
    lr_min: float = 1e-4
    warmup_steps: int = 5
    total_steps: int = 200
    clip_coef: float = 0.2
    vf_coef: float = 0.5
    ent_coef: float = 0.01
    grad_clip: float = 1.0
    target_kl: float = 6.0
    minibatch_size: int = 16
    rollout_batch_size: int = 64
    # --- corridor-MDP additions (absent from the LM source) ---
    gamma: float = 0.99
    hop_seconds: float = 3600.0


class TabularPolicy:
    """A masked-softmax lookup policy: ``logits[node, slot]`` scores each
    corridor edge (action slot) out of each node. A per-decision boolean mask
    zeroes the probability of slots that are not real edges OR that the S-KBM
    gate rejects for that decision's time budget, so the distribution is always
    over feasible corridor hops only.
    """

    def __init__(self, n_nodes: int, max_out_degree: int, *, logits: np.ndarray | None = None):
        self.n_nodes = n_nodes
        self.max_out_degree = max(max_out_degree, 1)
        if logits is None:
            logits = np.zeros((n_nodes, self.max_out_degree), dtype=np.float64)
        self.logits = np.asarray(logits, dtype=np.float64).reshape(n_nodes, self.max_out_degree)

    def copy(self) -> TabularPolicy:
        return TabularPolicy(self.n_nodes, self.max_out_degree, logits=self.logits.copy())

    def probs(self, states: np.ndarray, masks: np.ndarray) -> np.ndarray:
        """Softmax over the masked logits for each (state, mask) row.

        A fully-masked row (no feasible action) returns a uniform distribution
        over its slots; callers that reach such a state treat it as terminal,
        so this value is never actually sampled, but it keeps the softmax
        numerically defined instead of producing NaNs.
        """
        rows = self.logits[states]  # (n, K)
        neg_inf = np.where(masks, rows, -np.inf)
        row_max = np.max(np.where(masks, neg_inf, -np.inf), axis=1, keepdims=True)
        row_max = np.where(np.isfinite(row_max), row_max, 0.0)
        exp = np.where(masks, np.exp(neg_inf - row_max), 0.0)
        denom = exp.sum(axis=1, keepdims=True)
        empty = denom <= 0.0
        # fully-masked fallback: uniform over slots (never sampled; see docstring)
        exp = np.where(empty, 1.0, exp)
        denom = np.where(empty, float(self.max_out_degree), denom)
        return exp / denom

    def forward(
        self, states: np.ndarray, actions: np.ndarray, masks: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Return (log_prob of the taken action, entropy, full prob rows).

        Mirrors the source's ``policy.log_prob_with_entropy`` return contract
        (logp, entropy); the extra prob rows are returned for the closed-form
        gradient in ``PpoTrainer`` (torch would recompute them in backward)."""
        p = self.probs(states, masks)
        taken = p[np.arange(len(actions)), actions]
        logp = np.log(np.clip(taken, 1e-12, 1.0))
        safe = np.where(p > 0.0, p, 1.0)
        entropy = -np.sum(np.where(p > 0.0, p * np.log(safe), 0.0), axis=1)
        return logp, entropy, p


class ValueTable:
    """A per-node state-value baseline (the tabular stand-in for the source's
    torch value head)."""

    def __init__(self, n_nodes: int, *, values: np.ndarray | None = None):
        self.n_nodes = n_nodes
        if values is None:
            values = np.zeros(n_nodes, dtype=np.float64)
        self.values = np.asarray(values, dtype=np.float64).reshape(n_nodes)


@dataclass
class RouteBatch:
    """The ported ``_PpoBatch`` retargeted to corridor decisions: one row per
    (state, action) decision across the rollout batch."""

    states: np.ndarray  # (n,) node indices
    actions: np.ndarray  # (n,) action-slot indices
    masks: np.ndarray  # (n, K) bool feasibility masks
    action_logp: np.ndarray  # (n,) behavior-policy log-probs at rollout time
    returns: np.ndarray  # (n,) discounted Monte-Carlo returns
    advantages: np.ndarray  # (n,) returns - baseline, normalized
    ref_logp: np.ndarray  # (n,) reference-policy log-probs (frozen snapshot)


def global_grad_norm(*grads: np.ndarray) -> float:
    """L2 norm of all gradients concatenated: the numpy stand-in for the
    torch ``clip_grad_norm_`` the vendored header deliberately did not port."""
    total = 0.0
    for g in grads:
        total += float(np.sum(g * g))
    return float(np.sqrt(total))


class _Adam:
    """Minimal Adam over a flat parameter vector (weight decay off; see the
    module docstring for why the source's AdamW decay is not carried over)."""

    def __init__(self, size: int, *, beta1: float = 0.9, beta2: float = 0.999, eps: float = 1e-8):
        self.m = np.zeros(size, dtype=np.float64)
        self.v = np.zeros(size, dtype=np.float64)
        self.beta1 = beta1
        self.beta2 = beta2
        self.eps = eps
        self.t = 0

    def step(self, grad: np.ndarray, lr: float) -> np.ndarray:
        self.t += 1
        self.m = self.beta1 * self.m + (1 - self.beta1) * grad
        self.v = self.beta2 * self.v + (1 - self.beta2) * (grad * grad)
        m_hat = self.m / (1 - self.beta1**self.t)
        v_hat = self.v / (1 - self.beta2**self.t)
        return lr * m_hat / (np.sqrt(v_hat) + self.eps)


class PpoTrainer:
    """Ported ``PpoTrainer`` shape: same constructor seams (a policy, a frozen
    reference policy, a value head, a ``PpoConfig``) and the same
    ``step_update`` loss composition, over the tabular corridor policy.
    """

    def __init__(
        self,
        *,
        policy: TabularPolicy,
        ref_policy: TabularPolicy,
        value_head: ValueTable,
        cfg: PpoConfig,
    ) -> None:
        self.policy = policy
        self.ref = ref_policy
        self.vh = value_head
        self.cfg = cfg
        self.kl = AdaptiveKLController(target=cfg.target_kl)
        self.step = 0
        self._n_logits = policy.logits.size
        self.opt = _Adam(self._n_logits + value_head.values.size)

    def step_update(self, batch: RouteBatch) -> dict[str, float]:
        cfg = self.cfg
        n = len(batch.states)
        if n == 0:
            # No decisions in the batch: every rollout terminated immediately
            # (a start node with no feasible outgoing corridor under the hop
            # budget). There is nothing to learn from, so advance the step
            # counter and return a zero-metrics no-op rather than dividing by
            # zero. run_training over such a graph will simply not improve.
            self.step += 1
            return {
                "loss": 0.0,
                "pg_loss": 0.0,
                "vf_loss": 0.0,
                "kl": 0.0,
                "kl_coef": float(self.kl.kl_coef),
                "grad_norm": 0.0,
                "lr": 0.0,
                "entropy": 0.0,
            }
        new_logp, ent, probs = self.policy.forward(batch.states, batch.actions, batch.masks)

        # --- ported loss composition (the source's ppo_trainer.step_update) ---
        ratio = np.exp(new_logp - batch.action_logp)
        adv = batch.advantages
        unclipped = ratio * adv
        clipped = np.clip(ratio, 1 - cfg.clip_coef, 1 + cfg.clip_coef) * adv
        surrogate = np.minimum(unclipped, clipped)
        pg_loss = -float(np.mean(surrogate))

        v = self.vh.values[batch.states]
        vf_loss = float(np.mean((v - batch.returns) ** 2))

        kl = float(np.mean(batch.ref_logp - new_logp))
        loss = (
            pg_loss
            + cfg.vf_coef * vf_loss
            - cfg.ent_coef * float(np.mean(ent))
            + self.kl.kl_coef * kl
        )

        # --- closed-form gradients (torch does this in backward) ---
        # d pg_loss / d new_logp_i: the surrogate's grad flows only when the
        # UNCLIPPED term is the min (the clipped term is constant in ratio there),
        # exactly torch's ``min`` autograd semantics.
        flows = unclipped <= clipped
        d_pg_d_logp = -(1.0 / n) * (adv * ratio) * flows
        d_kl_d_logp = -(self.kl.kl_coef / n) * np.ones(n)
        d_logp = d_pg_d_logp + d_kl_d_logp

        # chain new_logp -> logits through the masked softmax:
        # d log p(a) / d logit_j = 1{j==a} - p_j (0 on masked slots, where p_j=0)
        g_logits = np.zeros_like(self.policy.logits)
        onehot = np.zeros_like(probs)
        onehot[np.arange(n), batch.actions] = 1.0
        contrib = d_logp[:, None] * (onehot - probs)  # (n, K)

        # entropy term: loss has -ent_coef * mean(ent);
        # d H_i / d logit_j = -p_j (log p_j + H_i)  (0 on masked slots)
        safe = np.where(probs > 0.0, probs, 1.0)
        H = -np.sum(np.where(probs > 0.0, probs * np.log(safe), 0.0), axis=1)
        d_ent_d_logits = -probs * (np.log(safe) + H[:, None])
        contrib += (-cfg.ent_coef / n) * d_ent_d_logits

        np.add.at(g_logits, batch.states, contrib)

        # value head: d loss / d V_s = vf_coef * (2/n) * (V_s - return_s)
        g_values = np.zeros_like(self.vh.values)
        d_v = cfg.vf_coef * (2.0 / n) * (v - batch.returns)
        np.add.at(g_values, batch.states, d_v)

        # --- global-norm clip (numpy), cosine LR, Adam step ---
        gnorm = global_grad_norm(g_logits, g_values)
        scale = 1.0 if gnorm <= cfg.grad_clip else cfg.grad_clip / (gnorm + 1e-12)
        flat = np.concatenate([g_logits.reshape(-1) * scale, g_values * scale])
        lr = cosine_lr(
            self.step,
            warmup=cfg.warmup_steps,
            total=cfg.total_steps,
            lr_max=cfg.lr,
            lr_min=cfg.lr_min,
        )
        update = self.opt.step(flat, lr)
        self.policy.logits -= update[: self._n_logits].reshape(self.policy.logits.shape)
        self.vh.values -= update[self._n_logits :]

        self.kl.update(kl, n_steps=1)
        self.step += 1
        return {
            "loss": float(loss),
            "pg_loss": pg_loss,
            "vf_loss": vf_loss,
            "kl": kl,
            "kl_coef": float(self.kl.kl_coef),
            "grad_norm": gnorm,
            "lr": lr,
            "entropy": float(np.mean(ent)),
        }

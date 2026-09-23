"""Vendored from an earlier reinforcement-learning repository of mine (gate 5.7).

Provenance: a local checkout of the earlier RL repository's trainer
module. The code is copied here and is not imported.

The `AdaptiveKLController` update rule is copied verbatim, byte for byte in
every arithmetic expression, so the cross-check test
(``mlops/tests/test_route_optimizer_kl.py``) can assert bit-for-bit equality
against the source on a fixed input, in the same spirit as Phase 3's
AUC/CRPS cross-checks against sklearn/scipy. `cosine_lr` is copied verbatim
too, since it is pure math. The only two departures from the source: the
gradient-clipping helper is not vendored, because it wraps a torch call the
numpy retarget does not need (its own global-norm clip lives in ``ppo.py``),
and the torch import goes with it. Do not change this file casually. A
change here breaks the bit-for-bit provenance claim and needs a fresh
cross-check run.

Ziegler et al. (2019), Fine-Tuning Language Models from Human Preferences
(arXiv:1909.08593, Section 2.2), introduced this adaptive KL controller, and
the class docstring below keeps that upstream credit line.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass
class AdaptiveKLController:
    """Heuristic KL controller from Stiennon et al. (2020), Ouyang et al. (2022).

    Doubles or halves `kl_coef` to keep KL near `target` over a horizon
    of `horizon` updates. Bounded so a runaway batch cannot push the
    coefficient to absurd values.
    """

    kl_coef: float = 0.2
    target: float = 6.0
    horizon: int = 10000
    clip_min: float = 1e-3
    clip_max: float = 100.0

    def update(self, current_kl: float, n_steps: int) -> None:
        if current_kl is None or math.isnan(current_kl):
            return
        proportional_error = (current_kl - self.target) / self.target
        proportional_error = float(max(min(proportional_error, 0.2), -0.2))
        mult = 1.0 + proportional_error * n_steps / self.horizon
        self.kl_coef = float(min(max(self.kl_coef * mult, self.clip_min), self.clip_max))


def cosine_lr(step: int, *, warmup: int, total: int, lr_max: float, lr_min: float) -> float:
    if step < warmup:
        return lr_max * step / max(1, warmup)
    progress = (step - warmup) / max(1, total - warmup)
    return lr_min + 0.5 * (lr_max - lr_min) * (1 + math.cos(math.pi * progress))

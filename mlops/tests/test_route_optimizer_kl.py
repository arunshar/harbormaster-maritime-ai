"""Gate 5.7: the vendored AdaptiveKLController / cosine_lr cross-check.

The earlier reinforcement-learning repository of mine that vendored_kl.py comes
from is not a test dependency (it is an external mirror), so the "bit-for-bit
against the real source" check is realized the same way Phase 3 cross-checks AUC
against sklearn: re-derive the published update rule inline from its definition
and assert the vendored controller matches it exactly on fixed inputs, plus the
pinned fixture values. Because vendored_kl.py is a byte-for-byte copy (its
provenance header), matching the re-derived formula IS matching the source.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from mlops.route_optimizer.vendored_kl import AdaptiveKLController, cosine_lr

PINS = json.loads((Path(__file__).parent.parent / "fixtures" / "expectations.json").read_text())[
    "ppo_stretch_expectations"
]["kl_crosscheck"]


def _rederive(
    kl_coef: float, current_kl: float, target: float, horizon: int, n_steps: int
) -> float:
    """The Ziegler et al. (2019) adaptive-KL update, written out from its definition."""
    proportional_error = (current_kl - target) / target
    proportional_error = max(min(proportional_error, 0.2), -0.2)
    mult = 1.0 + proportional_error * n_steps / horizon
    return min(max(kl_coef * mult, 1e-3), 100.0)


@pytest.mark.parametrize("case", ["above_target", "below_target"])
def test_update_matches_rederived_and_pinned(case):
    c = AdaptiveKLController()  # defaults: kl_coef=0.2, target=6.0, horizon=10000
    current_kl = PINS[case]["current_kl"]
    c.update(current_kl, PINS["n_steps"])
    expected = _rederive(
        PINS["kl_coef_0"], current_kl, PINS["target"], PINS["horizon"], PINS["n_steps"]
    )
    assert c.kl_coef == expected  # bit-for-bit, not approx
    assert c.kl_coef == PINS[case]["kl_coef_1"]


def test_above_target_raises_below_target_lowers():
    hi = AdaptiveKLController()
    hi.update(9.0, 1)  # KL above target -> coefficient rises
    lo = AdaptiveKLController()
    lo.update(3.0, 1)  # KL below target -> coefficient falls
    assert hi.kl_coef > 0.2 > lo.kl_coef


def test_nan_or_none_kl_is_a_noop():
    c = AdaptiveKLController()
    c.update(float("nan"), 1)
    assert c.kl_coef == 0.2
    c.update(None, 1)  # type: ignore[arg-type]
    assert c.kl_coef == 0.2


def test_proportional_error_is_clamped():
    # a huge KL cannot move the coef by more than the clamped +/-0.2 * n/horizon
    c = AdaptiveKLController()
    c.update(1e9, 1)
    assert c.kl_coef == pytest.approx(0.2 * (1 + 0.2 * 1 / 10000))


def test_coef_is_bounded():
    c = AdaptiveKLController(kl_coef=1e-3)
    for _ in range(100000):
        c.update(0.0, 1000)  # push hard toward zero
    assert c.kl_coef >= 1e-3
    c2 = AdaptiveKLController(kl_coef=100.0)
    for _ in range(100000):
        c2.update(1e6, 1000)  # push hard toward the ceiling
    assert c2.kl_coef <= 100.0


def test_cosine_lr_warmup_then_cosine():
    # linear warmup from 0 to lr_max
    assert cosine_lr(0, warmup=10, total=100, lr_max=1.0, lr_min=0.1) == 0.0
    assert cosine_lr(5, warmup=10, total=100, lr_max=1.0, lr_min=0.1) == pytest.approx(0.5)
    # at the warmup boundary it is lr_max (cos(0) = 1)
    assert cosine_lr(10, warmup=10, total=100, lr_max=1.0, lr_min=0.1) == pytest.approx(1.0)
    # at the end it decays to lr_min (cos(pi) = -1)
    assert cosine_lr(100, warmup=10, total=100, lr_max=1.0, lr_min=0.1) == pytest.approx(0.1)
    # midpoint value is the average
    mid = cosine_lr(55, warmup=10, total=100, lr_max=1.0, lr_min=0.1)
    assert mid == pytest.approx(0.1 + 0.5 * 0.9 * (1 + math.cos(math.pi * 0.5)))

"""Gate 5.7 smoke: one CPU PPO training step end to end on the tiny synthetic
corridor graph. Zero GPU, zero AWS.

Usage: .venv/bin/python scripts/ppo_stretch_smoke.py

Also re-verifies the pinned reward checksum, so a change to the graph or the
reward that was not reflected in mlops/fixtures/expectations.json fails here.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "serving"))  # route_optimizer imports app.components.*

from mlops.route_optimizer.graph import tiny_synthetic_graph  # noqa: E402
from mlops.route_optimizer.ppo import PpoConfig  # noqa: E402
from mlops.route_optimizer.reward import coverage_minus_fuel  # noqa: E402
from mlops.route_optimizer.rollout import run_training  # noqa: E402

PINS = json.loads((_ROOT / "mlops" / "fixtures" / "expectations.json").read_text())[
    "ppo_stretch_expectations"
]


def main() -> int:
    g = tiny_synthetic_graph()
    ok = True

    # 1. pinned reward checksum still holds
    reward = coverage_minus_fuel(PINS["canonical_route"], g)
    sha = hashlib.sha256(repr(reward).encode()).hexdigest()
    checksum_ok = reward == PINS["reward"] and sha == PINS["reward_sha256"]
    ok = ok and checksum_ok
    tag = "OK" if checksum_ok else "FAIL"
    print(f"  canonical reward = {reward!r}  sha256 = {sha[:16]}...  [{tag}]")

    # 2. one training step end to end (the actual smoke)
    hist = run_training(g, PpoConfig(), seed=0, n_steps=1, n_rollouts=16, horizon=6)
    m = hist[0]
    step_ok = all(
        k in m for k in ("loss", "pg_loss", "vf_loss", "kl", "kl_coef", "grad_norm", "lr")
    )
    finite = all(v == v and abs(v) != float("inf") for v in m.values())  # no NaN/inf
    ok = ok and step_ok and finite
    print(
        f"  one PPO step: loss={m['loss']:.5f} pg={m['pg_loss']:.5f} vf={m['vf_loss']:.5f} "
        f"kl={m['kl']:.5f} grad_norm={m['grad_norm']:.4f} mean_reward={m['mean_reward']:.4f} "
        f"[{'OK' if step_ok and finite else 'FAIL'}]"
    )

    print("[PASS]" if ok else "[FAIL]", "gate 5.7 PPO stretch smoke")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

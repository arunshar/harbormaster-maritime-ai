"""DEMO STAND-IN Pi-DPM inference server for the Phase 3 AWS showcase.

NOT the real Pi-DPM. `mlops/pidpm_container/server.py` (one directory up)
documents the real contract. That contract needs the `PiDpmScorer` from a
separate, un-vendored earlier reinforcement-learning repository of mine,
and a real checkpoint that the training cluster has not produced yet.
This is a minimal, REAL, buildable container implementing the exact same
SageMaker `/ping` + `/invocations` contract with the toy
sigmoid-over-a-standardized-feature scorer already exercised (and tested)
in `scripts/drill_l1_training_serving_skew.py`, so
`docs/runbooks/PHASE_3_AWS_SHOWCASE.md` has something genuinely deployable
to demo the real infrastructure (the SageMaker async endpoint,
scale-to-zero, the promotion pipeline) without waiting on a real
cluster-trained checkpoint. Every response and every artifact this produces
says "demo stand-in," never "Pi-DPM."
"""

from __future__ import annotations

import json
import math
import os

from flask import Flask, Response, request

app = Flask(__name__)

CHECKPOINT_PATH = os.environ.get("MODEL_PATH", "/opt/ml/model/checkpoint.json")
_checkpoint: dict | None = None


def score_trajectory(
    trajectory: list[list[float]], *, feature_mean: float, feature_std: float
) -> float:
    """The exact toy-scorer shape from drill_l1_training_serving_skew.py's
    `toy_scorer` (a sigmoid over a standardized feature), generalized to the
    gate 3.6 frozen-contract trajectory shape ([[lat, lon], [lat, lon]]): the
    "feature" here is the planar spread between the two fixes, standing in
    for a gap distance, a cheap approximation fine for a demo stand-in."""
    (lat1, lon1), (lat2, lon2) = trajectory[0], trajectory[-1]
    spread_m = math.hypot(lat2 - lat1, lon2 - lon1) * 111_000  # ~meters/degree, planar
    standardized = (spread_m - feature_mean) / feature_std
    return 1.0 / (1.0 + math.exp(-(standardized - 0.5)))


def _load_checkpoint() -> dict:
    global _checkpoint
    if _checkpoint is None:
        with open(CHECKPOINT_PATH) as f:
            _checkpoint = json.load(f)
    return _checkpoint


@app.route("/ping", methods=["GET"])
def ping() -> Response:
    try:
        _load_checkpoint()
        return Response(status=200)
    except Exception:
        return Response(status=503)


@app.route("/invocations", methods=["POST"])
def invocations() -> Response:
    payload = json.loads(request.data)
    checkpoint = _load_checkpoint()
    score = score_trajectory(
        payload["trajectory"],
        feature_mean=checkpoint["feature_mean"],
        feature_std=checkpoint["feature_std"],
    )
    return Response(
        json.dumps({"score": score, "model": "phase3-demo-standin"}),
        mimetype="application/json",
    )


# pragma: no cover - exercised via the real Flask test client, not __main__
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)  # nosec B104  # container entrypoint must bind all interfaces

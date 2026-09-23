"""Illustrative SageMaker inference server (Phase 3, gate 3.6), NOT run in
this repo. Implements the SageMaker /ping + /invocations contract over
the frozen PiDpmScorer.log_prob(trajectory) -> float contract from an
earlier reinforcement-learning repository of mine.

`pidpm.scoring` comes from the earlier RL repository (see the Dockerfile in this
directory for how it lands in the build context); it is not present in this
repo, so this file cannot be imported or run here. It documents the exact
contract serving/app/pidpm_client.py's PiDpmClient calls against.
"""

from __future__ import annotations

import json
import os

from flask import Flask, Response, request

app = Flask(__name__)

_MODEL_PATH = os.environ.get("MODEL_PATH", "/opt/ml/model/model.pt")
_scorer = None  # lazy-loaded on first request, not at import time


def _get_scorer():
    global _scorer
    if _scorer is None:
        from pidpm.scoring import PiDPM  # vendored from the earlier RL repository

        _scorer = PiDPM.from_checkpoint(_MODEL_PATH, map_location="cuda")
    return _scorer


@app.route("/ping", methods=["GET"])
def ping() -> Response:
    # SageMaker's liveness probe: 200 once the checkpoint is loaded and ready.
    try:
        _get_scorer()
        return Response(status=200)
    except Exception:
        return Response(status=503)


@app.route("/invocations", methods=["POST"])
def invocations() -> Response:
    # The exact request shape serving/app/pidpm_client.py's PiDpmClient
    # writes to S3: {"trajectory": [[lat, lon], [lat, lon], ...]}.
    payload = json.loads(request.data)
    trajectory = payload["trajectory"]
    score = _get_scorer().log_prob(trajectory)
    return Response(json.dumps({"score": float(score)}), mimetype="application/json")

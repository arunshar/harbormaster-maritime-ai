# Illustrative, NOT built or run in this repo (Phase 3, gate 3.6).
#
# Wraps the frozen PiDpmScorer.log_prob(trajectory) -> float contract (a
# module in an earlier reinforcement-learning repository of mine) as a
# SageMaker inference container. SageMaker async inference containers use
# the SAME real-time /ping + /invocations contract as synchronous endpoints;
# "async" is SageMaker's own orchestration (queue via S3, not synchronous
# HTTP), invisible to the container itself.
#
# This Dockerfile is not buildable as-is here: the scorer code lives in the
# earlier RL repository, which is separate and is not vendored into
# harbormaster. The real build step (operator-run, at demo-apply time) checks
# out that repository at a fixed release tag into build context alongside this
# Dockerfile, matching the "build + push, operator-run" pattern already used for
# the CDC connect/consumer images.

FROM python:3.11-slim

WORKDIR /opt/pidpm

# Real build step: `git clone --depth 1 --branch <release-tag> <rl-upstream-repo> rl_upstream_src`
# into the build context before `docker build`, so this COPY has something
# to copy. Left as an explicit, documented manual step rather than a
# submodule, since the earlier RL repository is separate and independently versioned.
COPY rl_upstream_src/pidpm /opt/pidpm/pidpm
COPY rl_upstream_src/pidpm/requirements.txt /opt/pidpm/requirements.txt
COPY server.py /opt/pidpm/server.py

RUN pip install --no-cache-dir -r requirements.txt "flask>=3.0" "gunicorn>=22.0"

ENV MODEL_PATH=/opt/ml/model/model.pt
EXPOSE 8080

# SageMaker's contract: /ping (health) and /invocations (score) on :8080.
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "server:app"]

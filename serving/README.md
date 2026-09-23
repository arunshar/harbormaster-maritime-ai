# serving

This directory holds the FastAPI scoring and review service for Harbormaster. The SageMaker async client and the Bedrock explainer are in `app/`, and the explainer stays disabled unless `HM_BEDROCK_MODEL_ID` is set. The deterministic scorer runs offline and needs no AWS resources.

## What runs here

`POST /v1/score-ais` scores one AIS fix against a vessel's recent history with a deterministic plan, and no LLM takes part in it:

- **HeuristicPlanner** routes by history length (0, 1, or at least 3 prior fixes). It replaces the LLM planner of GeoTrace-Agent, an earlier project of mine.
- Deterministic agents vendored from GeoTrace-Agent do the work. The space-time **prism** kernel bounds where a vessel could have been between two fixes. **GapDetector** scores abnormal reporting gaps with the Abnormal Gap Measure (AGM) and a NumPy surrogate for Pi-DPM. **Validator** applies a kinematic gate. **RendezvousFinder** is kept for a future multi-vessel path, and no route calls it.
- **CorridorDeviationDetector** checks each fix against the static sea-lane graph in `app/artifacts/corridors.json`.
- A fixed fusion turns the agent signals into an anomaly `score`, a verdict `confidence`, and the HITL (human-in-the-loop) decision. Anomalous and ambiguous events go to the **Postgres HITL queue**, which falls back to memory when no DSN is set.

The scorer emits the reasons `implausible_speed`, `abnormal_gap`, `off_corridor`, and `unexpected_node`. It rejects corrupt-grade teleports, which exceed the corrupt-data bound, with HTTP 422 and does not score them.

The service exposes `GET /healthz`, `GET /metrics` (Prometheus), `POST /v1/score-ais`, `POST /v1/feedback`, `GET /v1/hitl/pending`, and the `/v1/registry/...` routes for vessels, watchlist entries, and sanctions flags.

## Layout

| Path | Purpose |
| --- | --- |
| `app/main.py` | FastAPI app and routes |
| `app/orchestrator.py` | deterministic `run_plan`, scoring fusion, and HITL routing |
| `app/agents/` | heuristic planner, vendored agents, and the corridor detector |
| `app/components/` | space-time prism kernel and corridor graph geometry |
| `app/artifacts/corridors.json` | frozen synthetic sea-lane graph for the demo |
| `app/hitl.py` | Postgres and in-memory HITL backends |
| `app/pidpm_client.py` | async SageMaker client with an analytic fallback |
| `app/bedrock_explainer.py` | explanation layer, disabled by default and not called by any route |
| `app/cost.py`, `app/metrics.py` | per-inference cost ledger and Prometheus metrics |
| `examples/` | an example scoring request that creates a review case |
| `tests/` | unit and golden tests (golden cases come from the synthetic replay fixture) |
| `Dockerfile` | serving image (build from the repository root) |

## Run it

```bash
uv run --frozen pytest -q serving/tests                           # unit and golden tests
PYTHONPATH=serving uv run --frozen uvicorn app.main:app --port 8000   # API on :8000
make serve-docker                                                  # build the container image
```

Run the `uv sync` command from the top-level [Quick start](../README.md#quick-start) first, and run these commands from the repository root. Settings use the `HM_` environment prefix, and `env.example` lists them. The project scope and claim boundaries are described in [docs/HONESTY.md](../docs/HONESTY.md).

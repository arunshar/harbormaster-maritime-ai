# serving/frontend: HITL reviewer console

This directory holds a Streamlit console for the human-in-the-loop (HITL) review queue. The console reads pending anomalous and ambiguous events from the serving API (`GET /v1/hitl/pending`). It shows them as a table plus a best-effort map, which takes its points from any reason evidence that carries `lat` and `lon`. A reviewer then submits a verdict (`correct`, `incorrect`, or `ambiguous`) through `POST /v1/feedback`. Those verdicts land in the Postgres `hitl_queue`, and they do not retrain the model.

## Run

Run the `uv sync` command from the top-level [Quick start](../../README.md#quick-start) first, then start the console against a running API. Run these commands from the repository root:

```bash
SERVING_URL=http://localhost:8000 uv run --frozen --extra console streamlit run serving/frontend/console.py
```

## Layout

- `hitl_client.py` holds the thin urllib API client (`HitlApi`) and pure view helpers (`feedback_payload`, `format_row`, `reason_codes`, `positions_from_rows`). The tests in `tests/` cover them with no server and no Streamlit.
- `console.py` is the Streamlit UI, kept thin over those helpers. The test suite does not import it, so pytest needs no Streamlit. This repository holds no record of a console smoke run.

# streaming/flink

This directory holds the PyFlink job for Workflow 1. `job.py` keeps the last five fixes for each vessel in keyed state, applies the P_phys gate, and posts `{mmsi, fix, history}` to `POST /v1/score-ais`. It also writes the gated feature item to a DynamoDB online table. The job is a per-event keyed realization without event-time windows or watermarks, as [ADR 0001](../../docs/adr/0001-streaming-per-event-realization.md) explains.

- `job.py`: the Managed Flink entry point and the keyed process function.
- `transforms.py`: parsing, the scorer request body, and the DynamoDB feature item.
- `window_logic.py`: the per-vessel window functions, extracted from `job.py` so unit tests can import them (war story P31).
- `package_app.py` and `pom.xml`: packaging for Managed Flink, including the Kinesis connector jar.
- `tests/`: unit tests that run without a Flink runtime or AWS.

The per-vessel feature functions (`haversine_m`, `gap_since_last_s`, `v_required_mps`, and the `p_physical` gate pinned to the 25 kt vessel cap) live in `../features/`. A test asserts that the feature `v_max` equals the serving configuration exactly. The synthetic AIS replay fixture that the job consumes in the demo lives at `../fixtures/ais_recorded.jsonl`.

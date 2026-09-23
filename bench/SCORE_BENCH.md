# Scoring-kernel benchmark

This benchmark measures the latency and single-thread throughput of the
deterministic scoring kernel behind `/v1/score-ais`. The script is
`bench/bench_score.py`, and it runs locally with no AWS account and no network.

## What it measures

`bench_score.py` times the real in-process scoring path that the golden suite
exercises. That path is `Orchestrator.score(...)` in
`serving/app/orchestrator.py`, and `serving/tests/test_golden.py` asserts
`latency_ms < 200` on the same call. The script measures wall-clock time per
`score()` call with `time.perf_counter()`. This is the same quantity that the
golden `latency_ms` field and the `score_kernel_p95_ms` SLO in
`serving/app/slo.py` describe.

The timed path covers the plan build and all kinematic agents. These agents are
the space-time prism, the gap detector, the corridor detector, the speed check,
and the validator. The path also includes the noisy-OR fusion, the in-memory
HITL enqueue, and the cost record.

Before timing, the script checks that `Orchestrator.watchlist.enabled is False`
and that the Pi-DPM scorer is `None`. It also checks that the scored anomaly
still produces the fixture's expected reason (`abnormal_gap`) and HITL verdict.
If the path drifts onto a degenerate branch, the script exits non-zero and
reports no number.

## Hermetic and deterministic

- The benchmark needs no AWS and no network. With the default `Settings()`,
  `HM_ONLINE_TABLE` and `HM_PIDPM_ENDPOINT` are unset. The CDC watchlist lookup
  is therefore disabled, and the Pi-DPM SageMaker scorer is `None`. The score
  path then touches only `math`, `numpy`, and `shapely`.
- The input is fixed, so no seed is needed. Events are rebuilt from the
  checksummed synthetic golden fixture `streaming/fixtures/ais_recorded.jsonl`,
  which `streaming/replay/generate.py` builds despite its file name, and from
  `streaming/fixtures/expectations.json`. The scoring path contains no random
  number generator.
- The single-event case uses the first golden anomaly (`mmsi 200000001`), which
  has the longest history of the golden events. The mixed case cycles all three
  golden anomalies and all five normal samples.

## How to run

Run these commands from the repository root after the Quick start `uv sync`.

```bash
uv run --frozen python bench/bench_score.py                     # 1000 timed iterations, 200 warmup
uv run --frozen python bench/bench_score.py --iters 2000 --warmup 500
```

The script prints p50, p95, p99, max, and mean latency in milliseconds for the
single-event case and the mixed case. It also prints single-thread throughput in
scores per second.

## Reading the results

- The latency percentiles and the single-thread throughput come from the machine
  that runs the script. They change with the CPU, the Python build, and
  background load, so this document does not quote a fixed number.
- The benchmark measures one Python process on one core. It does not measure a
  deployed service, network overhead, or cloud cost.
- The `logging` filter in the script only suppresses per-inference log output
  so the transcript stays readable. The `log.info` calls still execute, so their
  cost is included in the timing.

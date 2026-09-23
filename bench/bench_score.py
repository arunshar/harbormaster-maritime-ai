#!/usr/bin/env python3
"""Hermetic local benchmark of the deterministic AIS scoring kernel (D1 capacity).

This times the REAL in-process scoring path the golden suite exercises:
`Orchestrator.score(...)` (serving/app/orchestrator.py), the same call
serving/tests/test_golden.py asserts `latency_ms < 200` on. It is a pure
CPU / NumPy / Shapely path: no AWS, no network, no tokens.

Hermetic by construction. Default `Settings()` leaves `online_table` and
`pidpm_endpoint` unset, so the CDC watchlist lookup is disabled
(WatchlistLookup.enabled is False, returns EMPTY_STATUS with zero boto3/redis)
and the Pi-DPM scorer is None. The score path then touches only math, numpy,
and shapely. No socket is opened.

Deterministic input. The events are rebuilt from the checksummed golden fixture
(streaming/fixtures/ais_recorded.jsonl, SHA256-verified by replay.loader) and
streaming/fixtures/expectations.json, the exact inputs the golden test uses.
The scoring path has no RNG, so there is nothing to seed; determinism comes from
the fixed fixture. We assert the per-event reason set matches the fixture's
expectation before timing, so the benchmark cannot silently drift onto a
degenerate no-op path.

Reported: p50 / p95 / p99 / max wall-clock latency per score, and single-thread
throughput (scores/sec). One measured event (the golden abnormal_gap anomaly,
121-fix history) is timed on its own so the number is comparable to the golden
gate; a mixed representative set (all golden anomalies + normals, cycled) gives
the fleet-extrapolation throughput. Both are printed.

Run:  .venv/bin/python bench/bench_score.py
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import platform
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path

import structlog

# Quiet the app's per-inference structlog rows so the timing transcript stays
# readable. The bound logger drops anything below WARNING before rendering, so
# the info-level hitl_enqueue / inference_cost rows are filtered cheaply. The
# calls still execute (their cost is measured); only their output is dropped.
# Nothing about the scoring path changes.
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING),
    cache_logger_on_first_use=True,
)

# Mirror pyproject's pytest `pythonpath = ["serving", "streaming", ...]` so the
# real app modules import exactly as the golden test imports them.
_REPO = Path(__file__).resolve().parents[1]
for _p in ("serving", "streaming"):
    _sp = str(_REPO / _p)
    if _sp not in sys.path:
        sys.path.insert(0, _sp)

from app.config import Settings  # noqa: E402
from app.models import AisFix, AisScoreIn  # noqa: E402
from app.orchestrator import Orchestrator  # noqa: E402
from replay.loader import load_expectations, load_fixture  # noqa: E402


def _as_fix(r) -> AisFix:
    return AisFix(lat=r.lat, lon=r.lon, t=r.t, sog=r.sog, cog=r.cog, heading=r.heading)


def _build_score_in(by_mmsi: dict, mmsi: int, t_iso: str) -> AisScoreIn:
    """Rebuild the AisScoreIn for a vessel's event at t_iso from the fixture.

    Identical construction to serving/tests/_helpers.build_score_in, kept local
    so the benchmark has no test-package dependency.
    """
    t = datetime.fromisoformat(t_iso.replace("Z", "+00:00"))
    history = [_as_fix(r) for r in by_mmsi[mmsi] if r.t < t]
    current = next(_as_fix(r) for r in by_mmsi[mmsi] if r.t == t)
    return AisScoreIn(mmsi=mmsi, fix=current, history=history)


def _percentile(sorted_ms: list[float], q: float) -> float:
    """Nearest-rank percentile (q in [0, 1]) over an already-sorted list."""
    if not sorted_ms:
        return float("nan")
    if q <= 0:
        return sorted_ms[0]
    if q >= 1:
        return sorted_ms[-1]
    rank = q * (len(sorted_ms) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_ms) - 1)
    frac = rank - lo
    return sorted_ms[lo] * (1 - frac) + sorted_ms[hi] * frac


async def _load_inputs():
    """Golden fixture -> (anomaly-under-test payload, mixed representative payloads)."""
    by: dict[int, list] = {}
    for r in load_fixture():  # SHA256-verified checksummed fixture
        by.setdefault(r.mmsi, []).append(r)
    for m in by:
        by[m].sort(key=lambda r: r.t)
    exp = load_expectations()

    anomalies = exp["anomalies"]
    normals = exp["normal_samples"]

    # The single event the golden latency gate uses: the first anomaly.
    a0 = anomalies[0]
    under_test = _build_score_in(by, a0["mmsi"], a0["t"])

    mixed: list[AisScoreIn] = []
    for a in anomalies:
        mixed.append(_build_score_in(by, a["mmsi"], a["t"]))
    for ns in normals:
        mixed.append(_build_score_in(by, ns["mmsi"], ns["t"]))
    return under_test, mixed, a0, anomalies, normals, by, exp


async def _assert_real_path(orch: Orchestrator, under_test: AisScoreIn, a0: dict) -> None:
    """Fail loud if the path is not the real one or has drifted off the golden
    expectation. Guards against timing a stubbed / degenerate kernel."""
    if orch.watchlist.enabled:
        raise SystemExit("watchlist lookup is ENABLED; benchmark is not hermetic")
    if orch.gap._pi_dpm_scorer is not None:
        raise SystemExit("Pi-DPM scorer is wired; benchmark is not hermetic")
    out = await orch.score(under_test)
    codes = {r.code.value for r in out.reasons}
    if a0["expect_reason"] not in codes:
        raise SystemExit(
            f"score path drifted: expected reason {a0['expect_reason']!r}, got {codes}"
        )
    if out.hitl_required is not a0["expect_hitl"]:
        raise SystemExit(
            f"score path drifted: expected hitl {a0['expect_hitl']}, got {out.hitl_required}"
        )


async def _time_series(orch: Orchestrator, payloads: list[AisScoreIn], iters: int) -> list[float]:
    """Time `iters` single-threaded, sequential score() calls, cycling payloads.

    Wall-clock per call via perf_counter, in milliseconds. This is the full
    score() cost (fusion + in-memory HITL enqueue + cost record), the same
    quantity the golden `latency_ms` and the `score_kernel_p95_ms` SLO refer to.
    """
    n = len(payloads)
    lat_ms: list[float] = []
    for i in range(iters):
        p = payloads[i % n]
        t0 = time.perf_counter()
        await orch.score(p)
        lat_ms.append((time.perf_counter() - t0) * 1000.0)
    return lat_ms


def _report(label: str, lat_ms: list[float]) -> dict[str, float]:
    s = sorted(lat_ms)
    p50 = _percentile(s, 0.50)
    p95 = _percentile(s, 0.95)
    p99 = _percentile(s, 0.99)
    mean = statistics.fmean(lat_ms)
    thr = 1000.0 / mean if mean > 0 else float("nan")
    print(f"\n[{label}]  n={len(lat_ms)}")
    print(f"  p50  = {p50:8.4f} ms")
    print(f"  p95  = {p95:8.4f} ms")
    print(f"  p99  = {p99:8.4f} ms")
    print(f"  max  = {s[-1]:8.4f} ms")
    print(f"  mean = {mean:8.4f} ms")
    print(f"  throughput (single thread) = {thr:10.1f} scores/sec")
    return {"p50": p50, "p95": p95, "p99": p99, "max": s[-1], "mean": mean, "thr": thr}


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--iters", type=int, default=1000, help="timed iterations (default 1000)")
    ap.add_argument("--warmup", type=int, default=200, help="warmup iterations (default 200)")
    args = ap.parse_args()

    print("=" * 68)
    print("Harbormaster D1 scoring-kernel benchmark (hermetic, local, no AWS)")
    print("=" * 68)
    print(f"python      : {platform.python_version()} ({platform.python_implementation()})")
    print(f"platform    : {platform.platform()}")
    print(f"machine     : {platform.machine()}")
    print(f"iters/warmup: {args.iters} / {args.warmup}")

    under_test, mixed, a0, anomalies, normals, _by, _exp = await _load_inputs()
    print(
        f"inputs      : anomaly-under-test mmsi={a0['mmsi']} "
        f"({a0['kind']}, {len(under_test.history)} history fixes); "
        f"mixed set = {len(anomalies)} anomalies + {len(normals)} normals"
    )

    orch = await Orchestrator.bootstrap(Settings())
    try:
        await _assert_real_path(orch, under_test, a0)
        print(
            "path check  : REAL score() path, watchlist disabled, Pi-DPM None, "
            "golden reason matched"
        )

        # Warmup: JIT-free Python, but this pages in shapely/numpy, fills the
        # corridor graph, and stabilises the allocator before timing.
        await _time_series(orch, [under_test], args.warmup)

        single = await _time_series(orch, [under_test], args.iters)
        r_single = _report(
            f"single event: golden {a0['kind']} anomaly, mmsi {a0['mmsi']}, "
            f"{len(under_test.history)}-fix history",
            single,
        )

        # Re-warm briefly, then the mixed representative mix for fleet throughput.
        await _time_series(orch, mixed, args.warmup)
        mix = await _time_series(orch, mixed, args.iters)
        r_mix = _report("mixed representative set (anomalies + normals, cycled)", mix)
    finally:
        await orch.shutdown()

    print("\n" + "-" * 68)
    print("Headline (single-event golden path):")
    print(f"  p95 latency     = {r_single['p95']:.4f} ms  (golden gate: < 200 ms)")
    print(f"  throughput      = {r_single['thr']:.1f} scores/sec/core (single thread)")
    print("Mixed set throughput:")
    print(f"  throughput      = {r_mix['thr']:.1f} scores/sec/core (single thread)")
    print("-" * 68)


if __name__ == "__main__":
    asyncio.run(main())

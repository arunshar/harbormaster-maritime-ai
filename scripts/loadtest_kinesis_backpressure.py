"""Phase 5 gate 5.3: Kinesis backpressure load generator (drill M3's tool).

Injects a synthetic burst WELL ABOVE the Phase 1 fixture's steady-state rate
directly onto the Kinesis stream, to force the Flink job's backpressure to
engage and, downstream, the gate 5.2 KEDA ScaledObject to scale the EKS
serving Deployment 0/1 -> N. Reuses the existing replay ingestor's
put-record path (streaming/ingestor/ingest.py: record_to_entry,
batch_entries, _kinesis_putter with its bounded full-jitter backoff), never
a new producer, per the gate's reuse anchor.

What is PURE and tested here (tests/e2e/test_phase5_loadtest.py): the
rate-shaping profile, a trapezoid in requests/second

    steady ______/RAMP\\________BURST________/RAMP\\______ steady
                 start          plateau           end+ramp

realized as a closed-form CUMULATIVE integral, so pacing is drift-free by
construction: at elapsed time t the generator owes exactly
floor(cumulative_records(t)) - already_sent records, and rounding error can
never accumulate across ticks (the same reasoning as the replay pacer's
absolute-timestamp scheduling).

What is NOT here, stated plainly per the honest-science rule: the live
drill itself. Gate 5.3's measured cold-start latency (0 replicas -> first
request served) and the backpressure postmortem
(docs/drills/M3_backpressure_loadtest.md) are W4 demo-window work against a
real stream and cluster; this module ships the tool and its tested pure
core, and no number in this repo is claimed from it until that window runs.

Runtime usage (demo window only, real AWS credentials, Phase 1 applied):

    W4_ARTIFACT_PATH=artifacts/w4/<stamp>/kinesis-load.json \\
    W4_OBSERVER_READY_PATH=artifacts/w4/<stamp>/scale-timeline.json \\
    W4_RUN_ID=<fresh-uuid4> \\
    KINESIS_STREAM_NAME=harbormaster-base-ais-raw \\
    STEADY_RPS=20 BURST_RPS=400 BURST_START_S=60 BURST_END_S=180 RAMP_S=15 \\
    DURATION_S=300 python scripts/loadtest_kinesis_backpressure.py
"""

from __future__ import annotations

import json
import math
import os
import sys
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

# Repo-root + streaming on sys.path so ingestor/replay import when run as a
# script from any cwd (the scripts/cdc_smoke.py convention).
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (REPO_ROOT, os.path.join(REPO_ROOT, "streaming")):
    if _p not in sys.path:
        sys.path.insert(0, _p)


@dataclass(frozen=True)
class BurstProfile:
    """Trapezoidal rate profile in records/second.

    steady_rps   baseline rate outside the burst (the fixture's steady state).
    burst_rps    plateau rate; must exceed steady_rps (a burst that is not
                 above steady state cannot force backpressure).
    burst_start_s  when the up-ramp begins, seconds from t=0.
    burst_end_s    when the plateau ends and the down-ramp begins.
    ramp_s       linear ramp duration on each side; 0 means a step change.
    """

    steady_rps: float
    burst_rps: float
    burst_start_s: float
    burst_end_s: float
    ramp_s: float = 0.0

    def __post_init__(self) -> None:
        for name, value in asdict(self).items():
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite, got {value}")
        if self.steady_rps < 0:
            raise ValueError(f"steady_rps must be >= 0, got {self.steady_rps}")
        if self.burst_rps <= self.steady_rps:
            raise ValueError(
                f"burst_rps ({self.burst_rps}) must exceed steady_rps "
                f"({self.steady_rps}); a burst at or below steady state cannot "
                "force backpressure"
            )
        if self.burst_start_s < 0:
            raise ValueError(f"burst_start_s must be >= 0, got {self.burst_start_s}")
        if self.burst_end_s < self.burst_start_s + self.ramp_s:
            raise ValueError(
                "burst_end_s must be >= burst_start_s + ramp_s "
                f"(got end {self.burst_end_s}, start {self.burst_start_s}, "
                f"ramp {self.ramp_s}); the plateau cannot begin after it ends"
            )
        if self.ramp_s < 0:
            raise ValueError(f"ramp_s must be >= 0, got {self.ramp_s}")


def rate_at(t: float, profile: BurstProfile) -> float:
    """Instantaneous target rate (records/second) at elapsed time t.

    Piecewise: steady | up-ramp | plateau | down-ramp | steady. Ramp
    boundaries belong to the ramp (continuous everywhere when ramp_s > 0;
    with ramp_s = 0 the burst window is a half-open step
    [burst_start_s, burst_end_s))."""
    s, e, r = profile.burst_start_s, profile.burst_end_s, profile.ramp_s
    lo, hi = profile.steady_rps, profile.burst_rps
    if t < s:
        return lo
    if r > 0 and t < s + r:
        return lo + (hi - lo) * (t - s) / r
    if t < e:
        return hi
    if r > 0 and t < e + r:
        return hi - (hi - lo) * (t - e) / r
    return lo


def cumulative_records(t: float, profile: BurstProfile) -> float:
    """Closed-form integral of rate_at over [0, t]: total records owed by t.

    Exact piecewise areas (rectangles + ramp trapezoids), never numeric
    accumulation, so the pacer derived from it cannot drift."""
    if t < 0:
        raise ValueError(f"t must be >= 0, got {t}")
    s, e, r = profile.burst_start_s, profile.burst_end_s, profile.ramp_s
    lo, hi = profile.steady_rps, profile.burst_rps

    def ramp_area(dt: float, from_rate: float, to_rate: float) -> float:
        # Area under a linear ramp segment of elapsed length dt (<= r).
        current = from_rate + (to_rate - from_rate) * dt / r
        return dt * (from_rate + current) / 2.0

    total = lo * min(t, s)
    if t <= s:
        return total
    if r > 0:
        dt = min(t - s, r)
        total += ramp_area(dt, lo, hi)
        if t <= s + r:
            return total
    total += hi * (min(t, e) - (s + r))
    if t <= e:
        return total
    if r > 0:
        dt = min(t - e, r)
        total += ramp_area(dt, hi, lo)
        if t <= e + r:
            return total
    total += lo * (t - (e + r))
    return total


def records_due(elapsed_s: float, already_sent: int, profile: BurstProfile) -> int:
    """How many records the generator owes NOW to stay on the profile.

    floor of the exact cumulative minus what is already out the door; never
    negative. Summing successive dues reproduces floor(cumulative) exactly
    (the drift-free property the tests pin)."""
    if already_sent < 0:
        raise ValueError(f"already_sent must be >= 0, got {already_sent}")
    return max(0, math.floor(cumulative_records(elapsed_s, profile)) - already_sent)


def profile_from_env(env: dict[str, str]) -> BurstProfile:
    """Build the profile from the drill's environment contract."""
    return BurstProfile(
        steady_rps=float(env.get("STEADY_RPS", "20")),
        burst_rps=float(env.get("BURST_RPS", "400")),
        burst_start_s=float(env.get("BURST_START_S", "60")),
        burst_end_s=float(env.get("BURST_END_S", "180")),
        ramp_s=float(env.get("RAMP_S", "15")),
    )


def validate_live_duration(profile: BurstProfile, duration_s: float) -> float:
    """Require a finite run that covers the complete burst and down-ramp."""
    if not math.isfinite(duration_s) or duration_s <= 0:
        raise ValueError(f"duration_s must be finite and > 0, got {duration_s}")
    minimum = profile.burst_end_s + profile.ramp_s
    if duration_s < minimum:
        raise ValueError(
            f"duration_s must be >= burst_end_s + ramp_s ({minimum}), got {duration_s}"
        )
    return duration_s


def run_loadtest(
    profile: BurstProfile,
    duration_s: float,
    entries: list,
    put,
    *,
    batch=None,
    monotonic=time.monotonic,
    sleep=time.sleep,
    tick_s: float = 0.2,
    echo=print,
) -> int:
    """Drive the shaped replay: the whole scheduling loop, I/O injected.

    put/monotonic/sleep are injected exactly like the replay ingestor's own
    replay() (tests run with a fake clock, no AWS, no real delay); batch
    defaults to the ingestor's Kinesis-legal batch_entries. Entries are
    cycled so any fixture length sustains any profile. Returns records sent.
    """
    if not entries:
        raise ValueError("entries is empty; nothing to send")
    if batch is None:
        from ingestor.ingest import batch_entries

        batch = batch_entries
    sent = 0
    start = monotonic()
    while True:
        elapsed = monotonic() - start
        if elapsed >= duration_s:
            break
        due = records_due(elapsed, sent, profile)
        if due:
            chunk = [entries[(sent + i) % len(entries)] for i in range(due)]
            for group in batch(chunk):
                put(group)
            sent += due
            echo(f"t={elapsed:7.1f}s rate={rate_at(elapsed, profile):8.1f}rps sent={sent}")
        sleep(tick_s)
    echo(f"loadtest done: sent={sent} records in {duration_s}s")
    return sent


def write_artifact(path: Path, payload: dict) -> None:
    """Atomically persist the inputs and outputs of one executed live run."""
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with open(descriptor, "w") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def claim_artifact(path: Path, payload: dict) -> None:
    """Atomically claim a fresh artifact path for exactly one live process."""
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with open(descriptor, "w") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
    except Exception:
        path.unlink(missing_ok=True)
        raise


def validate_run_id(value: str) -> str:
    """Require a canonical UUID4 for one live-window handshake."""
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError("run_id must be a canonical UUID4") from error
    if parsed.version != 4 or str(parsed) != value:
        raise ValueError("run_id must be a canonical UUID4")
    return value


def _parse_utc(value: str, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include a timezone")
    return parsed.astimezone(UTC)


def wait_for_observer_ready(
    path: Path,
    *,
    expected_run_id: str,
    expected_load_artifact: Path,
    timeout_seconds: float = 120,
    max_age_seconds: float = 120,
    utc_now=lambda: datetime.now(UTC),
) -> dict:
    """Wait for a fresh zero baseline bound to this exact load run."""
    validate_run_id(expected_run_id)
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be finite and > 0")
    if not math.isfinite(max_age_seconds) or max_age_seconds <= 0:
        raise ValueError("max_age_seconds must be finite and > 0")
    expected_load_path = expected_load_artifact.expanduser().resolve()
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() <= deadline:
        try:
            payload = json.loads(path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            time.sleep(0.2)
            continue

        status = payload.get("status")
        if status != "ready_for_load":
            if status in {
                "invalid_nonzero_replicas",
                "invalid_route_already_serving",
                "invalid_inference_probe",
                "observer_error",
                "interrupted",
            }:
                raise ValueError(f"observer entered terminal status {status}")
            time.sleep(0.2)
            continue
        if payload.get("schema_version") != 1:
            raise ValueError("observer artifact schema_version must equal 1")
        if payload.get("run_id") != expected_run_id:
            raise ValueError("observer run_id does not match W4_RUN_ID")
        try:
            bound_load_path = Path(payload["load_artifact"]).expanduser().resolve()
        except (KeyError, TypeError) as error:
            raise ValueError("observer artifact is missing load_artifact") from error
        if bound_load_path != expected_load_path:
            raise ValueError("observer artifact is bound to a different load artifact")
        baseline = payload.get("baseline")
        if not isinstance(baseline, dict):
            raise ValueError("observer artifact is missing its baseline")
        if (
            baseline.get("status") != "ready_for_load"
            or baseline.get("desired_replicas") != 0
            or baseline.get("available_replicas") != 0
        ):
            raise ValueError("observer baseline does not prove zero desired and available replicas")
        ready_at = _parse_utc(payload.get("ready_at"), "ready_at")
        age_seconds = (utc_now().astimezone(UTC) - ready_at).total_seconds()
        if age_seconds < 0 or age_seconds > max_age_seconds:
            raise ValueError(f"observer readiness is not fresh: age_seconds={age_seconds:.3f}")
        return payload
    raise TimeoutError(f"observer did not expose ready_for_load within {timeout_seconds}s: {path}")


def main() -> int:
    """Demo-window entry point (W4): shape the fixture replay onto Kinesis.

    All AWS wiring lives here, reusing the EXISTING ingestor put path; the
    tested pure core and run_loadtest own every scheduling decision."""
    artifact_value = os.environ.get("W4_ARTIFACT_PATH", "")
    if not artifact_value:
        print("W4_ARTIFACT_PATH is required so every live run leaves evidence", file=sys.stderr)
        return 2
    artifact_path = Path(artifact_value).expanduser().resolve()
    observer_value = os.environ.get("W4_OBSERVER_READY_PATH", "")
    if not observer_value:
        print(
            "W4_OBSERVER_READY_PATH is required to prove a pre-load zero baseline",
            file=sys.stderr,
        )
        return 2

    run_id_value = os.environ.get("W4_RUN_ID", "")
    try:
        run_id = validate_run_id(run_id_value)
    except ValueError as error:
        print(f"W4_RUN_ID is required: {error}", file=sys.stderr)
        return 2

    observer_path = Path(observer_value).expanduser().resolve()
    base_artifact: dict = {
        "schema_version": 1,
        "status": "preparing",
        "run_id": run_id,
        "load_artifact": str(artifact_path),
        "observer_ready_path": str(observer_path),
    }
    try:
        claim_artifact(artifact_path, base_artifact)
    except FileExistsError:
        print(
            f"W4_ARTIFACT_PATH already exists; use a fresh path: {artifact_path}",
            file=sys.stderr,
        )
        return 2

    try:
        import boto3

        from ingestor.ingest import _kinesis_putter, record_to_entry
        from replay.loader import load_fixture

        profile = profile_from_env(dict(os.environ))
        duration_s = validate_live_duration(
            profile,
            float(
                os.environ.get(
                    "DURATION_S",
                    str(profile.burst_end_s + profile.ramp_s + 60),
                )
            ),
        )
        stream = (
            os.environ.get("KINESIS_STREAM_NAME")
            or os.environ["KINESIS_STREAM_ARN"].split("/", 1)[-1]
        )
        region = os.environ.get("AWS_REGION", "us-east-1")
        put = _kinesis_putter(boto3.client("kinesis", region_name=region), stream)

        # Cycle the recorded Phase 1 fixture as the record source: same schema,
        # same partition keys, same put path as the real ingestor.
        entries = [record_to_entry(record) for record in load_fixture()]
        wait_for_observer_ready(
            observer_path,
            expected_run_id=run_id,
            expected_load_artifact=artifact_path,
        )
        started_at = datetime.now(UTC)
        started_monotonic_ns = time.monotonic_ns()
        base_artifact = {
            "schema_version": 1,
            "run_id": run_id,
            "load_artifact": str(artifact_path),
            "started_at": started_at.isoformat().replace("+00:00", "Z"),
            "stream_name": stream,
            "aws_region": region,
            "duration_seconds": duration_s,
            "profile": asdict(profile),
            "observer_ready_path": str(observer_path),
        }
        write_artifact(
            artifact_path,
            {**base_artifact, "status": "running", "records_sent": None},
        )
        print(
            f"loadtest start: stream={stream} profile={profile} duration_s={duration_s}",
            flush=True,
        )
        records_sent = run_loadtest(profile, duration_s, entries, put)
        if records_sent <= 0:
            raise RuntimeError("loadtest completed without sending any records")
    except Exception as error:
        write_artifact(
            artifact_path,
            {
                **base_artifact,
                "status": "failed",
                "ended_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "error": f"{type(error).__name__}: {error}",
                "records_sent": None,
            },
        )
        print(str(error), file=sys.stderr)
        return 1
    ended_at = datetime.now(UTC)
    elapsed_ns = time.monotonic_ns() - started_monotonic_ns
    write_artifact(
        artifact_path,
        {
            **base_artifact,
            "status": "completed",
            "ended_at": ended_at.isoformat().replace("+00:00", "Z"),
            "elapsed_seconds": elapsed_ns / 1_000_000_000,
            "records_sent": records_sent,
        },
    )
    print(f"evidence artifact: {artifact_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

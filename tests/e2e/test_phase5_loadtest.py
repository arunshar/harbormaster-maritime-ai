"""Gate 5.3 unit tests: the load generator's PURE rate-shaping core.

Only the pure functions run here (profile validation, the piecewise rate,
the closed-form cumulative integral, and the drift-free records_due pacer);
the live drill against a real stream/cluster is explicitly W4 demo-window
work (drill M3), and no measurement is claimed from this module until then.
"""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime

import pytest

from scripts.loadtest_kinesis_backpressure import (
    BurstProfile,
    claim_artifact,
    cumulative_records,
    profile_from_env,
    rate_at,
    records_due,
    run_loadtest,
    validate_live_duration,
    validate_run_id,
    wait_for_observer_ready,
)

# steady 20 rps, burst 400 rps over [60, 180) with 15 s linear ramps.
PROFILE = BurstProfile(steady_rps=20, burst_rps=400, burst_start_s=60, burst_end_s=180, ramp_s=15)
STEP = BurstProfile(steady_rps=20, burst_rps=400, burst_start_s=60, burst_end_s=180, ramp_s=0)
RUN_ID = "12345678-1234-4234-9234-123456789abc"


def ready_observer_payload(load_artifact, *, ready_at=None, run_id=RUN_ID):
    ready_at = ready_at or datetime.now(UTC)
    baseline = {
        "status": "ready_for_load",
        "captured_at": ready_at.isoformat().replace("+00:00", "Z"),
        "desired_replicas": 0,
        "available_replicas": 0,
        "inference_http_status": 503,
        "inference_error": "no healthy target",
    }
    return {
        "schema_version": 1,
        "status": "ready_for_load",
        "run_id": run_id,
        "ready_at": ready_at.isoformat().replace("+00:00", "Z"),
        "load_artifact": str(load_artifact.resolve()),
        "baseline": baseline,
    }


# --------------------------------------------------------------------------- #
# Profile validation
# --------------------------------------------------------------------------- #
def test_burst_must_exceed_steady():
    with pytest.raises(ValueError, match="must exceed steady_rps"):
        BurstProfile(steady_rps=50, burst_rps=50, burst_start_s=0, burst_end_s=10)


def test_negative_steady_rejected():
    with pytest.raises(ValueError, match="steady_rps"):
        BurstProfile(steady_rps=-1, burst_rps=10, burst_start_s=0, burst_end_s=10)


def test_plateau_cannot_end_before_ramp_completes():
    with pytest.raises(ValueError, match="plateau"):
        BurstProfile(steady_rps=1, burst_rps=10, burst_start_s=10, burst_end_s=15, ramp_s=10)


def test_negative_burst_start_rejected():
    with pytest.raises(ValueError, match="burst_start_s"):
        BurstProfile(steady_rps=1, burst_rps=10, burst_start_s=-1, burst_end_s=10)


def test_negative_ramp_rejected():
    with pytest.raises(ValueError, match="ramp_s"):
        BurstProfile(steady_rps=1, burst_rps=10, burst_start_s=0, burst_end_s=10, ramp_s=-1)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_profile_values_rejected(value):
    with pytest.raises(ValueError, match="must be finite"):
        BurstProfile(
            steady_rps=1,
            burst_rps=10,
            burst_start_s=0,
            burst_end_s=value,
        )


def test_negative_time_rejected():
    with pytest.raises(ValueError, match="t must be >= 0"):
        cumulative_records(-0.1, PROFILE)


def test_negative_already_sent_rejected():
    with pytest.raises(ValueError, match="already_sent"):
        records_due(10, -1, PROFILE)


# --------------------------------------------------------------------------- #
# rate_at: the trapezoid, boundary by boundary
# --------------------------------------------------------------------------- #
def test_rate_piecewise_shape():
    assert rate_at(0, PROFILE) == 20
    assert rate_at(59.999, PROFILE) == 20
    assert rate_at(60, PROFILE) == 20  # ramp starts AT steady
    assert rate_at(67.5, PROFILE) == pytest.approx(210)  # mid up-ramp
    assert rate_at(75, PROFILE) == 400  # plateau begins
    assert rate_at(120, PROFILE) == 400
    assert rate_at(180, PROFILE) == 400  # down-ramp starts AT burst
    assert rate_at(187.5, PROFILE) == pytest.approx(210)  # mid down-ramp
    assert rate_at(195, PROFILE) == 20  # back to steady
    assert rate_at(1000, PROFILE) == 20


def test_rate_step_profile_half_open_window():
    assert rate_at(59.999, STEP) == 20
    assert rate_at(60, STEP) == 400
    assert rate_at(179.999, STEP) == 400
    assert rate_at(180, STEP) == 20


def test_burst_is_well_above_steady():
    # The drill's premise: the plateau must dominate the steady state.
    assert max(rate_at(t, PROFILE) for t in range(0, 300)) == PROFILE.burst_rps
    assert PROFILE.burst_rps >= 10 * PROFILE.steady_rps


# --------------------------------------------------------------------------- #
# cumulative_records: exact closed-form areas
# --------------------------------------------------------------------------- #
def test_cumulative_exact_segment_values():
    # Steady until 60 s: 20 rps * 60 s.
    assert cumulative_records(60, PROFILE) == pytest.approx(1200)
    # Up-ramp trapezoid: 15 s * (20 + 400) / 2 = 3150.
    assert cumulative_records(75, PROFILE) == pytest.approx(1200 + 3150)
    # Plateau: 105 s * 400.
    assert cumulative_records(180, PROFILE) == pytest.approx(1200 + 3150 + 42000)
    # Down-ramp mirrors the up-ramp.
    assert cumulative_records(195, PROFILE) == pytest.approx(1200 + 3150 + 42000 + 3150)
    # Tail steady.
    assert cumulative_records(255, PROFILE) == pytest.approx(1200 + 3150 + 42000 + 3150 + 1200)


def test_cumulative_mid_ramp_is_the_partial_trapezoid():
    # 7.5 s into the up-ramp: area = 7.5 * (20 + 210) / 2.
    assert cumulative_records(67.5, PROFILE) == pytest.approx(1200 + 7.5 * (20 + 210) / 2)


def test_cumulative_step_profile():
    assert cumulative_records(180, STEP) == pytest.approx(20 * 60 + 400 * 120)


def test_cumulative_is_monotone_nondecreasing():
    times = [x / 4 for x in range(0, 4 * 300)]
    values = [cumulative_records(t, PROFILE) for t in times]
    assert all(b >= a for a, b in zip(values, values[1:], strict=False))


def test_cumulative_consistent_with_rate_numerically():
    # The closed form must agree with a fine Riemann sum of rate_at.
    dt = 0.01
    approx = sum(rate_at(k * dt, PROFILE) * dt for k in range(int(250 / dt)))
    assert cumulative_records(250, PROFILE) == pytest.approx(approx, rel=1e-3)


# --------------------------------------------------------------------------- #
# records_due: the drift-free pacer property
# --------------------------------------------------------------------------- #
def test_records_due_never_negative_and_zero_when_ahead():
    assert records_due(0, 0, PROFILE) == 0
    assert records_due(10, 10_000, PROFILE) == 0


def test_records_due_sums_to_floor_of_cumulative():
    # Feeding successive dues back in reproduces floor(cumulative) exactly at
    # every tick: rounding error cannot accumulate across ticks.
    sent = 0
    for tick in range(0, 2 * 260):
        t = tick / 2  # 0.5 s ticks across the whole profile
        sent += records_due(t, sent, PROFILE)
        assert sent == math.floor(cumulative_records(t, PROFILE))


def test_records_due_burst_dominates_steady_window():
    # One second of plateau owes 400; one second of steady owes 20.
    plateau = records_due(121, 0, PROFILE) - records_due(120, 0, PROFILE)
    assert cumulative_records(121, PROFILE) - cumulative_records(120, PROFILE) == pytest.approx(400)
    assert plateau in (399, 400, 401)  # floor at the two endpoints


# --------------------------------------------------------------------------- #
# Env contract
# --------------------------------------------------------------------------- #
def test_profile_from_env_defaults_and_overrides():
    default = profile_from_env({})
    assert (default.steady_rps, default.burst_rps) == (20, 400)
    custom = profile_from_env(
        {
            "STEADY_RPS": "5",
            "BURST_RPS": "50",
            "BURST_START_S": "10",
            "BURST_END_S": "30",
            "RAMP_S": "2",
        }
    )
    assert custom == BurstProfile(5, 50, 10, 30, 2)


def test_profile_from_env_rejects_inverted_burst():
    with pytest.raises(ValueError, match="must exceed steady_rps"):
        profile_from_env({"STEADY_RPS": "100", "BURST_RPS": "50"})


def test_live_duration_must_cover_the_full_burst_and_down_ramp():
    assert validate_live_duration(PROFILE, 195) == 195
    with pytest.raises(ValueError, match=r"burst_end_s \+ ramp_s"):
        validate_live_duration(PROFILE, 194.9)
    with pytest.raises(ValueError, match="finite and > 0"):
        validate_live_duration(PROFILE, float("nan"))


# --------------------------------------------------------------------------- #
# run_loadtest: the loop with a fake clock, no AWS, no real delay
# --------------------------------------------------------------------------- #
class FakeClock:
    """Deterministic monotonic clock: each sleep() advances it by the slept
    amount, the replay()-style injection this repo's ingestor tests use."""

    def __init__(self) -> None:
        self.t = 100.0  # arbitrary non-zero epoch: the loop must use deltas

    def monotonic(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += seconds


def _simple_batch(chunk):
    yield chunk  # identity batching keeps assertions about counts exact


def test_run_loadtest_paces_to_the_profile_with_a_fake_clock():
    profile = BurstProfile(steady_rps=2, burst_rps=20, burst_start_s=2, burst_end_s=4, ramp_s=0)
    clock = FakeClock()
    sent_batches: list[list] = []
    entries = [{"Data": b"a", "PartitionKey": "1"}, {"Data": b"b", "PartitionKey": "2"}]
    total = run_loadtest(
        profile,
        6.0,
        entries,
        sent_batches.append,
        batch=_simple_batch,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        tick_s=0.5,
        echo=lambda *_: None,
    )
    # Owed by the last tick before 6 s (elapsed 5.5): 2*2 steady + 20*2 burst
    # + 2*1.5 tail = 47; the fake clock makes this exact.
    assert total == math.floor(cumulative_records(5.5, profile))
    assert sum(len(b) for b in sent_batches) == total
    # Entries are cycled, never exhausted.
    assert {e["PartitionKey"] for batch in sent_batches for e in batch} == {"1", "2"}


def test_run_loadtest_rejects_empty_entries():
    with pytest.raises(ValueError, match="entries is empty"):
        run_loadtest(PROFILE, 1.0, [], lambda batch: None, batch=_simple_batch)


def test_run_loadtest_default_batching_is_the_ingestor_path():
    # No batch= injected: the loop must fall back to the ingestor's own
    # Kinesis-legal batch_entries (the reuse anchor, not a new producer).
    profile = BurstProfile(steady_rps=1, burst_rps=5, burst_start_s=0.5, burst_end_s=1.5)
    clock = FakeClock()
    batches: list[list] = []
    total = run_loadtest(
        profile,
        2.0,
        [{"Data": b"x", "PartitionKey": "1"}],
        batches.append,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        tick_s=0.5,
        echo=lambda *_: None,
    )
    assert total == sum(len(b) for b in batches) > 0


def test_main_wires_the_fixture_through_run_loadtest(monkeypatch, tmp_path):
    import scripts.loadtest_kinesis_backpressure as mod

    calls = {}

    def fake_run(profile, duration_s, entries, put, **kwargs):
        calls["profile"] = profile
        calls["duration_s"] = duration_s
        calls["n_entries"] = len(entries)
        return 1

    monkeypatch.setattr(mod, "run_loadtest", fake_run)
    monkeypatch.setenv("KINESIS_STREAM_NAME", "harbormaster-base-ais-raw")
    monkeypatch.setenv("DURATION_S", "195")
    artifact = tmp_path / "load.json"
    observer = tmp_path / "scale.json"
    observer.write_text(json.dumps(ready_observer_payload(artifact)))
    monkeypatch.setenv("W4_ARTIFACT_PATH", str(artifact))
    monkeypatch.setenv("W4_OBSERVER_READY_PATH", str(observer))
    monkeypatch.setenv("W4_RUN_ID", RUN_ID)
    assert mod.main() == 0
    assert calls["profile"] == profile_from_env(dict(__import__("os").environ))
    assert calls["duration_s"] == 195.0
    assert calls["n_entries"] > 0  # the real Phase 1 fixture, cycled
    payload = __import__("json").loads(artifact.read_text())
    assert payload["status"] == "completed"
    assert payload["stream_name"] == "harbormaster-base-ais-raw"
    assert payload["profile"] == {
        "steady_rps": 20.0,
        "burst_rps": 400.0,
        "burst_start_s": 60.0,
        "burst_end_s": 180.0,
        "ramp_s": 15.0,
    }
    assert payload["records_sent"] == 1
    assert payload["started_at"].endswith("Z")
    assert payload["ended_at"].endswith("Z")


def test_main_returns_1_on_an_empty_fixture(monkeypatch, tmp_path):
    import replay.loader as loader
    import scripts.loadtest_kinesis_backpressure as mod

    monkeypatch.setattr(loader, "load_fixture", lambda *a, **k: [])
    monkeypatch.setenv("KINESIS_STREAM_NAME", "harbormaster-base-ais-raw")
    monkeypatch.setenv("DURATION_S", "195")
    artifact = tmp_path / "load.json"
    observer = tmp_path / "scale.json"
    observer.write_text(json.dumps(ready_observer_payload(artifact)))
    monkeypatch.setenv("W4_OBSERVER_READY_PATH", str(observer))
    monkeypatch.setenv("W4_ARTIFACT_PATH", str(artifact))
    monkeypatch.setenv("W4_RUN_ID", RUN_ID)
    assert mod.main() == 1
    payload = json.loads(artifact.read_text())
    assert payload["status"] == "failed"
    assert payload["records_sent"] is None


def test_main_refuses_a_live_run_without_an_artifact_path(monkeypatch):
    import scripts.loadtest_kinesis_backpressure as mod

    monkeypatch.setenv("KINESIS_STREAM_NAME", "harbormaster-base-ais-raw")
    monkeypatch.delenv("W4_ARTIFACT_PATH", raising=False)
    assert mod.main() == 2


def test_main_refuses_a_live_run_without_an_observer_baseline(monkeypatch, tmp_path):
    import scripts.loadtest_kinesis_backpressure as mod

    monkeypatch.setenv("W4_ARTIFACT_PATH", str(tmp_path / "load.json"))
    monkeypatch.delenv("W4_OBSERVER_READY_PATH", raising=False)
    assert mod.main() == 2


def test_main_refuses_to_overwrite_an_existing_artifact(monkeypatch, tmp_path):
    import scripts.loadtest_kinesis_backpressure as mod

    artifact = tmp_path / "load.json"
    artifact.write_text("existing evidence")
    monkeypatch.setenv("W4_ARTIFACT_PATH", str(artifact))
    monkeypatch.setenv("W4_OBSERVER_READY_PATH", str(tmp_path / "scale.json"))
    monkeypatch.setenv("W4_RUN_ID", RUN_ID)
    assert mod.main() == 2
    assert artifact.read_text() == "existing evidence"


def test_wait_for_observer_ready_reads_the_preload_marker(tmp_path):
    artifact = tmp_path / "scale.json"
    load_artifact = tmp_path / "load.json"
    now = datetime(2026, 7, 13, tzinfo=UTC)
    expected = ready_observer_payload(load_artifact, ready_at=now)
    artifact.write_text(json.dumps(expected))
    assert (
        wait_for_observer_ready(
            artifact,
            expected_run_id=RUN_ID,
            expected_load_artifact=load_artifact,
            utc_now=lambda: now,
        )
        == expected
    )


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda payload, _path: payload.update(run_id="22345678-1234-4234-9234-123456789abc"),
            "run_id",
        ),
        (
            lambda payload, path: payload.update(
                load_artifact=str((path / "other.json").resolve())
            ),
            "different load artifact",
        ),
        (
            lambda payload, _path: payload["baseline"].update(desired_replicas=1),
            "zero desired",
        ),
        (
            lambda payload, _path: payload.update(ready_at="2026-07-13T03:00:00Z"),
            "not fresh",
        ),
    ],
)
def test_wait_for_observer_ready_rejects_unbound_stale_or_nonzero_evidence(
    tmp_path, mutate, message
):
    observer = tmp_path / "scale.json"
    load_artifact = tmp_path / "load.json"
    now = datetime(2026, 7, 13, 4, 0, tzinfo=UTC)
    payload = ready_observer_payload(load_artifact, ready_at=now)
    mutate(payload, tmp_path)
    observer.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match=message):
        wait_for_observer_ready(
            observer,
            expected_run_id=RUN_ID,
            expected_load_artifact=load_artifact,
            utc_now=lambda: now,
        )


def test_claim_artifact_is_one_use_and_never_overwrites(tmp_path):
    artifact = tmp_path / "load.json"
    claim_artifact(artifact, {"status": "preparing", "run_id": RUN_ID})
    with pytest.raises(FileExistsError):
        claim_artifact(artifact, {"status": "preparing", "run_id": RUN_ID})
    assert json.loads(artifact.read_text()) == {"status": "preparing", "run_id": RUN_ID}


@pytest.mark.parametrize(
    "value",
    ["", "not-a-uuid", "12345678-1234-1234-9234-123456789abc", RUN_ID.upper()],
)
def test_validate_run_id_requires_a_canonical_uuid4(value):
    with pytest.raises(ValueError, match="canonical UUID4"):
        validate_run_id(value)


def test_main_refuses_a_live_run_without_a_run_id(monkeypatch, tmp_path):
    import scripts.loadtest_kinesis_backpressure as mod

    monkeypatch.setenv("W4_ARTIFACT_PATH", str(tmp_path / "load.json"))
    monkeypatch.setenv("W4_OBSERVER_READY_PATH", str(tmp_path / "scale.json"))
    monkeypatch.delenv("W4_RUN_ID", raising=False)
    assert mod.main() == 2
    assert not (tmp_path / "load.json").exists()

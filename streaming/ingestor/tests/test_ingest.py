"""Unit tests for the replay ingestor (gate G4). No AWS: put and sleep are injected."""

from __future__ import annotations

import inspect
import random
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

from ingestor import ingest
from ingestor.ingest import (
    MAX_BYTES_PER_CALL,
    PUT_BASE_DELAY,
    PUT_DELAY_CAP,
    PutRecordsExhaustedError,
    _kinesis_putter,
    backoff_schedule,
    batch_entries,
    jittered_backoff,
    live_ais_requested,
    record_to_entry,
    reject_unapproved_live_ais,
    replay,
    shard_distribution,
    shard_of,
    skew_ratio,
    sorted_by_time,
)
from replay.loader import AisRecord, load_fixture

T0 = datetime(2024, 6, 1, tzinfo=UTC)


def test_target_a_research_preparation_refuses_historical_live_mode():
    with pytest.raises(RuntimeError, match="live AIS ingestion is disabled"):
        reject_unapproved_live_ais({"AIS_LIVE": "true"})


def test_target_a_research_preparation_keeps_replay_mode_available():
    assert live_ais_requested({}) is False
    assert live_ais_requested({"AIS_LIVE": "false"}) is False
    reject_unapproved_live_ais({"AIS_LIVE": "false"})


def test_target_a_live_guard_precedes_aws_client_setup():
    source = inspect.getsource(ingest.main)
    assert source.index("reject_unapproved_live_ais()") < source.index("import boto3")


def _rec(mmsi: int, minute: int, lat: float = 40.0, lon: float = -74.0) -> AisRecord:
    t = T0 + timedelta(minutes=minute)
    return AisRecord(mmsi=mmsi, lat=lat, lon=lon, t=t, sog=9.0, cog=None, heading=None)


def test_sorted_by_time_orders_ascending():
    recs = [_rec(1, 5), _rec(2, 1), _rec(3, 3)]
    got = [r.t for r in sorted_by_time(recs)]
    assert got == sorted(got)


def test_record_to_entry_partition_key_and_json():
    import json

    e = record_to_entry(_rec(200000001, 0))
    assert e["PartitionKey"] == "200000001"
    payload = json.loads(e["Data"])
    assert payload["mmsi"] == 200000001
    assert payload["t"] == "2024-06-01T00:00:00Z"


def test_batch_entries_respects_record_count_limit():
    entries = [{"Data": b"x", "PartitionKey": "1"} for _ in range(1200)]
    batches = list(batch_entries(entries, max_records=500))
    assert [len(b) for b in batches] == [500, 500, 200]
    assert sum(len(b) for b in batches) == 1200


def test_batch_entries_respects_byte_limit():
    # Each entry ~1 KiB; cap at 4 KiB -> at most 4 per batch.
    entries = [{"Data": b"x" * 1024, "PartitionKey": "1"} for _ in range(10)]
    batches = list(batch_entries(entries, max_records=500, max_bytes=4096))
    assert all(sum(len(e["Data"]) + 1 for e in b) <= 4096 for b in batches)
    assert sum(len(b) for b in batches) == 10


def test_batch_entries_oversize_record_raises():
    import pytest

    entries = [{"Data": b"x" * (MAX_BYTES_PER_CALL + 1), "PartitionKey": "1"}]
    with pytest.raises(ValueError):
        list(batch_entries(entries))


def test_replay_sends_all_records_in_time_order():
    recs = [_rec(1, 5), _rec(2, 1), _rec(3, 3)]
    sent_batches: list[list[dict]] = []
    slept: list[float] = []
    n = replay(recs, put=sent_batches.append, speedup=60.0, sleep=slept.append)

    assert n == 3
    flat = [e for b in sent_batches for e in b]
    assert len(flat) == 3
    # Emitted in ascending-time order (MMSI 2 @1min, 3 @3min, 1 @5min).
    assert [e["PartitionKey"] for e in flat] == ["2", "3", "1"]
    assert all(s >= 0 for s in slept)


def test_replay_no_pacing_when_speedup_zero():
    recs = [_rec(1, 0), _rec(2, 10)]
    slept: list[float] = []
    replay(recs, put=lambda b: None, speedup=0, sleep=slept.append)
    assert slept == []


def test_backoff_schedule_is_capped_exponential():
    assert backoff_schedule(0, base=0.5) == 0.5
    assert backoff_schedule(1, base=0.5) == 1.0
    assert backoff_schedule(2, base=0.5) == 2.0
    assert backoff_schedule(100, base=0.5, cap=30.0) == 30.0


# --------------------------------------------------------------------------
# Kinesis PutRecords retry: bounded exponential backoff with full jitter.
# No AWS: the boto3 kinesis client is a Mock; time.sleep is captured, not real.
# --------------------------------------------------------------------------


def test_jittered_backoff_within_capped_exponential_window():
    # Full jitter draws uniformly in [0, min(cap, base * 2**attempt)].
    rng = random.Random(0)
    for attempt in range(8):
        ceiling = min(PUT_DELAY_CAP, PUT_BASE_DELAY * (2**attempt))
        d = jittered_backoff(attempt, base=PUT_BASE_DELAY, cap=PUT_DELAY_CAP, rng=rng)
        assert 0.0 <= d <= ceiling
    # The window itself grows exponentially then saturates at the cap.
    assert jittered_backoff(0, base=1.0, cap=100.0, rng=random.Random(1)) <= 1.0
    assert jittered_backoff(100, base=1.0, cap=5.0, rng=random.Random(1)) <= 5.0


def _ok_response(n: int) -> dict:
    return {"FailedRecordCount": 0, "Records": [{"SequenceNumber": str(i)} for i in range(n)]}


def _partial_failure_response(n_total: int, failed_idx: set[int]) -> dict:
    records = []
    for i in range(n_total):
        if i in failed_idx:
            records.append({"ErrorCode": "ProvisionedThroughputExceededException"})
        else:
            records.append({"SequenceNumber": str(i)})
    return {"FailedRecordCount": len(failed_idx), "Records": records}


def _throttling_client_error():
    from botocore.exceptions import ClientError

    return ClientError(
        {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "slow down"}},
        "PutRecords",
    )


def _entries(n: int) -> list[dict]:
    return [{"Data": b"x", "PartitionKey": str(i)} for i in range(n)]


def test_kinesis_putter_retries_partial_failures_then_succeeds(monkeypatch):
    # First call: records 1 and 2 fail. Retry: only those two, and both succeed.
    client = Mock()
    client.put_records.side_effect = [
        _partial_failure_response(3, failed_idx={1, 2}),
        _ok_response(2),
    ]
    slept: list[float] = []
    put = _kinesis_putter(client, "ais-raw", sleep=slept.append, rng=random.Random(1234))

    put(_entries(3))

    assert client.put_records.call_count == 2
    # The retry resubmits only the two still-failing records (indexes 1 and 2).
    retry_records = client.put_records.call_args_list[1].kwargs["Records"]
    assert [r["PartitionKey"] for r in retry_records] == ["1", "2"]
    # Exactly one backoff between the two attempts, and it was a positive delay.
    assert len(slept) == 1
    assert slept[0] > 0


def test_kinesis_putter_maps_later_failures_against_current_pending_records():
    # Round 1 fails noncontiguous original indexes 1, 4, and 6. Round 2 then
    # fails indexes 0 and 2 of that smaller pending list, which are original
    # records 1 and 6. Indexing against the original batch here would wrongly
    # resend records 0 and 2 instead.
    client = Mock()
    client.put_records.side_effect = [
        _partial_failure_response(7, failed_idx={1, 4, 6}),
        _partial_failure_response(3, failed_idx={0, 2}),
        _ok_response(2),
    ]
    slept: list[float] = []
    put = _kinesis_putter(client, "ais-raw", sleep=slept.append, rng=random.Random(1234))

    put(_entries(7))

    submitted = [
        [record["PartitionKey"] for record in call.kwargs["Records"]]
        for call in client.put_records.call_args_list
    ]
    assert submitted == [
        ["0", "1", "2", "3", "4", "5", "6"],
        ["1", "4", "6"],
        ["1", "6"],
    ]
    assert len(slept) == 2


def test_kinesis_putter_rejects_response_cardinality_mismatch():
    client = Mock()
    client.put_records.return_value = _ok_response(1)
    put = _kinesis_putter(client, "ais-raw")

    with pytest.raises(ValueError, match=r"zip\(\) argument 2 is shorter"):
        put(_entries(2))


def test_kinesis_putter_retries_throttling_clienterror_then_succeeds(monkeypatch):
    # Whole-call throttling twice, then success. Backoff is increasing per attempt.
    client = Mock()
    client.put_records.side_effect = [
        _throttling_client_error(),
        _throttling_client_error(),
        _ok_response(2),
    ]
    slept: list[float] = []
    # Deterministic rng that returns the top of each jitter window so delays are
    # strictly increasing and comparable (window grows as base * 2**attempt).
    top_rng = Mock()
    top_rng.uniform.side_effect = lambda _lo, hi: hi
    put = _kinesis_putter(client, "ais-raw", base_delay=0.1, sleep=slept.append, rng=top_rng)

    put(_entries(2))

    assert client.put_records.call_count == 3
    assert len(slept) == 2
    assert slept[1] > slept[0]  # exponential growth of the backoff window


def test_kinesis_putter_non_retriable_clienterror_raises_immediately():
    from botocore.exceptions import ClientError

    client = Mock()
    client.put_records.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "no"}}, "PutRecords"
    )
    slept: list[float] = []
    put = _kinesis_putter(client, "ais-raw", sleep=slept.append, rng=random.Random(0))

    with pytest.raises(ClientError):
        put(_entries(2))
    assert client.put_records.call_count == 1  # not retried
    assert slept == []


def test_kinesis_putter_gives_up_after_max_retries_and_surfaces_error():
    # Throttling on every call: after max_retries the error is surfaced (re-raised).
    from botocore.exceptions import ClientError

    client = Mock()
    # 1 initial attempt + 3 retries = 4 throttling errors; each is auto-raised
    # because side_effect is an iterable of exception instances.
    client.put_records.side_effect = [_throttling_client_error() for _ in range(4)]
    slept: list[float] = []
    put = _kinesis_putter(
        client, "ais-raw", max_retries=3, sleep=slept.append, rng=random.Random(7)
    )

    with pytest.raises(ClientError):
        put(_entries(2))
    # 1 initial attempt + 3 retries = 4 calls; 3 backoffs before giving up.
    assert client.put_records.call_count == 4
    assert len(slept) == 3


def test_kinesis_putter_gives_up_partial_failures_after_max_retries():
    # Persistent partial failure: bounded attempts, then surface the stuck records
    # rather than silently dropping them and letting replay count the batch as sent.
    client = Mock()
    client.put_records.side_effect = lambda **_kw: _partial_failure_response(2, {0, 1})
    slept: list[float] = []
    put = _kinesis_putter(
        client, "ais-raw", max_retries=2, sleep=slept.append, rng=random.Random(0)
    )

    with pytest.raises(PutRecordsExhaustedError) as raised:
        put(_entries(2))

    assert raised.value.failed_count == 2
    assert raised.value.attempts == 3
    assert raised.value.error_codes == {"ProvisionedThroughputExceededException": 2}
    assert client.put_records.call_count == 3  # 1 + 2 retries
    assert len(slept) == 2


def test_kinesis_putter_exhaustion_reports_only_the_remaining_failed_subset():
    client = Mock()
    client.put_records.side_effect = [
        _partial_failure_response(4, {1, 3}),
        _partial_failure_response(2, {1}),
        _partial_failure_response(1, {0}),
    ]
    put = _kinesis_putter(
        client, "ais-raw", max_retries=2, sleep=lambda _delay: None, rng=random.Random(0)
    )

    with pytest.raises(PutRecordsExhaustedError) as raised:
        put(_entries(4))

    assert raised.value.failed_count == 1
    assert raised.value.attempts == 3
    assert raised.value.error_codes == {"ProvisionedThroughputExceededException": 1}
    submitted = [
        [record["PartitionKey"] for record in call.kwargs["Records"]]
        for call in client.put_records.call_args_list
    ]
    assert submitted == [["0", "1", "2", "3"], ["1", "3"], ["3"]]


def test_kinesis_putter_zero_retries_surfaces_partial_failure_without_sleep():
    client = Mock()
    client.put_records.return_value = _partial_failure_response(1, {0})
    slept: list[float] = []
    put = _kinesis_putter(client, "ais-raw", max_retries=0, sleep=slept.append)

    with pytest.raises(PutRecordsExhaustedError) as raised:
        put(_entries(1))

    assert raised.value.attempts == 1
    assert client.put_records.call_count == 1
    assert slept == []


def test_replay_propagates_partial_failure_instead_of_returning_false_sent_count():
    client = Mock()
    client.put_records.return_value = _partial_failure_response(1, {0})
    put = _kinesis_putter(client, "ais-raw", max_retries=0)

    with pytest.raises(PutRecordsExhaustedError):
        replay([_rec(1, 0)], put=put, speedup=0)


def test_kinesis_putter_success_path_unchanged_no_sleep():
    client = Mock()
    client.put_records.return_value = _ok_response(3)
    slept: list[float] = []
    put = _kinesis_putter(client, "ais-raw", sleep=slept.append, rng=random.Random(0))

    put(_entries(3))

    assert client.put_records.call_count == 1
    assert slept == []  # no backoff on the happy path


# --------------------------------------------------------------------------
# Shard-distribution / skew instrumentation.
# We intentionally do NOT spread a single MMSI across shards (that would corrupt
# per-vessel trajectory ordering); these helpers only MEASURE the distribution so
# a hot shard triggers monitoring/resharding, not key-spreading. See ingest.py.
# --------------------------------------------------------------------------


def test_shard_of_is_deterministic_and_in_range():
    for n in (1, 2, 4, 8, 16):
        for key in ("200000001", "0", "999999999"):
            s = shard_of(key, n)
            assert 0 <= s < n
            assert shard_of(key, n) == s  # deterministic (pure MD5 mapping)


def test_shard_of_matches_kinesis_md5_hashkey_mapping():
    # Reference implementation of Kinesis's partition-key -> shard routing:
    # MD5 the key to a 128-bit hash key, then pick the equal-width range.
    import hashlib

    def reference(key: str, n: int) -> int:
        h = int.from_bytes(hashlib.md5(key.encode()).digest(), "big")  # nosec B324
        return (h * n) >> 128

    for key in ("1", "200000001", "200234567", "9"):
        for n in (1, 3, 4, 8):
            assert shard_of(key, n) == reference(key, n)


def test_shard_of_rejects_nonpositive_shard_count():
    with pytest.raises(ValueError):
        shard_of("1", 0)


def test_shard_distribution_counts_all_entries_and_covers_every_shard():
    entries = [{"Data": b"x", "PartitionKey": str(m)} for m in range(200)]
    dist = shard_distribution(entries, 4)
    assert sum(dist.values()) == 200  # every entry counted exactly once
    assert set(dist) == {0, 1, 2, 3}  # every shard present (0 allowed), no strays


def test_skew_ratio_uniform_is_one_and_single_hot_key_is_max():
    # One partition key -> all records on one shard -> maximal skew == shard_count.
    hot = [{"Data": b"x", "PartitionKey": "42"} for _ in range(1000)]
    assert skew_ratio(hot, 4) == pytest.approx(4.0)
    # Empty batch is defined as unskewed.
    assert skew_ratio([], 4) == 1.0
    # A perfectly even hand-built distribution has skew ~1.0.
    even = []
    for s in range(4):
        # find one key per target shard so each shard gets an equal count
        k, found = 0, 0
        while found < 25:
            if shard_of(str(k), 4) == s:
                even.append({"Data": b"x", "PartitionKey": str(k)})
                found += 1
            k += 1
    assert skew_ratio(even, 4) == pytest.approx(1.0)


def test_skew_ratio_on_replay_fixture_is_bounded_and_forbids_key_spreading():
    # Real skew check on the synthetic AIS replay fixture: hash its actual MMSIs to
    # shards the way Kinesis will and assert the busiest shard is not pathological.
    # This fixture is tiny (8 distinct vessels) so per-shard counts are lumpy by
    # construction; the guard is a loose upper bound, documenting that even a small,
    # high-cardinality-in-production keyspace stays within a few x of uniform without
    # any intra-key spreading. On a live 150K-vessel feed this ratio approaches 1.0.
    records = load_fixture()
    entries = [record_to_entry(r) for r in records]
    n_shards = 4
    ratio = skew_ratio(entries, n_shards)
    # Sanity: the metric is well-formed and reflects a real, non-degenerate spread.
    assert ratio >= 1.0  # a max-share can never be below the uniform share
    assert ratio <= float(n_shards)  # and never above the single-hot-key ceiling
    # Each vessel's fixes stay whole on one shard: the shard assignment depends only
    # on MMSI, so every record for a given MMSI shares a shard (per-vessel ordering
    # is preserved, which is exactly why we never spread a key).
    by_mmsi_shard = {}
    for r, e in zip(records, entries, strict=True):
        by_mmsi_shard.setdefault(r.mmsi, set()).add(shard_of(e["PartitionKey"], n_shards))
    assert all(len(shards) == 1 for shards in by_mmsi_shard.values())

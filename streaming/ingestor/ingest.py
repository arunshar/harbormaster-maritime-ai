"""Replay ingestor (Phase 1.4, gate G4).

Reads the synthetic AIS replay fixture (from S3 or a local path), replays it in
timestamp order at ~10x real time, and PutRecords in batches to the ais-raw
Kinesis stream with partitionKey = MMSI (so a vessel's fixes stay ordered on one
shard). REPLAY is the default and the only permitted runtime mode in this
research-preparation build. The historical AISStream websocket
adapter remains as a pure, unit-tested parser and reconnect-loop reference, but
`AIS_LIVE=true` fails before an AWS or network client is created. A future
licensed-source workstream must replace that guard through a separately
reviewed successor. The pure helpers (ordering, batching, backoff) are
unit-tested without AWS; boto3 is imported lazily so the tests need no cloud.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import time
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path

from replay.loader import AisRecord, load_fixture

# Kinesis PutRecords hard limits.
MAX_RECORDS_PER_CALL = 500
MAX_BYTES_PER_CALL = 5 * 1024 * 1024  # 5 MiB

# PutRecords retry policy: bounded exponential backoff with full jitter.
# Kinesis surfaces throttling both as a whole-call ClientError (e.g.
# ProvisionedThroughputExceededException) and as partial per-record failures in
# the response, so we retry only the still-failing records each round.
PUT_MAX_RETRIES = 5
PUT_BASE_DELAY = 0.1  # seconds
PUT_DELAY_CAP = 20.0  # seconds

# Botocore error codes we treat as retriable (throttling / transient service side).
RETRIABLE_ERROR_CODES = frozenset(
    {
        "ProvisionedThroughputExceededException",
        "ThrottlingException",
        "Throttling",
        "RequestThrottled",
        "TooManyRequestsException",
        "ServiceUnavailable",
        "InternalFailure",
        "KMSThrottlingException",
    }
)

# A PutRecords entry: {"Data": bytes, "PartitionKey": str}.
KinesisEntry = dict

_RESEARCH_PREPARATION_LIVE_AIS_ERROR = (
    "live AIS ingestion is disabled in this research-preparation build; "
    "select a licensed source and complete a separately reviewed successor"
)


def live_ais_requested(env: Mapping[str, str] | None = None) -> bool:
    """True only for the historical flag value that previously enabled live mode."""
    values = os.environ if env is None else env
    return values.get("AIS_LIVE", "").lower() == "true"


def reject_unapproved_live_ais(env: Mapping[str, str] | None = None) -> None:
    """Fail before AWS or network setup when historical live mode is requested."""
    if live_ais_requested(env):
        raise RuntimeError(_RESEARCH_PREPARATION_LIVE_AIS_ERROR)


class PutRecordsExhaustedError(RuntimeError):
    """A bounded PutRecords retry ended with unresolved records."""

    def __init__(
        self,
        *,
        failed_count: int,
        attempts: int,
        error_codes: dict[str, int],
    ) -> None:
        self.failed_count = failed_count
        self.attempts = attempts
        self.error_codes = dict(error_codes)
        errors = ", ".join(
            f"{code}={count}" for code, count in sorted(self.error_codes.items())
        )
        super().__init__(
            f"Kinesis PutRecords exhausted after {attempts} attempt(s); "
            f"{failed_count} record(s) still failed ({errors})"
        )


def sorted_by_time(records: Sequence[AisRecord]) -> list[AisRecord]:
    """Replay order: ascending timestamp, MMSI as a stable tiebreak."""
    return sorted(records, key=lambda r: (r.t, r.mmsi))


def record_to_entry(r: AisRecord) -> KinesisEntry:
    """Map one AIS record to a PutRecords entry (compact JSON, MMSI partition key)."""
    payload = {
        "mmsi": r.mmsi,
        "lat": r.lat,
        "lon": r.lon,
        "t": r.t.isoformat().replace("+00:00", "Z"),
        "sog": r.sog,
        "cog": r.cog,
        "heading": r.heading,
    }
    data = json.dumps(payload, separators=(",", ":")).encode()
    return {"Data": data, "PartitionKey": str(r.mmsi)}


def _entry_size(e: KinesisEntry) -> int:
    # Kinesis counts the data blob plus the partition key toward the 5 MB cap.
    return len(e["Data"]) + len(e["PartitionKey"].encode())


def batch_entries(
    entries: Sequence[KinesisEntry],
    max_records: int = MAX_RECORDS_PER_CALL,
    max_bytes: int = MAX_BYTES_PER_CALL,
) -> Iterator[list[KinesisEntry]]:
    """Chunk entries into PutRecords-legal batches (<= max_records and <= max_bytes)."""
    batch: list[KinesisEntry] = []
    size = 0
    for e in entries:
        es = _entry_size(e)
        if es > max_bytes:
            raise ValueError(f"single record is {es} bytes, over the {max_bytes}-byte cap")
        if batch and (len(batch) >= max_records or size + es > max_bytes):
            yield batch
            batch, size = [], 0
        batch.append(e)
        size += es
    if batch:
        yield batch


def backoff_schedule(attempt: int, base: float = 0.5, cap: float = 30.0) -> float:
    """Exponential backoff (seconds), capped, for the live-mode reconnect loop (gate 1.9)."""
    return min(cap, base * (2 ** max(0, attempt)))


def jittered_backoff(
    attempt: int,
    base: float = PUT_BASE_DELAY,
    cap: float = PUT_DELAY_CAP,
    rng: random.Random | None = None,
) -> float:
    """Full-jitter backoff (seconds): uniform in [0, min(cap, base * 2**attempt)].

    Full jitter (AWS "Exponential Backoff and Jitter") spreads retries across the
    window so a fleet of putters does not resynchronize and re-throttle in lockstep.
    attempt is 0-based (first retry is attempt 0). rng is injected for deterministic
    tests; it defaults to the module random.
    """
    ceiling = min(cap, base * (2 ** max(0, attempt)))
    draw = (rng or random).uniform(0.0, ceiling)
    return draw


def _is_retriable_client_error(exc: Exception) -> bool:
    """True for a botocore ClientError whose code is throttling / transient."""
    resp = getattr(exc, "response", None)
    if not isinstance(resp, dict):
        return False
    code = resp.get("Error", {}).get("Code", "")
    return code in RETRIABLE_ERROR_CODES


# --------------------------------------------------------------------------
# Shard-distribution / skew instrumentation.
#
# We do NOT spread a single MMSI across shards. AIS anomaly detection is per-vessel
# and order-sensitive: the Flink job keys by MMSI and holds each vessel's previous
# fix in keyed state to compute inter-fix features (gap, distance, v_required). If a
# vessel's fixes landed on different shards they would arrive interleaved / out of
# order, and the prev-fix state would be wrong -- the trajectory would be corrupted.
# So the partition key stays exactly str(MMSI) (see record_to_entry), one vessel to
# one shard, ordered.
#
# The skew that this forbids intra-key spreading from fixing is naturally bounded:
# MMSI is high-cardinality (a live AIS feed carries on the order of 150K distinct
# vessels), so hashing MMSI -> shard spreads load evenly across a handful of shards
# by the law of large numbers; no single key is a large fraction of the stream. The
# pathological case is one genuinely hot vessel (a stationary sensor replaying at
# high rate). The correct mitigation for that is monitoring plus resharding
# (split the hot shard), NOT key-spreading, which would break per-vessel ordering.
# These helpers provide that monitoring: they replicate Kinesis's own
# partition-key-to-shard mapping so we can measure the distribution the stream will
# actually see and alert when it is skewed, rather than discover a hot shard from
# ProvisionedThroughputExceeded backpressure at load.


def shard_of(partition_key: str, shard_count: int) -> int:
    """Which of `shard_count` equal-width shards a partition key lands on.

    Mirrors Kinesis's routing: the partition key is MD5-hashed to a 128-bit integer
    (the "hash key"), and the shard whose hash-key range contains it owns the record.
    For a stream split into equal-width ranges this reduces to hash * n // 2**128.
    Real streams can have uneven explicit hash-key ranges, but equal width is the
    default and the right model for a skew estimate. Used for instrumentation only;
    the actual routing is done by Kinesis from the PartitionKey we set.
    """
    if shard_count < 1:
        raise ValueError("shard_count must be >= 1")
    h = int.from_bytes(hashlib.md5(partition_key.encode()).digest(), "big")  # nosec B324  # MD5 mirrors Kinesis's own partition-key routing, not security
    return (h * shard_count) >> 128


def shard_distribution(entries: Sequence[KinesisEntry], shard_count: int) -> Counter[int]:
    """Count how many entries route to each shard (0..shard_count-1)."""
    dist: Counter[int] = Counter()
    for e in entries:
        dist[shard_of(e["PartitionKey"], shard_count)] += 1
    for s in range(shard_count):
        dist.setdefault(s, 0)
    return dist


def skew_ratio(entries: Sequence[KinesisEntry], shard_count: int) -> float:
    """Hot-shard skew: busiest shard's share of records divided by the uniform share.

    1.0 is perfectly even; 2.0 means the hottest shard carries twice its fair share.
    Emit this as a stream metric; a sustained value well above 1 says reshard (or the
    upstream feed has a pathological hot vessel), NOT that we should spread a key.
    Returns 1.0 for an empty batch (nothing to be skewed).
    """
    if shard_count < 1:
        raise ValueError("shard_count must be >= 1")
    dist = shard_distribution(entries, shard_count)
    total = sum(dist.values())
    if total == 0:
        return 1.0
    max_share = max(dist.values()) / total
    return max_share * shard_count


def replay(
    records: Sequence[AisRecord],
    put: Callable[[list[KinesisEntry]], None],
    speedup: float = 10.0,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Replay records in time order, pacing inter-batch gaps by 1/speedup, and
    PutRecords in Kinesis-legal batches. Returns the count sent. `put` and `sleep`
    are injected so tests run with no AWS and no real delay. speedup <= 0 blasts
    with no pacing (used by the fast smoke test)."""
    ordered = sorted_by_time(records)
    entries = [record_to_entry(r) for r in ordered]
    times = [r.t for r in ordered]
    prev_t = None
    sent = 0
    for batch in batch_entries(entries):
        first_t = times[sent]  # sent is the index of this batch's first entry
        if prev_t is not None and speedup > 0:
            gap = (first_t - prev_t).total_seconds() / speedup
            if gap > 0:
                sleep(gap)
        put(batch)
        sent += len(batch)
        prev_t = times[sent - 1]
    return sent


# --------------------------------------------------------------------------
# Runtime wiring (only exercised in the container; not imported by the tests)
# --------------------------------------------------------------------------


def _name_from_arn(arn: str) -> str:
    # arn:aws:kinesis:<region>:<acct>:stream/<name> -> <name>
    return arn.split("/", 1)[-1]


def _load_records() -> list[AisRecord]:
    """Load the fixture from FIXTURE_URI (s3://bucket/key or a local path); default
    to the bundled local fixture for a laptop run."""
    uri = os.environ.get("FIXTURE_URI", "")
    if uri.startswith("s3://"):
        import tempfile

        import boto3

        bucket, key = uri[len("s3://") :].split("/", 1)
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        with tempfile.NamedTemporaryFile("wb", suffix=".jsonl", delete=False) as f:
            f.write(body)
            tmp = f.name
        return load_fixture(tmp)
    return load_fixture(Path(uri)) if uri else load_fixture()


def _kinesis_putter(
    client,
    stream_name: str,
    max_retries: int = PUT_MAX_RETRIES,
    base_delay: float = PUT_BASE_DELAY,
    delay_cap: float = PUT_DELAY_CAP,
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
) -> Callable[[list[KinesisEntry]], None]:
    """Build a PutRecords sink with bounded exponential backoff and full jitter.

    Retries up to max_retries on retriable/throttling conditions from two sources:
    a whole-call ClientError (e.g. ProvisionedThroughputExceededException) and the
    normal-at-load partial failure (per-record ErrorCode in the response), where
    only the still-failing records are resubmitted. Between attempts it sleeps a
    full-jitter delay. The success path (no failures) is unchanged. When retries
    are exhausted, both whole-call errors and unresolved partial failures raise;
    the caller must never count a partially delivered batch as sent.
    sleep/rng are injected so tests run with no real waiting and deterministic jitter.
    """

    def put(batch: list[KinesisEntry]) -> None:
        from botocore.exceptions import ClientError

        pending = batch
        for attempt in range(max_retries + 1):
            try:
                resp = client.put_records(StreamName=stream_name, Records=pending)
            except ClientError as exc:
                # Non-retriable client errors surface immediately; retriable ones
                # (throttling / transient) back off unless retries are exhausted.
                if not _is_retriable_client_error(exc) or attempt >= max_retries:
                    raise
                sleep(jittered_backoff(attempt, base=base_delay, cap=delay_cap, rng=rng))
                continue

            responses = resp.get("Records", [])
            failed_responses = [rec for rec in responses if rec.get("ErrorCode")]
            failed = [
                entry
                for entry, rec in zip(pending, responses, strict=True)
                if rec.get("ErrorCode")
            ]
            if not failed:
                return
            if attempt >= max_retries:
                error_counts = Counter(str(rec["ErrorCode"]) for rec in failed_responses)
                raise PutRecordsExhaustedError(
                    failed_count=len(failed),
                    attempts=attempt + 1,
                    error_codes=dict(error_counts),
                )
            pending = failed
            sleep(jittered_backoff(attempt, base=base_delay, cap=delay_cap, rng=rng))

    return put


def main() -> None:
    # Keep this guard before boto3 setup. A rejected live-mode request must not
    # create an AWS client, read a provider credential, or open a network path.
    reject_unapproved_live_ais()

    import boto3

    region = os.environ.get("AWS_REGION", "us-east-1")
    stream = os.environ.get("KINESIS_STREAM_NAME") or _name_from_arn(
        os.environ["KINESIS_STREAM_ARN"]
    )
    put = _kinesis_putter(boto3.client("kinesis", region_name=region), stream)

    speedup = float(os.environ.get("REPLAY_SPEEDUP", "10"))
    sent = replay(_load_records(), put, speedup=speedup)
    print(f"replayed {sent} records to {stream}")


if __name__ == "__main__":
    main()

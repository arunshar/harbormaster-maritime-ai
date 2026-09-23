"""Managed Flink (KDA) feature job (Phase 1.5, gate G5).

Pipeline: Kinesis ais-raw -> parse -> keyBy(MMSI) -> per-vessel keyed state holds
the previous fix -> window_features -> P_phys cheap-gate. Gated events are written
to the DynamoDB online feature store and POSTed to the serving scorer
(/v1/score-ais). The raw stream is teed to S3/Iceberg by the Firehose module, not
here.

The feature/transform logic lives in the bundled flink.window_logic module as a
deliberate, byte-identical duplicate of the pure, unit-tested functions in
streaming.features.features and streaming.flink.transforms. The driver imports
that module from the application zip, then cloudpickle.register_pickle_by_value
embeds it in the UDF payload. This matters because FeatureProcess runs in a
separate Python worker subprocess that does not inherit the driver's sys.path.
Three attempts to ship the packages through Managed Flink's pyFiles mechanisms
hit confirmed dependency-staging failures (env.add_python_file left the worker
module missing; comma-separated paths raised FileAlreadyExistsException; a merged
directory was rejected by UpdateApplication validation). Bundling the module for
the driver and serializing it by value keeps the logic testable without requiring
pyFiles in the worker. These are W1 live-run findings from 2026-07-04.

Config comes from Managed Flink's real Runtime Properties mechanism: AWS writes
/etc/flink/application_properties.json at container start (generated from the
Terraform module's environment_properties.property_group blocks), NOT plain OS
environment variables. Confirmed against AWS's own PyFlink example (aws-samples/
amazon-managed-service-for-apache-flink-examples), whose main.py reads this exact
path via a PropertyGroupId lookup; this repo's earlier os.environ[...] reads were
never validated against the real runtime (first-ever live KDA deploy, W1 sprint
window, 2026-07-04) and would have failed identically to the missing-JAR error
this same window caught.

A true 1-minute event-time tumbling window can replace the per-fix keyed process
below; per-fix processing against keyed prev-state is the equivalent realization
used for the demo and keeps the operator to standard, portable PyFlink APIs. The
rationale for that per-event realization is recorded in
docs/adr/0001-streaming-per-event-realization.md.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import cloudpickle
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kinesis import FlinkKinesisConsumer
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

# The pure feature/transform logic lives in the sibling window_logic module (no
# pyflink import), so it stays importable and unit-testable without a Flink
# runtime. Because it now lives in a real package module rather than inlined in
# __main__, cloudpickle would serialize each referenced function/class BY
# REFERENCE (just its qualified name), and the Python UDF worker subprocess would
# then need flink.window_logic importable on its OWN sys.path, which it is not
# (the exact dependency-staging failure the module docstring documents).
# register_pickle_by_value(window_logic) restores the original behavior: these
# functions/classes are shipped to the worker BY VALUE, byte-for-byte as the old
# __main__ inlining did, so nothing about the running job changes.
import flink.window_logic as _window_logic
from flink.window_logic import (
    Fix,
    WindowFeatures,
    feature_item,
    mmsi_partition_key,
    open_no_redirect,
    parse_ais_json,
    passes_gate,
    post_scorer_with_retry,
    quarantine_envelope,
    score_request,
    sigv4_headers,
    validate_ais_score_response,
    window_features,
)

cloudpickle.register_pickle_by_value(_window_logic)

APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"
FLINK_JOB_PROPERTY_GROUP = "FlinkJob"

# Task-manager logs surface in the KDA CloudWatch log stream; a real logger (not a
# bare `except: pass`) is how a dropped record or a failed scorer POST becomes
# visible for alarming instead of silently vanishing.
log = logging.getLogger("harbormaster.flink")


# --- Flink wiring ---


def _read_application_properties() -> list[dict]:
    with open(APPLICATION_PROPERTIES_FILE_PATH) as f:
        return json.load(f)


def _property_group(properties: list[dict], group_id: str) -> dict:
    for group in properties:
        if group["PropertyGroupId"] == group_id:
            return group["PropertyMap"]
    raise KeyError(f"no PropertyGroupId={group_id!r} in {APPLICATION_PROPERTIES_FILE_PATH}")


# serving's HeuristicPlanner only routes to abnormal-gap detection (STAGD + AGM)
# once n_history (the vessel's prior-fix count) is >= 3; sending fewer means gap
# anomalies are silently never evaluated, not merely under-scored (see
# score_request()'s docstring). 5 gives comfortable headroom above that
# threshold without growing the per-key state or the scorer payload much.
HISTORY_WINDOW = 5


def _fix_to_dict(f: Fix) -> dict:
    return {
        "lat": f.lat,
        "lon": f.lon,
        "t": f.t.isoformat(),
        "sog": f.sog,
        "cog": f.cog,
        "heading": f.heading,
    }


def _fix_from_dict(d: dict) -> Fix:
    return Fix(
        lat=d["lat"],
        lon=d["lon"],
        t=datetime.fromisoformat(d["t"]),
        sog=d["sog"],
        cog=d["cog"],
        heading=d["heading"],
    )


def _history_to_json(fixes: list[Fix]) -> str:
    return json.dumps([_fix_to_dict(f) for f in fixes])


def _history_from_json(s: str) -> list[Fix]:
    return [_fix_from_dict(d) for d in json.loads(s)]


class FeatureProcess(KeyedProcessFunction):
    """Per-MMSI: compute window features against the previous fix, gate, write the
    gated feature item to DynamoDB, and POST the scorer -- all inline in
    process_element, not a separate .add_sink(). PyFlink's SinkFunction is a thin
    wrapper around a real JVM-side sink class (its __init__ takes a Java class name
    or JavaObject, per pyflink.datastream.functions.SinkFunction); a bare Python
    subclass has no such backing and fails at runtime with
    "AttributeError: '...' object has no attribute '_j_function'" the moment
    .add_sink() tries to invoke it (a real, first-live-run finding, W1 sprint
    window, 2026-07-04). KeyedProcessFunction has no such restriction (it is a
    genuine Python-side function via ProcessFunction's RPC bridge, not a
    JavaFunctionWrapper), so side effects belong here instead. DynamoDB/HTTP
    clients are built lazily per task so the operator still serializes cleanly to
    the Flink cluster."""

    def __init__(self, table: str, region: str, scorer_url: str, quarantine_bucket: str = ""):
        self._table, self._region, self._url = table, region, scorer_url
        # Optional S3 dead-letter (quarantine) sink. Empty string disables it (the
        # local/demo path and the existing tests run without a bucket); when unset we
        # still LOG the drop rather than silently returning, so nothing is lost quietly.
        self._quarantine_bucket = quarantine_bucket
        self._ddb = None
        self._s3 = None

    def open(self, ctx: RuntimeContext):
        self._history = ctx.get_state(ValueStateDescriptor("history", Types.STRING()))

    def _dynamo(self):
        if self._ddb is None:
            import boto3

            self._ddb = boto3.resource("dynamodb", region_name=self._region).Table(self._table)
        return self._ddb

    def _s3_client(self):
        if self._s3 is None:
            import boto3

            self._s3 = boto3.client("s3", region_name=self._region)
        return self._s3

    def _quarantine(self, raw: str | bytes, reason: str) -> None:
        """Dead-letter a record the job cannot process: log it, count it, and (if a
        quarantine bucket is configured) write the envelope to S3 for later triage or
        replay. This replaces the previous silent drop so malformed / unroutable data
        is observable, not lost. A quarantine-write failure is itself only logged --
        the DLQ path must never take down the operator."""
        envelope = quarantine_envelope(raw, reason, datetime.now(UTC))
        # A counter the CloudWatch metric filter can alarm on (log-based metric); this
        # is the signal that upstream data quality or the scorer has degraded.
        log.warning("ais.quarantine reason=%s", reason)
        if not self._quarantine_bucket:
            return
        key = f"quarantine/dt={envelope['quarantined_at'][:10]}/{uuid.uuid4().hex}.json"
        try:
            self._s3_client().put_object(
                Bucket=self._quarantine_bucket,
                Key=key,
                Body=json.dumps(envelope).encode(),
                ContentType="application/json",
            )
        except Exception:  # noqa: BLE001  # DLQ write is best-effort; never crash the task
            log.exception("ais.quarantine.write_failed key=%s", key)

    def process_element(self, raw: str, ctx: KeyedProcessFunction.Context):
        try:
            mmsi, fix = parse_ais_json(raw)
        except ValueError as exc:
            # Parser-rejected AIS: dead-letter it instead of the old silent drop,
            # so bad upstream data is counted and recoverable, not invisible.
            self._quarantine(raw, f"parse_error: {exc}")
            return

        history_json = self._history.value()
        history = _history_from_json(history_json) if history_json else []
        prev = history[-1] if history else None
        feats: WindowFeatures = window_features(fix, prev)
        self._history.update(_history_to_json((history + [fix])[-HISTORY_WINDOW:]))

        if not passes_gate(feats):
            return

        item = feature_item(mmsi, feats, fix.t)
        request = score_request(mmsi, fix, history)
        # feature_item()'s ttl is computed from fix.t (the AIS event time), correct
        # for real live data but not for a replayed fixture whose timestamps are
        # historical (2024): that ttl is already years in the past, so DynamoDB's
        # TTL sweeper deletes every written item almost immediately (a real
        # first-live-run finding, W1 sprint window, 2026-07-04 -- items were seen
        # briefly in a table scan, then gone). Override with a wall-clock-based
        # ttl at the write site; feature_item() itself is unchanged (still correct
        # for production use with genuinely current event times).
        item["ttl"] = int(time.time()) + 7 * 86400
        # DynamoDB's put_item rejects native Python float (TypeError: Float types
        # are not supported. Use Decimal types instead), a real first-live-run
        # finding, W1 sprint window, 2026-07-04. feature_item() stays a plain,
        # JSON-friendly pure function (its own unit test asserts a plain float);
        # the round-trip-through-JSON conversion is local to this DynamoDB write.
        self._dynamo().put_item(Item=json.loads(json.dumps(item), parse_float=Decimal))
        body = json.dumps(request).encode()
        scorer_url = f"{self._url.rstrip('/')}/v1/score-ais"

        def _send() -> None:
            # Sign each attempt afresh. Managed Flink credentials rotate, and
            # API Gateway's AWS_IAM route rejects the old unsigned urllib POST.
            req = urllib.request.Request(
                scorer_url,
                data=body,
                headers=sigv4_headers(scorer_url, body, self._region),
                method="POST",
            )
            with open_no_redirect(req, timeout_seconds=5) as response:
                response_body = response.read()
                validate_ais_score_response(response.status, response_body, mmsi)

        # Scoring is best-effort from the stream, but the old bare `except: pass`
        # swallowed every failure with no signal and no second try. Replace it with a
        # LOGGED, bounded, non-blocking retry (a couple of quick attempts, capped
        # backoff) and dead-letter the request if they all fail, so a transient scorer
        # blip is retried, a sustained outage is visible and counted, and a slow scorer
        # still cannot stall the operator. HITL remains the backstop for any miss.
        ok, err = post_scorer_with_retry(
            _send,
            on_error=lambda attempt, exc: log.warning(
                "ais.scorer.post_failed mmsi=%s attempt=%s err=%s", mmsi, attempt, exc
            ),
        )
        if not ok:
            self._quarantine(body, f"scorer_post_failed: {err}")

        yield json.dumps({"mmsi": mmsi, "item": item, "request": request})


def main() -> None:
    props = _property_group(_read_application_properties(), FLINK_JOB_PROPERTY_GROUP)
    region = props.get("aws_region", "us-east-1")
    stream = props["kinesis_stream_name"]
    table = props["feast_online_table"]
    scorer_url = props["serving_endpoint"]  # API Gateway invoke URL or Cloud Map DNS
    # Optional S3 dead-letter sink for malformed AIS and unrecoverable scorer POSTs.
    # Absent in the local/demo path; when set (Terraform FlinkJob property group) the
    # operator writes quarantine envelopes there for triage/replay.
    quarantine_bucket = props.get("quarantine_bucket", "")

    env = StreamExecutionEnvironment.get_execution_environment()
    # boto3 (used lazily by FeatureProcess._dynamo) is present in the driver's
    # Python environment but not the separate Python UDF worker subprocess;
    # set_python_requirements ships it there. This is a single-file mechanism,
    # distinct from the pyFiles Runtime Property (which hit real, confirmed
    # bugs shipping multiple local-package directories); a real first-live-run
    # finding, W1 sprint window, 2026-07-04.
    env.set_python_requirements(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "requirements.txt")
    )
    consumer = FlinkKinesisConsumer(
        stream,
        SimpleStringSchema(),
        {"aws.region": region, "flink.stream.initpos": "LATEST"},
    )
    (
        env.add_source(consumer)
        .key_by(mmsi_partition_key, key_type=Types.LONG())
        .process(
            FeatureProcess(table, region, scorer_url, quarantine_bucket),
            output_type=Types.STRING(),
        )
        # .print() (a real Java-backed sink) terminates the graph; the DynamoDB
        # write and the scorer POST already happened as side effects above, so
        # this is just evidence-of-processing in the CloudWatch task logs, not
        # the actual output path.
        .print()
    )
    env.execute("harbormaster-feature-job")


if __name__ == "__main__":
    main()

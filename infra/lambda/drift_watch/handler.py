"""Harbormaster Phase 4 gate 4.6: drift-watch Lambda.

Thin glue only, per this repo's established real-infra boundary: the real
drift-detection logic lives in mlops/drift.py's check_input_drift, vendored
into this Lambda's package (see `make drift-lambda-package`) rather than
duplicated here. This file's own logic is limited to reading the two
parquet snapshots from S3, calling check_input_drift, and publishing an SNS
alert when any feature is flagged; the pure decision path (summarize_drift)
is unit-tested directly, the S3/SNS/boto3 plumbing around it is not.

The original 2026-07-04 sprint authored the monitor as a local, plan-verified
gate. The bounded foundation is now deployed; this handler remains limited to
the reviewed MarineCadastre prefix, drift snapshots, and the existing alert
topic. It does not establish a live AIS feed, retraining, or promotion.

Environment variables:
  LAKE_BUCKET             S3 bucket holding the reference/current snapshots.
  REFERENCE_SNAPSHOT_KEY  S3 key of the reference (most recent accepted
                          gate 3.3 training-set export) parquet snapshot.
  CURRENT_SNAPSHOT_KEY    S3 key of the current-window parquet snapshot.
  SNAPSHOT_SOURCE_PREFIX  Existing S3 raw-prefix containing AIS Parquet
                          candidates, newest candidate considered first.
  SNAPSHOT_WINDOW_PREFIX  Content-addressed validated-window/provenance keys.
  SNAPSHOT_MIN_ROWS       Minimum valid rows in a candidate window.
  SNAPSHOT_MAX_ROWS       Maximum time-ordered rows retained in a window.
  SNS_TOPIC_ARN           Existing Phase 0 finops SNS topic (no new topic).
"""

from __future__ import annotations

import logging
import os

try:
    import boto3
except ImportError:  # pragma: no cover - always present in the Lambda runtime
    boto3 = None

from snapshot_refresh import refresh_current_snapshot

from mlops.drift import DriftResult, check_input_drift

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def summarize_drift(results: list[DriftResult]) -> tuple[bool, str]:
    """Pure: given check_input_drift's output, decide whether to alert and
    build the message body."""
    drifted = [r for r in results if r.drifted]
    if not drifted:
        return False, "no input drift detected"
    lines = [f"{r.feature}: psi={r.psi:.4f} ks_pvalue={r.ks_pvalue:.2e}" for r in drifted]
    return True, "Harbormaster Phase 4 input-drift alert:\n" + "\n".join(lines)


def handler(event, context):
    bucket = os.environ["LAKE_BUCKET"]
    reference_key = os.environ["REFERENCE_SNAPSHOT_KEY"]
    current_key = os.environ["CURRENT_SNAPSHOT_KEY"]
    source_prefix = os.environ.get("SNAPSHOT_SOURCE_PREFIX", "raw/marinecadastre/")
    window_prefix = os.environ.get("SNAPSHOT_WINDOW_PREFIX", "drift/windows/")
    min_rows = int(os.environ.get("SNAPSHOT_MIN_ROWS", "1"))
    max_rows = int(os.environ.get("SNAPSHOT_MAX_ROWS", "10000"))
    topic_arn = os.environ.get("SNS_TOPIC_ARN")

    s3 = boto3.client("s3")
    try:
        refreshed = refresh_current_snapshot(
            s3,
            bucket=bucket,
            reference_key=reference_key,
            current_key=current_key,
            source_prefix=source_prefix,
            window_prefix=window_prefix,
            min_rows=min_rows,
            max_rows=max_rows,
        )
    except Exception as error:
        message = f"Harbormaster Phase 4 AIS snapshot refresh failed: {error}"
        logger.exception(message)
        if topic_arn:
            boto3.client("sns").publish(
                TopicArn=topic_arn,
                Subject="Harbormaster: AIS snapshot refresh failed",
                Message=message,
            )
        raise
    reference = refreshed.reference
    current = refreshed.current

    results = check_input_drift(reference, current)
    should_alert, message = summarize_drift(results)
    logger.info(message)

    if should_alert and topic_arn:
        sns = boto3.client("sns")
        sns.publish(
            TopicArn=topic_arn, Subject="Harbormaster: input drift detected", Message=message
        )

    return {
        "drifted_features": [r.feature for r in results if r.drifted],
        "alerted": should_alert and bool(topic_arn),
        "refresh": {
            "source_key": refreshed.source_key,
            "source_version_id": refreshed.source_version_id,
            "window_key": refreshed.window_key,
            "provenance_key": refreshed.provenance_key,
            "window_sha256": refreshed.window_sha256,
            "window_rows": refreshed.window_rows,
            "rejected_candidates": list(refreshed.rejected_candidates),
            "current_advanced": refreshed.current_advanced,
        },
    }

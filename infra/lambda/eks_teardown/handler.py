"""Harbormaster EKS teardown guard Lambda.

This Lambda is a structural cost guardrail for the one compute surface in the
project whose idle cost cannot be scaled to zero. The EKS control plane bills
a fixed per-cluster-hour charge whether or not any node or pod is running.
Switching the feature off after a demo is a procedural step, and this Lambda
makes the teardown a property of the infrastructure. A recurring EventBridge
Scheduler rate schedule invokes it; each run re-evaluates the cluster's age against MAX_AGE_HOURS
(default 4) and force-destroys the node groups and then the cluster once the
window is exceeded, unless the cluster carries a KeepAliveUntil tag whose
timestamp is still in the future.

Decision semantics (pure, tested in test_handler.py):
  - the cluster lives until created_at + MAX_AGE_HOURS;
  - a KeepAliveUntil tag (ISO 8601, e.g. "2026-07-12T02:00:00Z") extends the
    window until that instant;
  - an absent, empty, or UNPARSEABLE KeepAliveUntil grants no extension. The
    guard fails toward teardown by design: a typo in the keep-alive tag must
    never quietly keep a billed control plane alive.

Teardown ordering: EKS refuses to delete a cluster that still has node
groups, so a run deletes the node groups first and only issues DeleteCluster
when none remain. Node-group deletion is asynchronous; the recurring schedule
makes this convergent (a later run finds zero node groups and removes the
cluster) with no waiter logic held inside one invocation.

Design conventions copied from infra/lambda/teardown/handler.py (the Phase 0
nightly teardown Lambda this guard's packaging mirrors):
  - defensive per step: every AWS call block catches its own exceptions and
    accumulates them, never aborting the run;
  - tag-scoped: only a cluster tagged Project=<PROJECT_TAG> is touched;
  - DRY_RUN defaults true here for safe local/manual runs; the Terraform
    module (modules/eks_teardown_guard) sets it explicitly, default false,
    because an armed guard is the point;
  - boto3 only, no bundled dependencies.

Environment variables:
  CLUSTER_NAME        Name of the EKS cluster to guard. Required.
  MAX_AGE_HOURS       Hours after cluster creation before force-destroy. Default 4.
  KEEP_ALIVE_TAG_KEY  Cluster tag holding the keep-alive timestamp. Default KeepAliveUntil.
  DRY_RUN             "true" (default) logs intended actions; "false" performs them.
  PROJECT_TAG         Tag value that scopes the guard. Default "harbormaster".
  ALERT_TOPIC_ARN     SNS topic for the action summary. Optional; log-only if unset.
"""

import datetime
import json
import logging
import os

try:
    import boto3
except ImportError:  # pragma: no cover
    # boto3 is always present in the Lambda runtime. Off-cloud (a bare CI box)
    # it may be absent; the name stays defined so tests can monkeypatch
    # handler.boto3.client, matching infra/lambda/teardown/handler.py.
    boto3 = None

logger = logging.getLogger()
if logger.handlers:
    logger.setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)

DEFAULT_MAX_AGE_HOURS = 4.0
DEFAULT_KEEP_ALIVE_TAG_KEY = "KeepAliveUntil"
PROJECT_TAG_VALUE = os.environ.get("PROJECT_TAG", "harbormaster")


def _env_bool(name, default):
    """Parse a boolean-ish environment variable. Anything other than an
    explicit false-y string ("false", "0", "no", "off") is treated as True so
    the safe DRY_RUN default is preserved on typos."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("false", "0", "no", "off")


def _log(event_name, **fields):
    """One structured log line, greppable in CloudWatch Logs on "event"."""
    record = {"event": event_name}
    record.update(fields)
    logger.info(json.dumps(record, default=str))


def _as_utc(dt):
    """Normalize a datetime to timezone-aware UTC (naive input is assumed UTC,
    which is what the EKS API and ISO 8601 "Z" tags both produce anyway)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.UTC)
    return dt.astimezone(datetime.UTC)


def parse_keep_alive(raw):
    """Parse a KeepAliveUntil tag value to an aware UTC datetime, or None.

    None (no extension) on empty/absent/unparseable input: the guard fails
    toward teardown, never toward an immortal control plane."""
    if not raw or not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        return _as_utc(datetime.datetime.fromisoformat(text))
    except ValueError:
        return None


def should_teardown(created_at, keep_alive_until, now, max_age_hours=DEFAULT_MAX_AGE_HOURS):
    """Pure decision: True when the cluster must be force-destroyed.

    Teardown iff BOTH windows are exhausted:
      1. age window:        now >= created_at + max_age_hours
      2. keep-alive window: keep_alive_until is None or now >= keep_alive_until

    Boundary semantics are inclusive (>=): at exactly the deadline the guard
    fires. A keep_alive_until in the past grants nothing; one in the future
    holds the guard off regardless of age.
    """
    if max_age_hours < 0:
        raise ValueError(f"max_age_hours must be >= 0, got {max_age_hours}")
    now = _as_utc(now)
    expiry = _as_utc(created_at) + datetime.timedelta(hours=max_age_hours)
    if now < expiry:
        return False
    if keep_alive_until is not None and now < _as_utc(keep_alive_until):
        return False
    return True


def _tag_matches(tags, key="Project", value=None):
    """True if the flat EKS tag dict contains key=value."""
    value = value if value is not None else PROJECT_TAG_VALUE
    return isinstance(tags, dict) and tags.get(key) == value


def evaluate_cluster(client, cluster_name, keep_alive_tag_key, max_age_hours, now):
    """Describe the cluster and return (decision, detail dict).

    decision is one of "teardown", "keep", "absent", "skip_untagged".
    """
    try:
        cluster = client.describe_cluster(name=cluster_name)["cluster"]
    except Exception as err:  # noqa: BLE001
        # ResourceNotFoundException and transport errors both land here; either
        # way there is nothing to destroy this run.
        _log("eks_describe_absent_or_failed", cluster=cluster_name, error=str(err))
        return "absent", {"error": str(err)}

    tags = cluster.get("tags", {})
    if not _tag_matches(tags):
        _log("eks_skip_untagged", cluster=cluster_name, tags=tags)
        return "skip_untagged", {"tags": tags}

    created_at = _as_utc(cluster["createdAt"])
    keep_alive = parse_keep_alive(tags.get(keep_alive_tag_key))
    decision = should_teardown(created_at, keep_alive, now, max_age_hours)
    detail = {
        "created_at": created_at.isoformat(),
        "keep_alive_until": keep_alive.isoformat() if keep_alive else None,
        "max_age_hours": max_age_hours,
        "status": cluster.get("status"),
    }
    _log("eks_evaluated", cluster=cluster_name, teardown=decision, **detail)
    return ("teardown" if decision else "keep"), detail


def teardown_cluster(client, cluster_name, dry_run, results):
    """Delete every node group, then the cluster once none remain.

    Node-group deletion is async, so a cluster with node groups is only
    started on its way down this run; the recurring schedule converges it."""
    deleted_nodegroups = []
    cluster_deleted = False
    error = None
    try:
        nodegroups = []
        token = None
        while True:
            kwargs = {"clusterName": cluster_name}
            if token:
                kwargs["nextToken"] = token
            resp = client.list_nodegroups(**kwargs)
            nodegroups.extend(resp.get("nodegroups", []))
            token = resp.get("nextToken")
            if not token:
                break

        for ng in nodegroups:
            if dry_run:
                _log("eks_would_delete_nodegroup", cluster=cluster_name, nodegroup=ng)
                deleted_nodegroups.append(ng)
                continue
            client.delete_nodegroup(clusterName=cluster_name, nodegroupName=ng)
            _log("eks_nodegroup_delete_started", cluster=cluster_name, nodegroup=ng)
            deleted_nodegroups.append(ng)

        if nodegroups and not dry_run:
            # Deletion is in flight; the cluster delete would be rejected now.
            _log("eks_cluster_delete_deferred", cluster=cluster_name, pending=nodegroups)
        elif dry_run:
            _log("eks_would_delete_cluster", cluster=cluster_name)
            cluster_deleted = True
        else:
            client.delete_cluster(name=cluster_name)
            _log("eks_cluster_deleted", cluster=cluster_name)
            cluster_deleted = True
    except Exception as err:  # noqa: BLE001
        _log("eks_teardown_block_failed", cluster=cluster_name, error=str(err))
        error = str(err)

    results["eks"] = {
        "nodegroups_deleted": deleted_nodegroups,
        "cluster_deleted": cluster_deleted,
        "error": error,
    }
    return results


def publish_summary(dry_run, cluster_name, decision, results):
    """Publish a one-paragraph action summary to ALERT_TOPIC_ARN, or log it."""
    topic_arn = os.environ.get("ALERT_TOPIC_ARN")
    eks = results.get("eks", {})
    lines = [
        "Harbormaster EKS teardown guard",
        "DRY_RUN: {}".format("yes" if dry_run else "no"),
        f"Cluster: {cluster_name}",
        f"Decision: {decision}",
        "Node groups deleted: {}".format(eks.get("nodegroups_deleted", [])),
        "Cluster deleted: {}".format(eks.get("cluster_deleted", False)),
    ]
    if eks.get("error"):
        lines.append("Error: {}".format(eks["error"]))
    message = "\n".join(lines)

    if not topic_arn:
        _log("sns_skip_no_topic", message=message)
        results["sns"] = {"published": False, "error": None}
        return results
    if dry_run:
        _log("sns_would_publish", topic_arn=topic_arn, message=message)
        results["sns"] = {"published": False, "error": None}
        return results
    try:
        boto3.client("sns").publish(
            TopicArn=topic_arn,
            Subject="Harbormaster EKS teardown guard",
            Message=message,
        )
        _log("sns_published", topic_arn=topic_arn)
        results["sns"] = {"published": True, "error": None}
    except Exception as err:  # noqa: BLE001
        _log("sns_publish_failed", error=str(err))
        results["sns"] = {"published": False, "error": str(err)}
    return results


def lambda_handler(event, context):
    """EventBridge Scheduler entry point. Evaluates the guarded cluster and
    force-destroys it when the age and keep-alive windows are both exhausted.
    Returns a JSON-serializable dict for the Lambda console and step logs."""
    dry_run = _env_bool("DRY_RUN", True)
    cluster_name = os.environ.get("CLUSTER_NAME", "")
    keep_alive_tag_key = os.environ.get("KEEP_ALIVE_TAG_KEY", DEFAULT_KEEP_ALIVE_TAG_KEY)
    try:
        max_age_hours = float(os.environ.get("MAX_AGE_HOURS", str(DEFAULT_MAX_AGE_HOURS)))
    except ValueError:
        # An unparseable override must not disarm the guard: fall back to the
        # default window rather than erroring out of every scheduled run.
        max_age_hours = DEFAULT_MAX_AGE_HOURS
    now = datetime.datetime.now(datetime.UTC)

    _log(
        "eks_guard_start",
        dry_run=dry_run,
        cluster=cluster_name,
        max_age_hours=max_age_hours,
        project_tag=PROJECT_TAG_VALUE,
    )

    results = {}
    if not cluster_name:
        _log("eks_guard_no_cluster_name")
        decision = "misconfigured"
    else:
        client = boto3.client("eks")
        decision, detail = evaluate_cluster(
            client, cluster_name, keep_alive_tag_key, max_age_hours, now
        )
        results["evaluation"] = detail
        if decision == "teardown":
            teardown_cluster(client, cluster_name, dry_run, results)

    publish_summary(dry_run, cluster_name, decision, results)
    _log("eks_guard_complete", decision=decision, dry_run=dry_run)
    return {
        "dry_run": dry_run,
        "cluster": cluster_name,
        "decision": decision,
        "results": results,
    }


if __name__ == "__main__":
    # Local smoke run: with no AWS credentials the boto3 calls fail into the
    # defensive blocks, so this exits cleanly and prints the accumulated
    # result. DRY_RUN defaults true here, so nothing is ever destroyed.
    print(json.dumps(lambda_handler({"source": "local"}, None), indent=2, default=str))

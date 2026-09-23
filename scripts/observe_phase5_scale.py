"""Record the W4 KEDA 0 -> N -> 0 timeline as a JSON evidence artifact."""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

try:
    from scripts.loadtest_kinesis_backpressure import (
        claim_artifact,
        validate_run_id,
        write_artifact,
    )
except ModuleNotFoundError:  # direct `python scripts/observe_phase5_scale.py`
    from loadtest_kinesis_backpressure import claim_artifact, validate_run_id, write_artifact

try:
    from streaming.flink.window_logic import (
        open_no_redirect,
        validate_ais_score_response,
        validate_execute_api_url,
    )
except ModuleNotFoundError:  # direct script after loadtest adds streaming to sys.path
    from flink.window_logic import (
        open_no_redirect,
        validate_ais_score_response,
        validate_execute_api_url,
    )

INFERENCE_MMSI = 200000099
INFERENCE_TIMEOUT_SECONDS = 35.0
INFERENCE_BODY = json.dumps(
    {
        "mmsi": INFERENCE_MMSI,
        "fix": {"lat": 40.5, "lon": -73.95, "t": "2024-06-01T00:01:00Z"},
        "history": [{"lat": 40.5, "lon": -73.95, "t": "2024-06-01T00:00:00Z"}],
    },
    separators=(",", ":"),
).encode()


def _utc_now() -> datetime:
    return datetime.now(UTC)


def validate_positive_finite(value: float, name: str) -> float:
    """Reject unbounded or busy-loop timing controls."""
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and > 0")
    return value


def positive_float_arg(value: str) -> float:
    try:
        return validate_positive_finite(float(value), "value")
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def _parse_utc(value: str, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include a timezone")
    return parsed.astimezone(UTC)


def deployment_replicas(
    namespace: str,
    deployment: str,
    *,
    timeout_seconds: float = 15,
) -> tuple[int, int]:
    """Read desired and available replicas without mutating the cluster."""
    timeout_seconds = validate_positive_finite(timeout_seconds, "kubectl timeout_seconds")
    result = subprocess.run(
        [
            "kubectl",
            f"--request-timeout={timeout_seconds:g}s",
            "-n",
            namespace,
            "get",
            "deployment",
            deployment,
            "-o",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout_seconds + 1,
    )
    doc = json.loads(result.stdout)
    return int(doc.get("spec", {}).get("replicas", 0)), int(
        doc.get("status", {}).get("availableReplicas", 0)
    )


def validate_api_gateway_url(url: str, region: str) -> str:
    """Reject every destination except the exact regional scoring route."""
    return validate_execute_api_url(url, region)


def signed_inference_status(
    url: str,
    region: str,
    *,
    timeout_seconds: float = 5,
) -> tuple[int | None, str | None]:
    """Issue one signed request and validate its AisScoreOut response."""
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.session import get_session

    validate_positive_finite(timeout_seconds, "inference timeout_seconds")
    validate_api_gateway_url(url, region)
    credentials = get_session().get_credentials()
    if credentials is None:
        return None, "no AWS credentials available"
    aws_request = AWSRequest(
        method="POST",
        url=url,
        data=INFERENCE_BODY,
        headers={"Content-Type": "application/json"},
    )
    SigV4Auth(credentials.get_frozen_credentials(), "execute-api", region).add_auth(aws_request)
    request = urllib.request.Request(
        url,
        data=INFERENCE_BODY,
        headers={str(key): str(value) for key, value in aws_request.headers.items()},
        method="POST",
    )
    try:
        with open_no_redirect(request, timeout_seconds=timeout_seconds) as response:
            status_code = response.status
            response_body = response.read()
        try:
            validate_ais_score_response(status_code, response_body, INFERENCE_MMSI)
        except ValueError as error:
            return status_code, str(error)
        return status_code, None
    except urllib.error.HTTPError as error:
        return error.code, str(error)
    except OSError as error:
        return None, str(error)


def bounded_inference_status(url: str, region: str) -> tuple[int | None, str | None]:
    """Allow API Gateway enough time to return its no-target 503 at scale zero."""
    return signed_inference_status(
        url,
        region,
        timeout_seconds=INFERENCE_TIMEOUT_SECONDS,
    )


def capture_baseline(
    read_replicas: Callable[[], tuple[int, int]],
    probe_inference: Callable[[], tuple[int | None, str | None]],
    *,
    utc_now: Callable[[], datetime] = _utc_now,
) -> dict:
    """Prove zero EKS replicas and a non-serving EKS route before load starts."""
    desired, available = read_replicas()
    status_code, error = probe_inference()
    baseline = {
        "captured_at": utc_now().isoformat().replace("+00:00", "Z"),
        "desired_replicas": desired,
        "available_replicas": available,
        "inference_http_status": status_code,
        "inference_error": error,
    }
    if desired != 0 or available != 0:
        baseline["status"] = "invalid_nonzero_replicas"
    elif status_code == 200:
        baseline["status"] = "invalid_route_already_serving"
    elif status_code is None or not 500 <= status_code <= 599:
        baseline["status"] = "invalid_inference_probe"
    else:
        baseline["status"] = "ready_for_load"
    return baseline


def _event(now: datetime, load_started_at: datetime, **fields) -> dict:
    return {
        "at": now.isoformat().replace("+00:00", "Z"),
        "seconds_from_load_start": (now - load_started_at).total_seconds(),
        **fields,
    }


def _bound_load_state(
    payload: dict,
    *,
    expected_run_id: str,
    expected_load_artifact: Path,
    expected_observer_path: Path,
    expected_started_at: datetime,
) -> dict:
    """Validate the immutable handshake fields and current load state."""
    if not isinstance(payload, dict):
        raise ValueError("load artifact must contain a JSON object")
    if payload.get("schema_version") != 1:
        raise ValueError("load artifact schema_version must equal 1")
    if payload.get("run_id") != expected_run_id:
        raise ValueError("load artifact run_id does not match observer run_id")

    try:
        bound_load_path = Path(payload["load_artifact"]).expanduser().resolve()
        bound_observer_path = Path(payload["observer_ready_path"]).expanduser().resolve()
    except (KeyError, TypeError) as error:
        raise ValueError("load artifact is missing handshake path bindings") from error
    if bound_load_path != expected_load_artifact.expanduser().resolve():
        raise ValueError("load artifact self-binding does not match its path")
    if bound_observer_path != expected_observer_path.expanduser().resolve():
        raise ValueError("load artifact is bound to a different observer")

    started_at = _parse_utc(payload.get("started_at"), "started_at")
    if started_at != expected_started_at.astimezone(UTC):
        raise ValueError("load artifact started_at changed after the running handshake")

    status = payload.get("status")
    if not isinstance(status, str) or not status:
        raise ValueError("load artifact status must be a non-empty string")
    state = {
        "status": status,
        "started_at": started_at.isoformat().replace("+00:00", "Z"),
    }
    error = payload.get("error")
    if isinstance(error, str) and error:
        state["error"] = error
    if status == "completed":
        ended_at = _parse_utc(payload.get("ended_at"), "ended_at")
        if ended_at < started_at:
            raise ValueError("load artifact ended_at predates started_at")
        state["ended_at"] = ended_at.isoformat().replace("+00:00", "Z")
    return state


def observe_scale(
    *,
    load_started_at: datetime,
    baseline: dict,
    read_load_artifact: Callable[[], dict],
    expected_run_id: str,
    expected_load_artifact: Path,
    expected_observer_path: Path,
    read_replicas: Callable[[], tuple[int, int]],
    probe_inference: Callable[[], tuple[int | None, str | None]],
    timeout_seconds: float,
    poll_seconds: float = 1.0,
    checkpoint: Callable[[dict], None] | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    utc_now: Callable[[], datetime] = _utc_now,
) -> dict:
    """Observe ordered transitions and checkpoint every sampled state."""
    validate_run_id(expected_run_id)
    timeout_seconds = validate_positive_finite(timeout_seconds, "timeout_seconds")
    poll_seconds = validate_positive_finite(poll_seconds, "poll_seconds")
    payload = {
        "schema_version": 1,
        "status": "observing",
        "load_started_at": load_started_at.isoformat().replace("+00:00", "Z"),
        "baseline": baseline,
        "poll_seconds": poll_seconds,
        "events": {},
    }

    def persist() -> None:
        if checkpoint is not None:
            checkpoint(payload)

    persist()
    if baseline.get("status") != "ready_for_load":
        payload["status"] = "invalid_baseline"
        persist()
        return payload

    start = monotonic()
    while monotonic() - start <= timeout_seconds:
        try:
            load_state = _bound_load_state(
                read_load_artifact(),
                expected_run_id=expected_run_id,
                expected_load_artifact=expected_load_artifact,
                expected_observer_path=expected_observer_path,
                expected_started_at=load_started_at,
            )
        except (OSError, ValueError) as error:
            payload["status"] = "invalid_load_artifact"
            payload["load_error"] = f"{type(error).__name__}: {error}"
            persist()
            return payload

        payload["last_load_artifact"] = load_state
        load_status = load_state["status"]
        if load_status == "failed":
            payload["status"] = "load_failed"
            persist()
            return payload
        if load_status not in {"running", "completed"}:
            payload["status"] = "invalid_load_status"
            persist()
            return payload

        desired, available = read_replicas()
        status_code, inference_error = probe_inference()
        now = utc_now()
        events = payload["events"]
        inference_succeeded = status_code == 200 and inference_error is None
        payload["last_sample"] = {
            "sampled_at": now.isoformat().replace("+00:00", "Z"),
            "desired_replicas": desired,
            "available_replicas": available,
            "inference_http_status": status_code,
            "inference_error": inference_error,
        }

        if desired > 0 and "scale_requested" not in events:
            events["scale_requested"] = _event(now, load_started_at, desired_replicas=desired)
        if "scale_requested" in events and available > 0 and "pod_ready" not in events:
            events["pod_ready"] = _event(now, load_started_at, available_replicas=available)
        if inference_succeeded and "pod_ready" not in events:
            payload["status"] = "invalid_inference_success_before_pod_ready"
            persist()
            return payload
        if (
            "pod_ready" in events
            and inference_succeeded
            and "first_inference_success" not in events
        ):
            events["first_inference_success"] = _event(now, load_started_at, http_status=200)
        if (
            "first_inference_success" in events
            and load_status == "completed"
            and desired == 0
            and available == 0
        ):
            events["returned_to_zero"] = _event(
                now,
                load_started_at,
                desired_replicas=0,
                available_replicas=0,
            )
            payload["status"] = "completed"
            persist()
            return payload

        persist()
        sleep(poll_seconds)

    payload["status"] = "timeout"
    persist()
    return payload


def wait_for_load_start(
    path: Path,
    *,
    not_before: datetime,
    expected_run_id: str,
    expected_observer_path: Path,
    timeout_seconds: float = 120,
) -> datetime:
    """Wait for the one running marker bound to this observer and run ID."""
    validate_run_id(expected_run_id)
    timeout_seconds = validate_positive_finite(timeout_seconds, "timeout_seconds")
    load_path = path.expanduser().resolve()
    observer_path = expected_observer_path.expanduser().resolve()
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() <= deadline:
        try:
            payload = json.loads(load_path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            time.sleep(0.2)
            continue
        if payload.get("schema_version") != 1:
            raise ValueError("load artifact schema_version must equal 1")
        if payload.get("run_id") != expected_run_id:
            raise ValueError("load artifact run_id does not match observer run_id")
        try:
            bound_load_path = Path(payload["load_artifact"]).expanduser().resolve()
            bound_observer_path = Path(payload["observer_ready_path"]).expanduser().resolve()
        except (KeyError, TypeError) as error:
            raise ValueError("load artifact is missing handshake path bindings") from error
        if bound_load_path != load_path:
            raise ValueError("load artifact self-binding does not match its path")
        if bound_observer_path != observer_path:
            raise ValueError("load artifact is bound to a different observer")

        status = payload.get("status")
        if status == "preparing":
            time.sleep(0.2)
            continue
        if status != "running":
            raise ValueError(f"load artifact entered terminal status {status}")
        started_at = _parse_utc(payload.get("started_at"), "started_at")
        if started_at < not_before:
            raise ValueError("load artifact predates the observer baseline")
        return started_at
    raise TimeoutError(
        "load artifact did not expose a fresh running marker "
        f"within {timeout_seconds}s: {load_path}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--load-artifact", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--namespace", default="hm-serving")
    parser.add_argument("--deployment", default="serving")
    parser.add_argument("--timeout-seconds", type=positive_float_arg, default=1200.0)
    parser.add_argument("--poll-seconds", type=positive_float_arg, default=1.0)
    parser.add_argument("--kubectl-timeout-seconds", type=positive_float_arg, default=15.0)
    args = parser.parse_args()

    try:
        run_id = validate_run_id(args.run_id)
    except ValueError as error:
        parser.error(str(error))
    load_artifact = args.load_artifact.expanduser().resolve()
    output = args.output.expanduser().resolve()
    if load_artifact == output:
        parser.error("output and load-artifact must be different paths")

    inference_url = f"{args.api_url.rstrip('/')}/v1/score-ais"
    base_payload = {
        "schema_version": 1,
        "run_id": run_id,
        "api_url": args.api_url,
        "inference_url": inference_url,
        "aws_region": args.region,
        "namespace": args.namespace,
        "deployment": args.deployment,
        "load_artifact": str(load_artifact),
        "observer_artifact": str(output),
    }
    last_payload = {**base_payload, "status": "preparing"}
    try:
        claim_artifact(output, last_payload)
    except FileExistsError:
        parser.error(f"output already exists; use a fresh artifact path: {output}")

    def checkpoint(state: dict) -> None:
        nonlocal last_payload
        last_payload = json.loads(json.dumps({**base_payload, **state}))
        write_artifact(output, last_payload)

    try:
        validate_api_gateway_url(inference_url, args.region)
        baseline = capture_baseline(
            lambda: deployment_replicas(
                args.namespace,
                args.deployment,
                timeout_seconds=args.kubectl_timeout_seconds,
            ),
            lambda: bounded_inference_status(inference_url, args.region),
        )
        ready_payload = {
            "status": baseline["status"],
            "ready_at": _utc_now().isoformat().replace("+00:00", "Z"),
            "baseline": baseline,
        }
        checkpoint(ready_payload)
        if baseline["status"] != "ready_for_load":
            print(
                f"scale evidence artifact: {output} status={baseline['status']}",
                file=sys.stderr,
            )
            return 1

        baseline_at = _parse_utc(baseline["captured_at"], "baseline captured_at")
        load_started_at = wait_for_load_start(
            load_artifact,
            not_before=baseline_at,
            expected_run_id=run_id,
            expected_observer_path=output,
        )
        checkpoint(
            {
                "schema_version": 1,
                "status": "observing",
                "load_started_at": load_started_at.isoformat().replace("+00:00", "Z"),
                "baseline": baseline,
                "poll_seconds": args.poll_seconds,
                "events": {},
            }
        )
        payload = observe_scale(
            load_started_at=load_started_at,
            baseline=baseline,
            read_load_artifact=lambda: json.loads(load_artifact.read_text()),
            expected_run_id=run_id,
            expected_load_artifact=load_artifact,
            expected_observer_path=output,
            read_replicas=lambda: deployment_replicas(
                args.namespace,
                args.deployment,
                timeout_seconds=args.kubectl_timeout_seconds,
            ),
            probe_inference=lambda: bounded_inference_status(inference_url, args.region),
            timeout_seconds=args.timeout_seconds,
            poll_seconds=args.poll_seconds,
            checkpoint=checkpoint,
        )
    except KeyboardInterrupt as error:
        payload = {
            **last_payload,
            "status": "interrupted",
            "ended_at": _utc_now().isoformat().replace("+00:00", "Z"),
            "error": f"{type(error).__name__}: interrupted by operator",
        }
    except Exception as error:
        payload = {
            **last_payload,
            "status": "observer_error",
            "ended_at": _utc_now().isoformat().replace("+00:00", "Z"),
            "error": f"{type(error).__name__}: {error}",
        }

    checkpoint(payload)
    print(f"scale evidence artifact: {output} status={payload['status']}")
    return 0 if payload["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())

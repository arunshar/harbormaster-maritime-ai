"""Async-IO SageMaker client for the Pi-DPM async inference endpoint
(Phase 3, gate 3.6).

Mirrors `watchlist.py`'s WatchlistLookup conventions exactly: injected
clients (either may be None -> disabled), an `enabled` property, a
`from_settings` classmethod that builds real boto3 clients with tight
timeouts and degrades to disabled on construction failure, and an
async/sync split (`ascore`/`score`) where the blocking boto3/S3-polling work
runs off the event loop via `asyncio.to_thread`.

The one real behavioral difference from the watchlist lookup: "fail open"
here means "return None so the caller falls back to the existing analytic
`_pi_dpm_score` in gap_detector.py", not "return an empty/zero score" - the
analytic estimator is a real, reasonable estimate on its own (it is exactly
what Phase 1/2 already ship), so a SageMaker outage degrades quality, it
never stops scoring.

SageMaker async inference contract: the caller uploads the input payload to
S3, calls `invoke_endpoint_async` (returns an OutputLocation immediately,
the container scores in the background), then polls OutputLocation for the
result. The container itself, wrapping the frozen `PiDpmScorer.log_prob`
contract, is out of scope for this client (see mlops/pidpm_container/).
"""

from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import structlog

from app.config import Settings
from app.metrics import PIDPM_LOOKUP_ERRORS

log = structlog.get_logger(__name__)

# HM3-AUDIT-04: SageMaker otherwise permits an async invocation to occupy
# endpoint capacity for up to 15 minutes after this fail-open client has
# returned to the analytic scorer. The Phase 3 audit's 60-second cap is a
# conservative local default until a real Pi-DPM workload supplies a measured
# replacement.
ASYNC_INVOCATION_TIMEOUT_SECONDS = 60


@dataclass(frozen=True)
class PiDpmScore:
    """Phase 4 gate 4.3: the additive SageMaker payload contract. The
    endpoint's response may now carry {"score", "epistemic_variance"};
    epistemic_variance is None whenever the payload omits it (the current
    demo stand-in and any checkpoint without an uncertainty head both do),
    never a stand-in numeric value. Callers must treat None as "no signal",
    not "zero uncertainty"."""

    score: float
    epistemic_variance: float | None = None


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3 uri: {uri}")
    bucket, _, key = uri[len("s3://") :].partition("/")
    return bucket, key


class PiDpmClient:
    """Injected clients (either may be None -> disabled). `score`/`ascore`
    return None on any failure, timeout, or when disabled: the caller must
    treat None as "fall back to the existing estimate", never as a score of
    zero."""

    def __init__(
        self,
        *,
        sagemaker_client: Any | None,
        s3_client: Any | None,
        endpoint_name: str,
        input_bucket: str,
        input_prefix: str = "pidpm/async-input",
        poll_interval_s: float = 0.1,
        timeout_s: float = 1.0,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._sagemaker = sagemaker_client
        self._s3 = s3_client
        self._endpoint_name = endpoint_name
        self._input_bucket = input_bucket
        self._input_prefix = input_prefix
        self._poll_interval_s = poll_interval_s
        self._timeout_s = timeout_s
        self._sleep = sleep
        self._monotonic = monotonic

    @property
    def enabled(self) -> bool:
        return (
            self._sagemaker is not None
            and self._s3 is not None
            and bool(self._endpoint_name)
            and bool(self._input_bucket)
        )

    @classmethod
    def from_settings(cls, settings: Settings) -> PiDpmClient:
        """Build real clients from config; degrade to disabled when unset or
        on construction failure. Same tight-timeout shape as
        WatchlistLookup.from_settings: this runs on the scoring path."""
        sagemaker_client = None
        s3_client = None
        if settings.pidpm_endpoint:
            try:
                import boto3
                from botocore.config import Config as BotoConfig

                region = os.environ.get(
                    "AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
                )
                boto_config = BotoConfig(
                    connect_timeout=1,
                    read_timeout=1,
                    retries={"max_attempts": 2, "mode": "standard"},
                )
                sagemaker_client = boto3.client(
                    "sagemaker-runtime", region_name=region, config=boto_config
                )
                s3_client = boto3.client("s3", region_name=region, config=boto_config)
            except Exception as exc:
                log.warning("pidpm_client_unavailable_disabled", err=str(exc))
        return cls(
            sagemaker_client=sagemaker_client,
            s3_client=s3_client,
            endpoint_name=settings.pidpm_endpoint,
            input_bucket=settings.pidpm_input_bucket,
        )

    async def ascore(self, trajectory: list[list[float]]) -> float | None:
        """Event-loop-safe: the blocking upload/invoke/poll sequence runs in
        a worker thread so a slow or stalled endpoint cannot stall every
        request on the loop. Zero-cost when disabled."""
        if not self.enabled:
            return None
        import asyncio

        return await asyncio.to_thread(self.score, trajectory)

    def score(self, trajectory: list[list[float]]) -> float | None:
        """Pre-Phase-4 contract, byte-for-byte unchanged: unwraps
        score_full's PiDpmScore down to the bare float every existing
        caller (gap_detector.py's adapter) still expects."""
        result = self.score_full(trajectory)
        return result.score if result is not None else None

    async def ascore_full(self, trajectory: list[list[float]]) -> PiDpmScore | None:
        """Phase 4 gate 4.3: async counterpart to score_full, same
        off-event-loop dispatch as ascore."""
        if not self.enabled:
            return None
        import asyncio

        return await asyncio.to_thread(self.score_full, trajectory)

    def score_full(self, trajectory: list[list[float]]) -> PiDpmScore | None:
        """Phase 4 gate 4.3: the additive path. Returns the full
        {"score", "epistemic_variance"} contract; epistemic_variance is
        None whenever the endpoint's payload omits it. Old-shape payloads
        (a bare {"score": float}, exactly what the Phase 3 demo stand-in
        emits) parse here unchanged."""
        if not self.enabled:
            return None
        # `enabled` above already guarantees both clients exist; this local
        # re-check only narrows the Any | None attributes for mypy.
        s3, sagemaker = self._s3, self._sagemaker
        if s3 is None or sagemaker is None:  # pragma: no cover
            return None

        inference_id: str | None = None
        try:
            inference_id = str(uuid.uuid4())
            input_key = f"{self._input_prefix}/{inference_id}.json"
            s3.put_object(
                Bucket=self._input_bucket,
                Key=input_key,
                Body=json.dumps({"trajectory": trajectory}).encode(),
            )
            resp = sagemaker.invoke_endpoint_async(
                EndpointName=self._endpoint_name,
                InputLocation=f"s3://{self._input_bucket}/{input_key}",
                ContentType="application/json",
                InvocationTimeoutSeconds=ASYNC_INVOCATION_TIMEOUT_SECONDS,
                InferenceId=inference_id,
            )
            output_bucket, output_key = _parse_s3_uri(resp["OutputLocation"])
        except Exception as exc:  # fail open: caller falls back to the analytic estimate
            PIDPM_LOOKUP_ERRORS.inc()
            log.warning(
                "pidpm_invoke_failed_fallback_to_analytic",
                err=str(exc),
                inference_id=inference_id,
            )
            return None

        # Keep the durable ID, endpoint, and S3 object coordinates together
        # without logging the trajectory or score. Native SageMaker metrics
        # remain endpoint and variant scoped, so a later evidence record joins
        # its metric window through these fields and separately captured UTC
        # timestamps rather than adding a high-cardinality per-request metric
        # label.
        log.info(
            "pidpm_async_invocation_accepted",
            endpoint_name=self._endpoint_name,
            inference_id=inference_id,
            input_bucket=self._input_bucket,
            input_key=input_key,
            output_bucket=output_bucket,
            output_key=output_key,
            invocation_timeout_s=ASYNC_INVOCATION_TIMEOUT_SECONDS,
        )

        deadline = self._monotonic() + self._timeout_s
        while self._monotonic() < deadline:
            try:
                obj = s3.get_object(Bucket=output_bucket, Key=output_key)
                payload = json.loads(obj["Body"].read())
                variance = payload.get("epistemic_variance")
                log.info(
                    "pidpm_output_read_succeeded",
                    endpoint_name=self._endpoint_name,
                    inference_id=inference_id,
                    output_bucket=output_bucket,
                    output_key=output_key,
                )
                return PiDpmScore(
                    score=float(payload["score"]),
                    epistemic_variance=float(variance) if variance is not None else None,
                )
            except self._not_found_error():
                self._sleep(self._poll_interval_s)
            except Exception as exc:
                PIDPM_LOOKUP_ERRORS.inc()
                log.warning(
                    "pidpm_output_read_failed_fallback_to_analytic",
                    err=str(exc),
                    inference_id=inference_id,
                )
                return None

        PIDPM_LOOKUP_ERRORS.inc()
        log.warning(
            "pidpm_score_timeout_fallback_to_analytic",
            timeout_s=self._timeout_s,
            inference_id=inference_id,
        )
        return None

    def _not_found_error(self) -> type[Exception]:
        """The S3 client's own NoSuchKey exception type (only resolvable
        once a real boto3 client exists; a plain KeyError-shaped fake in
        tests can raise this same attribute path)."""
        s3 = self._s3
        # Only reachable from score_full after the `enabled` gate, so the
        # client is never None here; the check narrows Any | None for mypy.
        if s3 is None:  # pragma: no cover
            raise RuntimeError("pidpm s3 client is not configured")
        return s3.exceptions.NoSuchKey

"""Bedrock explanation layer (Phase 5, gate 5.6).

An explanation layer ONLY, never a scoring path (docs/ARCHITECTURE.md:71,
quoted as this gate's design constraint): the explainer takes an
already-computed, already-scored HITL item's deterministic reason codes and
score and asks Bedrock to narrate them in one paragraph. It never sees raw
trajectory data, so the model has no path to imply a different verdict than
the one the deterministic scorer already produced, and nothing it returns is
ever written into the item's ``score``/``reasons`` fields: the narrative is a
separate, optional string.

Mirrors ``pidpm_client.py``'s and ``watchlist.py``'s conventions exactly:
injected client (None -> disabled), an ``enabled`` property, a
``from_settings`` classmethod with tight timeouts that degrades to disabled on
construction failure, an async/sync split (``aexplain``/``explain``) with the
blocking boto3 call off the event loop, and FAIL-OPEN: ``explain`` returns
None on any failure, timeout, or when disabled, so a Bedrock outage never
blocks a HITL item from reaching the queue; only its narrative stays empty.

The no-leak guarantee is structural, not a convention: ``build_prompt`` only
accepts reason-CODE-shaped strings (lowercase snake_case, the ReasonCode
vocabulary) and rejects anything else, so a serialized Fix/Prism object, a
coordinate pair, or a free-text detail string cannot physically enter the
prompt. The gate's unit test asserts the output contains only the reason
codes and the numeric score.
"""

from __future__ import annotations

import os
import re
from typing import Any

import structlog

from app.config import Settings

log = structlog.get_logger(__name__)

# The only strings allowed into the prompt besides the fixed template: reason
# CODES (the ReasonCode enum's lowercase snake_case vocabulary). Anything else
# (coordinates, serialized Fix/Prism fields, free-text details) fails the
# charset and is rejected before a prompt exists.
_REASON_CODE_RE = re.compile(r"^[a-z][a-z0-9_]*$")

# Defense in depth on top of the charset: even a code-shaped token must not
# smuggle trajectory vocabulary (none of the real ReasonCode values do).
_FORBIDDEN_SUBSTRINGS = ("lat", "lon", "fix", "prism", "trajectory", "coordinate")

_PROMPT_TEMPLATE = (
    "You are narrating an already-completed maritime anomaly decision for a "
    "human reviewer.\n\n"
    "The deterministic scorer produced an anomaly score of {score:.3f} on a "
    "0-to-1 scale.\n"
    "The deterministic reason codes, already computed and final, are: {reasons}.\n\n"
    "Write exactly one short paragraph explaining what these reason codes mean "
    "together for a reviewer triaging this item. Do not question or revise the "
    "score, do not guess at vessel movements, and do not invent any data beyond "
    "the reason codes and the score given above."
)


def build_prompt(reasons: list[str], score: float) -> str:
    """The pure prompt constructor: only validated reason codes + the score.

    Raises ValueError on anything that is not a reason-code-shaped string or
    a sane score; ``explain`` treats that as any other failure (fail-open,
    None), so a bad caller cannot leak data by accident, it just gets no
    narrative.
    """
    if not reasons:
        raise ValueError("at least one reason code is required")
    for reason in reasons:
        if not _REASON_CODE_RE.match(reason):
            raise ValueError(f"not a reason code: {reason!r}")
        if any(bad in reason for bad in _FORBIDDEN_SUBSTRINGS):
            raise ValueError(f"reason code carries forbidden vocabulary: {reason!r}")
    if not 0.0 <= score <= 1.0:
        raise ValueError(f"score must be in [0, 1], got {score}")
    return _PROMPT_TEMPLATE.format(score=score, reasons=", ".join(reasons))


class BedrockExplainer:
    """Injected Bedrock Runtime client (None -> disabled). ``explain`` returns
    None on any failure or when disabled; callers must treat None as "no
    narrative", never as an error and never as a scoring signal."""

    def __init__(
        self,
        *,
        bedrock_client: Any | None,
        model_id: str,
        max_tokens: int = 300,
    ) -> None:
        self._bedrock = bedrock_client
        self._model_id = model_id
        self._max_tokens = max_tokens

    @property
    def enabled(self) -> bool:
        return self._bedrock is not None and bool(self._model_id)

    @classmethod
    def from_settings(cls, settings: Settings) -> BedrockExplainer:
        """Build a real client from config; degrade to disabled when unset or
        on construction failure. Same tight-timeout shape as
        PiDpmClient.from_settings: this may run near the HITL enqueue path."""
        bedrock_client = None
        if settings.bedrock_model_id:
            try:
                import boto3
                from botocore.config import Config as BotoConfig

                region = os.environ.get(
                    "AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
                )
                boto_config = BotoConfig(
                    connect_timeout=1,
                    read_timeout=3,
                    retries={"max_attempts": 2, "mode": "standard"},
                )
                bedrock_client = boto3.client(
                    "bedrock-runtime", region_name=region, config=boto_config
                )
            except Exception as exc:
                log.warning("bedrock_explainer_unavailable_disabled", err=str(exc))
        return cls(bedrock_client=bedrock_client, model_id=settings.bedrock_model_id)

    async def aexplain(self, reasons: list[str], score: float) -> str | None:
        """Event-loop-safe: the blocking boto3 call runs in a worker thread so
        a slow Bedrock endpoint cannot stall the loop. Zero-cost when
        disabled."""
        if not self.enabled:
            return None
        import asyncio

        return await asyncio.to_thread(self.explain, reasons, score)

    def explain(self, reasons: list[str], score: float) -> str | None:
        """One narrative paragraph, or None (disabled, bad input, or ANY
        failure). Fail-open: a Bedrock outage degrades the narrative, it
        never blocks the HITL item."""
        if not self.enabled:
            return None
        # `enabled` above already guarantees the client exists; this re-check
        # only narrows Any | None for mypy.
        bedrock = self._bedrock
        if bedrock is None:  # pragma: no cover
            return None

        try:
            prompt = build_prompt(reasons, score)
            resp = bedrock.converse(
                modelId=self._model_id,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig={"maxTokens": self._max_tokens, "temperature": 0.2},
            )
            text = resp["output"]["message"]["content"][0]["text"].strip()
            return text or None
        except Exception as exc:  # fail open: the item ships without a narrative
            log.warning("bedrock_explain_failed_fail_open", err=str(exc))
            return None

"""Per-inference cost ledger.

The deterministic score path costs zero tokens; its cost is compute time. This
records a per-inference latency and a rough Fargate cost estimate so the Phase 1.7
dashboard can report a real $/inference. In production this writes a row per
(trace_id) to Postgres / CloudWatch EMF; the scaffold logs a structured row.
"""

from __future__ import annotations

import structlog

from app.config import Settings

log = structlog.get_logger(__name__)

# ECS Fargate on-demand (us-east-1): ~$0.04048 / vCPU-hour.
_FARGATE_USD_PER_VCPU_S = 0.04048 / 3600.0


class CostTracker:
    def __init__(self, settings: Settings, vcpu: float = 0.5) -> None:
        self.settings = settings
        self.vcpu = vcpu

    def cost_per_inference_usd(self, latency_ms: float) -> float:
        return self.vcpu * (latency_ms / 1000.0) * _FARGATE_USD_PER_VCPU_S

    def record_inference(
        self, *, trace_id: str, mmsi: int, latency_ms: float, n_reasons: int
    ) -> float:
        usd = self.cost_per_inference_usd(latency_ms)
        log.info(
            "inference_cost",
            trace_id=trace_id,
            mmsi=mmsi,
            latency_ms=round(latency_ms, 3),
            n_reasons=n_reasons,
            cost_usd=usd,
        )
        return usd

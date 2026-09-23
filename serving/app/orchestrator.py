"""Deterministic scoring orchestrator. No LLM, no tokens.

HeuristicPlanner builds a typed PlanGraph by history length; `run_plan` executes
its nodes (the vendored deterministic agents) and a fixed fusion turns the agent
signals into an anomaly score, a verdict confidence, and the HITL decision.

Fusion:
  - each agent signal maps to a ScoreReason with severity in [0, 1]
  - score = noisy-OR over reason severities
  - confidence = 1 - 2 * min(score, 1 - score)  (decisive at the extremes, low
    in the ambiguous mid-band)
  - hitl_required = confidence < hitl_confidence_threshold OR score >= anomaly_hitl_threshold
"""

from __future__ import annotations

import time
from typing import Any
from uuid import uuid4

import structlog

from app.agents.corridor_detector import CorridorDeviationDetector
from app.agents.gap_detector import Gap, GapDetectorAgent
from app.agents.heuristic_planner import HeuristicPlanner
from app.agents.space_time_reasoner import SpaceTimeReasoner
from app.agents.validator import ValidatorAgent
from app.components.space_time_prism import Prism, haversine_m
from app.config import Settings, get_settings
from app.cost import CostTracker
from app.errors import CorruptInput
from app.hitl import HitlQueue
from app.metrics import (
    ANOMALIES_TOTAL,
    HITL_ENQUEUED_TOTAL,
    REASONS_TOTAL,
    REJECTS_TOTAL,
    SCORE_LATENCY,
    SCORES_TOTAL,
    WATCHLIST_HITS_TOTAL,
)
from app.models import (
    AisScoreIn,
    AisScoreOut,
    Anchor,
    AnchorPair,
    FeedbackIn,
    FeedbackOut,
    PlanGraph,
    PlanNodeKind,
    ReasonCode,
    ScoreReason,
)
from app.pidpm_client import PiDpmClient
from app.registry import RegistryStore
from app.watchlist import WatchlistLookup, WatchlistStatus

log = structlog.get_logger(__name__)


def _pidpm_scorer_adapter(client: PiDpmClient):
    """None when disabled (PiDpmClient.from_settings degrades gracefully),
    so GapDetectorAgent's pi_dpm_scorer stays None and Phase 1/2 goldens are
    unaffected; otherwise a closure adapting a Prism into the frozen scorer's
    (T, >=2) trajectory shape (here: the gap's two endpoint fixes as [lat, lon])."""
    if not client.enabled:
        return None

    async def score(prism: Prism) -> float | None:
        a, b = prism.pair.a, prism.pair.b
        return await client.ascore([[a.lat, a.lon], [b.lat, b.lon]])

    return score


class Orchestrator:
    def __init__(
        self,
        *,
        settings: Settings,
        planner: HeuristicPlanner,
        st_reasoner: SpaceTimeReasoner,
        gap_detector: GapDetectorAgent,
        validator: ValidatorAgent,
        corridor: CorridorDeviationDetector,
        hitl: HitlQueue,
        registry: RegistryStore,
        watchlist: WatchlistLookup,
        cost: CostTracker,
    ) -> None:
        self.settings = settings
        self.planner = planner
        self.st = st_reasoner
        self.gap = gap_detector
        self.validator = validator
        self.corridor = corridor
        self.hitl = hitl
        self.registry = registry
        self.watchlist = watchlist
        self.cost = cost

    @classmethod
    async def bootstrap(cls, settings: Settings | None = None) -> Orchestrator:
        s = settings or get_settings()
        pidpm_client = PiDpmClient.from_settings(s)
        return cls(
            settings=s,
            planner=HeuristicPlanner(),
            st_reasoner=SpaceTimeReasoner(s),
            gap_detector=GapDetectorAgent(s, pi_dpm_scorer=_pidpm_scorer_adapter(pidpm_client)),
            validator=ValidatorAgent(s),
            corridor=CorridorDeviationDetector(s),
            hitl=await HitlQueue.connect(s),
            registry=await RegistryStore.connect(s),
            watchlist=WatchlistLookup.from_settings(s),
            cost=CostTracker(s),
        )

    async def shutdown(self) -> None:
        await self.hitl.close()
        await self.registry.close()

    # --------------------------------------------------------------- score

    async def score(self, payload: AisScoreIn) -> AisScoreOut:
        t0 = time.perf_counter()
        trace_id = uuid4().hex
        anchors = [f.to_anchor() for f in payload.track()]
        n_history = len(anchors) - 1

        # Hard input gate: a teleport beyond corrupt_reject_factor x v_max is
        # non-physical sensor corruption, rejected (422), not a scored anomaly.
        self._corrupt_gate(anchors)

        plan = self.planner.plan(n_history)
        results = await self.run_plan(plan, payload, anchors)
        # The CDC-fed online watchlist runs on every score, outside the plan
        # graph: it is a lookup, not a kinematic agent, and it is fail-open.
        # aget keeps the blocking redis/boto3 clients off the event loop.
        results["watchlist"] = await self.watchlist.aget(payload.mmsi)
        reasons = self._fuse(results, anchors)

        score = self._noisy_or([r.severity for r in reasons])
        confidence = self._confidence(score)
        hitl_required = (
            confidence < self.settings.hitl_confidence_threshold
            or score >= self.settings.anomaly_hitl_threshold
        )
        latency_ms = (time.perf_counter() - t0) * 1000.0

        out = AisScoreOut(
            mmsi=payload.mmsi,
            score=score,
            confidence=confidence,
            reasons=reasons,
            hitl_required=hitl_required,
            trace_id=trace_id,
            latency_ms=latency_ms,
            n_history=n_history,
        )

        SCORES_TOTAL.inc()
        SCORE_LATENCY.observe(latency_ms / 1000.0)
        for r in reasons:
            REASONS_TOTAL.labels(code=r.code.value).inc()
        if reasons:
            ANOMALIES_TOTAL.inc()
        if hitl_required:
            await self.hitl.enqueue(trace_id, out, payload.fix.t)
            HITL_ENQUEUED_TOTAL.inc()
        self.cost.record_inference(
            trace_id=trace_id, mmsi=payload.mmsi, latency_ms=latency_ms, n_reasons=len(reasons)
        )
        return out

    async def record_feedback(self, payload: FeedbackIn) -> FeedbackOut:
        pos = await self.hitl.label(payload)
        return FeedbackOut(accepted=True, queue_position=pos)

    # ----------------------------------------------------------- run_plan

    async def run_plan(
        self, plan: PlanGraph, payload: AisScoreIn, anchors: list[Anchor]
    ) -> dict[str, Any]:
        """Execute the deterministic plan node by node. No LLM, no budget."""

        results: dict[str, Any] = {}
        for layer in plan.topo_layers():
            for node in layer:
                if node.kind is PlanNodeKind.CORRIDOR:
                    results["corridor"] = await self.corridor.detect(anchors)
                elif node.kind is PlanNodeKind.PRISM:
                    results["prism"] = await self.st.compute(
                        AnchorPair(a=anchors[-2], b=anchors[-1]), payload.domain
                    )
                elif node.kind is PlanNodeKind.SPEED:
                    results["speed"] = self._speed_signal(anchors)
                elif node.kind is PlanNodeKind.VALIDATE:
                    # S-KBM region gate: validates rendezvous regions (none in the
                    # single-vessel path). Speed is gated by _corrupt_gate above.
                    results["validate"] = await self.validator.validate([], domain=payload.domain)
                elif node.kind is PlanNodeKind.GAPS:
                    results["gaps"] = await self.gap.detect(
                        {"trajectory": anchors, "domain": payload.domain}
                    )
        return results

    # -------------------------------------------------------------- fusion

    def _fuse(self, results: dict[str, Any], anchors: list[Anchor]) -> list[ScoreReason]:
        reasons: list[ScoreReason] = []

        speed = results.get("speed")
        if speed and speed["severity"] > 0:
            reasons.append(
                ScoreReason(
                    code=ReasonCode.IMPLAUSIBLE_SPEED,
                    severity=speed["severity"],
                    detail=f"required speed {speed['v_req_mps']:.1f} m/s exceeds the "
                    f"{speed['v_max_mps']:.1f} m/s vessel cap",
                    evidence=speed,
                )
            )

        gaps: list[Gap] = results.get("gaps") or []
        if gaps:
            g = gaps[0]
            sev = self._gap_severity(g)
            if sev > 0:
                reasons.append(
                    ScoreReason(
                        code=ReasonCode.ABNORMAL_GAP,
                        severity=sev,
                        detail=f"{g.duration_s / 60:.0f}-minute coverage gap "
                        f"(AGM {g.abnormal_gap_measure:.2f})",
                        evidence={
                            "duration_s": g.duration_s,
                            "distance_m": g.distance_m,
                            "p_physical": g.p_physical,
                            "p_data": g.p_data,
                            "agm": g.abnormal_gap_measure,
                        },
                    )
                )

        cor = results.get("corridor")
        if cor is not None and cor.off_corridor:
            sev = self._corridor_severity(cor.distance_m)
            if sev > 0:
                reasons.append(
                    ScoreReason(
                        code=ReasonCode.OFF_CORRIDOR,
                        severity=sev,
                        detail=f"{cor.distance_m / 1000:.1f} km off the nearest sea-lane "
                        f"({cor.nearest_lane})",
                        evidence={"distance_m": cor.distance_m, "nearest_lane": cor.nearest_lane},
                    )
                )
        if cor is not None and cor.unexpected_node:
            reasons.append(
                ScoreReason(
                    code=ReasonCode.UNEXPECTED_NODE,
                    severity=min(1.0, max(0.5, cor.heading_change_deg / 90.0)),
                    detail=f"{cor.heading_change_deg:.0f} deg course change "
                    f"{cor.nearest_waypoint_m / 1000:.1f} km from any waypoint",
                    evidence={
                        "heading_change_deg": cor.heading_change_deg,
                        "nearest_waypoint_m": cor.nearest_waypoint_m,
                    },
                )
            )

        wl: WatchlistStatus | None = results.get("watchlist")
        if wl is not None and wl.watchlisted:
            reasons.append(
                ScoreReason(
                    code=ReasonCode.WATCHLIST_HIT,
                    severity=self.settings.watchlist_severity,
                    detail=f"vessel is on the analyst watchlist: {wl.reason or 'no reason given'}",
                    evidence={"reason": wl.reason, "list_severity": wl.severity},
                )
            )
        if wl is not None and wl.sanctioned:
            reasons.append(
                ScoreReason(
                    code=ReasonCode.SANCTIONS_HIT,
                    severity=self.settings.sanctions_severity,
                    detail=f"vessel is sanctions-flagged ({', '.join(wl.sanctions)})",
                    evidence={"regimes": list(wl.sanctions)},
                )
            )
        if wl is not None and (wl.watchlisted or wl.sanctioned):
            WATCHLIST_HITS_TOTAL.inc()
        return reasons

    def _speed_signal(self, anchors: list[Anchor]) -> dict[str, float]:
        # Max required speed across ALL consecutive segments, so an implausible jump
        # anywhere in the supplied history is scored, not just one on the last segment.
        v_max = self.settings.vessel_v_max_mps
        v_req = 0.0
        for a, b in zip(anchors, anchors[1:], strict=False):
            dt = (b.t - a.t).total_seconds()
            if dt <= 0:
                continue
            v_req = max(v_req, haversine_m(a.lat, a.lon, b.lat, b.lon) / dt)
        p_phys = min(1.0, v_max / max(v_req, 1e-6))
        if v_req <= v_max:
            severity = 0.0
        else:
            severity = min(1.0, (v_req / v_max - 1.0) / self.settings.speed_saturation_mult)
        return {"v_req_mps": v_req, "v_max_mps": v_max, "p_physical": p_phys, "severity": severity}

    def _gap_severity(self, g: Gap) -> float:
        cov = self.settings.coverage_threshold_s
        sat = self.settings.gap_saturation_s
        f_dur = max(0.0, min(1.0, (g.duration_s - cov) / max(sat - cov, 1e-6)))
        # AGM blends kinematic implausibility (1 - p_phys) and the Pi-DPM data-anomaly
        # tail (which rises with distance moved), so a covert long-distance reappearance
        # scores above a benign stationary silence of the same duration.
        return max(0.0, min(1.0, 0.5 * f_dur + 0.5 * g.abnormal_gap_measure))

    def _corridor_severity(self, dist_m: float) -> float:
        thr = self.settings.off_corridor_threshold_m
        sat = self.settings.corridor_saturation_m
        return max(0.0, min(1.0, (dist_m - thr) / max(sat - thr, 1e-6)))

    def _corrupt_gate(self, anchors: list[Anchor]) -> None:
        v_max = self.settings.vessel_v_max_mps
        limit = self.settings.corrupt_reject_factor * v_max
        for a, b in zip(anchors, anchors[1:], strict=False):
            dt = (b.t - a.t).total_seconds()
            if dt <= 0:
                continue
            v_req = haversine_m(a.lat, a.lon, b.lat, b.lon) / dt
            if v_req > limit:
                REJECTS_TOTAL.labels(reason="corrupt_input").inc()
                raise CorruptInput(
                    "AIS fix is non-physical (teleport beyond the corrupt-data bound)",
                    v_req=v_req,
                    v_max=v_max,
                    limit=limit,
                )

    @staticmethod
    def _noisy_or(severities: list[float]) -> float:
        prod = 1.0
        for s in severities:
            prod *= 1.0 - max(0.0, min(1.0, s))
        return 1.0 - prod

    @staticmethod
    def _confidence(score: float) -> float:
        return 1.0 - 2.0 * min(score, 1.0 - score)

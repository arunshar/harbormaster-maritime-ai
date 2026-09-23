"""Typed request / response / state objects. Pydantic v2.

The geometry models (Anchor, AnchorPair, SpeedBounds, GeoEllipse, RendezvousRegion)
are vendored from GeoTrace-Agent so the vendored kernel and agents import them
unchanged. The Ais* / Score* models and the deterministic PlanGraph are new for
the Harbormaster /v1/score-ais path.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class _Mutable(BaseModel):
    model_config = ConfigDict(extra="forbid")


# --- Geometry (vendored) ----------------------------------------------------


class HealthOut(_Frozen):
    status: Literal["ok", "degraded"] = "ok"
    version: str


class Anchor(_Frozen):
    """A space-time anchor (x, y, t)."""

    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    t: datetime

    @field_validator("t")
    @classmethod
    def _utc(cls, v: datetime) -> datetime:
        return v.astimezone(UTC) if v.tzinfo else v.replace(tzinfo=UTC)


class AnchorPair(_Frozen):
    a: Anchor
    b: Anchor


class SpeedBounds(_Frozen):
    v_max_mps: float = Field(..., gt=0)
    v_min_mps: float = 0.0
    domain: Literal["vessel", "vehicle", "pedestrian", "uav"] = "vessel"


class GeoEllipse(_Frozen):
    """Ellipse on the (lat, lon) plane: locus of d(p, A) + d(p, B) <= L."""

    a_lat: float
    a_lon: float
    b_lat: float
    b_lon: float
    semi_major_m: float
    semi_minor_m: float
    rotation_rad: float


class RendezvousRegion(_Frozen):
    polygon_geojson: dict[str, Any]
    earliest_meet_t: datetime
    latest_meet_t: datetime
    confidence: float = Field(..., ge=0, le=1)
    method: Literal["TGARD", "DC-TGARD", "STP", "STAGD", "STP-baseline"]


# --- AIS scoring (new) ------------------------------------------------------


class AisFix(_Mutable):
    """One AIS position report for a vessel."""

    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    t: datetime
    sog: float | None = Field(None, ge=0)  # speed over ground, knots
    cog: float | None = Field(None, ge=0, le=360)  # course over ground, deg
    heading: float | None = Field(None, ge=0, le=360)

    @field_validator("t")
    @classmethod
    def _utc(cls, v: datetime) -> datetime:
        return v.astimezone(UTC) if v.tzinfo else v.replace(tzinfo=UTC)

    def to_anchor(self) -> Anchor:
        return Anchor(lat=self.lat, lon=self.lon, t=self.t)


class AisScoreIn(_Mutable):
    """Score one current fix against this vessel's recent history."""

    mmsi: int = Field(..., ge=0, le=999_999_999)
    fix: AisFix
    history: list[AisFix] = Field(default_factory=list)
    domain: Literal["vessel", "vehicle", "pedestrian", "uav"] = "vessel"

    def track(self) -> list[AisFix]:
        """History + current fix, de-duplicated and sorted by time."""

        seen: set[datetime] = set()
        out: list[AisFix] = []
        for f in sorted([*self.history, self.fix], key=lambda f: f.t):
            if f.t in seen:
                continue
            seen.add(f.t)
            out.append(f)
        return out


class ReasonCode(StrEnum):
    IMPLAUSIBLE_SPEED = "implausible_speed"
    ABNORMAL_GAP = "abnormal_gap"
    OFF_CORRIDOR = "off_corridor"
    UNEXPECTED_NODE = "unexpected_node"
    WATCHLIST_HIT = "watchlist_hit"
    SANCTIONS_HIT = "sanctions_hit"


class ScoreReason(_Frozen):
    code: ReasonCode
    severity: float = Field(..., ge=0, le=1)
    detail: str
    evidence: dict[str, Any] = Field(default_factory=dict)


class AisScoreOut(_Mutable):
    mmsi: int
    score: float = Field(..., ge=0, le=1)  # anomaly severity; higher = more anomalous
    confidence: float = Field(..., ge=0, le=1)  # certainty of the verdict
    reasons: list[ScoreReason] = Field(default_factory=list)
    hitl_required: bool = False
    trace_id: str
    latency_ms: float
    n_history: int


class FeedbackIn(_Mutable):
    trace_id: str
    label: Literal["correct", "incorrect", "ambiguous"]
    notes: str | None = None
    reviewer: str


# --- Registry (Phase 2; Postgres is the system of record) ---------------------
# All strings are bounded: an unbounded value accepted here becomes a CDC event
# and a DynamoDB item, and DynamoDB caps items at 400 KB, so the API boundary
# is where oversized payloads must die (422), not the consumer.


class VesselIn(_Mutable):
    name: str = Field("", max_length=256)
    flag_state: str = Field("", max_length=256)
    vessel_type: str = Field("", max_length=256)


class WatchlistIn(_Mutable):
    reason: str = Field(..., min_length=1, max_length=4096)
    severity: float = Field(0.9, ge=0, le=1)
    added_by: str = Field("", max_length=256)

    @field_validator("reason")
    @classmethod
    def _reason_not_blank(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("reason must not be blank")
        return v


class SanctionsIn(_Mutable):
    regime: str = Field(..., min_length=1, max_length=128)
    reference: str = Field("", max_length=4096)

    @field_validator("regime")
    @classmethod
    def _regime_not_blank(cls, v: str) -> str:
        # The stripped, lowercased regime is the sanctions_flags id suffix
        # ("<mmsi>:<regime>"); a blank one would mint the "<mmsi>:" poison id
        # the CDC key mapper rejects, so it dies here with a 422.
        v = v.strip().lower()
        if not v:
            raise ValueError("regime must not be blank")
        return v


class FeedbackOut(_Frozen):
    accepted: bool = True
    queue_position: int | None = None


# --- Deterministic plan graph (new; no LLM) ---------------------------------


class PlanNodeKind(StrEnum):
    PRISM = "prism.compute"
    SPEED = "speed.physical"
    GAPS = "gaps.detect"
    CORRIDOR = "corridor.detect"
    VALIDATE = "validate.kinematic"


class PlanNode(_Frozen):
    id: str = Field(default_factory=lambda: uuid4().hex[:8])
    kind: PlanNodeKind
    deps: tuple[str, ...] = ()
    rationale: str = ""


class PlanGraph(_Frozen):
    nodes: tuple[PlanNode, ...]
    rationale: str

    def topo_layers(self) -> list[list[PlanNode]]:
        """Layers of nodes safe to run together (deps satisfied)."""

        remaining = {n.id: n for n in self.nodes}
        done: set[str] = set()
        layers: list[list[PlanNode]] = []
        while remaining:
            ready = [n for n in remaining.values() if all(d in done for d in n.deps)]
            if not ready:
                raise ValueError("plan graph has a cycle")
            layers.append(ready)
            for n in ready:
                done.add(n.id)
                del remaining[n.id]
        return layers

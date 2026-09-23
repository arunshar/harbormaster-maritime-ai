"""GapDetectorAgent. Extends STAGD + Dynamic Region Merge (DRM).

Detects abnormal trajectory gaps (signal-coverage denial, clandestine
rendezvous) over AIS data using a temporal scan for under-coverage plus a
maximal-union DRM merge over space-time prism geo-ellipses.

The agent computes the Abnormal Gap Measure (AGM):

    AGM(g) = lambda * (1 - P_phys(g)) + (1 - lambda) * P_data(g)

where P_phys is a kinematic plausibility score and P_data is the Pi-DPM
reconstruction-error tail probability for that gap. lambda is 0.6 by default.

Vendored from GeoTrace-Agent and trimmed for the Harbormaster serving slice:
the R*-tree DRM index is replaced by an O(n^2) bounding-box union (faithful for
the demo cardinality; swap rtree.index back in for scale). P_data defaults to
the numpy surrogate `_pi_dpm_score`; Phase 3 (gate 3.6) wires an optional
async `pi_dpm_scorer` callable (the real SageMaker Pi-DPM endpoint via
`app.pidpm_client.PiDpmClient.ascore`) that, when it returns a real score,
overrides the analytic estimate for that gap. The analytic estimate is
always computed first and is the guaranteed fallback: a disabled or failed
SageMaker call (the callable returns None) never blocks or empties a score,
it just falls back to exactly the Phase 1/2 behavior.
"""

from __future__ import annotations

import itertools
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import numpy as np
import structlog
from shapely.geometry import mapping

from app.components.space_time_prism import Polygonal, Prism, haversine_m, speed_bounds_for
from app.config import Settings
from app.models import Anchor, AnchorPair, GeoEllipse

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class Gap:
    start: Anchor
    end: Anchor
    duration_s: float
    distance_m: float
    p_physical: float
    p_data: float
    abnormal_gap_measure: float
    coverage_polygon_geojson: dict[str, Any]


class GapDetectorAgent:
    def __init__(
        self,
        settings: Settings,
        lam: float = 0.6,
        *,
        pi_dpm_scorer: Callable[[Prism], Awaitable[float | None]] | None = None,
    ) -> None:
        self.settings = settings
        self.lam = lam
        self._pi_dpm_scorer = pi_dpm_scorer

    async def detect(self, inputs: dict[str, Any]) -> list[Gap]:
        traj: list[Anchor] = [
            a if isinstance(a, Anchor) else Anchor(**a) for a in inputs.get("trajectory", [])
        ]
        coverage_threshold_s: float = float(
            inputs.get("coverage_threshold_s", self.settings.coverage_threshold_s)
        )
        domain: str = str(inputs.get("domain", "vessel"))
        if len(traj) < 2:
            return []

        # 1) raw gaps where consecutive samples are farther apart in time than threshold
        candidates: list[tuple[Anchor, Anchor]] = []
        for a, b in itertools.pairwise(traj):
            if (b.t - a.t).total_seconds() > coverage_threshold_s:
                candidates.append((a, b))
        if not candidates:
            return []

        # 2) build a prism per gap and DRM-merge prisms whose MOBR bboxes overlap
        prisms: list[Prism] = []
        bounds = speed_bounds_for(
            domain,
            vessel_v_max_kts=self.settings.vessel_v_max_kts,
            vehicle_v_max_kmh=self.settings.vehicle_v_max_kmh,
        )
        for a, b in candidates:
            try:
                prisms.append(Prism.compute(AnchorPair(a=a, b=b), bounds))
            except ValueError:
                continue
        if not prisms:
            return []

        mobrs = [p.mobr() for p in prisms]
        # Connected components over the "bboxes overlap" graph, union-find, so a
        # TRANSITIVE chain (0 overlaps 1, 1 overlaps 2, 0 disjoint from 2) merges
        # into ONE region (the DRM maximal-union the docstring promises). The
        # previous single-pass seed clustering put the middle prism in two
        # clusters (double-counting its coverage) and, because each cluster's
        # head is its lowest-index member, never emitted the tail prism's gap.
        n = len(prisms)
        parent = list(range(n))

        def _find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        for i in range(n):
            for j in range(i + 1, n):
                if _mobr_bounds_overlap(mobrs[i], mobrs[j]):
                    ri, rj = _find(i), _find(j)
                    if ri != rj:
                        parent[max(ri, rj)] = min(ri, rj)

        components: dict[int, list[int]] = {}
        for i in range(n):
            components.setdefault(_find(i), []).append(i)
        # deterministic order: components by their lowest (earliest-gap) member
        merged: list[list[int]] = [sorted(m) for _, m in sorted(components.items())]

        gaps: list[Gap] = []
        for cluster in merged:
            members = [prisms[i] for i in cluster]
            ellipses: list[GeoEllipse] = [m.base_ellipse for m in members]
            poly = Prism.merge_dynamic(ellipses, members[0].pair)
            head = members[0]
            p_phys = self._physical_plausibility(head)
            p_data = self._pi_dpm_score(head)  # the guaranteed fallback, always computed
            if self._pi_dpm_scorer is not None:
                real_p_data = await self._pi_dpm_scorer(head)
                if real_p_data is not None:
                    p_data = real_p_data
            agm = self.lam * (1.0 - p_phys) + (1.0 - self.lam) * p_data
            gaps.append(
                Gap(
                    start=head.pair.a,
                    end=head.pair.b,
                    duration_s=head.duration_s,
                    distance_m=self._anchor_distance_m(head.pair),
                    p_physical=p_phys,
                    p_data=p_data,
                    abnormal_gap_measure=float(agm),
                    coverage_polygon_geojson=mapping(poly),
                )
            )
        gaps.sort(key=lambda g: g.abnormal_gap_measure, reverse=True)
        return gaps

    # --------------------------------------------------------- internals

    @staticmethod
    def _anchor_distance_m(pair: AnchorPair) -> float:
        return haversine_m(pair.a.lat, pair.a.lon, pair.b.lat, pair.b.lon)

    def _physical_plausibility(self, prism: Prism) -> float:
        """min(1, v_max / v_required): 1.0 when the reappearance is reachable."""

        v_req = self._anchor_distance_m(prism.pair) / max(prism.duration_s, 1.0)
        return float(min(1.0, prism.v_max_mps / max(v_req, 1e-6)))

    def _pi_dpm_score(self, prism: Prism) -> float:
        """Pi-DPM reconstruction-error tail probability (numpy surrogate).

        Longer-duration, longer-distance gaps score as more anomalous, squashed
        to (0, 1). The real diffusion Pi-DPM (trained on an off-cloud GPU
        cluster) replaces this with the same signature once promoted into the
        serving plane.
        """

        distance = self._anchor_distance_m(prism.pair)
        z = math.log1p(distance) + 0.001 * prism.duration_s
        return float(1 / (1 + np.exp(-((z - 12) / 3))))


def _bbox_overlap(a: tuple, b: tuple) -> bool:
    """Axis-aligned bbox overlap test in (xmin, ymin, xmax, ymax)."""

    return not (a[2] < b[0] or b[2] < a[0] or a[3] < b[1] or b[3] < a[1])


def _mobr_bounds_overlap(a: Polygonal, b: Polygonal) -> bool:
    """Preserve bbox overlap semantics for antimeridian-cut components."""

    a_parts = a.geoms if a.geom_type == "MultiPolygon" else (a,)
    b_parts = b.geoms if b.geom_type == "MultiPolygon" else (b,)
    return any(
        _bbox_overlap(a_part.bounds, b_part.bounds) for a_part in a_parts for b_part in b_parts
    )

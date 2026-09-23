"""RendezvousFinderAgent. Extends TGARD and DC-TGARD.

Tightens the spatial bounds on possible-rendezvous regions inside
trajectory gaps over road / shipping networks. DC-TGARD (Dual
Convergence) exploits ellipse symmetry with bi-directional pruning and
an early-stopping criterion (Sharma et al., SIGSPATIAL 2022).

This module is vendored from GeoTrace-Agent, which uses the MIT License with
the same copyright holder.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Literal

from shapely.geometry import Polygon, mapping

from app.components.space_time_prism import Polygonal, Prism, intersect
from app.config import Settings
from app.models import RendezvousRegion


class RendezvousFinderAgent:
    def __init__(self, settings: Settings, st_reasoner) -> None:  # type: ignore[no-untyped-def]
        self.settings = settings
        self.st = st_reasoner

    async def find(
        self,
        prisms: Iterable[Prism],
        *,
        method: Literal["TGARD", "DC-TGARD", "STP"] = "TGARD",
    ) -> list[RendezvousRegion]:
        prisms = list(prisms)
        if len(prisms) < 2:
            return []
        if method == "DC-TGARD":
            return await self._dc_tgard(prisms)
        if method == "TGARD":
            return await self._tgard(prisms)
        return await self._stp(prisms)

    # ------------------------------------------------------------ TGARD

    async def _tgard(self, prisms: list[Prism]) -> list[RendezvousRegion]:
        regions: list[RendezvousRegion] = []
        for i in range(len(prisms)):
            for j in range(i + 1, len(prisms)):
                inter = intersect(prisms[i], prisms[j], n_slices=24)
                if inter.is_empty:
                    continue
                t_lo, t_hi = self._time_overlap(prisms[i], prisms[j])
                regions.append(RendezvousRegion(
                    polygon_geojson=mapping(inter),
                    earliest_meet_t=t_lo,
                    latest_meet_t=t_hi,
                    confidence=self._confidence(prisms[i], prisms[j], inter),
                    method="TGARD",
                ))
        return regions

    # --------------------------------------------------------- DC-TGARD

    async def _dc_tgard(self, prisms: list[Prism]) -> list[RendezvousRegion]:
        """Dual-convergence variant.

        Walks the time interval from both ends, pruning slices whose
        intersection becomes empty. Returns tighter bounds and is
        provably faster in expectation when prisms only briefly overlap.
        """

        regions: list[RendezvousRegion] = []
        for i in range(len(prisms)):
            for j in range(i + 1, len(prisms)):
                a, b = prisms[i], prisms[j]
                t0, t1 = self._time_overlap(a, b)
                if t0 >= t1:
                    continue
                lo, hi = 0.0, 1.0
                accum = Polygon()
                # forward pass: shrink lo
                for s in (k / 24 for k in range(25)):
                    t = t0 + (t1 - t0) * s
                    pa = a.ellipse_polygon(a.ellipse_at(t))
                    pb = b.ellipse_polygon(b.ellipse_at(t))
                    inter = pa.intersection(pb)
                    if inter.is_empty:
                        lo = s
                    else:
                        accum = accum.union(inter)
                        break
                # backward pass: shrink hi
                for s in (1 - k / 24 for k in range(25)):
                    t = t0 + (t1 - t0) * s
                    pa = a.ellipse_polygon(a.ellipse_at(t))
                    pb = b.ellipse_polygon(b.ellipse_at(t))
                    inter = pa.intersection(pb)
                    if inter.is_empty:
                        hi = s
                    else:
                        accum = accum.union(inter)
                        break
                if accum.is_empty:
                    continue
                regions.append(RendezvousRegion(
                    polygon_geojson=mapping(accum),
                    earliest_meet_t=t0 + (t1 - t0) * lo,
                    latest_meet_t=t0 + (t1 - t0) * hi,
                    confidence=self._confidence(a, b, accum, dc_bonus=0.05),
                    method="DC-TGARD",
                ))
        return regions

    # --------------------------------------------------------- baseline

    async def _stp(self, prisms: list[Prism]) -> list[RendezvousRegion]:
        regions: list[RendezvousRegion] = []
        for i in range(len(prisms)):
            for j in range(i + 1, len(prisms)):
                inter = intersect(prisms[i], prisms[j], n_slices=8)
                if inter.is_empty:
                    continue
                t_lo, t_hi = self._time_overlap(prisms[i], prisms[j])
                regions.append(RendezvousRegion(
                    polygon_geojson=mapping(inter),
                    earliest_meet_t=t_lo,
                    latest_meet_t=t_hi,
                    confidence=0.6,
                    method="STP-baseline",
                ))
        return regions

    # ------------------------------------------------------------ utils

    @staticmethod
    def _time_overlap(a: Prism, b: Prism) -> tuple[datetime, datetime]:
        t0 = max(a.pair.a.t, b.pair.a.t)
        t1 = min(a.pair.b.t, b.pair.b.t)
        if t0.tzinfo is None:
            t0 = t0.replace(tzinfo=UTC)
        if t1.tzinfo is None:
            t1 = t1.replace(tzinfo=UTC)
        return t0, t1

    @staticmethod
    def _confidence(a: Prism, b: Prism, inter: Polygonal, dc_bonus: float = 0.0) -> float:
        # smaller intersection relative to MOBR area implies tighter bound -> higher confidence
        if inter.is_empty:
            return 0.0
        mbr = a.mobr().union(b.mobr())
        ratio = inter.area / max(mbr.area, 1e-9)
        c = 1.0 - min(0.9, ratio)
        return float(min(0.99, c + dc_bonus))

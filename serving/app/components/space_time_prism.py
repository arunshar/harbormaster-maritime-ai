"""Hägerstrand space-time prism kernel.

Derived from GeoTrace-Agent. Harbormaster maintains a local fork for verified
serving fixes; supplied ellipses are centered on their own foci rather than the
Prism's anchor pair and use the local projection defined by those foci.

Given two anchors A = (lat_A, lon_A, t_A) and B = (lat_B, lon_B, t_B)
with t_A < t_B, and a maximum speed v_max, the set of points reachable
from A by t and able to reach B by t_B is, at each interior time t,
a geo-ellipse with foci A and B and semi-major axis

    a(t) = 0.5 * v_max * (t_B - t_A).

The full prism is the union of these ellipses across t in [t_A, t_B].
This module computes prisms, ellipses, MOBRs, and their intersections.

Math is done in a local spherical azimuthal-equidistant projection centered on
the spherical midpoint of the active ellipse's foci. The base ellipse uses the
prism anchor pair; a supplied ellipse uses its own foci. Longitude deltas follow
the shortest wrapped path, and inverse projection keeps longitude continuous so
geometry crossing the antimeridian can be cut into normalized components.

Near-pole footprints are supported only when the requested ellipse or MOBR does
not contain or touch a geographic pole. That boundary is rejected explicitly
because longitude is undefined at a pole and a lon/lat polygon around it cannot
honor the module's continuous-ring contract.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import numpy as np
from shapely.affinity import rotate, translate
from shapely.geometry import LineString, MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry
from shapely.geometry.polygon import orient as orient_polygon
from shapely.ops import split

from app.models import AnchorPair, GeoEllipse, SpeedBounds

EARTH_RADIUS_M = 6_371_000.0
KNOTS_TO_MPS = 0.514_444
KMH_TO_MPS = 1 / 3.6

# The normalized vector sum amplifies floating-point error by 1 / norm. This
# cutoff rejects focus pairs within about 6.37 m of antipodal separation and
# keeps the midpoint's conditioning near the millimeter scale.
_FOCUS_MIDPOINT_EPS = 1e-6
_PROJECTION_ANTIPODE_EPS = 1e-12
_POLE_LINEAR_TOL_M = 1e-6
_POLE_NORMALIZED_TOL = 1e-12
_MOBR_EDGE_SEGMENTS = 64
_MAX_ELLIPSE_SEGMENTS = 4096
_RING_COORD_TOL_DEG = 1e-12
_FOCUS_DOMAIN_ERROR = "active foci are antipodal or numerically singular"
_PROJECTION_DOMAIN_ERROR = "azimuthal projection is singular at the antipode"
_POLE_DOMAIN_ERROR = "prism footprint contains or touches a geographic pole"

Polygonal = Polygon | MultiPolygon


def speed_bounds_for(
    domain: str, *, vessel_v_max_kts: float, vehicle_v_max_kmh: float
) -> SpeedBounds:
    if domain == "vessel":
        return SpeedBounds(v_max_mps=vessel_v_max_kts * KNOTS_TO_MPS, domain="vessel")
    if domain == "vehicle":
        return SpeedBounds(v_max_mps=vehicle_v_max_kmh * KMH_TO_MPS, domain="vehicle")
    if domain == "pedestrian":
        return SpeedBounds(v_max_mps=2.0, domain="pedestrian")
    if domain == "uav":
        return SpeedBounds(v_max_mps=30.0, domain="uav")
    raise ValueError(f"unknown domain: {domain}")


@dataclass(frozen=True)
class _LocalProjection:
    """Spherical azimuthal-equidistant projection around a local center."""

    lat_ref_rad: float
    lon_ref_rad: float

    def to_xy(self, lat_deg: float, lon_deg: float) -> tuple[float, float]:
        lat = math.radians(lat_deg)
        lon = math.radians(lon_deg)
        lon_delta = _wrapped_radians(lon - self.lon_ref_rad)
        sin_ref = math.sin(self.lat_ref_rad)
        cos_ref = math.cos(self.lat_ref_rad)
        sin_lat = math.sin(lat)
        cos_lat = math.cos(lat)
        cos_delta = math.cos(lon_delta)
        half_chord_sq = math.sin(0.5 * (lat - self.lat_ref_rad)) ** 2 + (
            cos_ref * cos_lat * math.sin(0.5 * lon_delta) ** 2
        )
        c = 2.0 * math.asin(math.sqrt(_clamp_unit_interval(half_chord_sq)))
        if c >= math.pi - _PROJECTION_ANTIPODE_EPS:
            raise ValueError(_PROJECTION_DOMAIN_ERROR)
        sin_c = math.sin(c)
        k = 1.0 if c == 0.0 else c / sin_c
        x = EARTH_RADIUS_M * k * cos_lat * math.sin(lon_delta)
        y = EARTH_RADIUS_M * k * (cos_ref * sin_lat - sin_ref * cos_lat * cos_delta)
        return x, y

    def to_lonlat(self, x: float, y: float) -> tuple[float, float]:
        rho = math.hypot(x, y)
        if rho == 0.0:
            return math.degrees(self.lon_ref_rad), math.degrees(self.lat_ref_rad)
        c = rho / EARTH_RADIUS_M
        if c >= math.pi - _PROJECTION_ANTIPODE_EPS:
            raise ValueError(_PROJECTION_DOMAIN_ERROR)
        sin_c = math.sin(c)
        cos_c = math.cos(c)
        sin_ref = math.sin(self.lat_ref_rad)
        cos_ref = math.cos(self.lat_ref_rad)
        sin_lon = math.sin(self.lon_ref_rad)
        cos_lon = math.cos(self.lon_ref_rad)
        unit_x, unit_y = x / rho, y / rho
        direction_x = -unit_x * sin_lon - unit_y * sin_ref * cos_lon
        direction_y = unit_x * cos_lon - unit_y * sin_ref * sin_lon
        direction_z = unit_y * cos_ref
        point_x = cos_c * cos_ref * cos_lon + sin_c * direction_x
        point_y = cos_c * cos_ref * sin_lon + sin_c * direction_y
        point_z = cos_c * sin_ref + sin_c * direction_z
        lat = math.atan2(point_z, math.hypot(point_x, point_y))
        canonical_lon = math.atan2(point_y, point_x)
        lon_delta = _wrapped_radians(canonical_lon - self.lon_ref_rad)
        # Do not wrap here. The seam normalizer needs a continuous longitude
        # around the local reference so it can cut before canonicalizing.
        lon = self.lon_ref_rad + lon_delta
        return math.degrees(lon), math.degrees(lat)


def _clamp_unit_interval(value: float) -> float:
    """Clamp roundoff before inverse trigonometric calls."""

    return min(1.0, max(0.0, value))


def _wrapped_radians(angle: float) -> float:
    """Wrap to [-pi, pi] while preserving canonical inputs exactly."""

    if -math.pi <= angle <= math.pi:
        return angle
    return (angle + math.pi) % (2 * math.pi) - math.pi


def _midpoint_longitude_rad(a_lon_deg: float, b_lon_deg: float) -> float:
    """Longitude halfway along the shortest wrapped path from A to B."""

    a_lon_rad = math.radians(a_lon_deg)
    b_lon_rad = math.radians(b_lon_deg)
    return a_lon_rad + 0.5 * _wrapped_radians(b_lon_rad - a_lon_rad)


def _spherical_midpoint_rad(
    a_lat_deg: float,
    a_lon_deg: float,
    b_lat_deg: float,
    b_lon_deg: float,
) -> tuple[float, float]:
    """Return the continuous spherical midpoint of two non-antipodal foci."""

    a_lat = math.radians(a_lat_deg)
    a_lon = math.radians(a_lon_deg)
    b_lat = math.radians(b_lat_deg)
    b_lon = math.radians(b_lon_deg)
    ax = math.cos(a_lat) * math.cos(a_lon)
    ay = math.cos(a_lat) * math.sin(a_lon)
    az = math.sin(a_lat)
    bx = math.cos(b_lat) * math.cos(b_lon)
    by = math.cos(b_lat) * math.sin(b_lon)
    bz = math.sin(b_lat)
    sx, sy, sz = ax + bx, ay + by, az + bz
    norm = math.sqrt(sx * sx + sy * sy + sz * sz)
    if norm <= _FOCUS_MIDPOINT_EPS:
        raise ValueError(_FOCUS_DOMAIN_ERROR)
    sx, sy, sz = sx / norm, sy / norm, sz / norm
    lat_ref = math.atan2(sz, math.hypot(sx, sy))
    canonical_lon = math.atan2(sy, sx)
    lon_hint = _midpoint_longitude_rad(a_lon_deg, b_lon_deg)
    turns = round((lon_hint - canonical_lon) / (2 * math.pi))
    return lat_ref, canonical_lon + turns * 2 * math.pi


def _shifted_polygon(polygon: Polygon, x_offset: float) -> Polygon:
    """Shift one component and clamp seam roundoff to exact longitude limits."""

    def shift_ring(coords) -> list[tuple[float, float]]:  # type: ignore[no-untyped-def]
        shifted: list[tuple[float, float]] = []
        shifted_with_repeats: list[tuple[float, float]] = []
        for x, y in coords:
            longitude = x + x_offset
            if math.isclose(longitude, -180.0, rel_tol=0.0, abs_tol=1e-10):
                longitude = -180.0
            elif math.isclose(longitude, 180.0, rel_tol=0.0, abs_tol=1e-10):
                longitude = 180.0
            if not -180.0 <= longitude <= 180.0:
                raise ValueError("normalized polygon longitude is outside [-180, 180]")
            coordinate = (longitude, y)
            shifted_with_repeats.append(coordinate)
            if (
                shifted
                and math.isclose(
                    shifted[-1][0],
                    coordinate[0],
                    rel_tol=0.0,
                    abs_tol=_RING_COORD_TOL_DEG,
                )
                and math.isclose(
                    shifted[-1][1],
                    coordinate[1],
                    rel_tol=0.0,
                    abs_tol=_RING_COORD_TOL_DEG,
                )
            ):
                continue
            shifted.append(coordinate)
        if (
            shifted
            and math.isclose(
                shifted[0][0],
                shifted[-1][0],
                rel_tol=0.0,
                abs_tol=_RING_COORD_TOL_DEG,
            )
            and math.isclose(
                shifted[0][1],
                shifted[-1][1],
                rel_tol=0.0,
                abs_tol=_RING_COORD_TOL_DEG,
            )
        ):
            shifted[-1] = shifted[0]
        elif shifted:
            shifted.append(shifted[0])
        if len(shifted) < 4:
            return shifted_with_repeats
        return shifted

    shell = shift_ring(polygon.exterior.coords)
    holes = [shift_ring(ring.coords) for ring in polygon.interiors]
    return orient_polygon(Polygon(shell, holes), sign=1.0)


def _orient_polygonal(geometry: BaseGeometry) -> Polygonal:
    """Retain positive-area components and apply RFC 7946 winding.

    Shapely Boolean operations may return boundary-only Point or LineString
    intersections, or a GeometryCollection that mixes those with polygons.
    The prism API intentionally exposes only reachable regions with area, so
    non-polygonal components are discarded and boundary-only contact is empty.
    """

    if isinstance(geometry, Polygon):
        return orient_polygon(geometry, sign=1.0)
    if isinstance(geometry, MultiPolygon):
        parts = list(geometry.geoms)
    else:
        parts = []
        for part in getattr(geometry, "geoms", ()):
            if isinstance(part, Polygon):
                parts.append(part)
            elif isinstance(part, MultiPolygon):
                parts.extend(part.geoms)
    oriented = [
        orient_polygon(part, sign=1.0) for part in parts if not part.is_empty and part.area > 0
    ]
    if not oriented:
        return Polygon()
    if len(oriented) == 1:
        return oriented[0]
    return MultiPolygon(oriented)


def _normalize_polygon_longitudes(polygon: Polygon) -> Polygonal:
    """Cut a continuous polygon at every antimeridian seam and normalize it.

    Projection output stays continuous around its local reference and can use
    longitudes outside [-180, 180]. GeoJSON should not draw the closing edge
    across the planet, so each crossed 180 + 360k seam becomes a component
    boundary before the components are shifted into the canonical range.
    """

    min_x, min_y, max_x, max_y = polygon.bounds
    if max_x - min_x >= 360.0:
        raise ValueError("polygon spans 360 degrees or more")

    pieces = [polygon]
    first_k = math.floor((min_x - 180.0) / 360.0) + 1
    last_k = math.ceil((max_x - 180.0) / 360.0) - 1
    for k in range(first_k, last_k + 1):
        seam_x = 180.0 + 360.0 * k
        cutter = LineString([(seam_x, min_y - 1.0), (seam_x, max_y + 1.0)])
        next_pieces: list[Polygon] = []
        for piece in pieces:
            if piece.bounds[0] < seam_x < piece.bounds[2]:
                next_pieces.extend(
                    part
                    for part in split(piece, cutter).geoms
                    if isinstance(part, Polygon) and not part.is_empty
                )
            else:
                next_pieces.append(piece)
        pieces = next_pieces

    normalized: list[Polygon] = []
    for piece in pieces:
        center_x = piece.representative_point().x
        turns = math.floor((center_x + 180.0) / 360.0)
        normalized.append(_shifted_polygon(piece, -360.0 * turns))
    normalized.sort(key=lambda part: part.bounds)
    if len(normalized) == 1:
        return normalized[0]
    return MultiPolygon(normalized)


def _densify_closed_ring(
    coordinates: Iterable[tuple[float, float]],
    *,
    segments_per_edge: int,
) -> list[tuple[float, float]]:
    """Sample straight projected edges before their nonlinear inverse map."""

    vertices = list(coordinates)
    dense: list[tuple[float, float]] = []
    for (start_x, start_y), (end_x, end_y) in zip(vertices[:-1], vertices[1:], strict=True):
        for step in range(segments_per_edge):
            fraction = step / segments_per_edge
            dense.append(
                (
                    start_x + fraction * (end_x - start_x),
                    start_y + fraction * (end_y - start_y),
                )
            )
    dense.append(dense[0])
    return dense


def _unwrap_ring_longitudes(
    coordinates: Iterable[tuple[float, float]],
) -> list[tuple[float, float]]:
    """Lift sequential ring vertices onto one continuous longitude branch."""

    points = list(coordinates)
    if len(points) > 1 and points[-1] == points[0]:
        points.pop()
    if not points:
        return []
    unwrapped = [points[0]]
    for longitude, latitude in points[1:]:
        previous_longitude = unwrapped[-1][0]
        while longitude - previous_longitude > 180.0:
            longitude -= 360.0
        while longitude - previous_longitude < -180.0:
            longitude += 360.0
        unwrapped.append((longitude, latitude))
    return unwrapped


def _projection_for(pair: AnchorPair) -> _LocalProjection:
    lat_ref_rad, lon_ref_rad = _spherical_midpoint_rad(
        pair.a.lat,
        pair.a.lon,
        pair.b.lat,
        pair.b.lon,
    )
    return _LocalProjection(lat_ref_rad=lat_ref_rad, lon_ref_rad=lon_ref_rad)


def _ellipse_center_xy(proj: _LocalProjection, ellipse: GeoEllipse) -> tuple[float, float]:
    ax, ay = proj.to_xy(ellipse.a_lat, ellipse.a_lon)
    bx, by = proj.to_xy(ellipse.b_lat, ellipse.b_lon)
    return 0.5 * (ax + bx), 0.5 * (ay + by)


def _projection_for_ellipse(ellipse: GeoEllipse) -> _LocalProjection:
    lat_ref_rad, lon_ref_rad = _spherical_midpoint_rad(
        ellipse.a_lat,
        ellipse.a_lon,
        ellipse.b_lat,
        ellipse.b_lon,
    )
    return _LocalProjection(lat_ref_rad=lat_ref_rad, lon_ref_rad=lon_ref_rad)


def _axis_contains_or_touches(value: float, extent: float) -> bool:
    extent = abs(extent)
    return abs(value) <= extent or math.isclose(
        abs(value),
        extent,
        rel_tol=_POLE_NORMALIZED_TOL,
        abs_tol=_POLE_LINEAR_TOL_M,
    )


def _ellipse_contains_or_touches(u: float, v: float, a: float, b: float) -> bool:
    a, b = abs(a), abs(b)
    if a <= _POLE_LINEAR_TOL_M:
        return abs(u) <= _POLE_LINEAR_TOL_M and _axis_contains_or_touches(v, b)
    if b <= _POLE_LINEAR_TOL_M:
        return abs(v) <= _POLE_LINEAR_TOL_M and _axis_contains_or_touches(u, a)
    normalized = (u / a) ** 2 + (v / b) ** 2
    return normalized <= 1.0 or math.isclose(
        normalized,
        1.0,
        rel_tol=_POLE_NORMALIZED_TOL,
        abs_tol=_POLE_NORMALIZED_TOL,
    )


def _assert_poles_outside_footprint(
    proj: _LocalProjection,
    ellipse: GeoEllipse,
    center_xy: tuple[float, float],
    *,
    rectangle: bool,
) -> None:
    """Reject ellipse or MOBR containment and contact at either pole."""

    cx, cy = center_xy
    cos_theta = math.cos(ellipse.rotation_rad)
    sin_theta = math.sin(ellipse.rotation_rad)
    for pole_lat_rad in (math.pi / 2, -math.pi / 2):
        # In an azimuthal-equidistant plane, either pole lies due north or
        # south of the center at its exact spherical radial distance.
        pole_x = 0.0
        pole_y = EARTH_RADIUS_M * (pole_lat_rad - proj.lat_ref_rad)
        dx, dy = pole_x - cx, pole_y - cy
        u = dx * cos_theta + dy * sin_theta
        v = -dx * sin_theta + dy * cos_theta
        if rectangle:
            contains = _axis_contains_or_touches(u, ellipse.semi_major_m) and (
                _axis_contains_or_touches(v, ellipse.semi_minor_m)
            )
        else:
            contains = _ellipse_contains_or_touches(
                u,
                v,
                ellipse.semi_major_m,
                ellipse.semi_minor_m,
            )
        if contains:
            raise ValueError(_POLE_DOMAIN_ERROR)


def _ellipse_polygon_unwrapped(
    proj: _LocalProjection,
    ellipse: GeoEllipse,
    center_xy: tuple[float, float],
    *,
    n: int = 64,
) -> Polygon:
    """Sample an ellipse in continuous longitude before seam normalization."""

    cx, cy = center_xy
    a, b, theta = ellipse.semi_major_m, ellipse.semi_minor_m, ellipse.rotation_rad
    sample_count = n
    while True:
        ts = np.linspace(0, 2 * math.pi, sample_count, endpoint=False)
        xs = cx + a * np.cos(ts) * math.cos(theta) - b * np.sin(ts) * math.sin(theta)
        ys = cy + a * np.cos(ts) * math.sin(theta) + b * np.sin(ts) * math.cos(theta)
        coords = [proj.to_lonlat(float(x), float(y)) for x, y in zip(xs, ys, strict=True)]
        polygon = Polygon(_unwrap_ring_longitudes(coords))
        if polygon.is_valid or a == 0.0 or b == 0.0:
            return polygon
        if sample_count >= _MAX_ELLIPSE_SEGMENTS:
            raise ValueError("inverse-projected ellipse is numerically invalid")
        sample_count = min(2 * sample_count, _MAX_ELLIPSE_SEGMENTS)


def haversine_m(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    phi1, phi2 = math.radians(a_lat), math.radians(b_lat)
    dphi = phi2 - phi1
    dlam = _wrapped_radians(math.radians(b_lon - a_lon))
    s = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(_clamp_unit_interval(s)))


@dataclass(frozen=True)
class Prism:
    """A Hägerstrand space-time prism between two anchors.

    `feasible` is False when the anchors are unreachable under v_max,
    in which case `geo_ellipse_at_midpoint` returns a degenerate ellipse.
    """

    pair: AnchorPair
    v_max_mps: float
    duration_s: float
    feasible: bool
    base_ellipse: GeoEllipse
    proj: _LocalProjection

    @classmethod
    def compute(cls, pair: AnchorPair, bounds: SpeedBounds) -> Prism:
        proj = _projection_for(pair)
        ax, ay = proj.to_xy(pair.a.lat, pair.a.lon)
        bx, by = proj.to_xy(pair.b.lat, pair.b.lon)
        d_ab = haversine_m(pair.a.lat, pair.a.lon, pair.b.lat, pair.b.lon)
        duration_s = (pair.b.t - pair.a.t).total_seconds()
        if duration_s <= 0:
            raise ValueError("anchor B must be strictly after anchor A in time")
        L = bounds.v_max_mps * duration_s
        feasible = d_ab - 1e-6 <= L
        # semi-major / semi-minor for the boundary ellipse (the prism's largest cross-section)
        if feasible:
            a_sm = 0.5 * L
            b_sm = 0.5 * math.sqrt(max(0.0, L * L - d_ab * d_ab))
        else:
            a_sm = 0.5 * d_ab
            b_sm = 0.0
        rotation = math.atan2(by - ay, bx - ax)
        return cls(
            pair=pair,
            v_max_mps=bounds.v_max_mps,
            duration_s=duration_s,
            feasible=feasible,
            base_ellipse=GeoEllipse(
                a_lat=pair.a.lat,
                a_lon=pair.a.lon,
                b_lat=pair.b.lat,
                b_lon=pair.b.lon,
                semi_major_m=a_sm,
                semi_minor_m=b_sm,
                rotation_rad=rotation,
            ),
            proj=proj,
        )

    def to_payload(self) -> dict[str, Any]:
        """Flat, JSON-stable serialization for the Temporal activity boundary.

        A Prism holds two live pydantic models (the anchor pair and the base
        ellipse) and a projection dataclass, none of which survive a generic
        dict dump in a form that round-trips through Temporal's data converter.
        This packs the prism into plain JSON so a PRISM activity can hand it to a
        downstream TGARD activity across the boundary; `from_payload` is the exact
        inverse. The round trip is lossless: every field is restored verbatim,
        nothing is recomputed, so the reconstructed prism is identical to the one
        the reasoner produced.
        """

        return {
            "pair": self.pair.model_dump(mode="json"),
            "v_max_mps": self.v_max_mps,
            "duration_s": self.duration_s,
            "feasible": self.feasible,
            "base_ellipse": self.base_ellipse.model_dump(mode="json"),
            "proj": {
                "lat_ref_rad": self.proj.lat_ref_rad,
                "lon_ref_rad": self.proj.lon_ref_rad,
            },
        }

    @classmethod
    def from_payload(cls, data: dict[str, Any]) -> Prism:
        """Rebuild a Prism from `to_payload` output (the inverse round trip)."""

        return cls(
            pair=AnchorPair.model_validate(data["pair"]),
            v_max_mps=data["v_max_mps"],
            duration_s=data["duration_s"],
            feasible=data["feasible"],
            base_ellipse=GeoEllipse.model_validate(data["base_ellipse"]),
            proj=_LocalProjection(
                lat_ref_rad=data["proj"]["lat_ref_rad"],
                lon_ref_rad=data["proj"]["lon_ref_rad"],
            ),
        )

    def ellipse_at(self, t: datetime) -> GeoEllipse:
        """Geo-ellipse that contains all (x, y) reachable by time t.

        For an interior time t in (t_A, t_B), the constraint is

            d(p, A) / v_max <= (t - t_A)
            d(p, B) / v_max <= (t_B - t)

        The intersection of the two disks is an algebraic lens that we
        approximate with the inscribed ellipse for fast geometric
        operations. For exact lens geometry use `lens_at`.
        """

        if not (self.pair.a.t <= t <= self.pair.b.t):
            raise ValueError("t is outside prism time interval")
        r_a = self.v_max_mps * (t - self.pair.a.t).total_seconds()
        r_b = self.v_max_mps * (self.pair.b.t - t).total_seconds()
        d_ab = haversine_m(self.pair.a.lat, self.pair.a.lon, self.pair.b.lat, self.pair.b.lon)
        # Inscribed ellipse: foci at A and B, sum-of-distances <= r_a + r_b
        L = max(r_a + r_b, d_ab)
        a_sm = 0.5 * L
        b_sm = 0.5 * math.sqrt(max(0.0, L * L - d_ab * d_ab))
        return GeoEllipse(
            a_lat=self.pair.a.lat,
            a_lon=self.pair.a.lon,
            b_lat=self.pair.b.lat,
            b_lon=self.pair.b.lon,
            semi_major_m=a_sm,
            semi_minor_m=b_sm,
            rotation_rad=self.base_ellipse.rotation_rad,
        )

    def mobr(self, ellipse: GeoEllipse | None = None) -> Polygonal:
        """Minimum Orthogonal Bounding Rectangle in normalized lon/lat.

        A rectangle crossing the antimeridian is returned as a MultiPolygon so
        GeoJSON consumers do not draw a world-spanning edge.
        """

        e = ellipse or self.base_ellipse
        proj = _projection_for_ellipse(e)
        # rectangle in local xy aligned with ellipse axes and centered on its foci
        cx, cy = _ellipse_center_xy(proj, e)
        _assert_poles_outside_footprint(proj, e, (cx, cy), rectangle=True)
        a, b, theta = e.semi_major_m, e.semi_minor_m, e.rotation_rad
        local = Polygon([(-a, -b), (a, -b), (a, b), (-a, b)])
        local = rotate(local, math.degrees(theta), origin=(0, 0))
        local = translate(local, cx, cy)
        if a == 0.0 or b == 0.0:
            degenerate_coords = [proj.to_lonlat(x, y) for x, y in local.exterior.coords]
            return _normalize_polygon_longitudes(
                Polygon(_unwrap_ring_longitudes(degenerate_coords))
            )
        projected_ring = _densify_closed_ring(
            local.exterior.coords,
            segments_per_edge=_MOBR_EDGE_SEGMENTS,
        )
        coords = [proj.to_lonlat(x, y) for x, y in projected_ring]
        mapped_rectangle = Polygon(_unwrap_ring_longitudes(coords))
        # Chords between inverse-projected rectangle samples can fall inside
        # the true curved boundary near a pole. The smallest conservative
        # correction for the public sampled geometry is its polygonal union
        # with the mapped local minimum rectangle.
        sampled_ellipse = _ellipse_polygon_unwrapped(proj, e, (cx, cy))
        conservative_rectangle = mapped_rectangle.union(sampled_ellipse)
        if not isinstance(conservative_rectangle, Polygon):
            raise ValueError("projected MOBR union is not a single polygon")
        return _normalize_polygon_longitudes(conservative_rectangle)

    def ellipse_polygon(self, ellipse: GeoEllipse | None = None, n: int = 64) -> Polygonal:
        """Approximate an ellipse as normalized Polygon or MultiPolygon geometry."""

        e = ellipse or self.base_ellipse
        proj = _projection_for_ellipse(e)
        cx, cy = _ellipse_center_xy(proj, e)
        _assert_poles_outside_footprint(proj, e, (cx, cy), rectangle=False)
        return _normalize_polygon_longitudes(_ellipse_polygon_unwrapped(proj, e, (cx, cy), n=n))

    @staticmethod
    def merge_dynamic(ellipses: Iterable[GeoEllipse], pair: AnchorPair) -> Polygonal:
        """Dynamic Region Merge: maximal-union of overlapping ellipses.

        Mirrors STAGD-DRM: indexes ellipses by R*-tree and unions
        connected components. We do the trivial all-pairs union here;
        callers needing scale should swap in the rtree.index lookup.
        """

        proj = _projection_for(pair)
        polys: list[Polygonal] = []
        for e in ellipses:
            p = Prism(
                pair=pair,
                v_max_mps=0.0,
                duration_s=0.0,
                feasible=True,
                base_ellipse=e,
                proj=proj,
            )
            polys.append(p.ellipse_polygon())
        if not polys:
            return Polygon()
        merged = polys[0]
        for poly in polys[1:]:
            merged = merged.union(poly)
        return _orient_polygonal(merged)


def intersect(a: Prism, b: Prism, *, n_slices: int = 16) -> Polygonal:
    """Time-slice intersection of two prisms.

    Returns the union of per-slice ellipse intersections within the
    overlapping time window. Only positive-area polygonal overlap is retained;
    boundary-only point or line contact returns an empty Polygon. Empty also
    means the time windows do not overlap.
    """

    t0 = max(a.pair.a.t, b.pair.a.t)
    t1 = min(a.pair.b.t, b.pair.b.t)
    if t0 >= t1:
        return Polygon()
    slices = np.linspace(0.0, 1.0, n_slices)
    accum = Polygon()
    for s in slices:
        t = t0 + (t1 - t0) * float(s)
        ea = a.ellipse_at(t)
        eb = b.ellipse_at(t)
        pa = a.ellipse_polygon(ea)
        pb = b.ellipse_polygon(eb)
        inter = pa.intersection(pb)
        if not inter.is_empty:
            accum = accum.union(inter)
    return _orient_polygonal(accum)

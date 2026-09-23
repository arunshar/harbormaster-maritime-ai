"""Geometry regressions for the Harbormaster-maintained prism kernel."""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime, timedelta

import pytest
from shapely.geometry import MultiPolygon, Point, Polygon

from app.components.space_time_prism import (
    EARTH_RADIUS_M,
    Prism,
    _shifted_polygon,
    haversine_m,
    intersect,
    speed_bounds_for,
)
from app.models import Anchor, AnchorPair, GeoEllipse, SpeedBounds

T0 = datetime(2024, 6, 1, tzinfo=UTC)
POLE_DOMAIN_ERROR = "prism footprint contains or touches a geographic pole"
FOCUS_DOMAIN_ERROR = "active foci are antipodal or numerically singular"


def _prism() -> Prism:
    pair = AnchorPair(
        a=Anchor(lat=0.0, lon=0.0, t=T0),
        b=Anchor(lat=0.0, lon=0.02, t=T0 + timedelta(minutes=10)),
    )
    return Prism.compute(pair, SpeedBounds(v_max_mps=10.0))


def _ellipse(center_lon: float, center_lat: float = 1.0) -> GeoEllipse:
    return GeoEllipse(
        a_lat=center_lat - 0.01,
        a_lon=center_lon - 0.01,
        b_lat=center_lat + 0.01,
        b_lon=center_lon + 0.01,
        semi_major_m=1_500.0,
        semi_minor_m=500.0,
        rotation_rad=0.0,
    )


def _antimeridian_prism(start_lon: float = 179.9, end_lon: float = -179.9) -> Prism:
    pair = AnchorPair(
        a=Anchor(lat=0.0, lon=start_lon, t=T0),
        b=Anchor(lat=0.0, lon=end_lon, t=T0 + timedelta(hours=2)),
    )
    return Prism.compute(pair, SpeedBounds(v_max_mps=20.0))


def _stationary_prism(lat: float, lon: float = 0.0) -> Prism:
    pair = AnchorPair(
        a=Anchor(lat=lat, lon=lon, t=T0),
        b=Anchor(lat=lat, lon=lon, t=T0 + timedelta(minutes=1)),
    )
    return Prism.compute(pair, SpeedBounds(v_max_mps=1.0))


def _thin_near_pole_ellipse(lat: float) -> GeoEllipse:
    return GeoEllipse(
        a_lat=lat,
        a_lon=0.0,
        b_lat=lat,
        b_lon=0.0,
        semi_major_m=10.0,
        semi_minor_m=0.1,
        rotation_rad=0.0,
    )


def _longitudes(geometry: Polygon | MultiPolygon) -> list[float]:
    parts = geometry.geoms if isinstance(geometry, MultiPolygon) else (geometry,)
    return [
        x for part in parts for ring in (part.exterior, *part.interiors) for x, _ in ring.coords
    ]


def _assert_rfc7946_winding(geometry: Polygon | MultiPolygon) -> None:
    parts = geometry.geoms if isinstance(geometry, MultiPolygon) else (geometry,)
    assert all(part.exterior.is_ccw for part in parts)
    assert all(not ring.is_ccw for part in parts for ring in part.interiors)


def _all_coordinates(geometry: Polygon | MultiPolygon) -> list[tuple[float, float]]:
    parts = geometry.geoms if isinstance(geometry, MultiPolygon) else (geometry,)
    return [coordinate for part in parts for coordinate in part.exterior.coords]


def _assert_mobr_bounds_ellipse(
    mobr: Polygon | MultiPolygon,
    ellipse: Polygon | MultiPolygon,
    *,
    center_lon: float,
    center_lat: float,
) -> None:
    assert mobr.covers(Point(center_lon, center_lat))
    assert ellipse.difference(mobr).area <= ellipse.area * 1e-8
    assert mobr.bounds[0] <= ellipse.bounds[0]
    assert mobr.bounds[1] <= ellipse.bounds[1]
    assert mobr.bounds[2] >= ellipse.bounds[2]
    assert mobr.bounds[3] >= ellipse.bounds[3]


def test_speed_bounds_cover_supported_domains_and_reject_unknown():
    vessel = speed_bounds_for("vessel", vessel_v_max_kts=25.0, vehicle_v_max_kmh=130.0)
    vehicle = speed_bounds_for("vehicle", vessel_v_max_kts=25.0, vehicle_v_max_kmh=130.0)
    pedestrian = speed_bounds_for("pedestrian", vessel_v_max_kts=25.0, vehicle_v_max_kmh=130.0)
    uav = speed_bounds_for("uav", vessel_v_max_kts=25.0, vehicle_v_max_kmh=130.0)

    assert vessel.v_max_mps == pytest.approx(12.8611)
    assert vehicle.v_max_mps == pytest.approx(130.0 / 3.6)
    assert pedestrian.v_max_mps == 2.0
    assert uav.v_max_mps == 30.0
    with pytest.raises(ValueError, match="unknown domain"):
        speed_bounds_for("rail", vessel_v_max_kts=25.0, vehicle_v_max_kmh=130.0)


@pytest.mark.parametrize("method", ["mobr", "ellipse_polygon"])
def test_supplied_ellipse_is_centered_on_its_own_foci(method):
    prism = _prism()
    ellipse = _ellipse(center_lon=2.0)

    polygon = getattr(prism, method)(ellipse)

    # Spherical midpoint and inverse-projection curvature shift the lon/lat
    # centroid by about 3e-8 degrees from the old arithmetic midpoint.
    assert polygon.centroid.x == pytest.approx(2.0, abs=5e-8)
    assert polygon.centroid.y == pytest.approx(1.0, abs=5e-8)


def test_dynamic_merge_preserves_each_ellipse_location():
    merged = Prism.merge_dynamic([_ellipse(1.0), _ellipse(3.0)], _prism().pair)

    assert merged.geom_type == "MultiPolygon"
    assert len(merged.geoms) == 2
    assert merged.bounds[0] > 0.9
    assert merged.bounds[2] > 2.9


def test_dynamic_merge_accepts_an_empty_input():
    merged = Prism.merge_dynamic([], _prism().pair)

    assert merged.is_empty


@pytest.mark.parametrize("method", ["mobr", "ellipse_polygon"])
def test_supplied_ellipse_keeps_meter_scale_at_different_latitude(method):
    ellipse = _ellipse(center_lon=2.0, center_lat=60.0)

    polygon = getattr(_prism(), method)(ellipse)
    pair = AnchorPair(
        a=Anchor(lat=ellipse.a_lat, lon=ellipse.a_lon, t=T0),
        b=Anchor(lat=ellipse.b_lat, lon=ellipse.b_lon, t=T0 + timedelta(minutes=10)),
    )
    proj = Prism.compute(pair, SpeedBounds(v_max_mps=10.0)).proj
    center_lon, center_lat = proj.to_lonlat(0.0, 0.0)
    east_lon, east_lat = proj.to_lonlat(ellipse.semi_major_m, 0.0)
    north_lon, north_lat = proj.to_lonlat(0.0, ellipse.semi_minor_m)

    assert polygon.is_valid
    assert haversine_m(center_lat, center_lon, east_lat, east_lon) == pytest.approx(
        ellipse.semi_major_m, abs=1e-6
    )
    assert haversine_m(center_lat, center_lon, north_lat, north_lon) == pytest.approx(
        ellipse.semi_minor_m, abs=1e-6
    )


@pytest.mark.parametrize("method", ["mobr", "ellipse_polygon"])
def test_midlatitude_geometry_stays_within_pre_fix_bounds(method: str):
    geometry = getattr(_prism(), method)(_ellipse(center_lon=2.0))
    old_bounds = (1.986508121033, 0.995503391970, 2.013491878967, 1.004496608030)

    assert geometry.bounds == pytest.approx(old_bounds, abs=5e-8)


@pytest.mark.parametrize("lat", [89.9999, -89.9999])
def test_projection_center_round_trip_near_pole(lat: float):
    proj = _stationary_prism(lat, lon=37.0).proj

    center_lon, center_lat = proj.to_lonlat(0.0, 0.0)
    x, y = proj.to_xy(center_lat, center_lon)

    assert center_lon == pytest.approx(37.0, abs=1e-12)
    assert center_lat == pytest.approx(lat, abs=1e-12)
    assert x == pytest.approx(0.0, abs=1e-8)
    assert y == pytest.approx(0.0, abs=1e-8)


@pytest.mark.parametrize("lat", [89.9999, -89.9999])
@pytest.mark.parametrize("x,y", [(10.0, 0.0), (0.0, 10.0), (6.0, -8.0)])
def test_projection_preserves_radial_meters_near_pole(lat: float, x: float, y: float):
    proj = _stationary_prism(lat).proj
    center_lon, center_lat = proj.to_lonlat(0.0, 0.0)

    lon, point_lat = proj.to_lonlat(x, y)
    round_trip_x, round_trip_y = proj.to_xy(point_lat, lon)

    assert haversine_m(center_lat, center_lon, point_lat, lon) == pytest.approx(
        math.hypot(x, y), abs=1e-6
    )
    assert round_trip_x == pytest.approx(x, abs=1e-6)
    assert round_trip_y == pytest.approx(y, abs=1e-6)


@pytest.mark.parametrize("lat", [89.9999, -89.9999])
@pytest.mark.parametrize("method", ["mobr", "ellipse_polygon"])
def test_near_pole_non_containing_footprints_are_finite(lat: float, method: str):
    ellipse = _thin_near_pole_ellipse(lat)
    geometry = getattr(_prism(), method)(ellipse)

    assert geometry.is_valid
    assert not geometry.is_empty
    assert all(math.isfinite(value) for point in _all_coordinates(geometry) for value in point)
    assert all(-180.0 <= lon <= 180.0 for lon, _ in _all_coordinates(geometry))
    if method == "mobr":
        _assert_mobr_bounds_ellipse(
            geometry,
            _prism().ellipse_polygon(ellipse),
            center_lon=0.0,
            center_lat=lat,
        )


@pytest.mark.parametrize("lat", [89.99999, -89.99999])
@pytest.mark.parametrize("method", ["mobr", "ellipse_polygon"])
def test_very_near_pole_non_containing_footprints_are_supported(lat: float, method: str):
    ellipse = _thin_near_pole_ellipse(lat)
    geometry = getattr(_prism(), method)(ellipse)

    assert geometry.is_valid
    assert not geometry.is_empty
    assert all(math.isfinite(value) for point in _all_coordinates(geometry) for value in point)
    if method == "mobr":
        _assert_mobr_bounds_ellipse(
            geometry,
            _prism().ellipse_polygon(ellipse),
            center_lon=0.0,
            center_lat=lat,
        )


@pytest.mark.parametrize("lat", [89.99999, -89.99999])
def test_very_near_pole_mobr_conservatively_contains_supported_rotations(lat: float):
    supported = 0
    rejected = 0
    for step in range(62):
        ellipse = _thin_near_pole_ellipse(lat).model_copy(update={"rotation_rad": 0.05 * step})
        try:
            rectangle = _prism().mobr(ellipse)
        except ValueError as exc:
            assert str(exc) == POLE_DOMAIN_ERROR
            rejected += 1
            continue

        sampled_ellipse = _prism().ellipse_polygon(ellipse)
        assert rectangle.is_valid
        assert sampled_ellipse.is_valid
        _assert_mobr_bounds_ellipse(
            rectangle,
            sampled_ellipse,
            center_lon=0.0,
            center_lat=lat,
        )
        supported += 1

    assert supported == 58
    assert rejected == 4


@pytest.mark.parametrize("lat", [89.99999, -89.99999])
def test_supported_rotated_very_near_pole_geometry_is_valid_and_bounded(lat: float):
    ellipse = _thin_near_pole_ellipse(lat).model_copy(update={"rotation_rad": 1.45})

    sampled_ellipse = _prism().ellipse_polygon(ellipse)
    rectangle = _prism().mobr(ellipse)

    assert sampled_ellipse.is_valid
    assert rectangle.is_valid
    _assert_mobr_bounds_ellipse(
        rectangle,
        sampled_ellipse,
        center_lon=0.0,
        center_lat=lat,
    )


@pytest.mark.parametrize(
    ("a_lat", "a_lon", "b_lat", "b_lon"),
    [
        (0.0, 0.0, 0.001, 0.0),
        (60.0, 0.0, 60.0, 0.01),
        (89.0, 0.0, 89.0, 0.01),
    ],
)
def test_infeasible_prism_mobr_remains_a_finite_degenerate_polygon(
    a_lat: float,
    a_lon: float,
    b_lat: float,
    b_lon: float,
):
    pair = AnchorPair(
        a=Anchor(lat=a_lat, lon=a_lon, t=T0),
        b=Anchor(lat=b_lat, lon=b_lon, t=T0 + timedelta(seconds=1)),
    )
    prism = Prism.compute(pair, SpeedBounds(v_max_mps=1.0))

    geometry = prism.mobr()

    assert not prism.feasible
    assert isinstance(geometry, Polygon)
    assert geometry.area == 0.0
    assert all(math.isfinite(value) for value in geometry.bounds)


def test_high_latitude_focus_distance_matches_haversine():
    pair = AnchorPair(
        a=Anchor(lat=89.9, lon=-45.0, t=T0),
        b=Anchor(lat=89.9, lon=45.0, t=T0 + timedelta(hours=1)),
    )
    prism = Prism.compute(pair, SpeedBounds(v_max_mps=100.0))
    ax, ay = prism.proj.to_xy(pair.a.lat, pair.a.lon)
    bx, by = prism.proj.to_xy(pair.b.lat, pair.b.lon)

    assert math.hypot(bx - ax, by - ay) == pytest.approx(
        haversine_m(pair.a.lat, pair.a.lon, pair.b.lat, pair.b.lon),
        rel=1e-12,
    )


def test_near_pole_payload_round_trip_preserves_projection_and_geometry():
    pair = AnchorPair(
        a=Anchor(lat=89.0, lon=-1.0, t=T0),
        b=Anchor(lat=89.0, lon=1.0, t=T0 + timedelta(minutes=1)),
    )
    prism = Prism.compute(pair, SpeedBounds(v_max_mps=100.0))
    payload = json.loads(json.dumps(prism.to_payload()))

    rebuilt = Prism.from_payload(payload)

    assert rebuilt.to_payload() == payload
    assert rebuilt.ellipse_polygon().equals_exact(prism.ellipse_polygon(), tolerance=1e-12)


@pytest.mark.parametrize("end_lon", [180.0, 179.99999])
def test_antipodal_or_numerically_singular_foci_are_rejected(end_lon: float):
    pair = AnchorPair(
        a=Anchor(lat=0.0, lon=0.0, t=T0),
        b=Anchor(lat=0.0, lon=end_lon, t=T0 + timedelta(hours=1)),
    )

    with pytest.raises(ValueError, match=f"^{FOCUS_DOMAIN_ERROR}$"):
        Prism.compute(pair, SpeedBounds(v_max_mps=100.0))


@pytest.mark.parametrize("method", ["mobr", "ellipse_polygon"])
def test_supplied_ellipse_with_antipodal_foci_is_rejected(method: str):
    ellipse = GeoEllipse(
        a_lat=0.0,
        a_lon=0.0,
        b_lat=0.0,
        b_lon=180.0,
        semi_major_m=20_000_000.0,
        semi_minor_m=1_000.0,
        rotation_rad=0.0,
    )

    with pytest.raises(ValueError, match=f"^{FOCUS_DOMAIN_ERROR}$"):
        getattr(_prism(), method)(ellipse)


@pytest.mark.parametrize("pole_lat", [89.0, -89.0])
@pytest.mark.parametrize("relation", ["contains", "touches"])
def test_ellipse_containing_or_touching_a_pole_is_rejected(pole_lat: float, relation: str):
    pole_distance_m = EARTH_RADIUS_M * math.radians(1.0)
    semi_major_m = pole_distance_m + 1.0 if relation == "contains" else pole_distance_m
    ellipse = GeoEllipse(
        a_lat=pole_lat,
        a_lon=0.0,
        b_lat=pole_lat,
        b_lon=0.0,
        semi_major_m=semi_major_m,
        semi_minor_m=100.0,
        rotation_rad=math.pi / 2,
    )

    with pytest.raises(ValueError, match=f"^{POLE_DOMAIN_ERROR}$"):
        _prism().ellipse_polygon(ellipse)


@pytest.mark.parametrize("pole_lat", [89.0, -89.0])
def test_ellipse_with_one_centimeter_pole_clearance_is_supported(pole_lat: float):
    pole_distance_m = EARTH_RADIUS_M * math.radians(1.0)
    ellipse = GeoEllipse(
        a_lat=pole_lat,
        a_lon=0.0,
        b_lat=pole_lat,
        b_lon=0.0,
        semi_major_m=pole_distance_m - 0.01,
        semi_minor_m=100.0,
        rotation_rad=math.pi / 2,
    )

    geometry = _prism().ellipse_polygon(ellipse)

    assert geometry.is_valid
    assert all(abs(lat) < 90.0 for _, lat in _all_coordinates(geometry))


@pytest.mark.parametrize("pole_lat", [89.0, -89.0])
@pytest.mark.parametrize("relation", ["contains", "touches"])
def test_mobr_containing_or_touching_a_pole_is_rejected(pole_lat: float, relation: str):
    pole_distance_m = EARTH_RADIUS_M * math.radians(1.0)
    extent = 0.75 * pole_distance_m if relation == "contains" else pole_distance_m / math.sqrt(2.0)
    ellipse = GeoEllipse(
        a_lat=pole_lat,
        a_lon=0.0,
        b_lat=pole_lat,
        b_lon=0.0,
        semi_major_m=extent,
        semi_minor_m=extent,
        rotation_rad=math.pi / 4,
    )

    assert _prism().ellipse_polygon(ellipse).is_valid
    with pytest.raises(ValueError, match=f"^{POLE_DOMAIN_ERROR}$"):
        _prism().mobr(ellipse)


@pytest.mark.parametrize(
    ("start_lon", "end_lon", "expected_direction"),
    [(179.9, -179.9, 1.0), (-179.9, 179.9, -1.0)],
)
def test_antimeridian_focus_distance_uses_shortest_direction(
    start_lon: float, end_lon: float, expected_direction: float
):
    prism = _antimeridian_prism(start_lon, end_lon)
    ax, ay = prism.proj.to_xy(prism.pair.a.lat, prism.pair.a.lon)
    bx, by = prism.proj.to_xy(prism.pair.b.lat, prism.pair.b.lon)

    assert prism.feasible
    assert math.hypot(bx - ax, by - ay) == pytest.approx(
        haversine_m(0.0, start_lon, 0.0, end_lon), rel=1e-12
    )
    assert math.cos(prism.base_ellipse.rotation_rad) == pytest.approx(expected_direction)
    assert math.sin(prism.base_ellipse.rotation_rad) == pytest.approx(0.0, abs=1e-12)


@pytest.mark.parametrize("method", ["mobr", "ellipse_polygon"])
def test_exact_positive_and_negative_antimeridian_are_the_same_meridian(method: str):
    prism = _antimeridian_prism(180.0, -180.0)
    ax, ay = prism.proj.to_xy(prism.pair.a.lat, prism.pair.a.lon)
    bx, by = prism.proj.to_xy(prism.pair.b.lat, prism.pair.b.lon)

    assert prism.feasible
    assert math.hypot(bx - ax, by - ay) == pytest.approx(0.0, abs=1e-9)
    geometry = getattr(prism, method)()
    assert isinstance(geometry, MultiPolygon)
    _assert_rfc7946_winding(geometry)


@pytest.mark.parametrize("method", ["mobr", "ellipse_polygon"])
@pytest.mark.parametrize("start_lon,end_lon", [(179.9, -179.9), (-179.9, 179.9)])
def test_antimeridian_geometry_is_a_valid_narrow_multipolygon(
    method: str, start_lon: float, end_lon: float
):
    geometry = getattr(_antimeridian_prism(start_lon, end_lon), method)()

    assert isinstance(geometry, MultiPolygon)
    assert geometry.is_valid
    assert len(geometry.geoms) == 2
    assert all(part.bounds[2] - part.bounds[0] < 3.0 for part in geometry.geoms)
    assert all(-180.0 <= longitude <= 180.0 for longitude in _longitudes(geometry))
    _assert_rfc7946_winding(geometry)


@pytest.mark.parametrize("method", ["mobr", "ellipse_polygon"])
@pytest.mark.parametrize("center_lon", [540.0, -540.0])
def test_noncanonical_antimeridian_seams_are_cut_and_normalized(method: str, center_lon: float):
    geometry = getattr(_prism(), method)(_ellipse(center_lon=center_lon))

    assert isinstance(geometry, MultiPolygon)
    assert geometry.is_valid
    assert len(geometry.geoms) == 2
    assert all(-180.0 <= longitude <= 180.0 for longitude in _longitudes(geometry))
    _assert_rfc7946_winding(geometry)


def test_seam_cleanup_preserves_distinct_submeter_coordinates():
    polygon = Polygon(
        [
            (179.9999999, 1.0),
            (180.0, 1.0),
            (180.0, 2.0),
            (179.0, 2.0),
        ]
    )

    shifted = _shifted_polygon(polygon, 0.0)

    assert len(shifted.exterior.coords) == len(polygon.exterior.coords)
    assert (179.9999999, 1.0) in shifted.exterior.coords


@pytest.mark.parametrize("method", ["mobr", "ellipse_polygon"])
def test_midlatitude_geometry_remains_a_polygon(method: str):
    geometry = getattr(_prism(), method)()

    assert isinstance(geometry, Polygon)
    assert geometry.is_valid


def test_antimeridian_intersection_and_dynamic_merge_preserve_cut_topology():
    prism = _antimeridian_prism()
    ellipse = prism.base_ellipse
    overlapping = GeoEllipse(
        a_lat=ellipse.a_lat + 0.01,
        a_lon=ellipse.a_lon,
        b_lat=ellipse.b_lat + 0.01,
        b_lon=ellipse.b_lon,
        semi_major_m=ellipse.semi_major_m,
        semi_minor_m=ellipse.semi_minor_m,
        rotation_rad=ellipse.rotation_rad,
    )

    intersection = intersect(prism, prism, n_slices=3)
    merged = Prism.merge_dynamic([ellipse, overlapping], prism.pair)

    for geometry in (intersection, merged):
        assert isinstance(geometry, MultiPolygon)
        assert geometry.is_valid
        assert not geometry.is_empty
        assert all(part.bounds[2] - part.bounds[0] < 3.0 for part in geometry.geoms)
        _assert_rfc7946_winding(geometry)


def test_boundary_only_prism_contact_is_an_empty_polygonal_region(monkeypatch):
    duration = timedelta(hours=2)
    bounds = SpeedBounds(v_max_mps=20.0)
    first_pair = AnchorPair(
        a=Anchor(lat=0.0, lon=0.0, t=T0),
        b=Anchor(lat=0.0, lon=0.0, t=T0 + duration),
    )
    first = Prism.compute(first_pair, bounds)
    second_pair = AnchorPair(
        a=Anchor(lat=0.0, lon=1.0, t=T0),
        b=Anchor(lat=0.0, lon=1.0, t=T0 + duration),
    )
    second = Prism.compute(second_pair, bounds)

    def touching_polygons(prism, ellipse=None, n=64):  # noqa: ARG001
        if prism is first:
            return Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)])
        return Polygon([(1.0, 1.0), (2.0, 1.0), (2.0, 2.0), (1.0, 2.0)])

    monkeypatch.setattr(Prism, "ellipse_polygon", touching_polygons)

    intersection = intersect(first, second, n_slices=1)

    assert isinstance(intersection, Polygon)
    assert intersection.is_empty

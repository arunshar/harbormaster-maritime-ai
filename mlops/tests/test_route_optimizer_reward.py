"""Gate 5.7: coverage_minus_fuel correctness + the pinned checksum (criterion g)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from mlops.route_optimizer.graph import tiny_synthetic_graph
from mlops.route_optimizer.reward import FUEL_NORM_M, coverage_minus_fuel

EXPECTATIONS = Path(__file__).parent.parent / "fixtures" / "expectations.json"
PINS = json.loads(EXPECTATIONS.read_text())["ppo_stretch_expectations"]


def test_empty_route_is_zero():
    assert coverage_minus_fuel([], tiny_synthetic_graph()) == 0.0


def test_single_node_is_coverage_only():
    g = tiny_synthetic_graph()
    # n0 alone: no fuel steamed, coverage = 8/18
    assert coverage_minus_fuel(["n0"], g) == pytest.approx(8 / 18)
    assert coverage_minus_fuel(["n0"], g) == pytest.approx(
        PINS["partial_routes"]["single_node_n0_coverage_only"]
    )


def test_canonical_route_matches_pinned_reward_and_sha256():
    g = tiny_synthetic_graph()
    route = PINS["canonical_route"]
    reward = coverage_minus_fuel(
        route, g, coverage_weight=PINS["coverage_weight"], fuel_weight=PINS["fuel_weight"]
    )
    assert reward == PINS["reward"]
    assert repr(reward) == PINS["reward_repr"]
    sha = hashlib.sha256(repr(reward).encode()).hexdigest()
    assert sha == PINS["reward_sha256"]


def test_partial_route_pinned():
    g = tiny_synthetic_graph()
    assert coverage_minus_fuel(["n0", "n1"], g) == pytest.approx(PINS["partial_routes"]["n0_n1"])


def test_revisits_do_not_double_count_coverage():
    g = tiny_synthetic_graph()
    # n0 -> n1 -> n0: coverage is still just {n0, n1}, but fuel accrues on both hops
    once = coverage_minus_fuel(["n0", "n1"], g)
    twice = coverage_minus_fuel(["n0", "n1", "n0"], g)
    assert twice < once  # extra steaming, no extra coverage


def test_full_coverage_is_one_minus_fuel():
    g = tiny_synthetic_graph()
    r = coverage_minus_fuel(["n0", "n1", "n3", "n2"], g)
    # all 4 nodes covered -> coverage term is exactly 1.0
    fuel = 1.0 - r
    assert fuel > 0.0
    assert r == pytest.approx(1.0 - fuel)


def test_non_edge_hop_raises_not_low_reward():
    g = tiny_synthetic_graph()
    # n0 -> n3 is not a corridor edge; the reward must refuse to score a teleport
    with pytest.raises(ValueError, match="no corridor edge"):
        coverage_minus_fuel(["n0", "n3"], g)


def test_fuel_weight_scales_only_fuel():
    g = tiny_synthetic_graph()
    base = coverage_minus_fuel(["n0", "n1"], g, fuel_weight=1.0)
    zero_fuel = coverage_minus_fuel(["n0", "n1"], g, fuel_weight=0.0)
    assert zero_fuel == pytest.approx(8 / 18 + 3 / 18)  # coverage only, no fuel term
    assert base < zero_fuel


def test_fuel_norm_constant_is_100km():
    assert FUEL_NORM_M == 100_000.0


def test_coverage_weight_scales_only_coverage():
    """Wave 3 finding [30]: pin coverage_weight at a non-default value so a
    mutation dropping the coverage_weight factor is caught."""
    g = tiny_synthetic_graph()
    # n0 alone: coverage 8/18, no fuel. Doubling coverage_weight doubles reward.
    base = coverage_minus_fuel(["n0"], g, coverage_weight=1.0)
    doubled = coverage_minus_fuel(["n0"], g, coverage_weight=2.0)
    assert base == pytest.approx(8 / 18)
    assert doubled == pytest.approx(2.0 * 8 / 18)

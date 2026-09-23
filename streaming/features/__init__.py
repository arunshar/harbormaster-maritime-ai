"""Harbormaster streaming feature library (Phase 1.5).

Pure feature functions for the Flink per-vessel feature job, kept as a plain
library so they unit-test without a Flink runtime. The Flink per-vessel keyed
process function calls these once per fix, against the previous fix (ADR 0001).
The same `p_physical` gate runs inline here and is the cheap pre-filter before
the serving call.
"""

from .features import (
    KNOTS_TO_MPS,
    VESSEL_V_MAX_KTS,
    VESSEL_V_MAX_MPS,
    Fix,
    WindowFeatures,
    gap_since_last_s,
    haversine_m,
    p_physical,
    v_required_mps,
    window_features,
)

__all__ = [
    "KNOTS_TO_MPS",
    "VESSEL_V_MAX_KTS",
    "VESSEL_V_MAX_MPS",
    "Fix",
    "WindowFeatures",
    "gap_since_last_s",
    "haversine_m",
    "p_physical",
    "v_required_mps",
    "window_features",
]

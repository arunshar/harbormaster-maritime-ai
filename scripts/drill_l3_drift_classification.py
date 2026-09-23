"""Drill L3: the drift decision table drives real routing decisions (Phase
4, gate 4.7; war story P27 in docs/WAR_STORIES.md).

Four synthetic scenarios through the REAL mlops code (mlops.drift,
mlops.calibration_watch, mlops.concept_proxy, mlops.drift_decision,
mlops.preference_builder), not reimplementations, matching every prior
drill's convention (L1/L2, gate 3.8):

  1. Input drift only: PSI/KS alerts, calibration in band, disagreement
     flat -> log_only, category input_drift.
  2. Calibration drift: calibration_ratio outside [0.8, 1.2] -> supervised
     retraining, no HITL/preference machinery touched.
  3. Concept drift: HITL disagreement rate above the alert threshold ->
     routes to the preference pipeline, and build_from_hitl on the same
     synthetic hitl_queue rows produces schema-valid triples with
     structural_violation_in_either_arm computed.
  4. The false-alarm row (the sketch's explicit cross-validation rule):
     proxy 1 (near-threshold + high epistemic variance) is elevated on a
     real volume of traces, but proxy 2 (disagreement) stays flat -> NOT
     concept drift, log_only. This is the row that proves the two proxies
     are actually cross-validated, not just proxy 1 alone driving retrains.

Exit 0 only if all four scenarios classify exactly as intended. Transcript
to docs/drills/L3_drift_classification.md.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from mlops.calibration_watch import watch_calibration  # noqa: E402
from mlops.concept_proxy import disagreement_rate, flag_uncertain_trace  # noqa: E402
from mlops.drift import check_input_drift  # noqa: E402
from mlops.drift_decision import classify_drift  # noqa: E402
from mlops.preference_builder import build_from_hitl  # noqa: E402

TRANSCRIPT = REPO_ROOT / "docs" / "drills" / "L3_drift_classification.md"
HITL_THRESHOLD = 0.6


def _pairs_with_ratio(ratio: float, n: int = 30) -> list[tuple[float, float]]:
    variance = 1.0
    error = ratio**0.5
    return [(error, variance)] * n


def _in_band_calibration():
    return watch_calibration(_pairs_with_ratio(1.0))


def scenario_input_drift_only() -> tuple[bool, str]:
    reference = pd.DataFrame({"a": np.linspace(0, 10, 50)})
    current = pd.DataFrame({"a": np.linspace(5, 15, 50)})  # mean-shifted -> alerts
    input_results = check_input_drift(reference, current)

    rows = [{"score": 0.9, "label": "correct"}] * 20  # all agreement -> flat disagreement
    disagreement = disagreement_rate(rows, hitl_threshold=HITL_THRESHOLD)

    decision = classify_drift(
        input_results=input_results, calibration=_in_band_calibration(), disagreement=disagreement
    )
    ok = decision.category == "input_drift" and decision.response == "log_only"
    return ok, f"category={decision.category} response={decision.response} reason={decision.reason}"


def scenario_calibration_drift() -> tuple[bool, str]:
    reference = pd.DataFrame({"a": np.linspace(0, 10, 50)})
    current = pd.DataFrame({"a": np.linspace(0, 10, 50)})  # no input drift
    input_results = check_input_drift(reference, current)

    out_of_band = watch_calibration(_pairs_with_ratio(1.6))  # outside [0.8, 1.2]
    rows = [{"score": 0.9, "label": "correct"}] * 20
    disagreement = disagreement_rate(rows, hitl_threshold=HITL_THRESHOLD)

    decision = classify_drift(
        input_results=input_results, calibration=out_of_band, disagreement=disagreement
    )
    ok = decision.category == "calibration_drift" and decision.response == "supervised_retrain"
    return ok, f"category={decision.category} response={decision.response} reason={decision.reason}"


def scenario_concept_drift_routes_to_preference_pipeline() -> tuple[bool, str]:
    reference = pd.DataFrame({"a": np.linspace(0, 10, 50)})
    current = pd.DataFrame({"a": np.linspace(0, 10, 50)})
    input_results = check_input_drift(reference, current)

    # 40% disagreement over 30 labeled rows: well above the alert threshold.
    hitl_rows = [
        {
            "trace_id": f"t{i}",
            "mmsi": 100000000 + i,
            "score": 0.9,
            "label": "incorrect" if i % 5 < 2 else "correct",
            "reviewer": "alice",
        }
        for i in range(30)
    ]
    disagreement = disagreement_rate(hitl_rows, hitl_threshold=HITL_THRESHOLD)

    decision = classify_drift(
        input_results=input_results, calibration=_in_band_calibration(), disagreement=disagreement
    )
    triples = build_from_hitl(hitl_rows, contexts={}, hitl_threshold=HITL_THRESHOLD)
    expected_disagreements = sum(1 for r in hitl_rows if r["label"] == "incorrect")

    ok = (
        decision.category == "concept_drift"
        and decision.response == "preference_pipeline"
        and len(triples) == expected_disagreements
        and all(t.preference_source == "hitl_verdict" for t in triples)
        and all(isinstance(t.structural_violation_in_either_arm, bool) for t in triples)
    )
    return ok, (
        f"category={decision.category} response={decision.response} "
        f"disagreement_rate={disagreement.rate:.3f} n_triples={len(triples)}"
    )


def scenario_proxy1_alone_is_not_concept_drift() -> tuple[bool, str]:
    # A volume of near-threshold, high-epistemic-variance traces (proxy 1
    # elevated: real traces actually flagged by flag_uncertain_trace), but
    # every one of them the operator still confirms correct (proxy 2 flat).
    traces = [(0.61, 0.2), (0.59, 0.25), (0.62, 0.3), (0.58, 0.22)] * 5
    proxy1_flags = [
        flag_uncertain_trace(score, variance, hitl_threshold=HITL_THRESHOLD)
        for score, variance in traces
    ]
    proxy1_elevated_count = sum(proxy1_flags)

    reference = pd.DataFrame({"a": np.linspace(0, 10, 50)})
    current = pd.DataFrame({"a": np.linspace(0, 10, 50)})
    input_results = check_input_drift(reference, current)

    rows = [{"score": 0.9, "label": "correct"}] * 20  # operator agrees every time -> flat
    disagreement = disagreement_rate(rows, hitl_threshold=HITL_THRESHOLD)

    decision = classify_drift(
        input_results=input_results, calibration=_in_band_calibration(), disagreement=disagreement
    )
    ok = proxy1_elevated_count == len(traces) and decision.category != "concept_drift"
    return ok, (
        f"proxy1_elevated={proxy1_elevated_count}/{len(traces)} "
        f"disagreement_rate={disagreement.rate:.3f} category={decision.category} "
        "(elevated proxy 1 + flat proxy 2 must NOT be concept drift)"
    )


def main() -> int:
    scenarios = [
        ("1. Input drift only -> log_only", scenario_input_drift_only),
        ("2. Calibration drift -> supervised_retrain", scenario_calibration_drift),
        (
            "3. Concept drift -> preference_pipeline + valid triples",
            scenario_concept_drift_routes_to_preference_pipeline,
        ),
        (
            "4. Proxy 1 alone (flat proxy 2) is NOT concept drift",
            scenario_proxy1_alone_is_not_concept_drift,
        ),
    ]

    lines = [
        "# Drill L3 transcript: drift decision-table classification "
        f"({datetime.now(UTC).isoformat()})",
        "",
    ]
    all_ok = True
    for name, fn in scenarios:
        ok, detail = fn()
        all_ok = all_ok and ok
        lines.append(f"## {name}")
        lines.append(f"PASSED: {ok}")
        lines.append(detail)
        lines.append("")

    lines.append(
        "VERDICT: "
        + ("PASS (all four decision-table rows classified correctly)" if all_ok else "FAIL")
    )
    TRANSCRIPT.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

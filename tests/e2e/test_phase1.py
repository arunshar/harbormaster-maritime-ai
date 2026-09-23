"""Phase 1 end-to-end acceptance test (gate G8, the phase gate).

Runs ONLY with HM_E2E set, against a live demo apply (enable_phase1=true, the
serving + ingestor images pushed, the ingestor run over the fixture). `make e2e`
brings the env up, sets SERVING_URL from terraform output, runs this, then tears
down. The core assert is that the documented known anomaly reaches the HITL queue
within the end-to-end SLO; the Iceberg/Athena reconcile runs when its env input is
present. The pure helpers this leans on are unit-tested in test_helpers.py.
"""

from __future__ import annotations

import json
import os
import time
import urllib.request

import pytest

from e2e.helpers import anomaly_in_pending, reconciles, within_slo

pytestmark = pytest.mark.skipif(
    not os.environ.get("HM_E2E"), reason="set HM_E2E=1 to run against a live demo apply"
)

KNOWN_ANOMALY_MMSI = 200000001  # abnormal_gap (expectations.json)
FIXTURE_RECORDS = 2709


def _get_json(url: str) -> list | dict:
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read())


def test_known_anomaly_reaches_hitl_within_slo():
    base = os.environ["SERVING_URL"].rstrip("/")
    budget_s = float(os.environ.get("HM_E2E_TIMEOUT_S", "60"))  # replay + warmup slack
    t0 = time.time()
    deadline = t0 + budget_s
    row = None
    while time.time() < deadline:
        row = anomaly_in_pending(_get_json(f"{base}/v1/hitl/pending"), KNOWN_ANOMALY_MMSI)
        if row:
            break
        time.sleep(1)
    assert row is not None, "known anomaly never reached the HITL queue within the budget"
    assert within_slo(time.time() - t0, budget_s)


def test_iceberg_count_reconciles_when_provided():
    # make e2e sets HM_E2E_ICEBERG_COUNT from an Athena count(*) after the replay.
    got = os.environ.get("HM_E2E_ICEBERG_COUNT")
    if got is None:
        pytest.skip("HM_E2E_ICEBERG_COUNT not set")
    gate_dropped = int(os.environ.get("HM_E2E_GATE_DROPPED", "0"))
    assert reconciles(FIXTURE_RECORDS, int(got), gate_dropped)

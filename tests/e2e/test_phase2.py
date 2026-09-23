"""Phase 2 end-to-end acceptance (gate C9, the phase gate).

Runs ONLY with HM_CDC_E2E set, against a running CDC stack:
  local plane: `make cdc-up`, then `make cdc-smoke` ONCE (it registers the
               connector but stops its own consumer on exit), then keep
               `make cdc-consumer` running in one terminal and the serving API
               in another via `make serve-run-cdc` (the Phase 2 env), then
               `make cdc-e2e`
  AWS showcase: the same tests with env pointed at the demo apply (operator-run)

Env contract:
  SERVING_URL             the scoring API (with the registry routes)
  HM_ONLINE_TABLE         the online DynamoDB table
  HM_DDB_ENDPOINT_URL     DynamoDB Local endpoint (empty on real AWS)
  HM_KAFKA_BOOTSTRAP      for the replay test's fresh consumer group
  HM_CDC_PG_DSN           Postgres, for the slot-lag criterion
  HM_CDC_RESTART_CMD      command that restarts Debezium Connect and waits
                          (local: kubectl -n hm-cdc rollout restart ... )
  HM_CDC_FLAG_TARGET_S    flag-to-scored budget (default 5)

The five acceptance criteria:
  (a) flag -> scored watchlisted within ~5 s
  (b) topic replay -> no duplicate online state
  (c) Debezium restart -> no lost change
  (d) delete -> removed from the online watchlist
  (e) pg_replication_slots lag alerting fires on a stalled slot
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
import urllib.request
import uuid

import pytest

from e2e.cdc_helpers import (
    has_reason,
    item_is_online,
    missing_online,
    online_state_hash,
    score_payload,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("HM_CDC_E2E"), reason="set HM_CDC_E2E=1 to run against a live CDC stack"
)

BASE = os.environ.get("SERVING_URL", "http://localhost:8000").rstrip("/")
FLAG_TARGET_S = float(os.environ.get("HM_CDC_FLAG_TARGET_S", "5"))
BUDGET_S = float(os.environ.get("HM_CDC_E2E_TIMEOUT_S", "60"))
TENANT_ID = os.environ.get("HM_TENANT_ID") or "00000000-0000-0000-0000-000000000000"


def _req(method: str, path: str, body: dict | None = None) -> dict | list:
    req = urllib.request.Request(
        f"{BASE}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def _ddb():
    import boto3

    kwargs = {"region_name": os.environ.get("AWS_REGION", "us-east-1")}
    if os.environ.get("HM_DDB_ENDPOINT_URL"):
        kwargs["endpoint_url"] = os.environ["HM_DDB_ENDPOINT_URL"]
        kwargs["aws_access_key_id"] = "local"
        kwargs["aws_secret_access_key"] = "local"
    return boto3.client("dynamodb", **kwargs)


def _online_item(mmsi: int, feature: str = "watchlist") -> dict | None:
    resp = _ddb().get_item(
        TableName=os.environ["HM_ONLINE_TABLE"],
        Key={"entity_id": {"S": f"{TENANT_ID}:{mmsi}"}, "feature_name": {"S": feature}},
        ConsistentRead=True,
    )
    return resp.get("Item")


def _scan_online() -> list[dict]:
    client = _ddb()
    items: list[dict] = []
    kwargs = {"TableName": os.environ["HM_ONLINE_TABLE"], "ConsistentRead": True}
    while True:
        page = client.scan(**kwargs)
        items.extend(page.get("Items", []))
        if "LastEvaluatedKey" not in page:
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def _fresh_mmsi() -> int:
    return 200_850_000 + (uuid.uuid4().int % 40_000)


def _poll(predicate, deadline_s: float, interval_s: float = 0.2):
    t0 = time.time()
    while time.time() - t0 < deadline_s:
        value = predicate()
        if value:
            return value, time.time() - t0
        time.sleep(interval_s)
    return None, time.time() - t0


# ---------------------------------------------------------------- (a) flag


def test_a_flag_to_scored_watchlisted_within_target():
    mmsi = _fresh_mmsi()
    _req("PUT", f"/v1/registry/watchlist/{mmsi}", {"reason": "e2e flag", "added_by": "e2e"})

    def scored():
        out = _req("POST", "/v1/score-ais", score_payload(mmsi))
        return out if has_reason(out, "watchlist_hit") else None

    out, elapsed = _poll(scored, deadline_s=max(FLAG_TARGET_S * 4, 20))
    assert out is not None, "flagged vessel never scored watchlisted"
    assert out["hitl_required"] is True
    assert elapsed <= FLAG_TARGET_S, f"flag-to-scored took {elapsed:.2f}s > {FLAG_TARGET_S}s"


# -------------------------------------------------------------- (b) replay


def test_b_full_topic_replay_produces_no_duplicate_online_state():
    from cdc.consumer.applier import Applier
    from cdc.consumer.service import ConsumerConfig, ConsumerLoop, build_applier, build_consumer

    before = online_state_hash(_scan_online())

    # a FRESH consumer group re-consumes every topic from earliest through the
    # same guarded sinks: the honest full-replay test
    env = dict(os.environ)
    env["HM_KAFKA_GROUP"] = f"hm-cdc-e2e-replay-{uuid.uuid4().hex[:8]}"
    cfg = ConsumerConfig.from_env(env)
    loop = ConsumerLoop(consumer=build_consumer(cfg), applier=build_applier(cfg))
    assert isinstance(loop._applier, Applier)

    idle_polls = 0
    consumed_events = 0
    deadline = time.time() + BUDGET_S
    while idle_polls < 5 and time.time() < deadline:
        result = loop.run_once()
        idle_polls = idle_polls + 1 if result is None else 0
        consumed_events += result.events if result else 0

    # guard against a vacuous pass: an empty replay proves nothing
    assert consumed_events > 0, "fresh consumer group consumed nothing; replay never ran"
    after = online_state_hash(_scan_online())
    assert after == before, "replaying the CDC topics changed the online state"


# ------------------------------------------------------------- (c) restart


def test_c_debezium_restart_loses_no_change():
    restart_cmd = os.environ.get("HM_CDC_RESTART_CMD")
    if not restart_cmd:
        pytest.skip("HM_CDC_RESTART_CMD not set")

    written: list[int] = []

    def writer():
        for _ in range(20):
            mmsi = _fresh_mmsi()
            _req("PUT", f"/v1/registry/watchlist/{mmsi}", {"reason": "e2e restart"})
            written.append(mmsi)
            time.sleep(0.25)

    t = threading.Thread(target=writer)
    t.start()
    time.sleep(1.0)  # a few writes land pre-restart, the rest ride through it
    subprocess.run(restart_cmd, shell=True, check=True, timeout=300)
    t.join()

    def all_online():
        return not missing_online(written, _scan_online(), TENANT_ID)

    ok, elapsed = _poll(all_online, deadline_s=BUDGET_S, interval_s=1.0)
    assert ok, (
        f"changes lost across the Debezium restart after {elapsed:.0f}s: "
        f"{missing_online(written, _scan_online(), TENANT_ID)}"
    )


# -------------------------------------------------------------- (d) delete


def test_d_delete_removes_vessel_from_online_watchlist():
    mmsi = _fresh_mmsi()
    _req("PUT", f"/v1/registry/watchlist/{mmsi}", {"reason": "e2e delete"})
    item, _ = _poll(lambda: _online_item(mmsi), deadline_s=20)
    assert item_is_online(item), "flag never reached the online store"

    _req("DELETE", f"/v1/registry/watchlist/{mmsi}")

    def offline():
        return not item_is_online(_online_item(mmsi))

    gone, _ = _poll(offline, deadline_s=20)
    assert gone, "delete never removed the vessel from the online watchlist"

    def unflagged():
        out = _req("POST", "/v1/score-ais", score_payload(mmsi))
        return None if has_reason(out, "watchlist_hit") else out

    clean, _ = _poll(unflagged, deadline_s=20)
    assert clean is not None, "scorer still emits watchlist_hit after the delete"


# ----------------------------------------------------------- (e) lag alert


async def _run_stall_drill(dsn: str) -> None:
    import asyncpg

    from cdc.monitor.slot_lag import evaluate_lag_alert, fetch_slot_lag

    slot = "hm_e2e_stall"
    conn = await asyncpg.connect(dsn=dsn)
    try:
        await conn.execute("SELECT set_config('app.tenant_id', $1, false)", TENANT_ID)
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1", slot
        )
        if not exists:
            await conn.execute("SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot)
        for i in range(200):  # writes the stalled slot must retain
            await conn.execute(
                """
                INSERT INTO watchlist (mmsi, reason, added_by)
                VALUES ($1, 'e2e lag', 'e2e')
                ON CONFLICT (tenant_id, mmsi) DO UPDATE SET updated_at = now()
                """,
                200_890_000 + i,
            )
        await conn.execute("SELECT pg_switch_wal()")
        slots = await fetch_slot_lag(conn)
        stalled = next(s for s in slots if s.slot_name == slot)
        assert stalled.lag_bytes > 0, "stalled slot shows no lag"
        breaching = evaluate_lag_alert(slots, threshold_bytes=1024)
        assert any(s.slot_name == slot for s in breaching), "lag alert did not fire"
        for i in range(200):  # cleanup rows
            await conn.execute(
                "DELETE FROM watchlist WHERE tenant_id = $1::uuid AND mmsi = $2",
                TENANT_ID,
                200_890_000 + i,
            )
    finally:
        await conn.execute("SELECT pg_drop_replication_slot($1)", slot)
        await conn.close()


def test_e_slot_lag_alert_fires_for_a_stalled_consumer():
    import asyncio

    dsn = os.environ.get("HM_CDC_PG_DSN")
    if not dsn:
        pytest.skip("HM_CDC_PG_DSN not set")
    asyncio.run(_run_stall_drill(dsn))

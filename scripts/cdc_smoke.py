"""Local CDC smoke (Phase 2, gate C6): insert-to-online latency on the kind stack.

Run `make cdc-up` first, then `make cdc-smoke`. Needs the [postgres] and [cdc]
extras in .venv (pip install -e ".[dev,postgres,cdc]").

Steps:
  1. wait for Postgres / DynamoDB Local / Connect REST on their NodePorts
  2. create the feast_online-shaped table on DynamoDB Local (idempotent)
  3. apply the registry DDL (tables + REPLICA IDENTITY + publication)
  4. register the Debezium connector via Connect REST (PUT config, idempotent;
     the body comes from cdc.connector.config, so it is generated + validated)
  5. start the real consumer loop in a background thread
  6. INSERT a watchlist row in Postgres and poll DynamoDB until it is online
  7. print the insert-to-online latency (the ~5 s acceptance target)

Exit 0 on pass, 1 on failure/timeout.
"""

from __future__ import annotations

import asyncio
import os
import sys
import threading
import time
import urllib.request

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from cdc.connector.config import build_connector_config  # noqa: E402
from cdc.connector.registration import register_and_wait  # noqa: E402
from cdc.consumer.service import (  # noqa: E402
    ConsumerConfig,
    ConsumerLoop,
    build_applier,
    build_consumer,
)
from cdc.schema import ddl  # noqa: E402

# Local-plane defaults (kind NodePorts); override via env for other stacks.
DEFAULTS = {
    "HM_KAFKA_BOOTSTRAP": "127.0.0.1:30092",
    "HM_ONLINE_TABLE": "hm-local-feast-online",
    "HM_DDB_ENDPOINT_URL": "http://127.0.0.1:30800",
    "HM_REDIS_URL": "redis://127.0.0.1:30379/0",
    "HM_ICEBERG_CATALOG_URI": "sqlite:///.cdc-warehouse/catalog.db",
    "HM_ICEBERG_WAREHOUSE": f"file://{REPO_ROOT}/.cdc-warehouse",
    "HM_POLL_TIMEOUT_S": "0.5",
}
PG_DSN = os.environ.get(
    "HM_SMOKE_PG_DSN", "postgresql://hm_admin:hm_local_pw@127.0.0.1:30432/harbormaster"
)
CONNECT_URL = os.environ.get("HM_SMOKE_CONNECT_URL", "http://127.0.0.1:30083")
PG_IN_CLUSTER_HOST = os.environ.get("HM_SMOKE_PG_CLUSTER_HOST", "postgres.hm-cdc.svc")
SMOKE_MMSI = int(os.environ.get("HM_SMOKE_MMSI", str(int(time.time()) % 100_000 + 200_800_000)))
TIMEOUT_S = float(os.environ.get("HM_SMOKE_TIMEOUT_S", "60"))
TARGET_S = 5.0


def _env() -> dict[str, str]:
    e = dict(DEFAULTS)
    e.update({k: v for k, v in os.environ.items() if k.startswith("HM_")})
    return e


def wait_for(name: str, probe, timeout_s: float = 120.0) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        try:
            probe()
            print(f"  [ok] {name}")
            return
        except Exception:
            time.sleep(2.0)
    raise TimeoutError(f"{name} not ready after {timeout_s:.0f}s")


def ensure_online_table(cfg: ConsumerConfig) -> None:
    import boto3

    client = boto3.client(
        "dynamodb",
        endpoint_url=cfg.ddb_endpoint_url,
        region_name=cfg.aws_region,
        aws_access_key_id="local",
        aws_secret_access_key="local",
    )
    existing = client.list_tables()["TableNames"]
    if cfg.online_table not in existing:
        client.create_table(
            TableName=cfg.online_table,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": "entity_id", "AttributeType": "S"},
                {"AttributeName": "feature_name", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "entity_id", "KeyType": "HASH"},
                {"AttributeName": "feature_name", "KeyType": "RANGE"},
            ],
        )
        print(f"  [ok] created DynamoDB Local table {cfg.online_table}")


async def apply_ddl_and_insert(insert: bool) -> None:
    import asyncpg

    conn = await asyncpg.connect(dsn=PG_DSN)
    try:
        for stmt in ddl.statements():
            await conn.execute(stmt)
        if insert:
            await conn.execute(
                """
                INSERT INTO watchlist (mmsi, reason, severity, added_by)
                VALUES ($1, 'smoke: dark rendezvous', 0.9, 'cdc_smoke')
                ON CONFLICT (tenant_id, mmsi) DO UPDATE SET updated_at = now()
                """,
                SMOKE_MMSI,
            )
    finally:
        await conn.close()


def register_connector() -> None:
    body = build_connector_config(
        db_host=PG_IN_CLUSTER_HOST,
        db_port=5432,
    )
    result = register_and_wait(
        CONNECT_URL,
        body,
        timeout_s=TIMEOUT_S,
    )
    tasks = ",".join(result.task_states)
    print(
        f"  [ok] connector registered (HTTP {result.http_status}; "
        f"connector={result.connector_state}; tasks={tasks})"
    )


def poll_online(cfg: ConsumerConfig, deadline_s: float) -> float:
    import boto3

    client = boto3.client(
        "dynamodb",
        endpoint_url=cfg.ddb_endpoint_url,
        region_name=cfg.aws_region,
        aws_access_key_id="local",
        aws_secret_access_key="local",
    )
    t0 = time.time()
    while time.time() - t0 < deadline_s:
        resp = client.get_item(
            TableName=cfg.online_table,
            Key={
                "entity_id": {"S": f"{ddl.DEFAULT_TENANT_ID}:{SMOKE_MMSI}"},
                "feature_name": {"S": "watchlist"},
            },
            ConsistentRead=True,
        )
        item = resp.get("Item")
        if item and not item.get("deleted", {}).get("BOOL", False):
            return time.time() - t0
        time.sleep(0.1)
    raise TimeoutError(f"watchlist row for {SMOKE_MMSI} never reached the online store")


def main() -> int:
    env = _env()
    os.environ.update(env)
    cfg = ConsumerConfig.from_env()
    os.makedirs(".cdc-warehouse", exist_ok=True)

    print("waiting for the local stack ...")
    wait_for("postgres", lambda: asyncio.run(apply_ddl_and_insert(insert=False)))
    wait_for("dynamodb-local", lambda: ensure_online_table(cfg))
    wait_for(
        "connect REST",
        lambda: urllib.request.urlopen(f"{CONNECT_URL}/connectors", timeout=5).read(),
    )
    register_connector()

    print("starting the consumer loop ...")
    loop = ConsumerLoop(
        consumer=build_consumer(cfg),
        applier=build_applier(cfg),
        topic_prefix=cfg.topic_prefix,
        poll_timeout_s=cfg.poll_timeout_s,
        batch_max=cfg.batch_max,
    )
    stop = threading.Event()
    t = threading.Thread(target=loop.run_forever, args=(stop,), daemon=True)
    t.start()
    # let the group join + the snapshot drain before timing the live change
    time.sleep(10.0)

    print(f"inserting watchlist row for mmsi={SMOKE_MMSI} and timing ...")
    asyncio.run(apply_ddl_and_insert(insert=True))
    try:
        latency = poll_online(cfg, TIMEOUT_S)
    finally:
        stop.set()
        t.join(timeout=15.0)

    verdict = "PASS" if latency <= TARGET_S else "SLOW"
    print(f"[{verdict}] insert-to-online latency: {latency:.2f}s (target <= {TARGET_S:.0f}s)")
    return 0 if latency <= TARGET_S else 1


if __name__ == "__main__":
    sys.exit(main())

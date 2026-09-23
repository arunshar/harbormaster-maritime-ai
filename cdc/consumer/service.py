"""The CDC consumer service (Phase 2, gate C6).

Kafka (Debezium topics) -> envelope parse -> LSN-guarded Applier -> sinks,
with manual offset commit strictly after every sink acks (the applier's
commit protocol). At-least-once transport + idempotent sink; docs/phases/
PHASE_2.md invariants 1-5.

Structure mirrors streaming/ingestor: env-driven config, lazy heavy imports
(confluent-kafka / boto3 / redis / pyiceberg live behind the [cdc] extra), and
a ConsumerLoop whose Kafka consumer is injected so the unit suite runs with
zero Kafka, zero AWS, zero Docker.

Malformed data messages (EnvelopeError) are counted and skipped, never block
the partition: for the registry pipeline a malformed envelope is a bug to
alarm on (CDC_PARSE_ERRORS_TOTAL), not a reason to stall every consumer behind
it. Offsets still commit for the batch, which is the documented trade-off.
"""

from __future__ import annotations

import os
import signal
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

import structlog
from prometheus_client import Counter, Histogram

from cdc.connector.config import table_topics
from cdc.consumer.applier import Applier, BatchResult
from cdc.consumer.envelope import EnvelopeError, ParsedMessage, parse_envelope

log = structlog.get_logger(__name__)

CDC_EVENTS_TOTAL = Counter("hm_cdc_events_total", "Data change events consumed, by op", ["op"])
CDC_APPLIED_TOTAL = Counter("hm_cdc_applied_total", "Changes applied to the online store")
CDC_GUARD_REJECTED_TOTAL = Counter(
    "hm_cdc_guard_rejected_total", "Stale/duplicate deliveries rejected by the LSN guard"
)
CDC_PARSE_ERRORS_TOTAL = Counter(
    "hm_cdc_parse_errors_total", "Messages that failed envelope parsing (skipped, alarm-worthy)"
)
CDC_CONTENT_ERRORS_TOTAL = Counter(
    "hm_cdc_content_errors_total",
    "Events no sink mapping can apply (audited applied=false, skipped, alarm-worthy)",
)
CDC_BATCH_LATENCY = Histogram(
    "hm_cdc_batch_apply_seconds",
    "Batch apply latency (parse -> sinks -> commit)",
    buckets=(0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0),
)


@dataclass(frozen=True)
class ConsumerConfig:
    """12-factor config, HM_ prefix, mirroring serving/app/config.py."""

    kafka_bootstrap: str
    kafka_group: str = "hm-cdc-consumer"
    msk_iam: bool = False
    topic_prefix: str = "hm"
    online_table: str = ""
    ddb_endpoint_url: str = ""
    redis_url: str = ""
    iceberg_catalog_uri: str = ""
    iceberg_warehouse: str = ""
    iceberg_glue: bool = False
    poll_timeout_s: float = 1.0
    batch_max: int = 200
    metrics_port: int = 0  # 0 = no metrics HTTP server
    aws_region: str = "us-east-1"
    extra: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> ConsumerConfig:
        e = os.environ if env is None else env

        def truthy(name: str) -> bool:
            return e.get(name, "").strip().lower() in ("1", "true", "yes")

        bootstrap = e.get("HM_KAFKA_BOOTSTRAP", "")
        if not bootstrap:
            raise ValueError("HM_KAFKA_BOOTSTRAP is required")
        return cls(
            kafka_bootstrap=bootstrap,
            kafka_group=e.get("HM_KAFKA_GROUP", "hm-cdc-consumer"),
            msk_iam=truthy("HM_KAFKA_MSK_IAM"),
            topic_prefix=e.get("HM_TOPIC_PREFIX", "hm"),
            online_table=e.get("HM_ONLINE_TABLE", ""),
            ddb_endpoint_url=e.get("HM_DDB_ENDPOINT_URL", ""),
            redis_url=e.get("HM_REDIS_URL", ""),
            iceberg_catalog_uri=e.get("HM_ICEBERG_CATALOG_URI", ""),
            iceberg_warehouse=e.get("HM_ICEBERG_WAREHOUSE", ""),
            iceberg_glue=truthy("HM_ICEBERG_GLUE"),
            poll_timeout_s=float(e.get("HM_POLL_TIMEOUT_S", "1.0")),
            batch_max=int(e.get("HM_BATCH_MAX", "200")),
            metrics_port=int(e.get("HM_METRICS_PORT", "0")),
            aws_region=e.get("AWS_REGION", e.get("AWS_DEFAULT_REGION", "us-east-1")),
        )


def build_kafka_config(cfg: ConsumerConfig) -> dict[str, Any]:
    """The confluent-kafka (librdkafka) config dict. Pure; unit-tested.

    enable.auto.commit=false is THE setting: offsets move only when the
    applier's commit callback fires after every sink acks.
    """
    kafka: dict[str, Any] = {
        "bootstrap.servers": cfg.kafka_bootstrap,
        "group.id": cfg.kafka_group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
    }
    if cfg.msk_iam:
        kafka.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "OAUTHBEARER",
            }
        )
    return kafka


def _msk_oauth_cb(region: str):  # pragma: no cover - exercised on the AWS showcase only
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    def oauth_cb(_config: str):
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
        return token, expiry_ms / 1000.0

    return oauth_cb


def build_consumer(cfg: ConsumerConfig) -> Any:  # pragma: no cover - needs librdkafka
    """The real confluent-kafka Consumer (lazy import; [cdc] extra)."""
    from confluent_kafka import Consumer

    kafka = build_kafka_config(cfg)
    if cfg.msk_iam:
        kafka["oauth_cb"] = _msk_oauth_cb(cfg.aws_region)
    return Consumer(kafka)


def build_applier(cfg: ConsumerConfig) -> Applier:  # pragma: no cover - needs [cdc] deps
    """Wire the real sinks from config (boto3 / redis / pyiceberg, all lazy)."""
    from cdc.sinks.dynamo import OnlineStoreSink
    from cdc.sinks.iceberg_audit import CdcAuditSink, build_iceberg_writer
    from cdc.sinks.redis_cache import RedisInvalidationSink

    if not cfg.online_table:
        raise ValueError("HM_ONLINE_TABLE is required (the DynamoDB online store)")

    # Drill-only escape hatch (war story P2): HM_DRILL_NO_GUARD=1 strips the
    # LSN condition so redelivery visibly double-applies. Refused without
    # HM_DRILL=1 so it can never be reached by ordinary misconfiguration.
    no_guard = os.environ.get("HM_DRILL_NO_GUARD", "").strip().lower() in ("1", "true")
    if no_guard and os.environ.get("HM_DRILL", "").strip() != "1":
        raise ValueError("HM_DRILL_NO_GUARD requires HM_DRILL=1 (drill-only flag)")

    import boto3

    ddb_kwargs: dict[str, Any] = {"region_name": cfg.aws_region}
    if cfg.ddb_endpoint_url:
        ddb_kwargs["endpoint_url"] = cfg.ddb_endpoint_url
    store = OnlineStoreSink(
        client=boto3.client("dynamodb", **ddb_kwargs),
        table_name=cfg.online_table,
        guard=not no_guard,
    )

    effects = []
    if cfg.redis_url:
        import redis

        effects.append(RedisInvalidationSink(client=redis.Redis.from_url(cfg.redis_url)))

    audit = None
    if cfg.iceberg_glue or cfg.iceberg_catalog_uri:
        props: dict[str, str] = (
            {"type": "glue", "warehouse": cfg.iceberg_warehouse}
            if cfg.iceberg_glue
            else {
                "type": "sql",
                "uri": cfg.iceberg_catalog_uri,
                "warehouse": cfg.iceberg_warehouse,
            }
        )
        audit = CdcAuditSink(writer=build_iceberg_writer(catalog_props=props))

    return Applier(store=store, effects=tuple(effects), audit=audit)


class KafkaMessage(Protocol):
    def topic(self) -> str: ...
    def key(self) -> bytes | None: ...
    def value(self) -> bytes | None: ...
    def error(self) -> Any: ...


class KafkaConsumer(Protocol):
    def subscribe(self, topics: list[str]) -> None: ...
    def poll(self, timeout: float) -> KafkaMessage | None: ...
    def commit(self, asynchronous: bool = ...) -> Any: ...
    def close(self) -> None: ...


class ConsumerLoop:
    """Poll -> parse -> apply -> commit, batch by batch. Consumer injected."""

    def __init__(
        self,
        *,
        consumer: KafkaConsumer,
        applier: Applier,
        topics: list[str] | None = None,
        topic_prefix: str = "hm",
        poll_timeout_s: float = 1.0,
        batch_max: int = 200,
    ) -> None:
        self._consumer = consumer
        self._applier = applier
        self._topics = topics if topics is not None else list(table_topics(topic_prefix))
        self._poll_timeout_s = poll_timeout_s
        self._batch_max = batch_max
        self._subscribed = False

    def _ensure_subscribed(self) -> None:
        if not self._subscribed:
            self._consumer.subscribe(self._topics)
            self._subscribed = True

    def _drain(self) -> list[ParsedMessage]:
        """Poll up to batch_max messages; stop early on an empty poll."""
        parsed: list[ParsedMessage] = []
        while len(parsed) < self._batch_max:
            msg = self._consumer.poll(self._poll_timeout_s if not parsed else 0.0)
            if msg is None:
                break
            if msg.error():
                log.warning("cdc_kafka_message_error", error=str(msg.error()))
                continue
            try:
                parsed.append(parse_envelope(msg.topic(), msg.key(), msg.value()))
            except EnvelopeError as exc:
                CDC_PARSE_ERRORS_TOTAL.inc()
                log.error("cdc_envelope_parse_error", topic=msg.topic(), err=str(exc))
        return parsed

    def run_once(self) -> BatchResult | None:
        """One batch: returns None when the poll came back empty."""
        self._ensure_subscribed()
        batch = self._drain()
        if not batch:
            return None
        t0 = time.perf_counter()
        result = self._applier.apply_batch(
            batch, commit=lambda: self._consumer.commit(asynchronous=False)
        )
        CDC_BATCH_LATENCY.observe(time.perf_counter() - t0)
        for msg in batch:
            op = getattr(msg, "op", None)
            if op:
                CDC_EVENTS_TOTAL.labels(op=op).inc()
        CDC_APPLIED_TOTAL.inc(result.applied)
        CDC_GUARD_REJECTED_TOTAL.inc(result.guard_rejected)
        if result.content_errors:
            CDC_CONTENT_ERRORS_TOTAL.inc(result.content_errors)
        log.info(
            "cdc_batch_applied",
            events=result.events,
            applied=result.applied,
            guard_rejected=result.guard_rejected,
            tombstones=result.tombstones,
            content_errors=result.content_errors,
        )
        return result

    def run_forever(self, stop: threading.Event) -> None:
        """Batches until stop is set; the in-flight batch always completes and
        commits before exit (graceful SIGTERM drain)."""
        self._ensure_subscribed()
        while not stop.is_set():
            self.run_once()
        self._consumer.close()


def main() -> None:  # pragma: no cover - the container entrypoint
    import logging

    logging.basicConfig(level=logging.INFO)
    cfg = ConsumerConfig.from_env()
    if cfg.metrics_port:
        from prometheus_client import start_http_server

        start_http_server(cfg.metrics_port)
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    loop = ConsumerLoop(
        consumer=build_consumer(cfg),
        applier=build_applier(cfg),
        topic_prefix=cfg.topic_prefix,
        poll_timeout_s=cfg.poll_timeout_s,
        batch_max=cfg.batch_max,
    )
    log.info("cdc_consumer_started", bootstrap=cfg.kafka_bootstrap, group=cfg.kafka_group)
    loop.run_forever(stop)
    log.info("cdc_consumer_stopped")


if __name__ == "__main__":  # pragma: no cover
    main()

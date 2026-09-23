# ADR 0002: CDC is replication with an explicit staleness budget and an idempotent LSN guard

**Status:** Accepted

**Date:** 2026-07-06

## Context

The Workflow 3 change-capture pipeline replicates Postgres operational state into the online stores and an Iceberg audit table. Changes move from logical decoding (pgoutput) to Debezium on Kafka Connect and then to the LSN-guarded applier in `cdc/consumer/applier.py`. This is asynchronous replication, so the downstream stores always trail the primary by some amount. The design states that lag as a budget and does not promise zero staleness. The applier gets effectively-once state updates from at-least-once Kafka delivery plus an idempotent sink. It applies a batch in delivery order, flushes every sink, and only then calls `commit()`. If any sink raises an error, the offsets stay uncommitted, and the redelivered batch converges again.

## Decision

Treat CDC as replication under an explicit staleness budget. Under the freshness contract, the online stores reflect a committed Postgres change within the end-to-end replication lag (WAL decode, Debezium, Kafka, and the applier flush). Alerting on `pg_replication_slots` lag monitors that budget. A monotonic LSN guard for each `(table, pk)` enforces idempotency. An event whose LSN does not exceed the stored LSN does nothing, so duplicates and stale replays are dropped. A delete leaves a canonical marker that a lower-LSN event cannot resurrect, and snapshot reads apply at a floor LSN of 0.

## Consequences

The design converges safely after a replay, a restart, or a delete, and slot-lag alerting makes the budget observable. The audit table records transport truth, and the state store records state truth. The replay demo exists to show that these two records stay separate.

Staleness is nonzero, and it grows with lag or a stalled slot. State writes are durable per event, but audit rows buffer until the batch flushes. A crash in the middle of a batch can therefore re-record an already-applied event as `applied=False` after redelivery. Exact atomicity across stores would need an outbox, which is out of scope. The audit table therefore matches the state store exactly only when no crash interrupts a batch.

## Alternatives considered

**Synchronous dual-write from the app.** The project rejected this option. It couples the write path to every downstream store, and it loses replay safety. It also has no ordered log to converge against after a partial failure.

**At-least-once with no idempotent sink.** The project rejected this option, because redelivery would apply changes twice and stale replays would resurrect deleted rows. The monotonic LSN guard makes at-least-once transport safe.

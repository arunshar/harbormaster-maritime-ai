# cdc

This directory holds the change-data-capture pipeline for Workflow 3. Postgres (`wal_level=logical`, pgoutput) feeds Debezium on Kafka Connect. An idempotent, LSN-guarded consumer then updates the online stores (DynamoDB plus Redis invalidation) and appends to an Iceberg `cdc_audit` table.

The design and its invariants are in [docs/WORKFLOWS.md](../docs/WORKFLOWS.md#workflow-3-keep-registry-copies-and-cached-answers-current) and [ADR 0002](../docs/adr/0002-cdc-staleness-budget.md). Layout:

- `schema/` holds the DDL for `vessels`, `watchlist`, and `sanctions_flags`, the replica identity settings, and the `harbormaster_cdc` publication. Postgres is the system of record.
- `connector/` holds the Debezium Postgres connector config generator and validator (pgoutput, explicit publication, heartbeats on).
- `consumer/` holds the envelope parser, the LSN-guarded applier (at-least-once transport plus an idempotent sink), and the Kafka consumer service.
- `sinks/` holds the DynamoDB online store with its conditional-write guard, the Redis invalidation, and the Iceberg `cdc_audit` appender.
- `monitor/` holds the `pg_replication_slots` lag reader and alert evaluator, which feed the slot-lag Lambda and the slot-bloat drill.
- `fixtures/` holds the recorded Debezium envelopes and the `expectations.json` checksums.

The vessel names, MMSIs, watchlist entries, and sanctions references in the fixtures are fictional. Every MMSI starts with the MID 200, which the ITU Table of Maritime Identification Digits does not allocate to any country. The sanctions reference `SDN-EXAMPLE-0001` is a placeholder and does not come from any real sanctions list. The envelopes were recorded from the local stack, and their identifiers were then rewritten to these fictional values. `scripts/cdc_record_fixture.py --repin-only` re-derived every checksum in `fixtures/expectations.json` after that rewrite.

Honesty boundary (see [docs/HONESTY.md](../docs/HONESTY.md)): Harbormaster consumes managed Postgres plus Debezium. It does not implement a sharded query router or a consensus layer, and the documentation states that boundary plainly.

"""Harbormaster Phase 2: the change-data-capture pipeline.

Postgres (wal_level=logical, pgoutput) -> Debezium on Kafka Connect -> an
idempotent, LSN-guarded consumer -> DynamoDB/Redis online stores + an Iceberg
cdc_audit table. Plan and gates: docs/phases/PHASE_2.md.
"""

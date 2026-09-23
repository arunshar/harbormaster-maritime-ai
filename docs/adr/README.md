# Architecture Decision Records

This directory records significant design decisions for the Harbormaster platform. Each ADR states what was decided, why, the consequences, and the alternatives considered. Every claim rests on the real code and infrastructure, and each ADR labels the capabilities that the platform does not have.

| ADR | Title | Status |
| --- | --- | --- |
| [0001](0001-streaming-per-event-realization.md) | Streaming plane is a per-event keyed realization, not an event-time windowed pipeline | Accepted |
| [0002](0002-cdc-staleness-budget.md) | CDC is replication with an explicit staleness budget and an idempotent LSN guard | Accepted |
| [0003](0003-single-region-dr-rpo-rto.md) | Single-region, single-AZ RDS is a deliberate cost posture under a hard monthly budget cap | Accepted |
| [0004](0004-no-consensus-no-sharded-query-router.md) | No consensus protocol and no sharded query router, with coordination delegated to managed services | Accepted |
| [Production V1 front door](ADR_PRODUCTION_V1_ECS_FRONT_DOOR.md) | Production V1 retains ECS Fargate as the serving front door | Accepted for planning only. Production V1 is not accepted. |

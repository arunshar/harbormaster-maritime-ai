# ADR 0003: Single-region, single-AZ RDS is a deliberate cost posture under a hard monthly budget cap

**Status:** Accepted

**Date:** 2026-07-06

## Context

The platform ran under a monthly hard-cap budget action and a lower soft budget, and both were provisioned before anything else ([ARCHITECTURE.md](../ARCHITECTURE.md), FinOps module). The hard cap attaches a deny policy that blocks new actions, and war story P7 explains its limits. The RDS module (`infra/terraform/modules/rds/main.tf`) sets `instance_class = db.t4g.micro` (free-tier) in a single region and a single Availability Zone (AZ). It also sets `multi_az = false`, `backup_retention_period = 1` (one day of automated backups), `skip_final_snapshot = true`, and `deletion_protection = false`. These values are cost choices, and the project did not design them for disaster recovery. This ADR states the recovery posture they imply so no one reads more durability into the stack than exists.

## Decision

Accept single-region, single-AZ RDS with one-day backups as the deliberate cost posture, and do not claim any disaster-recovery capability beyond what these settings provide.

The real settings imply the following recovery posture.

- **RPO (recovery point objective):** With `multi_az = false` there is no synchronous standby, so an instance or AZ loss falls back to backups. `backup_retention_period = 1` bounds point-in-time recovery to at most a one-day window. The worst-case loss is whatever committed after the last recoverable point. With `skip_final_snapshot = true`, a `terraform destroy` takes no final snapshot, so a destroy is an unrecoverable loss.
- **RTO (recovery time objective):** A single-AZ instance has no automatic standby promotion. Recovery is a manual restore from backup into a new instance, and it takes tens of minutes to hours. Recovery has no bound if the backup or the region is gone. `deletion_protection = false` removes the guardrail against accidental deletion.

## Consequences

The database stays in the free tier and inside the cap, and the posture is explicit and auditable. In exchange, the stack cannot survive an AZ failure, and it keeps at most one day of backups. It has no cross-region protection, and it loses its data on `terraform destroy` because it skips the final snapshot.

## Alternatives considered

**Multi-AZ.** Multi-AZ adds a synchronous standby with automatic failover, which gives an RTO of a few minutes and near-zero RPO on an AZ loss. The project rejected it because it roughly doubles the instance cost, and the hard cap does not allow that for a personal demo.

**Cross-region automated backups or a read replica in a second region.** Cross-region backups or a replica in a second region would let the database survive a full region outage. The project rejected them because they add storage, transfer, and standing-replica costs outside the cap. This stack therefore cannot survive the loss of an Availability Zone or a region.

# ADR: Production V1 retains ECS Fargate as the serving front door

**Status:** This ADR is accepted for planning only, and Production V1 is not accepted.

**Date:** 2026-08-16

**Decision owner:** Arun Sharma

This ADR records a planning decision, and Production V1 itself is not accepted. No Production V1 environment is running, and this repository deploys nothing by default.

## Context

At the date of this ADR, the retained Harbormaster baseline had an ECS Fargate serving service behind the API boundary. A later bounded window separately ran an EKS and KEDA serving path. That run covered scaling, backpressure handling, ECS rollback, scheduled teardown, and cleanup. The EKS cluster was intentionally removed after the bounded window.

Production V1 is Target A, a single-region operated pilot under a provisional monthly AWS ceiling. It requires a stable serving front door, an evidence-backed rollback, explicit SLOs and ownership, and a representative 24-hour soak. It does not require permanent EKS or automatic model promotion.

A permanent EKS control plane carries a fixed monthly charge, and that charge does not include nodes, ingress, logs, or the rest of Harbormaster. That charge alone would use almost the whole provisional ceiling, and it does not satisfy any Target A requirement that ECS cannot meet.

## Decision

Retain ECS Fargate as the permanent Production V1 serving front door.

The Target A request path is:

```text
client
  -> retained API boundary and authentication
  -> ECS Fargate serving service
  -> inline lightweight detector or asynchronous managed Pi-DPM path
```

The exact production domain, DNS, TLS, client authentication, and rate-limit contract remain unresolved. Until those decisions are frozen, Production V1 scope retains the existing API boundary and does not claim new public exposure.

EKS remains a reproducible optional deployment profile. The bounded window is historical evidence that the KEDA path can work. The project would reconsider EKS first if ECS cannot meet a measured Target A requirement. EKS stays disabled as a standing Production V1 surface.

## Decision drivers

1. **Finite cost:** ECS avoids a fixed EKS control-plane charge that would consume almost the entire provisional budget.
2. **Accepted rollback:** ECS was retained as the rollback path during the EKS and KEDA demonstration.
3. **Lower operational surface:** Target A does not need cluster upgrades, node lifecycle, Kubernetes add-on management, GitOps ownership, ingress controllers, or KEDA maintenance.
4. **Existing evidence:** The retained ECS service and API path ran one bounded one-hour load test, and the [README Evaluation table](../../README.md#evaluation) reports its result.
5. **Target fit:** A single-region operated pilot does not require Kubernetes to satisfy its product contract.
6. **Honest scope:** Proving EKS once does not make a permanent cluster necessary.

## Consequences

### Positive

- The default front door stays on the simplest serving surface that has already run.
- Monthly cost retains room for live AIS, scoped CDC, model serving, observability, backups, and contingency.
- Deployment and rollback operate on immutable ECS task definitions and image digests.
- The project avoids maintaining two permanent front doors.
- Production V1 acceptance can focus on data, model, CDC, SLO, and recovery gaps rather than cluster operations.

### Negative and accepted

- The product does not claim Kubernetes or GitOps as its permanent operating boundary.
- KEDA-specific scaling behavior is not part of the Production V1 contract.
- Advanced Kubernetes scheduling, multi-tenant namespace controls, and cluster-level policy are excluded.
- ECS service and task scaling must satisfy the pilot's measured traffic without relying on KEDA.
- Any later ECS limitation that materially affects availability, latency, isolation, or cost must be measured before anyone reopens this decision.

## Required ECS production controls

This ADR selects an architecture. It does not claim that all production controls already exist. Before ECS behavior is accepted for Production V1, evidence must show that each statement below is true.

- The serving image is a digest-pinned Linux image, and the task definition is immutable.
- Deployment roles and runtime roles are separated under least privilege.
- Health, readiness, graceful shutdown, CPU, memory, and scaling behavior are explicit.
- Logs and metrics carry request and model lineage without secrets or disallowed raw data.
- Availability and latency SLIs come from the Production V1 SLO definition.
- Alarms reach a verified owner and a runbook.
- One controlled rolling restart or replacement drill succeeds.
- Rollback to the last accepted task definition works.
- Representative operation stays within the approved cost envelope.
- The integrated Target A 24-hour attended soak passes.

## Rollback

For a failed ECS deployment:

1. Stop further traffic shift or deployment activity.
2. Restore the last accepted task definition and immutable image digest.
3. Verify the desired, running, and pending task counts and the request behavior.
4. Preserve the failed task, deployment, image, log, metric, timing, and alarm evidence.
5. Reconcile the infrastructure state before another reviewed deployment.

If a future architecture replaces ECS, ECS remains the rollback path until the replacement passes shadowing, limited traffic, rollback, cost, and soak evidence. Removing ECS as rollback requires a separate ADR.

## Alternatives considered

### Permanent EKS with KEDA

The project rejected this option for Target A. EKS ran in a bounded window, but its control plane costs almost the entire provisional monthly ceiling. It also adds cluster upgrades, node and add-on lifecycle, ingress, policy, GitOps, and on-call obligations. It should be reconsidered only for a measured requirement and an approved higher budget.

### Dual permanent ECS and EKS front doors

The project rejected this option. It duplicates operating surfaces and cost without a Target A requirement. Temporary shadowing during a future migration is acceptable, but permanent dual operation is not.

### Direct SageMaker endpoint as the public front door

The project rejected this option. The serving API owns request validation, lightweight detector execution, authorization context, versioning, response schema, and fallback behavior. The heavy model remains an asynchronous dependency rather than the complete product boundary.

### Lambda-only serving

The project did not select this option. It could reduce idle compute for some workloads, but the current service is already containerized and has already run on ECS. No measured requirement justifies changing the application runtime during the first production target.

## Reconsideration triggers

Reopen this ADR only when at least one of the following is true:

- A measured ECS limitation prevents the approved availability or latency objective.
- A validated multi-tenant or workload-isolation requirement cannot be met responsibly on ECS.
- A permanent Kubernetes requirement has a named owner, a maintenance plan, a public-ingress contract, and an explicit business justification.
- The approved monthly budget covers EKS plus the rest of Production V1 with contingency.
- A replacement path passes shadowing against ECS and passes the rollback, security, cost, and required soak checks.

Absent one of these triggers, `serving_target = ecs` and `enable_phase5 = false` are the intended Target A posture. This is a planning statement only.

# ADR 0001: Streaming plane is a per-event keyed realization, not an event-time windowed pipeline

**Status:** Accepted

**Date:** 2026-07-06

## Context

`streaming/flink/job.py` keys `ais-raw` by MMSI and runs a `KeyedProcessFunction` for each vessel. For each fix, the function computes window features against the previous fix held in `ValueState` and gates on `p_physical`. It then writes the gated item to DynamoDB and POSTs it to the serving scorer. This is per-event processing against the keyed previous state. It is not a true event-time tumbling window with watermarks and checkpointed window state. The docstring states the same limit: "A true 1-minute event-time tumbling window can replace the per-fix keyed process below." The `.print()` tail only shows processing in the CloudWatch logs. The DynamoDB write and the scorer POST are side effects inside `process_element`, and they form the real output path.

## Decision

Ship the streaming plane as a per-event keyed realization for the demo, on standard portable PyFlink APIs. Defer a real event-time tumbling window (watermarks, checkpointed window state) as optional future work. Adopt it only if late-fix correctness becomes a hard requirement.

## Consequences

The per-event design is portable and keeps little state per key. Its first live run on Managed Flink (2026-07-04) exposed several real findings. They were dependency-staging bugs, the `application_properties.json` config path, the `_j_function` sink restriction, DynamoDB float and TTL handling, and the scorer's history and schema contract.

The job has no watermark handling, so it does not reconcile late or out-of-order fixes the way a windowed job would. Its correctness rests on delivery order plus the previous state. Managed Flink event-time windowing is untested in this build, and the project has deferred it.

## Alternatives considered

**True event-time tumbling window (watermarks + checkpoints).** A true event-time tumbling window with watermarks and checkpoints is the correct model for late and out-of-order AIS, and it is the stated replacement. The project deferred it because it adds checkpoint and watermark tuning and a heavier state backend. Managed Flink windowing is also untested in this build, and the demo does not need it.

**Spark Structured Streaming.** [ARCHITECTURE.md](../ARCHITECTURE.md) rejects this option. Micro-batch processing adds latency, and it makes per-vessel event-time logic harder than Flink's keyed state and timers.

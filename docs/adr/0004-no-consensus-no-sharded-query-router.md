# ADR 0004: No consensus protocol and no sharded query router, with coordination delegated to managed services

**Status:** Accepted

**Date:** 2026-07-06

## Context

Harbormaster is a personal platform-engineering build with two named out-of-scope items. [HONESTY.md](../HONESTY.md) states them directly:

> - Harbormaster has no sharded query router, and it does not implement query sharding or a routing layer across shards.
> - Harbormaster has no consensus implementation. It does not implement Raft, Paxos, or any consensus protocol, and it uses managed services for coordination.
>
> If someone asks whether this project proves I can build a consensus system or a sharded query router, the honest answer is no.

Kleppmann covers partitioned request routing in chapter 6 and consensus in chapter 9 (Consistency and Consensus) of *Designing Data-Intensive Applications*. Chapter 9 treats total-order broadcast, linearizability, leader election, and the failure modes that make hand-rolled consensus prone to correctness bugs.

## Decision

Keep consensus (Raft/Paxos) and a sharded query router permanently out of scope. Delegate ordering, leader election, and coordinated state to managed services that already solve them. Kinesis provides durable ordered transport, managed Postgres (RDS) holds the operational store, and Kafka with Debezium carries the ordered change log. AWS-managed control planes cover the rest. Harbormaster uses these services and does not re-implement them.

## Consequences

The project has no hand-rolled consensus code that could hide subtle bugs, and its surface stays small. The effort goes to the CDC, streaming, serving, and observability skills that the build shows. This project does not prove that I can build Vitess-style or Multigres-style sharded routing or a Raft or Paxos implementation.

Coordination correctness now depends on the managed services and their guarantees. The platform inherits their limits, such as Kinesis retention and per-shard ordering, and it does not control them.

## Alternatives considered

**Implement Raft/Paxos or a sharded query router in-house.** The project rejected building Raft, Paxos, or a sharded router in-house. That work is the scale of Vitess or Multigres and far exceeds a personal demo. Using managed coordination is the better choice for this project.

**Claim these capabilities anyway.** The project rejected this option outright. It would violate the locked honesty framing in HONESTY.md.

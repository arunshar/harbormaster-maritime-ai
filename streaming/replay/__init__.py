"""AIS replay: the synthetic fixture, its loader, and the deterministic generator.

The fixture (streaming/fixtures/ais_recorded.jsonl) is the replay-first AIS source
for the Phase 1 slice (gate G1) and the golden source for the serving tests. The
1.4 Fargate ingestor reuses `loader.load_fixture` to read and PutRecords to Kinesis.
"""

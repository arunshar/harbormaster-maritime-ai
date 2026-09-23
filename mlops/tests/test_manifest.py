from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import pytest

from mlops.manifest import (
    REQUIRED_META_FIELDS,
    IncompleteManifestMeta,
    ManifestEntry,
    latest,
    read_manifest,
    resolve_blob_path,
    save,
)

EXPECTATIONS = Path(__file__).parent.parent / "fixtures" / "expectations.json"

GOOD_META = {
    "git_sha": "abc1234",
    "config_hash": "cfg-9f8e",
    "data_fingerprint": "fp-1122",
    "mirror_synthetic_anomaly_version": "v3",
    "wandb_run_id": "run-42",
}


def test_save_writes_blob_and_manifest_entry(tmp_path):
    entry = save(tmp_path, 0, b"checkpoint-bytes", meta=GOOD_META, now=lambda: 100.0)
    assert entry.step == 0
    assert entry.ts == 100.0
    assert resolve_blob_path(tmp_path, entry).read_bytes() == b"checkpoint-bytes"


def test_latest_returns_the_most_recently_appended_entry(tmp_path):
    save(tmp_path, 0, b"v0", meta=GOOD_META, now=lambda: 1.0)
    save(tmp_path, 1, b"v1", meta=GOOD_META, now=lambda: 2.0)
    entry = latest(tmp_path)
    assert entry.step == 1
    assert resolve_blob_path(tmp_path, entry).read_bytes() == b"v1"


def test_latest_is_none_when_no_manifest_exists(tmp_path):
    assert latest(tmp_path) is None


@pytest.mark.parametrize("missing_field", REQUIRED_META_FIELDS)
def test_save_rejects_meta_missing_any_required_field(tmp_path, missing_field):
    incomplete = {k: v for k, v in GOOD_META.items() if k != missing_field}
    with pytest.raises(IncompleteManifestMeta) as exc:
        save(tmp_path, 0, b"data", meta=incomplete)
    assert missing_field in str(exc.value)


def test_save_rejects_blank_value_not_just_missing_key(tmp_path):
    blank = dict(GOOD_META, git_sha="")
    with pytest.raises(IncompleteManifestMeta):
        save(tmp_path, 0, b"data", meta=blank)


def test_resaving_identical_content_is_idempotent_on_the_blob(tmp_path):
    e1 = save(tmp_path, 0, b"same-bytes", meta=GOOD_META, now=lambda: 1.0)
    e2 = save(tmp_path, 0, b"same-bytes", meta=GOOD_META, now=lambda: 2.0)
    assert e1.sha == e2.sha
    assert e1.path == e2.path
    # content-addressed: re-saving never corrupts or duplicates the blob file
    assert resolve_blob_path(tmp_path, e1).read_bytes() == b"same-bytes"


def test_resaving_still_appends_a_manifest_line_delivery_log_semantics(tmp_path):
    save(tmp_path, 0, b"same-bytes", meta=GOOD_META, now=lambda: 1.0)
    save(tmp_path, 0, b"same-bytes", meta=GOOD_META, now=lambda: 2.0)
    entries = read_manifest(tmp_path)
    assert len(entries) == 2
    assert [e.ts for e in entries] == [1.0, 2.0]


def test_content_addressing_is_deterministic_regardless_of_step(tmp_path):
    e_a = save(tmp_path, 0, b"identical-payload", meta=GOOD_META)
    e_b = save(tmp_path, 7, b"identical-payload", meta=GOOD_META)
    assert e_a.sha == e_b.sha  # same content, same address, different step directories
    assert e_a.path != e_b.path


def test_different_content_gets_different_addresses(tmp_path):
    e_a = save(tmp_path, 0, b"payload-a", meta=GOOD_META)
    e_b = save(tmp_path, 0, b"payload-b", meta=GOOD_META)
    assert e_a.sha != e_b.sha


def test_manifest_schema_matches_the_pinned_expectation():
    pinned = json.loads(EXPECTATIONS.read_text())["manifest_schema"]
    assert list(REQUIRED_META_FIELDS) == pinned["required_meta_fields"]
    sample = ManifestEntry(step=0, sha="s", path="p", meta={}, ts=0.0)
    assert list(asdict(sample).keys()) == pinned["entry_fields"]


def test_golden_manifest_entry_matches_the_pinned_expectation(tmp_path):
    pinned = json.loads(EXPECTATIONS.read_text())["golden_manifest_entry"]
    entry = save(
        tmp_path,
        0,
        b"golden-example-checkpoint-bytes",
        meta={
            "git_sha": "deadbeef",
            "config_hash": "cfg0001",
            "data_fingerprint": "fp0001",
            "mirror_synthetic_anomaly_version": "v1",
            "wandb_run_id": "wandb-golden-0001",
        },
        now=lambda: 1719878400.0,
    )
    live_line = json.dumps(asdict(entry), sort_keys=True)
    assert live_line == pinned["line"], (
        "the manifest entry shape changed; if intentional, update "
        "mlops/fixtures/expectations.json AND docs/phases/PHASE_3.md in the same commit"
    )


def test_module_exposes_no_resume_or_pull_entry_point():
    """One-way enforcement by absence: the public API must never grow a
    function that turns an S3/manifest entry back into something the training cluster could
    resume from. Training on the cluster is out of scope by design."""
    import mlops.manifest as manifest_module

    public_names = [n for n in dir(manifest_module) if not n.startswith("_")]
    forbidden_substrings = ("resume", "pull_checkpoint", "restore_training", "load_for_training")
    offenders = [n for n in public_names if any(bad in n.lower() for bad in forbidden_substrings)]
    assert offenders == [], f"one-way violation: found resume-shaped entry points {offenders}"

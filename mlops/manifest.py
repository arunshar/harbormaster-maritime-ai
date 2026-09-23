"""GPU cluster -> S3 one-way, content-addressed checkpoint manifest (Phase 3, gate 3.4).

Extends the shape of a checkpoint-manifest module from an earlier
reinforcement-learning repository of mine (content-addressed
`runs/<run_id>/step_<n:07d>/<sha256[:16]>.bin` + an appended `MANIFEST.jsonl`
line) with the lineage fields neither of the earlier RL repository's two existing checkpoint
conventions populate today (its raw `PiDPM.save()` and its own manifest
service both leave `meta` caller-supplied and empty by default): git_sha,
config_hash, data_fingerprint (from lake/export_training_set.py),
mirror_synthetic_anomaly_version, and wandb_run_id are REQUIRED here, not
optional, so a checkpoint's provenance is never silently missing.

One-way by construction: this module only ever writes a manifest entry
(`save`) or reads the latest one back for serving (`latest`); there is no
function anywhere in this module's public API that turns a manifest entry
back into a resumable cluster training checkpoint. Training on the cluster stays
the cluster's own concern and is out of scope here.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path

REQUIRED_META_FIELDS: tuple[str, ...] = (
    "git_sha",
    "config_hash",
    "data_fingerprint",
    "mirror_synthetic_anomaly_version",
    "wandb_run_id",
)


class IncompleteManifestMeta(ValueError):
    """Raised when `save()` is called without every required lineage field."""


@dataclass(frozen=True)
class ManifestEntry:
    step: int
    sha: str
    path: str
    meta: dict[str, str]
    ts: float


def _validate_meta(meta: dict[str, str]) -> None:
    missing = [f for f in REQUIRED_META_FIELDS if f not in meta or not meta[f]]
    if missing:
        raise IncompleteManifestMeta(
            f"checkpoint export is missing required lineage fields: {missing}"
        )


def _manifest_path(run_dir: Path) -> Path:
    return run_dir / "MANIFEST.jsonl"


def save(
    run_dir: Path,
    step: int,
    blob: bytes,
    *,
    meta: dict[str, str],
    now: Callable[[], float] | None = None,
) -> ManifestEntry:
    """Write a content-addressed checkpoint blob and append its manifest
    entry. Idempotent: re-saving the identical (step, blob) content is a
    no-op on the blob write (same sha, same path) and still appends its own
    manifest line (the manifest is an append-only delivery log, mirroring
    cdc_audit's transport-truth convention: redeliveries are recorded, not
    hidden, even though the underlying content never changes).
    """
    _validate_meta(meta)
    now = now or time.time

    sha = hashlib.sha256(blob).hexdigest()[:16]
    step_dir = run_dir / f"step_{step:07d}"
    step_dir.mkdir(parents=True, exist_ok=True)
    blob_path = step_dir / f"{sha}.bin"
    if not blob_path.exists():
        blob_path.write_bytes(blob)

    entry = ManifestEntry(
        step=step,
        sha=sha,
        path=str(blob_path.relative_to(run_dir)),
        meta=dict(meta),
        ts=now(),
    )
    with _manifest_path(run_dir).open("a") as f:
        f.write(json.dumps(asdict(entry), sort_keys=True) + "\n")
    return entry


def read_manifest(run_dir: Path) -> list[ManifestEntry]:
    manifest_path = _manifest_path(run_dir)
    if not manifest_path.exists():
        return []
    entries = []
    for line in manifest_path.read_text().splitlines():
        if not line.strip():
            continue
        raw = json.loads(line)
        entries.append(ManifestEntry(**raw))
    return entries


def latest(run_dir: Path) -> ManifestEntry | None:
    """The most recently appended manifest entry (by manifest order, not
    step number, since a redelivery of an older step could in principle be
    re-recorded after a newer one; "latest delivered", not "highest step")."""
    entries = read_manifest(run_dir)
    return entries[-1] if entries else None


def resolve_blob_path(run_dir: Path, entry: ManifestEntry) -> Path:
    return run_dir / entry.path

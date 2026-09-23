"""W&B experiment/lineage adapter (Phase 3, gate 3.5).

Mirrors exactly the `observability/wandb_adapter.py` shape from an earlier
reinforcement-learning repository of mine: a thin,
lazy adapter that only calls the real `wandb` SDK if `WANDB_API_KEY` is set
in the environment, otherwise logs locally via structlog so unit tests never
need network access or an API key. Extended here with `log_lineage`, which
neither the earlier RL repository's adapter nor either of its two checkpoint conventions
currently provide: a run's lineage (git SHA, config hash, data fingerprint,
the checkpoint manifest path from gate 3.4) written into the run's config/
summary so a promoted checkpoint is traceable end to end, from the raw
MarineCadastre extract through to the SageMaker endpoint that serves it.
"""

from __future__ import annotations

import os

import structlog

log = structlog.get_logger(__name__)


class WandbAdapter:
    def __init__(self, project: str, run_name: str | None = None) -> None:
        self._run = None
        if os.environ.get("WANDB_API_KEY"):
            import wandb

            self._run = wandb.init(project=project, name=run_name)

    def log(self, payload: dict[str, float], *, step: int) -> None:
        if self._run is not None:
            self._run.log(payload, step=step)
        else:
            log.info("metric", step=step, **payload)

    def log_lineage(
        self,
        *,
        run_id: str,
        git_sha: str,
        config_hash: str,
        data_fingerprint: str,
        checkpoint_manifest_path: str,
    ) -> None:
        """Write the checkpoint's full lineage into the run's config/summary
        (or, with no live W&B run, structlog). Required, not optional: a
        promoted checkpoint without recorded lineage is exactly the gap
        gate 3.4 closes for the manifest itself; this is the same
        traceability surfaced into the experiment-tracking side."""
        lineage = {
            "run_id": run_id,
            "git_sha": git_sha,
            "config_hash": config_hash,
            "data_fingerprint": data_fingerprint,
            "checkpoint_manifest_path": checkpoint_manifest_path,
        }
        if self._run is not None:
            self._run.config.update(lineage, allow_val_change=True)
            self._run.summary.update(lineage)
        else:
            log.info("lineage", **lineage)

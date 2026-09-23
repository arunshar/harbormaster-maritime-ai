"""Build the DEMO STAND-IN checkpoint artifact for the Phase 3 AWS showcase.

NOT a real Pi-DPM checkpoint: just the {"feature_mean", "feature_std"}
constants mlops/pidpm_container/demo/server.py's score_trajectory needs,
packaged as the tar.gz SageMaker's BYOC contract expects at model_data_url
(extracted to /opt/ml/model/ inside the container at startup).

Usage: .venv/bin/python scripts/build_demo_pidpm_checkpoint.py [output_path]
  (default output: dist/demo_pidpm_checkpoint/model.tar.gz)
"""

from __future__ import annotations

import json
import sys
import tarfile
from pathlib import Path

DEFAULT_OUTPUT = (
    Path(__file__).resolve().parent.parent / "dist" / "demo_pidpm_checkpoint" / "model.tar.gz"
)


def build(output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_json = output_path.parent / "checkpoint.json"
    checkpoint_json.write_text(json.dumps({"feature_mean": 900.0, "feature_std": 400.0}))

    with tarfile.open(output_path, "w:gz") as tar:
        tar.add(checkpoint_json, arcname="checkpoint.json")

    return output_path


def main(argv: list[str]) -> int:
    output_path = Path(argv[1]) if len(argv) > 1 else DEFAULT_OUTPUT
    path = build(output_path)
    print(f"[PASS] built {path} (upload to S3, then pass its s3:// URI as pidpm_model_data_url)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

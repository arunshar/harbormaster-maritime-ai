"""Local regression tests for exact saved Terraform plan evidence."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "plan_artifact.sh"
MAKEFILE = REPO / "Makefile"


def _fake_terraform(bin_dir: Path) -> None:
    terraform = bin_dir / "terraform"
    terraform.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
command_name="$2"
if [ "$command_name" = plan ]; then
  for argument in "$@"; do
    case "$argument" in
      -out=*) printf 'exact-saved-plan' > "${argument#-out=}" ;;
    esac
  done
elif [ "$command_name" = show ] && [ "$3" = -json ]; then
  printf '%s' '{"resource_changes":['
  printf '%s\n' '{"address":"module.w4.aws_x.test","change":{"actions":["create"]}}]}'
else
  printf 'unexpected terraform arguments: %s\n' "$*" >&2
  exit 2
fi
"""
    )
    terraform.chmod(0o755)


def test_plan_artifact_retains_and_hash_binds_the_exact_binary(tmp_path):
    bin_dir = tmp_path / "bin"
    output_dir = tmp_path / "summaries"
    bin_dir.mkdir()
    _fake_terraform(bin_dir)
    relative_plan = Path("artifacts/w4.tfplan")
    environment = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "HM_PLAN_ARTIFACT_DIR": str(output_dir),
    }

    result = subprocess.run(
        ["bash", str(SCRIPT), "w4-test", str(relative_plan)],
        cwd=tmp_path,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )

    plan = tmp_path / relative_plan
    assert plan.read_bytes() == b"exact-saved-plan"
    expected_hash = hashlib.sha256(plan.read_bytes()).hexdigest()
    summaries = list(output_dir.glob("*-w4-test.json"))
    assert len(summaries) == 1
    summary_path = summaries[0]
    summary = json.loads(summary_path.read_text())
    assert summary["plan_sha256"] == expected_hash
    assert summary["add"] == 1
    assert summary["change"] == 0
    assert summary["destroy"] == 0
    assert summary["resource_changes"] == [
        {"address": "module.w4.aws_x.test", "actions": ["create"]}
    ]
    assert f"binary plan: {plan}" in result.stdout
    assert f"binary plan sha256: {expected_hash}" in result.stdout

    rerun = subprocess.run(
        ["bash", str(SCRIPT), "w4-test", str(relative_plan)],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert rerun.returncode == 2
    assert "saved plan already exists; use a fresh path" in rerun.stderr
    assert plan.read_bytes() == b"exact-saved-plan"


def test_makefile_applies_only_the_caller_supplied_saved_plan():
    text = MAKEFILE.read_text()
    block = text.split("apply-plan: init", 1)[1].split("\ndestroy:", 1)[0]
    assert 'test -n "$(PLAN)"' in block
    assert 'test -f "$(PLAN)"' in block
    assert "Type 'yes' to continue" in block
    assert 'terraform -chdir=$(TF_DIR) apply "$(abspath $(PLAN))"' in block

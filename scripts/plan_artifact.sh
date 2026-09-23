#!/usr/bin/env bash
# Capture a reviewable Terraform plan artifact for the envs/base root.
#
# Every operator-run apply window starts with this script, so the exact
# planned resource changes are recorded before anything is applied. The binary
# plan file and the raw `show -json` output stay local, because both can embed
# sensitive resolved values. The address+actions summary and the binary plan
# SHA-256 are written to artifacts/plan/, which is gitignored, so the summary
# is never committed. When plan-file is supplied, the binary is retained so
# Terraform can apply that exact plan.
#
# Usage:
#   scripts/plan_artifact.sh <label> [plan-file]
#   scripts/plan_artifact.sh phase4-flywheel artifacts/phase4.tfplan
#
# Writes artifacts/plan/<UTC-date>-<label>.json (gitignored):
#   {generated_utc, label, plan_sha256, add, change, destroy,
#    resource_changes: [{address, actions}]}
# and echoes the add/change/destroy counts. A replace counts as both an add
# and a destroy, matching Terraform's own "X to add ... Y to destroy" summary.
#
# Requires: AWS credentials (plan reads real state through the S3 backend) and
# jq or python3 for the summary step. operator-run only, same as every apply in
# this repo; CI never runs terraform plan.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="$REPO_ROOT/infra/terraform/envs/base"
OUT_DIR="${HM_PLAN_ARTIFACT_DIR:-$REPO_ROOT/artifacts/plan}"

usage() {
  echo "usage: $0 <label> [plan-file]   (label: [A-Za-z0-9._-]+)" >&2
  exit 2
}

[ "$#" -ge 1 ] && [ "$#" -le 2 ] || usage
label="$1"
case "$label" in
  "" | *[!A-Za-z0-9._-]*) usage ;;
esac

retain_plan=false
plan_complete=false
if [ "$#" -eq 2 ]; then
  plan_tmp="$2"
  [ -n "$plan_tmp" ] || usage
  case "$plan_tmp" in
    /*) ;;
    *) plan_tmp="$PWD/$plan_tmp" ;;
  esac
  if [ -e "$plan_tmp" ]; then
    echo "saved plan already exists; use a fresh path: $plan_tmp" >&2
    exit 2
  fi
  mkdir -p "$(dirname "$plan_tmp")"
  retain_plan=true
else
  plan_tmp="$(mktemp -t hm-plan.XXXXXX)"
fi
json_tmp="$(mktemp -t hm-plan-json.XXXXXX)"
cleanup() {
  rm -f "$json_tmp"
  if [ "$retain_plan" != true ] || [ "$plan_complete" != true ]; then
    rm -f "$plan_tmp"
  fi
}
trap cleanup EXIT

terraform -chdir="$TF_DIR" plan -input=false -out="$plan_tmp"
terraform -chdir="$TF_DIR" show -json "$plan_tmp" > "$json_tmp"
plan_sha256="$(shasum -a 256 "$plan_tmp" | awk '{print $1}')"

mkdir -p "$OUT_DIR"
artifact="$OUT_DIR/$(date -u +%Y-%m-%d)-${label}.json"
generated="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if command -v jq >/dev/null 2>&1; then
  jq --arg generated "$generated" --arg label "$label" --arg plan_sha256 "$plan_sha256" '
    [.resource_changes[]? | {address, actions: .change.actions}] as $rc
    | {
        generated_utc: $generated,
        label: $label,
        plan_sha256: $plan_sha256,
        add: ([$rc[] | select(.actions | index("create"))] | length),
        change: ([$rc[] | select(.actions | index("update"))] | length),
        destroy: ([$rc[] | select(.actions | index("delete"))] | length),
        resource_changes: $rc
      }
  ' "$json_tmp" > "$artifact"
else
  HM_GENERATED="$generated" HM_LABEL="$label" HM_ARTIFACT="$artifact" \
    HM_PLAN_SHA256="$plan_sha256" \
    python3 - "$json_tmp" <<'PY'
import json
import os
import sys

with open(sys.argv[1]) as f:
    plan = json.load(f)
rc = [
    {"address": c["address"], "actions": c["change"]["actions"]}
    for c in plan.get("resource_changes") or []
]
summary = {
    "generated_utc": os.environ["HM_GENERATED"],
    "label": os.environ["HM_LABEL"],
    "plan_sha256": os.environ["HM_PLAN_SHA256"],
    "add": sum(1 for c in rc if "create" in c["actions"]),
    "change": sum(1 for c in rc if "update" in c["actions"]),
    "destroy": sum(1 for c in rc if "delete" in c["actions"]),
    "resource_changes": rc,
}
with open(os.environ["HM_ARTIFACT"], "w") as f:
    json.dump(summary, f, indent=2)
    f.write("\n")
PY
fi

counts="$(python3 -c '
import json, sys
d = json.load(open(sys.argv[1]))
print(d["add"], d["change"], d["destroy"])
' "$artifact")"
read -r add change destroy <<< "$counts"
plan_complete=true

echo "plan artifact: $artifact"
echo "binary plan: $plan_tmp"
echo "binary plan sha256: $plan_sha256"
echo "plan summary: $add to add, $change to change, $destroy to destroy"

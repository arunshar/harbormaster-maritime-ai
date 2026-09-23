#!/usr/bin/env bash
# Package the Phase 3 lake backfill for EMR Serverless (runbook Part 1.3).
#
# Produces in dist/lake-emr/:
#   lake_backfill_job.py   entrypoint (verbatim copy of lake/backfill/job.py)
#   lake_pkg.zip           --py-files archive of the pure-Python `lake` package
#   pyspark_venv.tar.gz    (--with-venv only) linux/amd64 virtualenv built
#                          inside the real EMR Serverless base image via
#                          venv-pack, so native wheels (pandas / scikit-learn /
#                          pyarrow) match the EMR runtime, not this Mac
#
# The venv step needs Docker and pulls public.ecr.aws/emr-serverless/spark
# (pinned to the emr_backfill module's release_label); everything else is
# plain cp/zip and runs anywhere. --upload shells out to the aws CLI with
# --region pinned to us-east-1 and is operator-run at demo time per the runbook.
#
# Usage:
#   scripts/package_lake_for_emr.sh                      # entrypoint + zip only
#   scripts/package_lake_for_emr.sh --with-venv          # + Docker venv archive
#   scripts/package_lake_for_emr.sh --upload s3://B/code # + upload artifacts
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$REPO_ROOT/dist/lake-emr"
# Keep in sync with infra/terraform/modules/emr_backfill/main.tf release_label.
EMR_RELEASE="emr-7.2.0"
IMAGE="public.ecr.aws/emr-serverless/spark/${EMR_RELEASE}:latest"
AWS_REGION="us-east-1"

# Runtime deps the job needs on EMR beyond the Spark runtime itself. Keep in
# sync with pyproject.toml's `lake` extra, plus pyiceberg[glue]: locally the
# cdc extra installs pyiceberg[sql-sqlite] for the sqlite catalog, but the EMR
# job writes through the Glue catalog, which needs the glue extra instead.
PIP_DEPS=(
  "pandas>=2.0"
  "scikit-learn>=1.3"
  "great-expectations>=0.18,<1.0"
  "pyarrow>=15"
  "pyiceberg[glue]>=0.7"
)

WITH_VENV=0
UPLOAD_PREFIX=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-venv) WITH_VENV=1; shift ;;
    --upload) UPLOAD_PREFIX="${2:?--upload needs an s3://bucket/prefix}"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
UPLOAD_PREFIX="${UPLOAD_PREFIX%/}"

rm -rf "$OUT"
mkdir -p "$OUT"

# ---- 1. entrypoint --------------------------------------------------------
cp "$REPO_ROOT/lake/backfill/job.py" "$OUT/lake_backfill_job.py"

# ---- 2. --py-files zip of the pure-Python lake package --------------------
(cd "$REPO_ROOT" && zip -qr "$OUT/lake_pkg.zip" lake \
  --include 'lake/*.py' \
  --exclude 'lake/tests/*' 'lake/*/tests/*')

# Self-check: every module the entrypoint imports must resolve via zipimport
# exactly as Spark's --py-files will load it. find_spec imports the parent
# packages, so this also proves the package __init__ files are dependency-free.
PYTHONPATH="$OUT/lake_pkg.zip" python3 - <<'PY'
import importlib.util
import sys

# Drop the implicit cwd entry stdin-mode python prepends; otherwise `lake`
# resolves from the repo checkout and the check proves nothing about the zip.
sys.path = [p for p in sys.path if p not in ("", ".")]

for mod in (
    "lake.backfill.transforms",
    "lake.quality.marinecadastre_suite",
    "lake.iceberg",
):
    spec = importlib.util.find_spec(mod)
    assert spec is not None, f"{mod} not importable from lake_pkg.zip"
    assert "lake_pkg.zip" in (spec.origin or ""), f"{mod} resolved outside the zip: {spec.origin}"
print("lake_pkg.zip importable via zipimport: OK")
PY

# ---- 3. optional linux/amd64 venv archive via the real EMR image ----------
if [[ "$WITH_VENV" -eq 1 ]]; then
  deps_quoted=""
  for d in "${PIP_DEPS[@]}"; do deps_quoted+=" '$d'"; done
  docker run --rm --platform linux/amd64 \
    -v "$OUT":/output \
    --entrypoint /bin/bash \
    "$IMAGE" \
    -c "set -euo pipefail && \
        python3 -m venv --copies /tmp/venv && \
        source /tmp/venv/bin/activate && \
        pip install --quiet --upgrade pip && \
        pip install --quiet venv-pack ${deps_quoted} && \
        venv-pack -f -o /output/pyspark_venv.tar.gz"
  echo "pyspark_venv.tar.gz built against ${IMAGE}"
fi

# ---- 4. optional upload (operator-run at demo time) ----------------------------
if [[ -n "$UPLOAD_PREFIX" ]]; then
  artifacts=(lake_backfill_job.py lake_pkg.zip)
  [[ -f "$OUT/pyspark_venv.tar.gz" ]] && artifacts+=(pyspark_venv.tar.gz)
  for f in "${artifacts[@]}"; do
    aws s3 cp "$OUT/$f" "$UPLOAD_PREFIX/$f" --region "$AWS_REGION"
  done
fi

cat <<EOF

Packaged $(ls -1 "$OUT" | tr '\n' ' ')
Submit with (runbook Part 1.3; CODE = the s3 prefix these were uploaded to):

  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "CODE/lake_backfill_job.py",
      "entryPointArguments": ["<raw_extract_s3_uri>", "glue", "<warehouse_s3_uri>"],
      "sparkSubmitParameters": "--py-files CODE/lake_pkg.zip --archives CODE/pyspark_venv.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
    }
  }'
EOF

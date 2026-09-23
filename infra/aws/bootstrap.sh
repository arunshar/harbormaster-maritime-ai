#!/usr/bin/env bash
#
# infra/aws/bootstrap.sh
#
# One-time AWS scaffolding for Harbormaster. It creates ONLY the things
# Terraform cannot bootstrap itself:
#
#   1. the "harbormaster-permissions-boundary" customer-managed policy  (the
#      ceiling for every role the platform role later creates; must exist before
#      the platform role's inline policy, which is conditioned on it), then
#   2. the "harbormaster-platform" IAM role  (the $75 budget action attaches its
#      deny policy to THIS role on breach, so it must exist before `make apply`;
#      it is deliberately NOT your admin identity, so a spend-freeze cannot lock
#      you out of `terraform destroy`; its inline IAM-management policy is scoped
#      to harbormaster-* resources and gated by the boundary above, so it can
#      create the module roles but cannot escalate itself to admin), and
#   3. a dedicated, versioned, encrypted, public-access-blocked S3 bucket for
#      remote Terraform state  (kept separate from the data lake, per backend.tf).
#
# It does NOT run `terraform apply`, touch the data lake, or create any
# spend-incurring streaming/compute resource. Everything here is free or
# negligible (an IAM role and an empty S3 bucket).
#
# Idempotent: re-running skips anything that already exists.
#
# Usage:
#   bash infra/aws/bootstrap.sh --dry-run     # print the plan, change nothing (no creds needed)
#   bash infra/aws/bootstrap.sh               # interactive, confirm each mutating step
#   bash infra/aws/bootstrap.sh --yes         # non-interactive (assume yes)
#   bash infra/aws/bootstrap.sh --region us-east-1 --suffix myuniq
#
set -euo pipefail

PROJECT="harbormaster"
REGION="us-east-1"
ROLE_NAME="harbormaster-platform"
# Permissions boundary that caps every role the deploy identity creates. The
# platform inline policy (harbormaster-platform-permissions.json) will only let
# CreateRole / AttachRolePolicy / PutRolePolicy succeed when this exact boundary
# is set on the target role, so a created role can never exceed this ceiling and
# the deploy identity cannot escalate itself to admin.
BOUNDARY_NAME="harbormaster-permissions-boundary"
# The DynamoDB lock table is created by Phase 0 Terraform (state_stores module);
# we only PRINT its name here for the later backend-migration step.
LOCK_TABLE="harbormaster-base-tf-state-lock"
DRY_RUN=0
ASSUME_YES=0
SUFFIX=""
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  grep -E '^#( |$)' "$0" | sed -E 's/^# ?//'
}

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run)       DRY_RUN=1 ;;
    --yes|-y)        ASSUME_YES=1 ;;
    --region)        REGION="${2:?--region needs a value}"; shift ;;
    --suffix)        SUFFIX="${2:?--suffix needs a value}"; shift ;;
    -h|--help)       usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; echo "run with --help" >&2; exit 2 ;;
  esac
  shift
done

# ---- output helpers ----------------------------------------------------------
log()  { printf '\n==> %s\n' "$*"; }
info() { printf '    %s\n' "$*"; }
# run: echo the command, then execute it unless --dry-run.
run() {
  printf '    $ %s\n' "$*"
  [ "$DRY_RUN" -eq 1 ] && return 0
  "$@"
}
# confirm: yes automatically under --yes or --dry-run; otherwise prompt.
confirm() {
  { [ "$ASSUME_YES" -eq 1 ] || [ "$DRY_RUN" -eq 1 ]; } && return 0
  printf '    %s [y/N] ' "$1"
  read -r _ans
  [ "$_ans" = "y" ] || [ "$_ans" = "Y" ]
}
need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required tool: $1" >&2; exit 1; }; }

# ---- preflight ---------------------------------------------------------------
log "Preflight: required tools + AWS identity + region ($REGION)"
need aws
need jq
info "aws:       $(aws --version 2>&1 | head -1)"

if ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null)"; then
  info "AWS account: $ACCOUNT_ID"
  CALLER="$(aws sts get-caller-identity --query Arn --output text 2>/dev/null || echo '?')"
  info "caller ARN:  $CALLER"
else
  if [ "$DRY_RUN" -eq 1 ]; then
    ACCOUNT_ID="__ACCOUNT_ID__"
    info "no AWS credentials configured; dry-run continues with a placeholder account id"
  else
    echo "    ERROR: 'aws sts get-caller-identity' failed." >&2
    echo "    Configure the AWS CLI first: aws configure" >&2
    exit 1
  fi
fi

[ -n "$SUFFIX" ] || SUFFIX="$ACCOUNT_ID"
STATE_BUCKET="${PROJECT}-tfstate-${SUFFIX}"
BOUNDARY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${BOUNDARY_NAME}"

# ---- step 1: permissions-boundary policy ------------------------------------
# Must exist BEFORE the platform role's inline policy is put, because that inline
# policy's CreateRole/AttachRolePolicy/PutRolePolicy statements are conditioned on
# iam:PermissionsBoundary == this policy's ARN. Idempotent: if the policy already
# exists we leave it (and its versions) untouched rather than mutating it here.
log "Step 1: permissions-boundary policy '$BOUNDARY_NAME' (ceiling for every role the platform role creates)"
if [ "$DRY_RUN" -eq 0 ] && aws iam get-policy --policy-arn "$BOUNDARY_ARN" >/dev/null 2>&1; then
  info "boundary policy already exists; leaving it unchanged"
  info "arn: $BOUNDARY_ARN"
else
  info "plan: create customer-managed policy '$BOUNDARY_NAME' from"
  info "      harbormaster-permissions-boundary.json (the service ceiling + IAM-escalation"
  info "      and boundary-removal denies)"
  if confirm "create permissions-boundary policy '$BOUNDARY_NAME'?"; then
    run aws iam create-policy \
      --policy-name "$BOUNDARY_NAME" \
      --policy-document "file://$SCRIPT_DIR/harbormaster-permissions-boundary.json" \
      --description "Harbormaster permissions boundary; caps any role the platform role creates and blocks IAM escalation" \
      --tags "Key=Project,Value=${PROJECT}"
    info "arn: $BOUNDARY_ARN"
    info "done."
  else
    info "skipped."
  fi
fi

# ---- step 2: platform IAM role ----------------------------------------------
log "Step 2: IAM role '$ROLE_NAME' (the \$75 deny-on-breach target; NOT your admin identity)"
if [ "$DRY_RUN" -eq 0 ] && aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  # The role already exists (Phase 0 bootstrap created it). Do NOT skip: an
  # existing role kept its OLD inline policy, which for a pre-boundary role is
  # the iam:*-on-Resource-* escalation. Reconcile the policies idempotently so a
  # re-run actually applies the hardened, boundary-gated definitions (war story
  # P32). attach-role-policy and put-role-policy are both idempotent.
  info "role already exists; reconciling its managed + inline policies to the committed definitions"
  info "plan: (re)attach PowerUserAccess and (re)put the scoped, boundary-gated inline"
  info "      policy 'harbormaster-iam-management' from harbormaster-platform-permissions.json"
  info "      (this overwrites any prior same-named inline policy, e.g. a pre-boundary version"
  info "      with iam:* on Resource *). The trust policy is left unchanged."
  if confirm "reconcile IAM policies on existing role '$ROLE_NAME'?"; then
    run aws iam attach-role-policy \
      --role-name "$ROLE_NAME" \
      --policy-arn "arn:aws:iam::aws:policy/PowerUserAccess"
    run aws iam put-role-policy \
      --role-name "$ROLE_NAME" \
      --policy-name "harbormaster-iam-management" \
      --policy-document "file://$SCRIPT_DIR/harbormaster-platform-permissions.json"
    info "done (policies reconciled)."
  else
    info "skipped."
  fi
else
  info "plan: create the role (trust = this account's identities, MFA required),"
  info "      attach the AWS-managed PowerUserAccess policy, and add the scoped,"
  info "      boundary-gated IAM-management inline policy from"
  info "      harbormaster-platform-permissions.json (which references boundary '$BOUNDARY_NAME')"
  if confirm "create IAM role '$ROLE_NAME'?"; then
    TRUST_JSON="$(jq --arg a "$ACCOUNT_ID" \
      '.Statement[0].Principal.AWS = ("arn:aws:iam::" + $a + ":root")' \
      "$SCRIPT_DIR/harbormaster-platform-trust.json")"
    run aws iam create-role \
      --role-name "$ROLE_NAME" \
      --assume-role-policy-document "$TRUST_JSON" \
      --description "Harbormaster platform/deploy role; the \$75 budget action attaches a deny policy here on breach" \
      --tags "Key=Project,Value=${PROJECT}"
    run aws iam attach-role-policy \
      --role-name "$ROLE_NAME" \
      --policy-arn "arn:aws:iam::aws:policy/PowerUserAccess"
    run aws iam put-role-policy \
      --role-name "$ROLE_NAME" \
      --policy-name "harbormaster-iam-management" \
      --policy-document "file://$SCRIPT_DIR/harbormaster-platform-permissions.json"
    info "done."
  else
    info "skipped."
  fi
fi

# ---- step 3: terraform state bucket -----------------------------------------
log "Step 3: Terraform state bucket 's3://$STATE_BUCKET' (versioned, encrypted, private)"
if [ "$DRY_RUN" -eq 0 ] && aws s3api head-bucket --bucket "$STATE_BUCKET" >/dev/null 2>&1; then
  info "bucket already exists; leaving it unchanged"
else
  info "plan: create the bucket in $REGION, then enable versioning, AES256 default"
  info "      encryption, and a full public-access block"
  if confirm "create S3 bucket '$STATE_BUCKET'?"; then
    # us-east-1 must NOT pass a LocationConstraint; every other region must.
    if [ "$REGION" = "us-east-1" ]; then
      run aws s3api create-bucket --bucket "$STATE_BUCKET" --region "$REGION"
    else
      run aws s3api create-bucket --bucket "$STATE_BUCKET" --region "$REGION" \
        --create-bucket-configuration "LocationConstraint=$REGION"
    fi
    run aws s3api put-bucket-versioning --bucket "$STATE_BUCKET" \
      --versioning-configuration "Status=Enabled"
    run aws s3api put-bucket-encryption --bucket "$STATE_BUCKET" \
      --server-side-encryption-configuration \
      '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
    run aws s3api put-public-access-block --bucket "$STATE_BUCKET" \
      --public-access-block-configuration \
      "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
    info "done."
  else
    info "skipped."
  fi
fi

# ---- step 4: print the backend.tf values ------------------------------------
log "Step 4: values for the remote backend (edit infra/terraform/envs/base/backend.tf AFTER 'make apply')"
cat <<EOF
    Once Phase 0 is applied (so the lock table exists), migrate state:
      1. comment out the backend "local" block
      2. uncomment the backend "s3" block and set:
           bucket         = "$STATE_BUCKET"
           key            = "base/terraform.tfstate"
           region         = "$REGION"
           dynamodb_table = "$LOCK_TABLE"   # confirm via: terraform -chdir=infra/terraform/envs/base output tf_state_lock_table_name
           encrypt        = true
      3. terraform -chdir=infra/terraform/envs/base init -migrate-state

EOF

log "Bootstrap ${DRY_RUN:+plan }complete."
if [ "$DRY_RUN" -eq 1 ]; then
  info "This was a DRY RUN. Re-run without --dry-run to create the role and bucket."
else
  info "Next: confirm alert_email in terraform.tfvars, then 'make plan' and 'make apply'."
fi
info "Before 'make plan', review infra/terraform/envs/base/terraform.tfvars.example and README.md."

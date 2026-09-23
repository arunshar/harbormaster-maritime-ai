# envs/base/backend.tf
#
# State backend for the Harbormaster "base" root.
#
# DEFAULT: LOCAL backend. State is written to ./terraform.tfstate in this
# directory. This is intentional for Phase 0 so the very first `terraform apply`
# (which creates the S3 bucket and DynamoDB lock table the remote backend would
# need) does not face a chicken-and-egg problem: you cannot store state in a
# bucket that does not exist yet.
#
# The block below selects the local backend explicitly.

terraform {
  backend "local" {
    path = "terraform.tfstate"
  }
}

# -----------------------------------------------------------------------------
# OPTIONAL: migrate to a remote S3 + DynamoDB backend AFTER the first apply.
#
# How to enable (do this once, after `terraform apply` has created the
# state_stores buckets and the tf-state-lock table):
#
#   1. Note the bucket and lock-table names from `terraform output`:
#        - lake_bucket_name is the DATA lake, do NOT use it for state.
#        - tf_state_lock_table_name is the lock table to use below.
#      Create (or reuse) a dedicated, separate state bucket for Terraform state;
#      keeping platform state out of the data lake is the safer convention.
#        aws s3api create-bucket --bucket harbormaster-tfstate-<youruniqsuffix> \
#          --region us-east-1
#        aws s3api put-bucket-versioning --bucket harbormaster-tfstate-<...> \
#          --versioning-configuration Status=Enabled
#
#   2. Comment OUT the `backend "local"` block above.
#
#   3. Uncomment the `backend "s3"` block below, filling in the real bucket name
#      and the tf-state-lock table name. Do not commit real account values if
#      they are sensitive in your context; the names here are not secrets.
#
#   4. Run `terraform init -migrate-state`. Terraform copies local state into S3.
#
# terraform {
#   backend "s3" {
#     bucket         = "harbormaster-tfstate-<your-unique-suffix>"
#     key            = "base/terraform.tfstate"
#     region         = "us-east-1"
#     dynamodb_table = "harbormaster-base-tf-state-lock"
#     encrypt        = true
#   }
# }
#
# Note: a backend block cannot use variables or interpolation; the values must
# be literals. That is a Terraform constraint, not an oversight here.
# -----------------------------------------------------------------------------

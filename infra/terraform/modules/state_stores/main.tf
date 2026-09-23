# modules/state_stores/main.tf
#
# Durable state for Harbormaster Phase 0:
#   - one lake bucket holding raw/, iceberg/, and features/ prefixes
#   - a separate models bucket for trained-model artifacts
#   - both with versioning, SSE, full public-access block, and lifecycle rules
#   - a DynamoDB table for the Feast online store (on-demand billing)
#   - a DynamoDB table for the Terraform state lock
#
# Bucket names are globally unique via a random_id suffix so a fresh apply in
# any account does not collide with an already-taken global name. War-story
# context: an apply once failed late with BucketAlreadyExists after creating
# half the stack, so the suffix is non-negotiable.

locals {
  name_prefix = "${var.project}-${var.environment}"

  tags = merge(var.tags, {
    Module = "state_stores"
  })
}

# 4-byte (8 hex char) suffix keeps bucket names readable yet collision-safe.
resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  lake_bucket_name   = "${local.name_prefix}-lake-${random_id.suffix.hex}"
  models_bucket_name = "${local.name_prefix}-models-${random_id.suffix.hex}"
}

# -----------------------------------------------------------------------------
# Lake bucket (raw/ iceberg/ features/)
# -----------------------------------------------------------------------------

resource "aws_s3_bucket" "lake" {
  bucket = local.lake_bucket_name

  tags = merge(local.tags, {
    Name    = local.lake_bucket_name
    Purpose = "data-lake"
  })
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  # aws:kms with the CMK when kms_key_arn is set; otherwise the original
  # SSE-AES256, so the default (no-CMK) plan stays a zero diff. length() > 0
  # rather than != "": semantically identical, but checkov's expression
  # evaluator resolves only the length() form, and the CKV_AWS_19 encryption
  # check must keep passing outright (never baselined).
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = length(var.kms_key_arn) > 0 ? "aws:kms" : "AES256"
      kms_master_key_id = length(var.kms_key_arn) > 0 ? var.kms_key_arn : null
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket = aws_s3_bucket.lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  # Expire old object versions so versioning does not silently grow cost.
  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.lake_noncurrent_expiration_days
    }
  }

  # Transition raw landing data to Standard-IA; it is read rarely after ingest.
  rule {
    id     = "raw-transition-ia"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = var.raw_transition_ia_days
      storage_class = "STANDARD_IA"
    }
  }

  # Clean up incomplete multipart uploads everywhere.
  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = var.abort_multipart_days
    }
  }
}

# Zero-byte marker objects so the raw/ iceberg/ features/ prefixes exist and are
# discoverable in the console before any real data lands. S3 has no real folders;
# these are conventional keep-markers.
resource "aws_s3_object" "lake_prefixes" {
  for_each = toset(["raw/", "iceberg/", "features/"])

  bucket  = aws_s3_bucket.lake.id
  key     = "${each.value}.keep"
  content = "Harbormaster lake prefix marker. Managed by Terraform.\n"

  tags = local.tags
}

# -----------------------------------------------------------------------------
# Models bucket
# -----------------------------------------------------------------------------

resource "aws_s3_bucket" "models" {
  bucket = local.models_bucket_name

  tags = merge(local.tags, {
    Name    = local.models_bucket_name
    Purpose = "model-artifacts"
  })
}

resource "aws_s3_bucket_versioning" "models" {
  bucket = aws_s3_bucket.models.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "models" {
  bucket = aws_s3_bucket.models.id

  # Same CMK-or-AES256 switch as the lake bucket above (length() > 0 for the
  # same checkov-resolvability reason).
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = length(var.kms_key_arn) > 0 ? "aws:kms" : "AES256"
      kms_master_key_id = length(var.kms_key_arn) > 0 ? var.kms_key_arn : null
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "models" {
  bucket = aws_s3_bucket.models.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# The Pi-DPM async-failure alert path consumes only model-bucket events through
# EventBridge. This bucket owns its notification configuration so a future
# consumer cannot silently overwrite it from another module. The root enables
# it only while the matching endpoint and alert resources are both enabled.
resource "aws_s3_bucket_notification" "models_eventbridge" {
  count = var.enable_models_eventbridge ? 1 : 0

  bucket      = aws_s3_bucket.models.id
  eventbridge = true
}

resource "aws_s3_bucket_lifecycle_configuration" "models" {
  bucket = aws_s3_bucket.models.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.lake_noncurrent_expiration_days
    }
  }

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = var.abort_multipart_days
    }
  }
}

# -----------------------------------------------------------------------------
# DynamoDB: Feast online store (on-demand billing)
# -----------------------------------------------------------------------------

# Feast's DynamoDB online store keys each row by entity ("entity_id") and a
# feature view name as range key ("feature_name"). On-demand (PAY_PER_REQUEST)
# billing means zero cost when idle, which fits a personal platform.
resource "aws_dynamodb_table" "feast_online" {
  name         = "${local.name_prefix}-feast-online"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "entity_id"
  range_key    = "feature_name"

  attribute {
    name = "entity_id"
    type = "S"
  }

  attribute {
    name = "feature_name"
    type = "S"
  }

  dynamic "ttl" {
    for_each = var.feast_online_table_ttl_enabled ? [1] : []
    content {
      attribute_name = "ttl"
      enabled        = true
    }
  }

  # CMK-encrypted only when kms_key_arn is set; with no block DynamoDB keeps
  # its default AWS-owned-key encryption, so the default plan stays a zero diff.
  dynamic "server_side_encryption" {
    for_each = var.kms_key_arn != "" ? [1] : []
    content {
      enabled     = true
      kms_key_arn = var.kms_key_arn
    }
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = merge(local.tags, {
    Name    = "${local.name_prefix}-feast-online"
    Purpose = "feast-online-store"
  })
}

# -----------------------------------------------------------------------------
# DynamoDB: Terraform state lock table
# -----------------------------------------------------------------------------

# Used by the optional S3 remote backend (see envs/base/backend.tf). The schema
# is fixed by Terraform: a single string hash key named "LockID".
resource "aws_dynamodb_table" "tf_state_lock" {
  name         = "${local.name_prefix}-tf-state-lock"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  # Same CMK-or-default switch as the Feast online table above.
  dynamic "server_side_encryption" {
    for_each = var.kms_key_arn != "" ? [1] : []
    content {
      enabled     = true
      kms_key_arn = var.kms_key_arn
    }
  }

  tags = merge(local.tags, {
    Name    = "${local.name_prefix}-tf-state-lock"
    Purpose = "terraform-state-lock"
  })
}

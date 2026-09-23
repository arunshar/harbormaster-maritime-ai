# modules/kinesis/main.tf
#
# Single-shard Kinesis Data Stream carrying raw AIS events (ais-raw). One shard
# (~1 MB/s in, 1000 records/s) comfortably covers the replay fixture and a modest
# live feed under the $75 cap. Server-side encryption uses the AWS-managed
# aws/kinesis key (no KMS spend). Retention is the 24h default.

variable "project" {
  type    = string
  default = "harbormaster"
}

variable "environment" {
  type = string
}

variable "shard_count" {
  type    = number
  default = 1
}

variable "retention_hours" {
  type    = number
  default = 24
}

variable "tags" {
  type    = map(string)
  default = {}
}

locals {
  name_prefix = "${var.project}-${var.environment}"
  tags        = merge(var.tags, { Module = "kinesis" })
}

resource "aws_kinesis_stream" "ais_raw" {
  name             = "${local.name_prefix}-ais-raw"
  shard_count      = var.shard_count
  retention_period = var.retention_hours

  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis"

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-ais-raw"
  })
}

output "stream_name" {
  value = aws_kinesis_stream.ais_raw.name
}

output "stream_arn" {
  value = aws_kinesis_stream.ais_raw.arn
}

# modules/sagemaker_pidpm
#
# The Pi-DPM async inference endpoint (Phase 3, gate 3.6). One model
# (single-region: Harbormaster's entire footprint is us-east-1, so the
# design's "per-region checkpoints" language simplifies here to one region,
# noted here and not built as unused multi-region scaffolding)
# behind a single named ProductionVariant ("champion"), fronted by an async
# inference EndpointConfig so the container is invoked via S3, not a
# synchronous HTTP call. Scale-to-zero is the AWS-documented two-part
# pattern for SageMaker async endpoints (target tracking alone cannot detect
# a 0->1 transition, since there is no running instance to measure a
# per-instance metric from):
#   (1) a target-tracking policy on ApproximateBacklogSizePerInstance
#       handles scale-out beyond 1 and, because min_capacity=0 is set on the
#       scalable target, scale-in all the way back to zero;
#   (2) a step-scaling policy + a CloudWatch alarm on HasBacklogWithoutCapacity
#       handles the 0->1 transition an idle endpoint cannot detect on its own.
# Whole-module gate at the envs/base call site (image AND model-artifact
# vars both non-empty), matching the ecs_connect/ecs_cdc_consumer
# image-gated convention: an apply before the container is built and the
# checkpoint is exported creates no half-configured endpoint.

variable "project" {
  type    = string
  default = "harbormaster"
}

variable "environment" {
  type = string
}

# Unused inside this module today (the provider region comes from the root),
# but envs/base passes aws_region to every regional module, so the input stays
# for interface uniformity rather than breaking the caller.
# tflint-ignore: terraform_unused_declarations
variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "container_image" {
  description = "ECR image URI wrapping the frozen PiDpmScorer.log_prob contract (mlops/pidpm_container/Dockerfile)."
  type        = string
}

variable "model_data_url" {
  description = "S3 URI to the exported checkpoint artifact (from mlops/manifest.py's one-way export)."
  type        = string
}

variable "candidate_model_data_url" {
  description = <<-EOT
    Reserved compatibility input for a challenger checkpoint. The current
    SageMaker Async Inference endpoint supports one production variant, so a
    non-empty value is rejected during planning. Candidate comparison needs a
    separately reviewed endpoint design. Same exported-artifact provenance as
    model_data_url (mlops/manifest.py).
  EOT
  type        = string
  default     = ""
}

variable "models_bucket_arn" {
  type = string
}

variable "instance_type" {
  description = "GPU instance for the Pi-DPM head (the platform's one deliberate non-CPU-serving component; ECS stays CPU throughout)."
  type        = string
  default     = "ml.g4dn.xlarge"
}

variable "max_concurrent_invocations_per_instance" {
  type    = number
  default = 4
}

variable "backlog_target_value" {
  description = "Target-tracking setpoint for ApproximateBacklogSizePerInstance."
  type        = number
  default     = 5
}

variable "enable_failure_alerts" {
  description = "Create the opt-in Pi-DPM failure alert route: EventBridge to SNS for async-failure artifacts plus the scoped InvocationFailures CloudWatch alarm."
  type        = bool
  default     = false
}

variable "failure_alert_email" {
  description = "Email endpoint for the dedicated Pi-DPM async-failure SNS topic. AWS requires recipient confirmation after apply."
  type        = string
  default     = ""

  validation {
    condition     = !var.enable_failure_alerts || trimspace(var.failure_alert_email) != ""
    error_message = "failure_alert_email must be non-empty when enable_failure_alerts is true."
  }
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable "permissions_boundary_arn" {
  description = "ARN of the IAM permissions boundary to attach to roles this module creates. Empty attaches no boundary. The harbormaster-platform deploy policy requires the harbormaster-permissions-boundary on every managed role (see war story P32, the two-sided contract), so envs/base sets this at apply time."
  type        = string
  default     = ""
}

locals {
  name_prefix   = "${var.project}-${var.environment}"
  tags          = merge(var.tags, { Module = "sagemaker_pidpm" })
  variant_name  = "champion"
  models_bucket = replace(var.models_bucket_arn, "arn:aws:s3:::", "")

  candidate_enabled = var.candidate_model_data_url != ""
}

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

data "aws_iam_policy_document" "sagemaker_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["sagemaker.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name                 = "${local.name_prefix}-pidpm-endpoint"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.sagemaker_assume.json
  tags                 = local.tags
}

data "aws_iam_policy_document" "execution" {
  statement {
    sid    = "ReadModelArtifactAndWriteAsyncIO"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket",
    ]
    resources = [
      var.models_bucket_arn,
      "${var.models_bucket_arn}/*",
    ]
  }

  statement {
    sid    = "PullContainerImage"
    effect = "Allow"
    actions = [
      "ecr:GetAuthorizationToken",
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "PublishAsyncInferenceMetricsAndLogs"
    effect = "Allow"
    actions = [
      "cloudwatch:PutMetricData",
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "execution" {
  name   = "${local.name_prefix}-pidpm-endpoint"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution.json
}

resource "aws_sagemaker_model" "pidpm" {
  name               = "${local.name_prefix}-pidpm"
  execution_role_arn = aws_iam_role.execution.arn

  primary_container {
    image          = var.container_image
    model_data_url = var.model_data_url
  }

  tags = local.tags
}

# The promotion registry (mlops/registry.py's register_candidate) calls
# create_model_package with a ModelPackageGroupName, which creates a
# VERSIONED model package; SageMaker requires that named group to already
# exist (create_model_package does not create it). Provisioning it here,
# not via a manual runbook CLI step, so it tears down with the rest of the
# module on enable_phase3=false, matching this repo's "guardrails/resources
# as code, not a checklist" convention (audit finding HM3-AUDIT-02, option (b)).
resource "aws_sagemaker_model_package_group" "pidpm" {
  model_package_group_name        = "${local.name_prefix}-pidpm"
  model_package_group_description = "Pi-DPM promotion approval group (Phase 3)"
  tags                            = local.tags
}

resource "aws_sagemaker_endpoint_configuration" "pidpm" {
  # Endpoint configurations are immutable.  A generated suffix lets Terraform
  # create the next configuration before it updates the live endpoint, then
  # delete the retired configuration only after the endpoint releases it.
  name_prefix = "${local.name_prefix}-pidpm-"

  lifecycle {
    create_before_destroy = true

    # SageMaker Async Inference permits exactly one production variant per
    # endpoint. Evaluate this at plan time so a candidate URI cannot create a
    # partial model before AWS rejects the immutable endpoint configuration.
    precondition {
      condition     = !local.candidate_enabled
      error_message = "SageMaker Async Inference supports one production variant per endpoint. Use a separately reviewed endpoint for candidate comparison; do not set candidate_model_data_url here."
    }
  }

  production_variants {
    variant_name           = local.variant_name
    model_name             = aws_sagemaker_model.pidpm.name
    instance_type          = var.instance_type
    initial_instance_count = 1
    initial_variant_weight = 1.0
  }

  async_inference_config {
    output_config {
      s3_output_path  = "s3://${local.models_bucket}/pidpm/async-output"
      s3_failure_path = "s3://${local.models_bucket}/pidpm/async-failure"
    }
    client_config {
      max_concurrent_invocations_per_instance = var.max_concurrent_invocations_per_instance
    }
  }

  tags = local.tags
}

resource "aws_sagemaker_endpoint" "pidpm" {
  name                 = "${local.name_prefix}-pidpm"
  endpoint_config_name = aws_sagemaker_endpoint_configuration.pidpm.name
  tags                 = local.tags
}

# ---- Async failure alerting -------------------------------------------------
#
# S3 supplies durable Object Created events to EventBridge when the root has
# enabled model-bucket EventBridge delivery. The rule forwards only metadata
# for the failure prefix to a dedicated topic. A second, tightly scoped
# CloudWatch alarm sends a notification when SageMaker reports an invocation
# failure for this endpoint variant. Neither path sends an input or output
# payload by email. The EventBridge source is at-least-once, so the operator
# runbook deduplicates artifact investigations by bucket, key, and object
# version.

resource "aws_sns_topic" "async_failure" {
  count = var.enable_failure_alerts ? 1 : 0

  name = "${local.name_prefix}-pidpm-async-failure"

  tags = merge(local.tags, {
    Name    = "${local.name_prefix}-pidpm-async-failure"
    Purpose = "pidpm-async-failure-alerts"
  })
}

resource "aws_sns_topic_subscription" "async_failure_email" {
  count = var.enable_failure_alerts ? 1 : 0

  topic_arn = aws_sns_topic.async_failure[0].arn
  protocol  = "email"
  endpoint  = var.failure_alert_email
}

# The default topic policy is not treated as an alarm-delivery contract. Keep
# owner administration within this account, then allow only this account's
# named CloudWatch alarm to publish as the CloudWatch service principal.
data "aws_iam_policy_document" "async_failure_topic" {
  count = var.enable_failure_alerts ? 1 : 0

  statement {
    sid       = "AllowAccountOwnerAdministration"
    effect    = "Allow"
    actions   = ["sns:*"]
    resources = [aws_sns_topic.async_failure[0].arn]

    principals {
      type        = "AWS"
      identifiers = ["arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }

  statement {
    sid       = "AllowOnlyPiDpmInvocationFailureAlarm"
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.async_failure[0].arn]

    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values = [
        "arn:${data.aws_partition.current.partition}:cloudwatch:${var.aws_region}:${data.aws_caller_identity.current.account_id}:alarm:${local.name_prefix}-pidpm-invocation-failures",
      ]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_sns_topic_policy" "async_failure" {
  count = var.enable_failure_alerts ? 1 : 0

  arn    = aws_sns_topic.async_failure[0].arn
  policy = data.aws_iam_policy_document.async_failure_topic[0].json
}

data "aws_iam_policy_document" "async_failure_eventbridge_assume" {
  count = var.enable_failure_alerts ? 1 : 0

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "async_failure_eventbridge" {
  count = var.enable_failure_alerts ? 1 : 0

  name                 = "${local.name_prefix}-pidpm-async-failure-events"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.async_failure_eventbridge_assume[0].json
  tags                 = local.tags
}

data "aws_iam_policy_document" "async_failure_eventbridge_publish" {
  count = var.enable_failure_alerts ? 1 : 0

  statement {
    sid       = "PublishPiDpmFailureAlerts"
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.async_failure[0].arn]
  }
}

resource "aws_iam_role_policy" "async_failure_eventbridge_publish" {
  count = var.enable_failure_alerts ? 1 : 0

  name   = "${local.name_prefix}-pidpm-async-failure-events"
  role   = aws_iam_role.async_failure_eventbridge[0].id
  policy = data.aws_iam_policy_document.async_failure_eventbridge_publish[0].json
}

resource "aws_cloudwatch_event_rule" "async_failure_artifact" {
  count = var.enable_failure_alerts ? 1 : 0

  name        = "${local.name_prefix}-pidpm-async-failure-artifact"
  description = "Route Pi-DPM asynchronous failure-artifact events to the dedicated SNS alert topic"

  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = {
        name = [local.models_bucket]
      }
      object = {
        key = [{ prefix = "pidpm/async-failure/" }]
      }
    }
  })

  tags = local.tags
}

resource "aws_cloudwatch_event_target" "async_failure_alert" {
  count = var.enable_failure_alerts ? 1 : 0

  rule      = aws_cloudwatch_event_rule.async_failure_artifact[0].name
  target_id = "pidpm-async-failure-sns"
  arn       = aws_sns_topic.async_failure[0].arn
  role_arn  = aws_iam_role.async_failure_eventbridge[0].arn

  depends_on = [
    aws_iam_role_policy.async_failure_eventbridge_publish,
    aws_sns_topic_policy.async_failure,
  ]
}

# Async endpoint metrics publish InvocationFailures with EndpointName and
# VariantName dimensions. A single failure is actionable during a declared
# operating window, so this candidate alarms on Sum >= 1 over one
# minute. A latency threshold is deliberately not added here: it requires the
# representative real-scorer measurements specified in the capacity protocol,
# rather than an invented microsecond value.
resource "aws_cloudwatch_metric_alarm" "invocation_failures" {
  count = var.enable_failure_alerts ? 1 : 0

  alarm_name          = "${local.name_prefix}-pidpm-invocation-failures"
  alarm_description   = "Pi-DPM async invocation failures for the champion variant"
  namespace           = "AWS/SageMaker"
  metric_name         = "InvocationFailures"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  datapoints_to_alarm = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.async_failure[0].arn]

  dimensions = {
    EndpointName = aws_sagemaker_endpoint.pidpm.name
    VariantName  = local.variant_name
  }

  tags = merge(local.tags, {
    Name    = "${local.name_prefix}-pidpm-invocation-failures"
    Purpose = "pidpm-async-invocation-failure-alerts"
  })

  depends_on = [aws_sns_topic_policy.async_failure]
}

# ---- Scale-to-zero: target tracking (1 <-> 0, and above 1) ----
#
# The sole async-inference champion variant scales between zero and one. A
# candidate comparison must use a separately reviewed endpoint design.

resource "aws_appautoscaling_target" "pidpm" {
  service_namespace  = "sagemaker"
  resource_id        = "endpoint/${aws_sagemaker_endpoint.pidpm.name}/variant/${local.variant_name}"
  scalable_dimension = "sagemaker:variant:DesiredInstanceCount"
  min_capacity       = 0
  max_capacity       = 1
}

resource "aws_appautoscaling_policy" "backlog_target_tracking" {
  name               = "${local.name_prefix}-pidpm-backlog-tracking"
  policy_type        = "TargetTrackingScaling"
  service_namespace  = aws_appautoscaling_target.pidpm.service_namespace
  resource_id        = aws_appautoscaling_target.pidpm.resource_id
  scalable_dimension = aws_appautoscaling_target.pidpm.scalable_dimension

  target_tracking_scaling_policy_configuration {
    target_value = var.backlog_target_value

    # ApproximateBacklogSizePerInstance is published per-endpoint under the
    # EndpointName dimension. Application Auto Scaling's own rule: if a
    # metric is published with dimensions, the policy must specify the same
    # ones, or it queries a metric series that doesn't exist. Without this,
    # the target-tracking alarms sit in INSUFFICIENT_DATA forever and
    # scale-in to zero never fires -- with initial_instance_count = 1 on
    # ml.g4dn.xlarge (~$0.74/hr, ~$530/mo), that's a standing GPU cost that
    # breaches the $75/mo cap. Caught by an external audit before this was
    # ever applied against real AWS; verified fixed in the live W2 window,
    # 2026-07-04.
    customized_metric_specification {
      metric_name = "ApproximateBacklogSizePerInstance"
      namespace   = "AWS/SageMaker"
      statistic   = "Average"

      dimensions {
        name  = "EndpointName"
        value = aws_sagemaker_endpoint.pidpm.name
      }
    }
  }
}

# ---- Scale-out from absolute zero (target tracking cannot see this transition) ----

resource "aws_appautoscaling_policy" "scale_out_from_zero" {
  name               = "${local.name_prefix}-pidpm-scale-out-from-zero"
  policy_type        = "StepScaling"
  service_namespace  = aws_appautoscaling_target.pidpm.service_namespace
  resource_id        = aws_appautoscaling_target.pidpm.resource_id
  scalable_dimension = aws_appautoscaling_target.pidpm.scalable_dimension

  step_scaling_policy_configuration {
    adjustment_type         = "ExactCapacity"
    cooldown                = 60
    metric_aggregation_type = "Maximum"

    step_adjustment {
      scaling_adjustment          = 1
      metric_interval_lower_bound = 0
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "has_backlog_without_capacity" {
  alarm_name  = "${local.name_prefix}-pidpm-has-backlog-without-capacity"
  namespace   = "AWS/SageMaker"
  metric_name = "HasBacklogWithoutCapacity"
  # EndpointName only, matching AWS's own canonical scale-from-zero example
  # (docs.aws.amazon.com/sagemaker/latest/dg/async-inference-autoscale.html)
  # exactly. An extra VariantName dimension here queries a metric series
  # AWS doesn't publish (unlike ApproximateBacklogSize, which SageMaker
  # does emit per-variant): the alarm sat with zero datapoints for 15+
  # minutes with real, persistent backlog the whole time, so the 0-to-1
  # scale-out from absolute zero would never have fired. A real, first-
  # live-run finding, W2 sprint window, 2026-07-04 -- caught live, not by
  # the prior audit (whose HM3-AUDIT-01 finding was the separate
  # target-tracking scale-IN policy's missing EndpointName dimension).
  dimensions = {
    EndpointName = aws_sagemaker_endpoint.pidpm.name
  }
  statistic           = "Average"
  period              = 60
  evaluation_periods  = 2
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_appautoscaling_policy.scale_out_from_zero.arn]
  tags                = local.tags
}

output "endpoint_name" {
  value = aws_sagemaker_endpoint.pidpm.name
}

output "model_name" {
  value = aws_sagemaker_model.pidpm.name
}

output "model_package_group_name" {
  value = aws_sagemaker_model_package_group.pidpm.model_package_group_name
}

output "candidate_model_name" {
  value = ""
}

output "failure_alert_topic_arn" {
  description = "Dedicated SNS topic for Pi-DPM async-failure artifacts, or null while disabled."
  value       = var.enable_failure_alerts ? aws_sns_topic.async_failure[0].arn : null
}

output "invocation_failure_alarm_name" {
  description = "CloudWatch alarm for Pi-DPM async invocation failures, or null while disabled."
  value       = var.enable_failure_alerts ? aws_cloudwatch_metric_alarm.invocation_failures[0].alarm_name : null
}

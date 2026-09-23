# modules/observability/main.tf
#
# Phase 1 observability (gate G7): a CloudWatch dashboard over the AWS-native
# metrics of the slice (ECS serving, API Gateway, Kinesis) plus two SLO alarms
# (API Gateway 5xx and p95 latency) wired to the budget SNS topic. The app-level
# $/inference and anomalies/min ship via CloudWatch EMF from the serving task
# (metrics.py / cost.py) and are added to the dashboard once EMF is enabled.

variable "project" {
  type    = string
  default = "harbormaster"
}

variable "environment" {
  type = string
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "api_id" {
  type = string
}

variable "cluster_name" {
  type = string
}

variable "service_name" {
  type = string
}

variable "kinesis_stream_name" {
  type = string
}

variable "sns_topic_arn" {
  type = string
}

variable "serving_metric_namespace" {
  description = "EMF namespace the serving task publishes score counts under (metrics.py / cost.py)."
  type        = string
  default     = "Harbormaster/Serving"
}

variable "score_success_target" {
  description = "The score-success SLO (serving/app/slo.py SCORE_SUCCESS_TARGET). Error budget = 1 - this."
  type        = number
  default     = 0.999
}

variable "tags" {
  type    = map(string)
  default = {}
}

locals {
  name_prefix = "${var.project}-${var.environment}"
  tags        = merge(var.tags, { Module = "observability" })

  # Error budget for the score-success SLO: the fraction of scores allowed to
  # fail. A burn rate of 1 exhausts this over the SLO window; the fast/slow
  # tiers below alarm when the OBSERVED failure ratio exceeds
  # (burn_rate x error_budget). This mirrors serving/app/burn_rate.py's
  # DEFAULT_TIERS exactly (14.4x fast, 6x slow), so the Terraform alarm and the
  # in-process calculator raise on the same signal.
  error_budget = 1.0 - var.score_success_target

  fast_burn_rate     = 14.4
  slow_burn_rate     = 6.0
  fast_fail_ratio    = local.fast_burn_rate * local.error_budget # 0.0144 at 99.9%
  slow_fail_ratio    = local.slow_burn_rate * local.error_budget # 0.006  at 99.9%
  fast_long_seconds  = 3600                                      # 1h
  fast_short_seconds = 300                                       # 5m
  slow_long_seconds  = 21600                                     # 6h
  slow_short_seconds = 1800                                      # 30m

  dashboard = {
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title  = "Serving CPU / running tasks"
          region = var.aws_region
          period = 60
          stat   = "Average"
          metrics = [
            ["AWS/ECS", "CPUUtilization", "ClusterName", var.cluster_name, "ServiceName", var.service_name],
            ["ECS/ContainerInsights", "RunningTaskCount", "ClusterName", var.cluster_name, "ServiceName", var.service_name],
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title  = "API Gateway: requests + p95 latency"
          region = var.aws_region
          period = 60
          metrics = [
            ["AWS/ApiGateway", "Count", "ApiId", var.api_id, { stat = "Sum" }],
            ["AWS/ApiGateway", "Latency", "ApiId", var.api_id, { stat = "p95" }],
          ]
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "API Gateway errors"
          region = var.aws_region
          period = 60
          stat   = "Sum"
          metrics = [
            ["AWS/ApiGateway", "5xx", "ApiId", var.api_id],
            ["AWS/ApiGateway", "4xx", "ApiId", var.api_id],
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "Kinesis ais-raw throughput"
          region = var.aws_region
          period = 60
          metrics = [
            ["AWS/Kinesis", "IncomingRecords", "StreamName", var.kinesis_stream_name, { stat = "Sum" }],
            ["AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds", "StreamName", var.kinesis_stream_name, { stat = "Maximum" }],
          ]
        }
      },
    ]
  }
}

resource "aws_cloudwatch_dashboard" "phase1" {
  dashboard_name = "${local.name_prefix}-phase1"
  dashboard_body = jsonencode(local.dashboard)
}

# SLO alarm: score-success 99.9% -> proxy on API Gateway 5xx over the demo window.
resource "aws_cloudwatch_metric_alarm" "api_5xx" {
  alarm_name          = "${local.name_prefix}-api-5xx"
  alarm_description   = "API Gateway 5xx >= 1 (score-success SLO 99.9%)"
  namespace           = "AWS/ApiGateway"
  metric_name         = "5xx"
  dimensions          = { ApiId = var.api_id }
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 5
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.sns_topic_arn]
  tags                = local.tags
}

# SLO alarm: kernel-path p95 < 300 ms -> API Gateway p95 latency.
resource "aws_cloudwatch_metric_alarm" "api_latency_p95" {
  alarm_name          = "${local.name_prefix}-api-latency-p95"
  alarm_description   = "API Gateway p95 latency > 300 ms (kernel-path SLO)"
  namespace           = "AWS/ApiGateway"
  metric_name         = "Latency"
  dimensions          = { ApiId = var.api_id }
  extended_statistic  = "p95"
  period              = 60
  evaluation_periods  = 5
  threshold           = 300
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.sns_topic_arn]
  tags                = local.tags
}

# --- Multi-window multi-burn-rate SLO alarms (DR-13) -----------------------
#
# The Google SRE multi-window pattern: a tier PAGES only when its long AND its
# short window both exceed the tier's failure-ratio threshold. The short window
# makes the alert reset fast once the outage stops; the long window keeps one
# bad minute from paging. Each tier is expressed as two window alarms combined
# by a composite AND, so no single-window blip pages on its own. These author
# the same fast (14.4x, 1h+5m) and slow (6x, 6h+30m) tiers the in-process
# calculator uses; authored only, not applied.
#
# Failure ratio per window is metric math over the EMF score counts:
#   m_bad   = ScoreFailures (Sum)
#   m_total = ScoreTotal    (Sum)
#   ratio   = m_bad / m_total     (guarded: 0 when no traffic -> never breaches)

locals {
  burn_windows = {
    fast_long  = { seconds = local.fast_long_seconds, threshold = local.fast_fail_ratio, tier = "fast" }
    fast_short = { seconds = local.fast_short_seconds, threshold = local.fast_fail_ratio, tier = "fast" }
    slow_long  = { seconds = local.slow_long_seconds, threshold = local.slow_fail_ratio, tier = "slow" }
    slow_short = { seconds = local.slow_short_seconds, threshold = local.slow_fail_ratio, tier = "slow" }
  }
}

resource "aws_cloudwatch_metric_alarm" "burn_window" {
  for_each = local.burn_windows

  alarm_name          = "${local.name_prefix}-slo-burn-${each.key}"
  alarm_description   = "score-success burn (${each.value.tier} tier, ${each.value.seconds}s window): failure ratio > ${each.value.threshold}"
  comparison_operator = "GreaterThanThreshold"
  threshold           = each.value.threshold
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching" # idle (no scores) is not a breach

  metric_query {
    id          = "ratio"
    expression  = "IF(m_total > 0, m_bad / m_total, 0)"
    label       = "score failure ratio (${each.value.seconds}s)"
    return_data = true
  }

  metric_query {
    id = "m_bad"
    metric {
      namespace   = var.serving_metric_namespace
      metric_name = "ScoreFailures"
      period      = each.value.seconds
      stat        = "Sum"
    }
  }

  metric_query {
    id = "m_total"
    metric {
      namespace   = var.serving_metric_namespace
      metric_name = "ScoreTotal"
      period      = each.value.seconds
      stat        = "Sum"
    }
  }

  tags = local.tags
}

# Fast burn PAGES only when the 1h AND 5m windows are both in ALARM.
resource "aws_cloudwatch_composite_alarm" "burn_fast" {
  alarm_name        = "${local.name_prefix}-slo-burn-fast-page"
  alarm_description = "Fast burn (14.4x over 1h AND 5m): budget exhausts in ~2h. Auto-rollback trigger (DR-3)."
  alarm_rule = join(" AND ", [
    "ALARM(${aws_cloudwatch_metric_alarm.burn_window["fast_long"].alarm_name})",
    "ALARM(${aws_cloudwatch_metric_alarm.burn_window["fast_short"].alarm_name})",
  ])
  alarm_actions = [var.sns_topic_arn]
  ok_actions    = [var.sns_topic_arn]
  tags          = local.tags
}

# Slow burn PAGES only when the 6h AND 30m windows are both in ALARM.
resource "aws_cloudwatch_composite_alarm" "burn_slow" {
  alarm_name        = "${local.name_prefix}-slo-burn-slow-page"
  alarm_description = "Slow burn (6x over 6h AND 30m): budget exhausts in ~5d. Auto-rollback trigger (DR-3)."
  alarm_rule = join(" AND ", [
    "ALARM(${aws_cloudwatch_metric_alarm.burn_window["slow_long"].alarm_name})",
    "ALARM(${aws_cloudwatch_metric_alarm.burn_window["slow_short"].alarm_name})",
  ])
  alarm_actions = [var.sns_topic_arn]
  ok_actions    = [var.sns_topic_arn]
  tags          = local.tags
}

output "dashboard_name" {
  value = aws_cloudwatch_dashboard.phase1.dashboard_name
}

output "burn_fast_alarm_name" {
  value = aws_cloudwatch_composite_alarm.burn_fast.alarm_name
}

output "burn_slow_alarm_name" {
  value = aws_cloudwatch_composite_alarm.burn_slow.alarm_name
}

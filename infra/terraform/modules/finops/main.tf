# modules/finops/main.tf
#
# FinOps cost guardrails for Harbormaster. Built BEFORE any spend so the
# platform has layered controls against accidental W4 spend:
#
#   1. SNS topic + email subscription for all cost alerts.
#   2. $30 SOFT budget: SNS alerts at $5/$15/$25 ACTUAL and $30 FORECASTED.
#   3. $75 HARD budget: a budget action that attaches an IAM deny policy to the
#      platform role on breach, freezing creation of expensive resources.
#   4. Cost Explorer anomaly monitor + subscription routed to SNS.
#   5. Nightly teardown Lambda (with least-privilege role) triggered by an
#      EventBridge schedule, which stops/terminates lingering streaming/EMR/MSK
#      workloads so nothing runs overnight by accident.
#
# Notes:
#   - AWS Budgets and Cost Explorer are global services billed/reported in
#     us-east-1; budgets work regardless of the provider region, and the
#     thresholds here use absolute USD per the spec.

locals {
  name_prefix = "${var.project}-${var.environment}"

  tags = merge(var.tags, {
    Module = "finops"
  })
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

# -----------------------------------------------------------------------------
# 1. SNS topic + email subscription
# -----------------------------------------------------------------------------

resource "aws_sns_topic" "budget_alerts" {
  name = "${local.name_prefix}-budget-alerts"

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-budget-alerts"
  })
}

# Allow AWS Budgets and Cost Explorer (cost anomaly detection) to publish.
data "aws_iam_policy_document" "sns_topic_policy" {
  statement {
    sid    = "AllowBudgetsPublish"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["budgets.amazonaws.com"]
    }

    actions   = ["SNS:Publish"]
    resources = [aws_sns_topic.budget_alerts.arn]
  }

  statement {
    sid    = "AllowCostAnomalyPublish"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["costalerts.amazonaws.com"]
    }

    actions   = ["SNS:Publish"]
    resources = [aws_sns_topic.budget_alerts.arn]
  }
}

resource "aws_sns_topic_policy" "budget_alerts" {
  arn    = aws_sns_topic.budget_alerts.arn
  policy = data.aws_iam_policy_document.sns_topic_policy.json
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.budget_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# -----------------------------------------------------------------------------
# 2. $30 SOFT budget: SNS-only alerts
# -----------------------------------------------------------------------------

resource "aws_budgets_budget" "soft" {
  name         = "${local.name_prefix}-soft-30"
  budget_type  = "COST"
  limit_amount = tostring(var.soft_budget_amount)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # ACTUAL-spend alerts at absolute dollar thresholds. Budgets notifications use
  # a percentage of the limit, so each absolute USD target is expressed as a
  # percentage of the $30 limit (5 -> 16.67%, 15 -> 50%, 25 -> 83.33%).
  dynamic "notification" {
    for_each = var.soft_actual_thresholds_usd
    content {
      comparison_operator       = "GREATER_THAN"
      threshold                 = (notification.value / var.soft_budget_amount) * 100
      threshold_type            = "PERCENTAGE"
      notification_type         = "ACTUAL"
      subscriber_sns_topic_arns = [aws_sns_topic.budget_alerts.arn]
    }
  }

  # FORECASTED alert at the $30 limit (100%).
  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = (var.soft_forecast_threshold_usd / var.soft_budget_amount) * 100
    threshold_type            = "PERCENTAGE"
    notification_type         = "FORECASTED"
    subscriber_sns_topic_arns = [aws_sns_topic.budget_alerts.arn]
  }

  tags = local.tags
}

# -----------------------------------------------------------------------------
# 3. $75 HARD budget + IAM deny action
# -----------------------------------------------------------------------------

# The deny policy the budget action attaches to the platform role on breach.
# Blocks creation of the expensive resource types; leaves read/describe/delete
# intact so you can still inspect and tear down.
data "aws_iam_policy_document" "spend_freeze" {
  statement {
    sid    = "DenyExpensiveResourceCreation"
    effect = "Deny"

    actions = [
      "ec2:RunInstances",
      "ec2:CreateFleet",
      "ec2:RequestSpotFleet",
      "ec2:RequestSpotInstances",
      "ec2:CreateCapacityReservation",
      "ec2:CreateCapacityReservationFleet",
      "ec2:AllocateHosts",
      "ec2:PurchaseHostReservation",
      "ec2:PurchaseReservedInstancesOffering",
      "ec2:AllocateAddress",
      "ec2:CreateNatGateway",
      "ec2:CreateVpcEndpoint",
      "eks:CreateCluster",
      "eks:CreateFargateProfile",
      "eks:CreateNodegroup",
      "eks:UpdateNodegroupConfig",
      "sagemaker:Create*",
      "kinesis:Create*",
      "kinesisanalytics:CreateApplication",
      "kinesisanalytics:StartApplication",
      "kinesisanalytics:UpdateApplication",
      "emr-serverless:CreateApplication",
      "emr-serverless:StartApplication",
      "emr-serverless:StartJobRun",
      "elasticmapreduce:RunJobFlow",
      "kafka:Create*",
      "rds:Create*",
      "redshift:Create*",
      "elasticloadbalancing:CreateLoadBalancer",
      "autoscaling:CreateAutoScalingGroup",
      "autoscaling:SetDesiredCapacity",
      "autoscaling:UpdateAutoScalingGroup",
    ]

    resources = ["*"]
  }
}

resource "aws_iam_policy" "spend_freeze" {
  name        = "${local.name_prefix}-spend-freeze"
  description = "Deny policy attached to the platform role by the $${var.hard_budget_amount} budget action on breach."
  policy      = data.aws_iam_policy_document.spend_freeze.json

  tags = local.tags
}

# IAM role the Budgets service assumes to execute the budget action.
data "aws_iam_policy_document" "budget_action_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["budgets.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values = [
        "arn:${data.aws_partition.current.partition}:budgets::${data.aws_caller_identity.current.account_id}:budget/${local.name_prefix}-hard-75",
      ]
    }
  }
}

resource "aws_iam_role" "budget_action" {
  name                 = "${local.name_prefix}-budget-action"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.budget_action_assume.json

  tags = local.tags
}

# The execution role must be allowed to attach the deny policy to the target
# role. Scope the attach/detach to exactly the platform role.
data "aws_iam_policy_document" "budget_action_exec" {
  statement {
    sid    = "AttachSpendFreezeToPlatformRole"
    effect = "Allow"

    actions = [
      "iam:AttachRolePolicy",
      "iam:DetachRolePolicy",
    ]

    resources = [
      "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/${var.platform_role_name}",
    ]

    condition {
      test     = "ArnEquals"
      variable = "iam:PolicyARN"
      values   = [aws_iam_policy.spend_freeze.arn]
    }
  }
}

resource "aws_iam_role_policy" "budget_action_exec" {
  name   = "${local.name_prefix}-budget-action-exec"
  role   = aws_iam_role.budget_action.id
  policy = data.aws_iam_policy_document.budget_action_exec.json
}

resource "aws_budgets_budget" "hard" {
  name         = "${local.name_prefix}-hard-75"
  budget_type  = "COST"
  limit_amount = tostring(var.hard_budget_amount)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # A notification on the hard budget so the alert email also fires at breach.
  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 100
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_sns_topic_arns = [aws_sns_topic.budget_alerts.arn]
  }

  tags = local.tags
}

# The action: when ACTUAL spend exceeds 100% of the $75 limit, attach the deny
# policy to the platform role. approval_model APPROVE_ASSUME_ROLE = automatic.
resource "aws_budgets_budget_action" "spend_freeze" {
  budget_name        = aws_budgets_budget.hard.name
  action_type        = "APPLY_IAM_POLICY"
  approval_model     = "AUTOMATIC"
  execution_role_arn = aws_iam_role.budget_action.arn
  notification_type  = "ACTUAL"

  action_threshold {
    action_threshold_type  = "PERCENTAGE"
    action_threshold_value = 100
  }

  definition {
    iam_action_definition {
      policy_arn = aws_iam_policy.spend_freeze.arn
      roles      = [var.platform_role_name]
    }
  }

  subscriber {
    address           = aws_sns_topic.budget_alerts.arn
    subscription_type = "SNS"
  }

  subscriber {
    address           = var.alert_email
    subscription_type = "EMAIL"
  }

  tags = local.tags

  depends_on = [
    aws_iam_role_policy.budget_action_exec,
    aws_sns_topic_policy.budget_alerts,
  ]
}

# -----------------------------------------------------------------------------
# 4. Cost Explorer anomaly monitor + subscription
# -----------------------------------------------------------------------------

resource "aws_ce_anomaly_monitor" "service" {
  # AWS permits only one DIMENSIONAL SERVICE monitor per account. Skip creating
  # ours when an existing monitor ARN is supplied (e.g. AWS' auto-created
  # Default-Services-Monitor) and attach the subscription to that instead.
  count = var.existing_cost_anomaly_monitor_arn == "" ? 1 : 0

  name              = "${local.name_prefix}-anomaly-monitor"
  monitor_type      = "DIMENSIONAL"
  monitor_dimension = "SERVICE"

  tags = local.tags
}

resource "aws_ce_anomaly_subscription" "service" {
  name      = "${local.name_prefix}-anomaly-subscription"
  frequency = "IMMEDIATE"

  monitor_arn_list = [coalesce(var.existing_cost_anomaly_monitor_arn, one(aws_ce_anomaly_monitor.service[*].arn))]

  subscriber {
    type    = "SNS"
    address = aws_sns_topic.budget_alerts.arn
  }

  # Only notify when the dollar impact crosses the threshold, so $0.10 blips stay
  # quiet. Uses the newer expression form (ANOMALY_TOTAL_IMPACT_ABSOLUTE).
  threshold_expression {
    dimension {
      key           = "ANOMALY_TOTAL_IMPACT_ABSOLUTE"
      match_options = ["GREATER_THAN_OR_EQUAL"]
      values        = [tostring(var.anomaly_threshold_usd)]
    }
  }

  tags = local.tags

  depends_on = [aws_sns_topic_policy.budget_alerts]
}

# -----------------------------------------------------------------------------
# 5. Nightly teardown Lambda + IAM role + EventBridge schedule
# -----------------------------------------------------------------------------

# Zip the Lambda source at plan time. The source lives outside the module, the
# caller passes lambda_source_dir (see envs/base/main.tf).
data "archive_file" "teardown" {
  type        = "zip"
  source_dir  = var.lambda_source_dir
  output_path = "${path.module}/.build/teardown-${var.environment}.zip"
}

# Lambda execution role: assume by Lambda only.
data "aws_iam_policy_document" "teardown_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "teardown" {
  name                 = "${local.name_prefix}-teardown"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.teardown_assume.json

  tags = local.tags
}

# Least-privilege policy: write its own logs, read Cost Explorer, and
# describe/stop/terminate the streaming and batch services that cost money.
data "aws_iam_policy_document" "teardown" {
  statement {
    sid    = "Logs"
    effect = "Allow"

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]

    resources = ["arn:${data.aws_partition.current.partition}:logs:*:${data.aws_caller_identity.current.account_id}:*"]
  }

  statement {
    sid    = "CostExplorerRead"
    effect = "Allow"

    actions = [
      "ce:GetCostAndUsage",
      "ce:GetAnomalies",
    ]

    resources = ["*"]
  }

  # Managed Flink (Kinesis Data Analytics v2): describe + stop running apps.
  statement {
    sid    = "FlinkDescribeStop"
    effect = "Allow"

    actions = [
      "kinesisanalytics:ListApplications",
      "kinesisanalytics:ListTagsForResource",
      "kinesisanalytics:DescribeApplication",
      "kinesisanalytics:StopApplication",
    ]

    resources = ["*"]
  }

  # EMR Serverless: list + cancel jobs + stop applications.
  statement {
    sid    = "EmrServerlessDescribeStop"
    effect = "Allow"

    actions = [
      "emr-serverless:ListApplications",
      "emr-serverless:GetApplication",
      "emr-serverless:ListJobRuns",
      "emr-serverless:CancelJobRun",
      "emr-serverless:StopApplication",
    ]

    resources = ["*"]
  }

  # EMR on EC2: list + terminate clusters.
  statement {
    sid    = "EmrClassicDescribeTerminate"
    effect = "Allow"

    actions = [
      "elasticmapreduce:ListClusters",
      "elasticmapreduce:DescribeCluster",
      "elasticmapreduce:TerminateJobFlows",
    ]

    resources = ["*"]
  }

  # MSK: describe clusters (no managed "stop"; teardown deletes provisioned
  # clusters, which is destructive and gated by dry-run in the handler).
  statement {
    sid    = "MskDescribeDelete"
    effect = "Allow"

    actions = [
      "kafka:ListClusters",
      "kafka:ListClustersV2",
      "kafka:DescribeCluster",
      "kafka:DescribeClusterV2",
      "kafka:ListTagsForResource",
      "kafka:DeleteCluster",
    ]

    resources = ["*"]
  }

  statement {
    sid    = "AutoScalingDescribeZero"
    effect = "Allow"

    # The handler drains tagged Auto Scaling Groups (desired/min -> 0). Describe
    # has no resource-level scope, so it must be "*"; the handler filters by the
    # Project tag at runtime.
    actions = [
      "autoscaling:DescribeAutoScalingGroups",
      "autoscaling:UpdateAutoScalingGroup",
    ]

    resources = ["*"]
  }

  statement {
    sid    = "NetworkLoadBalancerDescribe"
    effect = "Allow"

    actions = [
      "elasticloadbalancing:DescribeLoadBalancers",
      "elasticloadbalancing:DescribeTags",
    ]

    resources = ["*"]
  }

  statement {
    sid    = "DeleteTaggedEksFrontdoorNetworkLoadBalancer"
    effect = "Allow"

    actions = ["elasticloadbalancing:DeleteLoadBalancer"]

    resources = [
      "arn:${data.aws_partition.current.partition}:elasticloadbalancing:${var.aws_region}:${data.aws_caller_identity.current.account_id}:loadbalancer/net/${local.name_prefix}-eks/*",
    ]

    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Project"
      values   = [var.project]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Environment"
      values   = [var.environment]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/Module"
      values   = ["eks_frontdoor"]
    }
  }

  statement {
    sid    = "NatGatewayAndElasticIpDescribe"
    effect = "Allow"

    actions = [
      "ec2:DescribeAddresses",
      "ec2:DescribeNatGateways",
    ]

    resources = ["*"]
  }

  statement {
    sid       = "DeleteTaggedNetworkNatGateway"
    effect    = "Allow"
    actions   = ["ec2:DeleteNatGateway"]
    resources = ["arn:${data.aws_partition.current.partition}:ec2:${var.aws_region}:${data.aws_caller_identity.current.account_id}:natgateway/*"]

    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/Project"
      values   = [var.project]
    }

    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/Environment"
      values   = [var.environment]
    }

    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/Module"
      values   = ["network"]
    }
  }

  statement {
    sid       = "ReleaseTaggedNetworkElasticIp"
    effect    = "Allow"
    actions   = ["ec2:ReleaseAddress"]
    resources = ["arn:${data.aws_partition.current.partition}:ec2:${var.aws_region}:${data.aws_caller_identity.current.account_id}:elastic-ip/*"]

    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/Project"
      values   = [var.project]
    }

    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/Environment"
      values   = [var.environment]
    }

    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/Module"
      values   = ["network"]
    }
  }

  statement {
    sid    = "SnsPublish"
    effect = "Allow"

    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.budget_alerts.arn]
  }
}

resource "aws_iam_role_policy" "teardown" {
  name   = "${local.name_prefix}-teardown"
  role   = aws_iam_role.teardown.id
  policy = data.aws_iam_policy_document.teardown.json
}

resource "aws_lambda_function" "teardown" {
  function_name = "${local.name_prefix}-teardown"
  role          = aws_iam_role.teardown.arn
  handler       = "handler.lambda_handler"
  runtime       = var.lambda_runtime
  timeout       = var.lambda_timeout_seconds

  filename         = data.archive_file.teardown.output_path
  source_code_hash = data.archive_file.teardown.output_base64sha256

  # Env-var names match the handler's contract exactly: handler.py reads
  # DRY_RUN, PROJECT_TAG and ENVIRONMENT (the tag values that scope teardown
  # actions), and ALERT_TOPIC_ARN (where it publishes its summary).
  environment {
    variables = {
      DRY_RUN         = tostring(var.teardown_dry_run)
      PROJECT_TAG     = var.project
      ALERT_TOPIC_ARN = aws_sns_topic.budget_alerts.arn
      ENVIRONMENT     = var.environment
    }
  }

  tags = local.tags
}

# EventBridge Scheduler schedule for the nightly sweep, gated by the toggle.
resource "aws_scheduler_schedule" "nightly_teardown" {
  count = var.enable_nightly_teardown ? 1 : 0

  name = "${local.name_prefix}-nightly-teardown"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.teardown_schedule_expression
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_lambda_function.teardown.arn
    role_arn = aws_iam_role.scheduler.arn
  }
}

# Role EventBridge Scheduler assumes to invoke the Lambda.
data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name                 = "${local.name_prefix}-teardown-scheduler"
  permissions_boundary = var.permissions_boundary_arn != "" ? var.permissions_boundary_arn : null
  assume_role_policy   = data.aws_iam_policy_document.scheduler_assume.json

  tags = local.tags
}

data "aws_iam_policy_document" "scheduler_invoke" {
  statement {
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.teardown.arn]
  }
}

resource "aws_iam_role_policy" "scheduler_invoke" {
  name   = "${local.name_prefix}-teardown-scheduler-invoke"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler_invoke.json
}

# Explicit permission allowing EventBridge Scheduler to invoke the function.
resource "aws_lambda_permission" "allow_scheduler" {
  count = var.enable_nightly_teardown ? 1 : 0

  statement_id  = "AllowExecutionFromScheduler"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.teardown.function_name
  principal     = "scheduler.amazonaws.com"
  source_arn    = aws_scheduler_schedule.nightly_teardown[0].arn
}

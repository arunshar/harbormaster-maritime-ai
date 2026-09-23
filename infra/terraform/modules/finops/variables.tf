# modules/finops/variables.tf

variable "project" {
  description = "Project name, used in tags and resource names."
  type        = string
  default     = "harbormaster"

  validation {
    condition     = var.project == "harbormaster"
    error_message = "project must be harbormaster because the permissions boundary scopes FinOps actions to Harbormaster names."
  }
}

variable "environment" {
  description = "Deployment environment: base or demo."
  type        = string

  validation {
    condition     = contains(["base", "demo"], var.environment)
    error_message = "environment must be one of: base, demo."
  }
}

# Unused inside this module today (the provider region comes from the root),
# but envs/base passes aws_region to every regional module, so the input stays
# for interface uniformity rather than breaking the caller.
# tflint-ignore: terraform_unused_declarations
variable "aws_region" {
  description = "AWS region for the teardown Lambda and EventBridge schedule."
  type        = string
  default     = "us-east-1"
}

variable "alert_email" {
  description = "Email address subscribed to the SNS budget-alert topic. AWS sends a confirmation link that must be clicked once."
  type        = string
}

variable "platform_role_name" {
  description = <<-EOT
    Name of the IAM role that the $75 hard-budget action attaches a deny policy
    to when breached. This should be the role your platform/CI assumes to create
    spend-incurring resources, NOT your break-glass admin identity. On breach the
    deny policy is applied to this role, freezing new expensive resource creation
    until you intervene.
  EOT
  type        = string

  validation {
    condition     = var.platform_role_name == "harbormaster-platform"
    error_message = "platform_role_name must be harbormaster-platform because the permissions boundary grants the budget action only on that role."
  }
}

variable "soft_budget_amount" {
  description = "Soft monthly budget in USD that drives SNS alerts."
  type        = number
  default     = 30
}

variable "hard_budget_amount" {
  description = "Hard monthly budget in USD. Breaching ACTUAL spend triggers the IAM deny action."
  type        = number
  default     = 75
}

variable "soft_actual_thresholds_usd" {
  description = "Absolute USD ACTUAL-spend thresholds on the soft budget that each fire an SNS alert."
  type        = list(number)
  default     = [5, 15, 25]
}

variable "soft_forecast_threshold_usd" {
  description = "Absolute USD FORECASTED-spend threshold on the soft budget that fires an SNS alert."
  type        = number
  default     = 30
}

variable "anomaly_threshold_usd" {
  description = "Dollar impact above which a Cost Explorer anomaly notifies the SNS topic."
  type        = number
  default     = 10
}

variable "existing_cost_anomaly_monitor_arn" {
  description = <<-EOT
    ARN of an existing Cost Explorer DIMENSIONAL SERVICE anomaly monitor to reuse.
    AWS allows only one such monitor per account and auto-creates a
    "Default-Services-Monitor" for many accounts, so creating a second fails with
    "Limit exceeded on dimensional spend monitor creation". Leave empty to create
    one; set to an existing monitor ARN to attach the subscription to it instead.
  EOT
  type        = string
  default     = ""
}

variable "enable_nightly_teardown" {
  description = <<-EOT
    Whether to create the EventBridge schedule that invokes the teardown Lambda
    nightly. The Lambda itself and its role are always created so it can be
    invoked manually; only the recurring trigger is gated. Default true so an
    idle account never runs a forgotten streaming job overnight.
  EOT
  type        = bool
  default     = true
}

variable "teardown_schedule_expression" {
  description = "EventBridge Scheduler expression for the nightly teardown sweep (UTC). Default 07:00 UTC, roughly overnight in US Central."
  type        = string
  default     = "cron(0 7 * * ? *)"
}

variable "teardown_dry_run" {
  description = "When true, the teardown Lambda logs what it would stop/terminate but takes no destructive action. Set false to actually tear down."
  type        = bool
  default     = true
}

variable "lambda_source_dir" {
  description = "Path to the teardown Lambda source directory, zipped by archive_file at plan time."
  type        = string
}

variable "lambda_runtime" {
  description = "Python runtime for the teardown Lambda."
  type        = string
  default     = "python3.12"
}

variable "lambda_timeout_seconds" {
  description = "Teardown Lambda timeout in seconds."
  type        = number
  default     = 120
}

variable "permissions_boundary_arn" {
  description = "ARN of the IAM permissions boundary to attach to roles this module creates. Empty attaches no boundary. The harbormaster-platform deploy policy requires the harbormaster-permissions-boundary on every managed role (see war story P32, the two-sided contract), so envs/base sets this at apply time."
  type        = string
  default     = ""
}

variable "tags" {
  description = "Common tags applied to every resource."
  type        = map(string)
  default     = {}
}

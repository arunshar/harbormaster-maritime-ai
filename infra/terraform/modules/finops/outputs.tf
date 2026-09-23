# modules/finops/outputs.tf

output "sns_topic_arn" {
  description = "ARN of the SNS topic that receives all budget and anomaly alerts."
  value       = aws_sns_topic.budget_alerts.arn
}

output "soft_budget_name" {
  description = "Name of the $30 soft budget."
  value       = aws_budgets_budget.soft.name
}

output "hard_budget_name" {
  description = "Name of the $75 hard budget."
  value       = aws_budgets_budget.hard.name
}

output "spend_freeze_policy_arn" {
  description = "ARN of the IAM deny policy attached to the platform role on hard-budget breach."
  value       = aws_iam_policy.spend_freeze.arn
}

output "budget_action_id" {
  description = "ID of the $75 budget action that applies the spend-freeze policy."
  value       = aws_budgets_budget_action.spend_freeze.action_id
}

output "anomaly_monitor_arn" {
  description = "ARN of the Cost Explorer anomaly monitor the subscription is attached to (created or reused)."
  value       = coalesce(var.existing_cost_anomaly_monitor_arn, one(aws_ce_anomaly_monitor.service[*].arn))
}

output "teardown_lambda_name" {
  description = "Name of the nightly teardown Lambda function."
  value       = aws_lambda_function.teardown.function_name
}

output "teardown_lambda_arn" {
  description = "ARN of the nightly teardown Lambda function."
  value       = aws_lambda_function.teardown.arn
}

output "teardown_schedule_name" {
  description = "Name of the EventBridge schedule, or null when nightly teardown is disabled."
  value       = var.enable_nightly_teardown ? aws_scheduler_schedule.nightly_teardown[0].name : null
}

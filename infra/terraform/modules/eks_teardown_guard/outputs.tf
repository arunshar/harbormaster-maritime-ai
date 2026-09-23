# modules/eks_teardown_guard/outputs.tf

output "function_name" {
  description = "Name of the guard Lambda function. W4 proves it through its Scheduler target, not a direct manual invocation."
  value       = aws_lambda_function.guard.function_name
}

output "function_arn" {
  description = "ARN of the guard Lambda function."
  value       = aws_lambda_function.guard.arn
}

output "schedule_name" {
  description = "Name of the recurring EventBridge Scheduler schedule."
  value       = aws_scheduler_schedule.guard.name
}

output "guarded_cluster_name" {
  description = "The cluster name the guard evaluates (deterministic, shared with modules/eks_cluster via envs/base)."
  value       = local.cluster_name
}

# modules/apigw/outputs.tf

output "api_endpoint" {
  description = "Invoke URL for the serving HTTP API (POST /v1/score-ais)."
  value       = aws_apigatewayv2_stage.default.invoke_url
}

output "api_id" {
  value = aws_apigatewayv2_api.this.id
}

output "api_execution_arn" {
  description = "Execution ARN used to scope SigV4 invoke permission for the Managed Flink caller."
  value       = aws_apigatewayv2_api.this.execution_arn
}

output "access_log_group_name" {
  description = "CloudWatch log group receiving access logs (null when disabled)."
  value       = var.enable_access_logging ? aws_cloudwatch_log_group.access[0].name : null
}

output "waf_web_acl_arn" {
  description = "ARN of the WAF web ACL associated with the stage (null when disabled)."
  value       = var.enable_waf ? aws_wafv2_web_acl.this[0].arn : null
}

# modules/eks_frontdoor/outputs.tf

output "listener_arn" {
  description = "Internal NLB listener ARN consumed by the API Gateway private integration."
  value       = aws_lb_listener.serving.arn
}

output "load_balancer_arn" {
  description = "Internal serving Network Load Balancer ARN."
  value       = aws_lb.serving.arn
}

output "load_balancer_dns_name" {
  description = "Internal serving Network Load Balancer DNS name."
  value       = aws_lb.serving.dns_name
}

output "target_group_arn" {
  description = "Serving instance target group ARN."
  value       = aws_lb_target_group.serving.arn
}

output "security_group_id" {
  description = "Security group attached to the internal NLB."
  value       = aws_security_group.nlb.id
}

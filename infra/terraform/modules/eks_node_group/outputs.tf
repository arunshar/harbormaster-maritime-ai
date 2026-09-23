# modules/eks_node_group/outputs.tf

output "node_group_name" {
  description = "Name of the spot node group."
  value       = aws_eks_node_group.this.node_group_name
}

output "node_group_arn" {
  description = "ARN of the spot node group."
  value       = aws_eks_node_group.this.arn
}

output "node_group_status" {
  description = "Lifecycle status of the node group."
  value       = aws_eks_node_group.this.status
}

output "autoscaling_group_name" {
  description = "Underlying managed-node-group Auto Scaling group, consumed by the Terraform-owned NLB target-group attachment."
  value       = aws_eks_node_group.this.resources[0].autoscaling_groups[0].name
}

output "node_security_group_id" {
  description = "Dedicated worker security group used by the NLB NodePort rule."
  value       = aws_security_group.node.id
}

output "launch_template_id" {
  description = "Custom worker launch template that prevents EKS from adding the cluster security group."
  value       = aws_launch_template.node.id
}

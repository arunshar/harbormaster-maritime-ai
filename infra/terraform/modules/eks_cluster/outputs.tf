# modules/eks_cluster/outputs.tf

output "cluster_name" {
  description = "Name of the EKS cluster (deterministic; matches the teardown guard's watched name)."
  value       = aws_eks_cluster.this.name
}

output "cluster_arn" {
  description = "ARN of the EKS cluster."
  value       = aws_eks_cluster.this.arn
}

output "cluster_endpoint" {
  description = "API server endpoint (private unless endpoint_public_access was flipped for a demo window)."
  value       = aws_eks_cluster.this.endpoint
}

output "cluster_certificate_authority_data" {
  description = "Base64-encoded cluster CA bundle."
  value       = aws_eks_cluster.this.certificate_authority[0].data
}

output "cluster_security_group_id" {
  description = "The EKS-managed cluster security group id."
  value       = aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
}

output "control_plane_security_group_id" {
  description = "Terraform-owned bridge security group attached to EKS control-plane ENIs."
  value       = aws_security_group.control_plane.id
}

output "node_role_arn" {
  description = "IAM role ARN for worker nodes, consumed by modules/eks_node_group."
  value       = aws_iam_role.node.arn

  # CreateNodegroup must not race the eventual consistency of its three
  # required managed-policy attachments.
  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr_read,
  ]
}

output "kms_key_arn" {
  description = "Module-local CMK encrypting EKS secrets and the control-plane log group."
  value       = aws_kms_key.eks.arn
}

output "keda_operator_role_arn" {
  description = "IRSA role used by the KEDA operator to read its CloudWatch scaling metric."
  value       = aws_iam_role.keda_operator.arn
}

output "oidc_provider_arn" {
  description = "IAM OIDC provider backing KEDA IRSA."
  value       = aws_iam_openid_connect_provider.eks.arn
}

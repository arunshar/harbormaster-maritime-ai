# modules/eks_node_group/variables.tf

variable "project" {
  description = "Project name, used in tags and resource names."
  type        = string
  default     = "harbormaster"
}

variable "environment" {
  description = "Deployment environment: base or demo."
  type        = string

  validation {
    condition     = contains(["base", "demo"], var.environment)
    error_message = "environment must be one of: base, demo."
  }
}

variable "cluster_name" {
  description = "Name of the EKS cluster this node group joins (module.eks_cluster[0].cluster_name, which also orders creation after the control plane)."
  type        = string
}

variable "node_role_arn" {
  description = "Worker-node IAM role ARN (module.eks_cluster[0].node_role_arn; the role lives with the rest of the EKS IAM surface)."
  type        = string
}

variable "private_subnet_ids" {
  description = "Phase 1 VPC private subnets the nodes launch into."
  type        = list(string)
}

variable "vpc_id" {
  description = "Phase 1 VPC that contains the worker nodes."
  type        = string
}

variable "vpc_cidr" {
  description = "Phase 1 VPC CIDR used for DNS and optional private data-service egress."
  type        = string
}

variable "control_plane_security_group_id" {
  description = "Terraform-owned bridge security group attached to EKS control-plane ENIs."
  type        = string
}

variable "instance_types" {
  description = "Spot instance-type candidates. Two families so a single spot pool interruption cannot starve the group."
  type        = list(string)
  default     = ["t3.medium", "t3a.medium"]
}

variable "ami_type" {
  description = "EKS-optimized Amazon Linux 2023 AMI family. Kubernetes 1.34 and later do not publish new Amazon Linux 2 EKS AMIs."
  type        = string
  default     = "AL2023_x86_64_STANDARD"
}

variable "disk_size" {
  description = "Node root volume size in GiB."
  type        = number
  default     = 20
}

variable "min_size" {
  description = "Scale-to-zero floor. 0 is the point of the phase (the GKE min_node_count=0 pattern); raise only for a demo that cannot tolerate cold node starts."
  type        = number
  default     = 0

  validation {
    condition     = var.min_size >= 0
    error_message = "min_size must be >= 0."
  }
}

variable "max_size" {
  description = "Node ceiling. W4 is deliberately single-node because no node autoscaler or untrusted in-cluster workload exists in this bounded demo."
  type        = number
  default     = 1

  validation {
    condition     = var.max_size >= var.min_size
    error_message = "max_size must be >= min_size."
  }
}

variable "desired_size" {
  description = "Terraform-managed desired capacity. The module default is 0; envs/base sets 1 during W4 because no node autoscaler is installed."
  type        = number
  default     = 0

  validation {
    condition     = var.desired_size >= var.min_size && var.desired_size <= var.max_size
    error_message = "desired_size must be between min_size and max_size."
  }
}

variable "tags" {
  description = "Common tags applied to every resource."
  type        = map(string)
  default     = {}
}

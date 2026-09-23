# modules/eks_node_group
#
# Phase 5 gate 5.1: the scale-to-zero worker capacity behind the EKS front
# door. This is the AWS-provider equivalent of a GKE scale-to-zero pattern
# from an earlier project of mine (a GPU node pool with
# min_node_count = 0, "scales to zero when idle"), mirrored as a PATTERN,
# not a resource port, per the PHASE_5.md anchor fact-check:
# aws_eks_node_group with scaling_config { min_size = 0, max_size = 1,
# desired_size = 0 }. Spot capacity, matching the platform's standing
# "spot everywhere non-critical" FinOps principle (the ECS serving service
# already runs FARGATE_SPOT).
#
# desired_size is the demo-window knob. It starts at 0 at the module boundary,
# while envs/base explicitly sets it to 1 for W4 because KEDA scales pods, not
# EC2 nodes. Terraform retains ownership until a real node autoscaler exists;
# ignoring this field without one makes a later 0 -> 1 correction impossible.
# The IAM node role lives in modules/eks_cluster (one module owns every
# EKS-trust IAM surface); this module is pure capacity.
#
# Whole-module gate at the envs/base call site (count = var.enable_phase5 ?
# 1 : 0), so the default plan stays a zero diff by construction.

locals {
  name_prefix = "${var.project}-${var.environment}"

  tags = merge(var.tags, {
    Module = "eks_node_group"
  })
}

# A custom launch-template security group prevents EKS from attaching the
# cluster security group, whose default all-traffic self rule would make the
# NodePort reachable from any other node. W4 has one trusted worker; external
# serving traffic reaches port 30080 only through the front-door NLB rule owned
# by modules/eks_frontdoor.
resource "aws_security_group" "node" {
  name        = "${local.name_prefix}-eks-node"
  description = "Restricted Phase 5 worker traffic; NodePort ingress is added by eks_frontdoor"
  vpc_id      = var.vpc_id

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-eks-node"
  })
}

resource "aws_vpc_security_group_ingress_rule" "control_plane_https" {
  security_group_id            = aws_security_group.node.id
  description                  = "EKS control-plane webhook traffic"
  referenced_security_group_id = var.control_plane_security_group_id
  from_port                    = 443
  to_port                      = 443
  ip_protocol                  = "tcp"

  tags = local.tags
}

resource "aws_vpc_security_group_ingress_rule" "control_plane_kubelet" {
  security_group_id            = aws_security_group.node.id
  description                  = "EKS control-plane kubelet traffic"
  referenced_security_group_id = var.control_plane_security_group_id
  from_port                    = 10250
  to_port                      = 10250
  ip_protocol                  = "tcp"

  tags = local.tags
}

# Aggregated API servers run as pods and are reached directly by the EKS API
# server. KEDA serves external.metrics.k8s.io on 6443, so the control-plane
# bridge needs this narrow SG-to-SG rule in addition to kubelet and webhook
# traffic. Without it, KEDA can wake a zero-replica target through its
# operator, but the HPA cannot read external metrics or scale beyond one.
resource "aws_vpc_security_group_ingress_rule" "control_plane_metrics_apiserver" {
  security_group_id            = aws_security_group.node.id
  description                  = "EKS control plane to aggregated metrics API servers"
  referenced_security_group_id = var.control_plane_security_group_id
  from_port                    = 6443
  to_port                      = 6443
  ip_protocol                  = "tcp"

  tags = local.tags
}

# Nodes that do not carry the EKS-created cluster security group need an
# explicit reciprocal path to the private Kubernetes API endpoint. Both sides
# reference Terraform-owned groups, never the EKS-managed cluster group.
resource "aws_vpc_security_group_ingress_rule" "node_to_control_plane_https" {
  security_group_id            = var.control_plane_security_group_id
  description                  = "Worker nodes to the private EKS API endpoint"
  referenced_security_group_id = aws_security_group.node.id
  from_port                    = 443
  to_port                      = 443
  ip_protocol                  = "tcp"

  tags = local.tags
}

resource "aws_vpc_security_group_egress_rule" "https" {
  security_group_id = aws_security_group.node.id
  description       = "AWS APIs, image pulls, and the private EKS endpoint"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 443
  to_port           = 443
  ip_protocol       = "tcp"

  tags = local.tags
}

resource "aws_vpc_security_group_egress_rule" "dns_udp" {
  security_group_id = aws_security_group.node.id
  description       = "VPC DNS over UDP"
  cidr_ipv4         = var.vpc_cidr
  from_port         = 53
  to_port           = 53
  ip_protocol       = "udp"

  tags = local.tags
}

resource "aws_vpc_security_group_egress_rule" "dns_tcp" {
  security_group_id = aws_security_group.node.id
  description       = "VPC DNS over TCP"
  cidr_ipv4         = var.vpc_cidr
  from_port         = 53
  to_port           = 53
  ip_protocol       = "tcp"

  tags = local.tags
}

resource "aws_vpc_security_group_egress_rule" "vpc_data_services" {
  for_each = {
    postgres = 5432
    redis    = 6379
  }

  security_group_id = aws_security_group.node.id
  description       = "Optional ${each.key} serving dependency inside the VPC"
  cidr_ipv4         = var.vpc_cidr
  from_port         = each.value
  to_port           = each.value
  ip_protocol       = "tcp"

  tags = local.tags
}

resource "aws_vpc_security_group_egress_rule" "time_sync" {
  security_group_id = aws_security_group.node.id
  description       = "Amazon Time Sync Service"
  cidr_ipv4         = "169.254.169.123/32"
  from_port         = 123
  to_port           = 123
  ip_protocol       = "udp"

  tags = local.tags
}

resource "aws_launch_template" "node" {
  # checkov:skip=CKV_AWS_126:Detailed EC2 monitoring adds cost to a short-lived W4 worker; EKS, Kubernetes, and application metrics remain enabled.
  name_prefix            = "${local.name_prefix}-eks-node-"
  update_default_version = true
  ebs_optimized          = true
  vpc_security_group_ids = [aws_security_group.node.id]

  block_device_mappings {
    device_name = "/dev/xvda"

    ebs {
      delete_on_termination = true
      encrypted             = true
      volume_size           = var.disk_size
      volume_type           = "gp3"
    }
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_protocol_ipv6          = "disabled"
    http_put_response_hop_limit = 1
    http_tokens                 = "required"
    instance_metadata_tags      = "disabled"
  }

  tag_specifications {
    resource_type = "instance"
    tags          = merge(local.tags, { Name = "${local.name_prefix}-eks-node" })
  }

  tag_specifications {
    resource_type = "volume"
    tags          = merge(local.tags, { Name = "${local.name_prefix}-eks-node" })
  }

  tags = local.tags
}

resource "aws_eks_node_group" "this" {
  cluster_name    = var.cluster_name
  node_group_name = "${local.name_prefix}-serving-spot"
  node_role_arn   = var.node_role_arn
  subnet_ids      = var.private_subnet_ids

  capacity_type  = "SPOT"
  ami_type       = var.ami_type
  instance_types = var.instance_types

  launch_template {
    id      = aws_launch_template.node.id
    version = aws_launch_template.node.latest_version
  }

  scaling_config {
    min_size     = var.min_size
    max_size     = var.max_size
    desired_size = var.desired_size
  }

  update_config {
    max_unavailable = 1
  }

  labels = {
    "harbormaster.io/pool" = "serving"
  }

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-serving-spot"
  })
}

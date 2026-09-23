# modules/eks_frontdoor
#
# Terraform owns the Phase 5 internal NLB and attaches its instance target
# group to the managed node group's Auto Scaling group. Kubernetes only owns a
# fixed NodePort Service. This avoids a second in-cluster load-balancer
# controller, gives API Gateway a plan-time listener ARN, and ensures the
# billable NLB remains visible in Terraform state during teardown.

locals {
  name_prefix = "${var.project}-${var.environment}"
  tags        = merge(var.tags, { Module = "eks_frontdoor" })
}

resource "aws_security_group" "nlb" {
  name        = "${local.name_prefix}-eks-nlb"
  description = "Internal Phase 5 NLB; ingress is limited to the API Gateway VPC link"
  vpc_id      = var.vpc_id

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-eks-nlb"
  })
}

resource "aws_vpc_security_group_egress_rule" "nlb_to_nodes" {
  security_group_id            = aws_security_group.nlb.id
  description                  = "NLB health checks and serving traffic to the EKS NodePort"
  referenced_security_group_id = var.node_security_group_id
  from_port                    = var.node_port
  to_port                      = var.node_port
  ip_protocol                  = "tcp"

  tags = local.tags
}

resource "aws_lb" "serving" {
  # checkov:skip=CKV_AWS_91:This short-lived internal NLB sits behind API Gateway access logging; a dedicated S3 log bucket adds standing scope and cost.
  # checkov:skip=CKV_AWS_150:Deletion protection must stay off so bounded W4 cleanup can remove the NLB before the EKS guard fires.
  # checkov:skip=CKV2_AWS_20:TLS and IAM authorization terminate at API Gateway; this NLB accepts private VPC-link traffic only.
  name                             = "${local.name_prefix}-eks"
  internal                         = true
  load_balancer_type               = "network"
  subnets                          = var.private_subnet_ids
  security_groups                  = [aws_security_group.nlb.id]
  enable_cross_zone_load_balancing = true

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-eks-serving"
  })
}

resource "aws_lb_target_group" "serving" {
  name                 = "${local.name_prefix}-eks-serving"
  port                 = var.node_port
  protocol             = "TCP"
  target_type          = "instance"
  vpc_id               = var.vpc_id
  deregistration_delay = 30

  health_check {
    enabled             = true
    healthy_threshold   = 2
    unhealthy_threshold = 2
    interval            = 10
    path                = "/healthz"
    port                = "traffic-port"
    protocol            = "HTTP"
    matcher             = "200-399"
  }

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-eks-serving"
  })
}

resource "aws_lb_listener" "serving" {
  load_balancer_arn = aws_lb.serving.arn
  port              = 80
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.serving.arn
  }

  tags = local.tags
}

resource "aws_autoscaling_attachment" "serving" {
  # checkov:skip=CKV2_AWS_15:EKS owns the underlying managed-node-group ASG, so this module cannot set its health_check_type.
  autoscaling_group_name = var.node_autoscaling_group_name
  lb_target_group_arn    = aws_lb_target_group.serving.arn
}

# Only the NLB security group can reach the NodePort. The API Gateway module
# separately limits NLB ingress to the VPC-link security group, preventing a
# VPC workload from bypassing API Gateway IAM through either node IPs or NLB.
resource "aws_vpc_security_group_ingress_rule" "serving_node_port" {
  security_group_id            = var.node_security_group_id
  description                  = "Phase 5 serving NodePort from the internal NLB only"
  referenced_security_group_id = aws_security_group.nlb.id
  from_port                    = var.node_port
  to_port                      = var.node_port
  ip_protocol                  = "tcp"

  tags = local.tags
}

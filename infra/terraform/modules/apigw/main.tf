# modules/apigw/main.tf
#
# API Gateway HTTP API fronting the ecs_serving Fargate service via a VPC Link
# to its Cloud Map service. Scale-to-zero, ~$1 per million requests, no standing
# cost (chosen over an ALB to protect the $75 cap). External callers hit the
# invoke URL; in-VPC callers (Flink) can skip this and use the Cloud Map DNS.
#
# Hardening (all author-only, safe defaults; see variables.tf):
#   - stage default-route throttling (rate + burst), always on
#   - structured JSON access logging to CloudWatch (14-day retention), on
#   - route authorization defaulting to IAM (SigV4), free for HTTP APIs
#   - optional WAFv2 web ACL, off by default because WAF carries standing cost

data "aws_region" "current" {}

locals {
  name_prefix = "${var.project}-${var.environment}"
  tags        = merge(var.tags, { Module = "apigw" })

  # Route authorization wiring. AWS_IAM uses the built-in SigV4 authorizer type
  # (no authorizer resource). JWT references the authorizer created below. NONE
  # leaves the route anonymous (author-only escape hatch).
  authorization_type = (
    var.authorization_mode == "AWS_IAM" ? "AWS_IAM" :
    var.authorization_mode == "JWT" ? "JWT" :
    "NONE"
  )
  jwt_authorizer_id = var.authorization_mode == "JWT" ? aws_apigatewayv2_authorizer.jwt[0].id : null
}

# SG for the VPC link ENIs; egress-only (they originate connections to serving).
resource "aws_security_group" "vpc_link" {
  name        = "${local.name_prefix}-apigw-vpclink-sg"
  description = "API Gateway VPC link egress to the serving service"
  vpc_id      = var.vpc_id

  egress {
    description = "all outbound to in-VPC targets"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${local.name_prefix}-apigw-vpclink-sg" })
}

resource "aws_apigatewayv2_vpc_link" "this" {
  name               = "${local.name_prefix}-serving-vpclink"
  subnet_ids         = var.private_subnet_ids
  security_group_ids = [aws_security_group.vpc_link.id]
  tags               = local.tags
}

resource "aws_apigatewayv2_api" "this" {
  name          = "${local.name_prefix}-serving-api"
  protocol_type = "HTTP"
  tags          = local.tags
}

# Private integration to the Cloud Map service via the VPC link.
resource "aws_apigatewayv2_integration" "serving" {
  api_id             = aws_apigatewayv2_api.this.id
  integration_type   = "HTTP_PROXY"
  integration_method = "ANY"
  integration_uri    = var.cloudmap_service_arn
  connection_type    = "VPC_LINK"
  connection_id      = aws_apigatewayv2_vpc_link.this.id
}

# Phase 5 (gate 5.2): the EKS-path integration over the SAME VPC Link,
# authored behind the plan-time-known enable_eks_integration flag. The URI is
# produced by the NLB in the same apply, so it cannot safely determine count.
# The ECS integration above and the aws_ecs_service it fronts are NOT deleted:
# they are the documented rollback path until a live demo proves the EKS path
# (PHASE_5.md gate 5.2's no-untested-cutover decision).
resource "aws_apigatewayv2_integration" "serving_eks" {
  count = var.enable_eks_integration ? 1 : 0

  api_id             = aws_apigatewayv2_api.this.id
  integration_type   = "HTTP_PROXY"
  integration_method = "ANY"
  integration_uri    = var.eks_integration_uri
  connection_type    = "VPC_LINK"
  connection_id      = aws_apigatewayv2_vpc_link.this.id
}

# Complete the private trust chain without opening the NLB to the full VPC.
# The frontdoor module owns the NLB SG; this module owns the source VPC-link SG.
resource "aws_vpc_security_group_ingress_rule" "eks_nlb_from_vpc_link" {
  # checkov:skip=CKV_AWS_260:The rule uses only the VPC-link security group as its source; it has no IPv4 CIDR ingress.
  count = var.enable_eks_integration ? 1 : 0

  security_group_id            = var.eks_nlb_security_group_id
  description                  = "API Gateway VPC link to the Phase 5 internal NLB"
  referenced_security_group_id = aws_security_group.vpc_link.id
  from_port                    = 80
  to_port                      = 80
  ip_protocol                  = "tcp"

  tags = local.tags
}

# JWT authorizer, only when authorization_mode = JWT. Free for HTTP APIs (no
# Lambda invoke). Requires jwt_issuer and jwt_audience from the caller.
resource "aws_apigatewayv2_authorizer" "jwt" {
  count = var.authorization_mode == "JWT" ? 1 : 0

  api_id           = aws_apigatewayv2_api.this.id
  authorizer_type  = "JWT"
  identity_sources = ["$request.header.Authorization"]
  name             = "${local.name_prefix}-serving-jwt"

  jwt_configuration {
    issuer   = var.jwt_issuer
    audience = var.jwt_audience
  }
}

resource "aws_apigatewayv2_route" "proxy" {
  api_id    = aws_apigatewayv2_api.this.id
  route_key = "ANY /{proxy+}"
  # The gate 5.2 retarget point: at the default (serving_target = "ecs") this
  # expression evaluates to exactly the pre-Phase-5 value, so the shipped
  # route is unchanged; "eks" swaps the route to the EKS integration while
  # the ECS service keeps running as the rollback path.
  target = (
    var.serving_target == "eks"
    ? "integrations/${aws_apigatewayv2_integration.serving_eks[0].id}"
    : "integrations/${aws_apigatewayv2_integration.serving.id}"
  )

  # Default posture is AWS_IAM (SigV4), so the public route is not anonymously
  # open. Switch to JWT (with issuer/audience) or NONE via authorization_mode.
  authorization_type = local.authorization_type
  authorizer_id      = local.jwt_authorizer_id
}

# Access-log destination. Retention is short (default 14 days) to stay cheap.
resource "aws_cloudwatch_log_group" "access" {
  count = var.enable_access_logging ? 1 : 0

  name              = "/aws/apigateway/${local.name_prefix}-serving-api"
  retention_in_days = var.access_log_retention_days
  # CMK when set; null keeps the CloudWatch Logs default encryption (zero diff).
  kms_key_id = var.kms_key_arn != "" ? var.kms_key_arn : null
  tags       = local.tags
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.this.id
  name        = "$default"
  auto_deploy = true
  tags        = local.tags

  # Stage-wide default route settings: bound throughput on the public endpoint.
  default_route_settings {
    throttling_rate_limit  = var.throttling_rate_limit
    throttling_burst_limit = var.throttling_burst_limit
  }

  # Structured JSON access logs. Fields are a compact, greppable subset:
  # request id, source ip, route, status, and integration/response latency.
  dynamic "access_log_settings" {
    for_each = var.enable_access_logging ? [1] : []
    content {
      destination_arn = aws_cloudwatch_log_group.access[0].arn
      format = jsonencode({
        requestId          = "$context.requestId"
        ip                 = "$context.identity.sourceIp"
        requestTime        = "$context.requestTime"
        httpMethod         = "$context.httpMethod"
        routeKey           = "$context.routeKey"
        status             = "$context.status"
        protocol           = "$context.protocol"
        responseLength     = "$context.responseLength"
        latency            = "$context.responseLatency"
        integrationLatency = "$context.integrationLatency"
      })
    }
  }
}

# --- WAFv2 web ACL (off by default; has standing cost) -----------------------
# When enable_waf = true, associate a regional web ACL with the stage. Rules:
# AWS common managed rule set (broad OWASP-style coverage), the known-bad-inputs
# managed rule set (which carries the Log4j/CVE-2021-44228 AMR), plus a
# rate-based rule to blunt volumetric abuse per source IP.
resource "aws_wafv2_web_acl" "this" {
  count = var.enable_waf ? 1 : 0

  name        = "${local.name_prefix}-serving-waf"
  description = "WAF for the serving HTTP API stage"
  scope       = "REGIONAL"

  default_action {
    allow {}
  }

  rule {
    name     = "aws-common-rule-set"
    priority = 1

    override_action {
      none {}
    }

    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesCommonRuleSet"
        vendor_name = "AWS"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${local.name_prefix}-common-rules"
      sampled_requests_enabled   = true
    }
  }

  # Known-bad-inputs managed rule group. This is the AMR that covers the Log4j
  # (CVE-2021-44228) exploit patterns, so an attached WAF is not blind to them.
  rule {
    name     = "aws-known-bad-inputs"
    priority = 2

    override_action {
      none {}
    }

    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesKnownBadInputsRuleSet"
        vendor_name = "AWS"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${local.name_prefix}-known-bad-inputs"
      sampled_requests_enabled   = true
    }
  }

  # Anonymous-IP reputation list (VPNs, Tor exit nodes, hosting-provider ranges).
  # Paired with known-bad-inputs above, this gives the attached WAF the full
  # AMR coverage the Log4j-vulnerability control expects.
  rule {
    name     = "aws-anonymous-ip-list"
    priority = 3

    override_action {
      none {}
    }

    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesAnonymousIpList"
        vendor_name = "AWS"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${local.name_prefix}-anonymous-ip"
      sampled_requests_enabled   = true
    }
  }

  rule {
    name     = "rate-limit-per-ip"
    priority = 4

    action {
      block {}
    }

    statement {
      rate_based_statement {
        limit              = var.waf_rate_limit
        aggregate_key_type = "IP"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${local.name_prefix}-rate-limit"
      sampled_requests_enabled   = true
    }
  }

  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "${local.name_prefix}-serving-waf"
    sampled_requests_enabled   = true
  }

  tags = local.tags
}

# WAF associates with the deployed stage ARN. HTTP API stage ARNs follow
# arn:aws:apigateway:<region>::/apis/<api-id>/stages/<stage-name>.
resource "aws_wafv2_web_acl_association" "this" {
  count = var.enable_waf ? 1 : 0

  resource_arn = "arn:aws:apigateway:${data.aws_region.current.name}::/apis/${aws_apigatewayv2_api.this.id}/stages/${aws_apigatewayv2_stage.default.name}"
  web_acl_arn  = aws_wafv2_web_acl.this[0].arn
}

# WAF logging destination. Only created alongside the WAF (enable_waf), so it
# adds no standing cost at rest. WAF-to-CloudWatch requires the log group name
# to start with "aws-waf-logs-". Retention matches the access-log group.
resource "aws_cloudwatch_log_group" "waf" {
  count = var.enable_waf ? 1 : 0

  name              = "aws-waf-logs-${local.name_prefix}-serving"
  retention_in_days = var.access_log_retention_days
  # CMK when set; null keeps the CloudWatch Logs default encryption (zero diff).
  kms_key_id = var.kms_key_arn != "" ? var.kms_key_arn : null
  tags       = local.tags
}

# Send WAF request logs to the group above so the web ACL is not operating
# without an audit trail.
resource "aws_wafv2_web_acl_logging_configuration" "this" {
  count = var.enable_waf ? 1 : 0

  log_destination_configs = [aws_cloudwatch_log_group.waf[0].arn]
  resource_arn            = aws_wafv2_web_acl.this[0].arn
}

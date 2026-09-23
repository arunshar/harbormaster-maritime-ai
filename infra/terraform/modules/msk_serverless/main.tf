# modules/msk_serverless/main.tf
#
# The on-demand CDC showcase Kafka (Phase 2, gate C7). MSK Serverless with IAM
# auth (SASL/OAUTHBEARER from Python, AWS_MSK_IAM from Connect). COST WARNING:
# ~$0.75/cluster-hour (~$18/day) whether or not traffic flows; this module
# exists ONLY behind enable_phase2 for demo windows, and the Phase 0 nightly
# teardown Lambda sweeps orphaned MSK. The local plane (kind + Strimzi) is the
# default Kafka; see docs/phases/PHASE_2.md.

variable "project" {
  type    = string
  default = "harbormaster"
}

variable "environment" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "subnet_ids" {
  description = "At least two subnets in distinct AZs (MSK Serverless requirement)."
  type        = list(string)
}

variable "vpc_cidr" {
  type    = string
  default = "10.0.0.0/16"
}

variable "tags" {
  type    = map(string)
  default = {}
}

locals {
  name_prefix = "${var.project}-${var.environment}"
  tags        = merge(var.tags, { Module = "msk_serverless" })
}

resource "aws_security_group" "msk" {
  name        = "${local.name_prefix}-msk-sg"
  description = "MSK Serverless IAM listener (9098) from in-VPC clients"
  vpc_id      = var.vpc_id

  ingress {
    description = "Kafka SASL/IAM from in-VPC"
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    description = "all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${local.name_prefix}-msk-sg" })
}

resource "aws_msk_serverless_cluster" "this" {
  cluster_name = "${local.name_prefix}-cdc"

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.msk.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  tags = merge(local.tags, { Name = "${local.name_prefix}-cdc" })
}

data "aws_msk_bootstrap_brokers" "this" {
  cluster_arn = aws_msk_serverless_cluster.this.arn
}

output "cluster_arn" {
  value = aws_msk_serverless_cluster.this.arn
}

output "bootstrap_brokers_sasl_iam" {
  value = data.aws_msk_bootstrap_brokers.this.bootstrap_brokers_sasl_iam
}

output "security_group_id" {
  value = aws_security_group.msk.id
}

# kafka-cluster IAM actions scope to cluster/topic/group ARNs derived from the
# cluster ARN (arn:...:cluster/name/uuid -> :topic|:group/name/uuid/*).
output "topic_wildcard_arn" {
  value = "${replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":topic/")}/*"
}

output "group_wildcard_arn" {
  value = "${replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":group/")}/*"
}

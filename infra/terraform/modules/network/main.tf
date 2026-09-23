# modules/network/main.tf
#
# Network foundation for Harbormaster Phase 0:
#   - one VPC (10.0.0.0/16 by default)
#   - public and private subnets across 2 AZs
#   - an internet gateway and a public route table
#   - S3 and DynamoDB GATEWAY VPC endpoints (free; keeps that traffic off NAT)
#   - a single, optional NAT gateway gated by var.enable_nat (default false)
#
# Cost posture: the gateway endpoints cost nothing and are always created so
# lake/state traffic never traverses NAT. The NAT gateway is the one expensive
# piece here, so it is off by default and a single AZ when on.

locals {
  # Subnet sizing: /20 carve-outs from the /16. With az_count = 2 this yields
  # public  10.0.0.0/20, 10.0.16.0/20 and private 10.0.128.0/20, 10.0.144.0/20.
  public_subnet_cidrs  = [for i in range(var.az_count) : cidrsubnet(var.vpc_cidr, 4, i)]
  private_subnet_cidrs = [for i in range(var.az_count) : cidrsubnet(var.vpc_cidr, 4, i + 8)]

  name_prefix = "${var.project}-${var.environment}"

  tags = merge(var.tags, {
    Module = "network"
  })
}

# Resolve the first az_count availability zones available in the region.
data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-vpc"
  })
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-igw"
  })
}

# -----------------------------------------------------------------------------
# Public subnets + routing
# -----------------------------------------------------------------------------

resource "aws_subnet" "public" {
  count = var.az_count

  vpc_id                  = aws_vpc.this.id
  cidr_block              = local.public_subnet_cidrs[count.index]
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-public-${count.index}"
    Tier = "public"
  })
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-public-rt"
    Tier = "public"
  })
}

resource "aws_route" "public_internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this.id
}

resource "aws_route_table_association" "public" {
  count = var.az_count

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# -----------------------------------------------------------------------------
# Private subnets + routing
# -----------------------------------------------------------------------------

resource "aws_subnet" "private" {
  count = var.az_count

  vpc_id            = aws_vpc.this.id
  cidr_block        = local.private_subnet_cidrs[count.index]
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-private-${count.index}"
    Tier = "private"
  })
}

# One private route table per AZ so each can independently route to NAT if it is
# ever enabled. Without NAT they have only the implicit local route.
resource "aws_route_table" "private" {
  count = var.az_count

  vpc_id = aws_vpc.this.id

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-private-rt-${count.index}"
    Tier = "private"
  })
}

resource "aws_route_table_association" "private" {
  count = var.az_count

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}

# -----------------------------------------------------------------------------
# Single, optional NAT gateway (gated by var.enable_nat)
# -----------------------------------------------------------------------------

resource "aws_eip" "nat" {
  count = var.enable_nat ? 1 : 0

  domain = "vpc"

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-nat-eip"
  })

  depends_on = [aws_internet_gateway.this]
}

# A single NAT gateway in the first public subnet. Cheaper than one-per-AZ; the
# trade-off is that an AZ outage takes private egress with it, acceptable for a
# personal Phase 0 platform.
resource "aws_nat_gateway" "this" {
  count = var.enable_nat ? 1 : 0

  allocation_id = aws_eip.nat[0].id
  subnet_id     = aws_subnet.public[0].id

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-nat"
  })

  depends_on = [aws_internet_gateway.this]
}

# Default route for every private route table through the single NAT, only when
# NAT is enabled.
resource "aws_route" "private_nat" {
  count = var.enable_nat ? var.az_count : 0

  route_table_id         = aws_route_table.private[count.index].id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.this[0].id
}

# -----------------------------------------------------------------------------
# Gateway VPC endpoints (free): keep S3 and DynamoDB traffic off NAT/IGW
# -----------------------------------------------------------------------------

# Associate the endpoints with both public and private route tables so any
# subnet reaches S3 and DynamoDB privately.
locals {
  all_route_table_ids = concat(
    [aws_route_table.public.id],
    aws_route_table.private[*].id,
  )
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = local.all_route_table_ids

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-s3-endpoint"
  })
}

resource "aws_vpc_endpoint" "dynamodb" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${var.aws_region}.dynamodb"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = local.all_route_table_ids

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-dynamodb-endpoint"
  })
}

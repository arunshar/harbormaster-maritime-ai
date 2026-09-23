# modules/network/outputs.tf

output "vpc_id" {
  description = "ID of the Harbormaster VPC."
  value       = aws_vpc.this.id
}

output "vpc_cidr" {
  description = "CIDR block of the VPC."
  value       = aws_vpc.this.cidr_block
}

output "public_subnet_ids" {
  description = "IDs of the public subnets, one per AZ."
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "IDs of the private subnets, one per AZ."
  value       = aws_subnet.private[*].id
}

output "public_route_table_id" {
  description = "ID of the public route table."
  value       = aws_route_table.public.id
}

output "private_route_table_ids" {
  description = "IDs of the private route tables, one per AZ."
  value       = aws_route_table.private[*].id
}

output "nat_gateway_id" {
  description = "ID of the single NAT gateway, or null when enable_nat is false."
  value       = var.enable_nat ? aws_nat_gateway.this[0].id : null
}

output "s3_vpc_endpoint_id" {
  description = "ID of the S3 gateway VPC endpoint."
  value       = aws_vpc_endpoint.s3.id
}

output "dynamodb_vpc_endpoint_id" {
  description = "ID of the DynamoDB gateway VPC endpoint."
  value       = aws_vpc_endpoint.dynamodb.id
}

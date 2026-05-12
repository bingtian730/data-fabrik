output "vpc_id" {
  description = "ID of the VPC."
  value       = aws_vpc.this.id
}

output "vpc_cidr" {
  description = "CIDR block of the VPC."
  value       = aws_vpc.this.cidr_block
}

output "public_subnet_ids" {
  description = "Public subnet IDs."
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "Private subnet IDs."
  value       = aws_subnet.private[*].id
}

output "emr_master_sg_id" {
  description = "Security group for EMR master nodes."
  value       = aws_security_group.emr_master.id
}

output "emr_slave_sg_id" {
  description = "Security group for EMR core / task nodes."
  value       = aws_security_group.emr_slave.id
}

output "emr_service_sg_id" {
  description = "Security group for EMR service access."
  value       = aws_security_group.emr_service.id
}

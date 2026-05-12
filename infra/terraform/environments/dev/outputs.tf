output "vpc_id" {
  description = "VPC ID."
  value       = module.vpc.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnet IDs (EMR clusters launch here)."
  value       = module.vpc.private_subnet_ids
}

output "public_subnet_ids" {
  description = "Public subnet IDs."
  value       = module.vpc.public_subnet_ids
}

output "data_bucket_names" {
  description = "Map of logical name -> actual bucket name."
  value       = module.s3.bucket_names
}

output "log_group_names" {
  description = "Map of logical name -> CloudWatch log group name."
  value       = module.cloudwatch.log_group_names
}

output "airflow_role_arn" {
  description = "Airflow IAM role ARN."
  value       = module.iam.airflow_role_arn
}

output "fastapi_role_arn" {
  description = "FastAPI IAM role ARN."
  value       = module.iam.fastapi_role_arn
}

output "emr_service_role_arn" {
  description = "EMR service role ARN."
  value       = module.emr.service_role_arn
}

output "emr_ec2_instance_profile_arn" {
  description = "EMR EC2 instance profile ARN."
  value       = module.emr.ec2_instance_profile_arn
}

output "emr_autoscaling_role_arn" {
  description = "EMR autoscaling role ARN."
  value       = module.emr.autoscaling_role_arn
}

output "emr_security_group_ids" {
  description = "Security group IDs for EMR clusters."
  value = {
    master  = module.vpc.emr_master_sg_id
    slave   = module.vpc.emr_slave_sg_id
    service = module.vpc.emr_service_sg_id
  }
}

output "service_role_arn" {
  description = "EMR service role ARN. Pass to `aws_emr_cluster.service_role`."
  value       = aws_iam_role.service.arn
}

output "service_role_name" {
  description = "EMR service role name."
  value       = aws_iam_role.service.name
}

output "ec2_role_arn" {
  description = "EMR EC2 instance profile role ARN."
  value       = aws_iam_role.ec2.arn
}

output "ec2_instance_profile_arn" {
  description = "EMR EC2 instance profile ARN. Pass to `aws_emr_cluster.ec2_attributes.instance_profile`."
  value       = aws_iam_instance_profile.ec2.arn
}

output "ec2_instance_profile_name" {
  description = "EMR EC2 instance profile name."
  value       = aws_iam_instance_profile.ec2.name
}

output "autoscaling_role_arn" {
  description = "EMR managed-scaling role ARN."
  value       = aws_iam_role.autoscaling.arn
}

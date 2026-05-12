output "airflow_role_arn" {
  description = "ARN of the Airflow worker role."
  value       = aws_iam_role.airflow.arn
}

output "airflow_role_name" {
  description = "Name of the Airflow worker role."
  value       = aws_iam_role.airflow.name
}

output "airflow_instance_profile_arn" {
  description = "Instance profile ARN for attaching to Airflow EC2 hosts."
  value       = aws_iam_instance_profile.airflow.arn
}

output "fastapi_role_arn" {
  description = "ARN of the FastAPI service role."
  value       = aws_iam_role.fastapi.arn
}

output "fastapi_instance_profile_arn" {
  description = "Instance profile ARN for attaching to FastAPI EC2 hosts."
  value       = aws_iam_instance_profile.fastapi.arn
}

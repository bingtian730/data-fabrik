output "log_group_arns" {
  description = "Logical name -> log group ARN."
  value       = { for k, g in aws_cloudwatch_log_group.this : k => g.arn }
}

output "log_group_names" {
  description = "Logical name -> actual log group name."
  value       = { for k, g in aws_cloudwatch_log_group.this : k => g.name }
}

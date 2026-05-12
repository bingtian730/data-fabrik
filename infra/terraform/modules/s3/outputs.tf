output "bucket_ids" {
  description = "Logical name -> bucket ID."
  value       = { for k, b in aws_s3_bucket.this : k => b.id }
}

output "bucket_arns" {
  description = "Logical name -> bucket ARN."
  value       = { for k, b in aws_s3_bucket.this : k => b.arn }
}

output "bucket_names" {
  description = "Logical name -> actual bucket name."
  value       = { for k, b in aws_s3_bucket.this : k => b.bucket }
}

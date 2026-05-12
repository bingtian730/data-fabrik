variable "name_prefix" {
  description = "Prefix for resource names, e.g. \"datafabrik-dev\"."
  type        = string
}

variable "data_bucket_arns" {
  description = "S3 bucket ARNs that EMR EC2 instances may read from and write to."
  type        = list(string)
}

variable "log_bucket_arn" {
  description = "S3 bucket ARN where EMR cluster logs are written."
  type        = string
}

variable "log_group_arns" {
  description = "CloudWatch Log Group ARNs that EMR EC2 instances may publish to."
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Additional tags applied to every IAM resource."
  type        = map(string)
  default     = {}
}

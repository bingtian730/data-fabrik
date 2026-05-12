variable "name_prefix" {
  description = "Prefix for IAM role names, e.g. \"datafabrik-dev\"."
  type        = string
}

variable "data_bucket_arns" {
  description = "S3 bucket ARNs the platform reads/writes (raw, staging, curated)."
  type        = list(string)
}

variable "log_group_arns" {
  description = "CloudWatch log group ARNs that application roles may write to."
  type        = list(string)
  default     = []
}

variable "passable_role_arns" {
  description = "Role ARNs that orchestration may pass to AWS services via iam:PassRole (e.g. EMR service + EC2 instance profile roles)."
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Additional tags applied to every IAM resource."
  type        = map(string)
  default     = {}
}

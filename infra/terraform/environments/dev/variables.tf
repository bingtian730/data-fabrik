variable "project" {
  description = "Project name used as a tag and name prefix component."
  type        = string
  default     = "datafabrik"
}

variable "environment" {
  description = "Environment name, e.g. dev, staging, prod."
  type        = string
  default     = "dev"
}

variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.20.0.0/16"
}

variable "enable_nat_gateway" {
  description = "Provision a NAT gateway (adds ~$32/mo to AWS bill)."
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "Default retention for CloudWatch log groups in this environment."
  type        = number
  default     = 14
}

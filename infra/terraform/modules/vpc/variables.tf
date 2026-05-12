variable "name_prefix" {
  description = "Prefix for resource names, e.g. \"datafabrik-dev\"."
  type        = string
}

variable "cidr_block" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.20.0.0/16"
}

variable "az_count" {
  description = "Number of availability zones to span (>= 2 for HA)."
  type        = number
  default     = 2
}

variable "enable_nat_gateway" {
  description = "Provision a NAT gateway so private subnets can reach the internet (~$32/mo on AWS)."
  type        = bool
  default     = true
}

variable "tags" {
  description = "Additional tags applied to every resource."
  type        = map(string)
  default     = {}
}

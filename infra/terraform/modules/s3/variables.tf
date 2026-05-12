variable "name_prefix" {
  description = "Prefix for bucket names, e.g. \"datafabrik-dev\"."
  type        = string
}

variable "name_suffix" {
  description = "Suffix appended to bucket names to ensure global uniqueness (typically the AWS account ID)."
  type        = string
}

variable "buckets" {
  description = "Map of logical bucket name to bucket configuration."
  type = map(object({
    versioning                   = optional(bool, true)
    force_destroy                = optional(bool, false)
    expire_noncurrent_after_days = optional(number, null)
  }))
}

variable "tags" {
  description = "Additional tags applied to every bucket."
  type        = map(string)
  default     = {}
}

variable "name_prefix" {
  description = "Prefix for log group names, e.g. \"datafabrik-dev\"."
  type        = string
}

variable "log_groups" {
  description = "Map of logical name to log group config. Each becomes /<name_prefix>/<key>."
  type = map(object({
    retention_days = optional(number, 14)
  }))
}

variable "tags" {
  description = "Additional tags applied to every log group."
  type        = map(string)
  default     = {}
}

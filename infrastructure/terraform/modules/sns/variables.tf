variable "name_prefix" {
  description = "Prefix for resource names (e.g. 'lakehouse-prod')."
  type        = string
}

variable "alert_email" {
  description = "Email subscribed to the alerts topic. Operator must confirm via the link in the first email."
  type        = string
}

variable "tags" {
  description = "Tags to apply to all resources."
  type        = map(string)
  default     = {}
}

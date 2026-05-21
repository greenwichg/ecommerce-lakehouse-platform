variable "bucket_name" {
  description = "S3 bucket name. Must be globally unique."
  type        = string
}

variable "env" {
  description = "Environment name; used for resource naming consistency."
  type        = string
}

variable "kms_key_deletion_window_days" {
  description = "Pending-deletion window for the bucket's KMS key."
  type        = number
}

variable "raw_lifecycle_archive_after_days" {
  description = "Days before raw/ objects transition to Glacier Flexible Retrieval."
  type        = number
}

variable "tags" {
  description = "Tags to apply to all resources in this module."
  type        = map(string)
  default     = {}
}

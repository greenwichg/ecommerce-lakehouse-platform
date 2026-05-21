variable "name_prefix" {
  description = "Resource name prefix."
  type        = string
}

variable "log_retention_days" {
  description = "Retention for CloudWatch log groups in days."
  type        = number
  default     = 30
}

variable "sns_alerts_topic_arn" {
  description = "SNS topic ARN that CloudWatch alarms publish into."
  type        = string
}

variable "bucket_name" {
  description = "S3 bucket name, used in dashboard widgets that query metrics by bucket."
  type        = string
}

variable "tags" {
  description = "Tags to apply to all resources."
  type        = map(string)
  default     = {}
}

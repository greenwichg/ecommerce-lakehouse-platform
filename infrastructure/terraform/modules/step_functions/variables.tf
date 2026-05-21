variable "name_prefix" {
  description = "Resource name prefix."
  type        = string
}

variable "sfn_role_arn" {
  description = "IAM role ARN that the state machine assumes."
  type        = string
}

variable "log_group_arn" {
  description = "CloudWatch log group ARN for state machine execution logs."
  type        = string
}

variable "validator_function_arn" {
  description = "ARN of the file_validator Lambda (used by WaitForOperatorDecision)."
  type        = string
}

variable "alerts_topic_arn" {
  description = "SNS topic for operator notifications."
  type        = string
}

variable "bucket_id" {
  description = "Bucket name, referenced in operator-instruction SNS messages."
  type        = string
}

variable "tags" {
  description = "Tags to apply to all resources."
  type        = map(string)
  default     = {}
}

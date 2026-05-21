variable "name_prefix" {
  description = "Resource name prefix."
  type        = string
}

variable "bucket_arn" {
  description = "S3 lakehouse bucket ARN. All four roles scope their S3 actions to this bucket (or a prefix within it)."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key ARN for bucket encryption. Roles writing/reading need kms:GenerateDataKey/Decrypt."
  type        = string
}

variable "secrets_arns" {
  description = "Map of logical secret name → ARN. Each role gets GetSecretValue on the subset it needs."
  type        = map(string)
}

variable "alerts_topic_arn" {
  description = "SNS alerts topic. Lambda + SFN need sns:Publish."
  type        = string
}

variable "lambda_log_group_arn" {
  description = "CloudWatch log group ARN for the validator Lambda."
  type        = string
}

variable "sfn_log_group_arn" {
  description = "CloudWatch log group ARN for the Step Functions state machine."
  type        = string
}

variable "tags" {
  description = "Tags to apply to all resources."
  type        = map(string)
  default     = {}
}

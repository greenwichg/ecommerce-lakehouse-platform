variable "name_prefix" {
  description = "Resource name prefix."
  type        = string
}

variable "bucket_id" {
  description = "S3 bucket name (id), used for the queue access policy + S3 notification ARN reference."
  type        = string
}

variable "bucket_arn" {
  description = "S3 bucket ARN, used in the SQS access policy to grant S3 publish."
  type        = string
}

variable "image_uri" {
  description = "ECR image URI for the Lambda Container Image deployment."
  type        = string
}

variable "lambda_role_arn" {
  description = "IAM role the Lambda assumes. Provided by modules/iam."
  type        = string
}

variable "log_group_name" {
  description = "Name of the CloudWatch log group the Lambda writes into. Provided by modules/cloudwatch."
  type        = string
}

variable "alerts_topic_arn" {
  description = "SNS topic for warn/quarantine notifications. Passed to the Lambda as an env var."
  type        = string
}

variable "tags" {
  description = "Tags to apply to all resources."
  type        = map(string)
  default     = {}
}

variable "visibility_timeout_seconds" {
  description = "SQS visibility timeout. Must be >= Lambda timeout × 6 per AWS best practice (concurrent retries can hold the message)."
  type        = number
  default     = 360
}

variable "lambda_timeout_seconds" {
  description = "Lambda execution timeout. Validator should finish in <30s for normal files; 60s leaves headroom for cold-start pyarrow imports."
  type        = number
  default     = 60
}

variable "lambda_memory_mb" {
  description = "Memory allocation. pyarrow + S3 read benefits from RAM; 1024 is a reasonable starting point."
  type        = number
  default     = 1024
}

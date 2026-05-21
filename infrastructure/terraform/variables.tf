# Top-level inputs. Per-environment values live in environments/<env>.tfvars.

variable "env" {
  description = "Environment name (dev / prod). Used in resource naming + tagging."
  type        = string
  validation {
    condition     = contains(["dev", "prod"], var.env)
    error_message = "env must be 'dev' or 'prod'."
  }
}

variable "region" {
  description = "AWS region. Single-region by design — DR cross-region is out of scope."
  type        = string
  default     = "us-east-1"
}

variable "account_id" {
  description = "AWS account ID. Used in cross-service ARNs (Secrets Manager, etc.)."
  type        = string
}

variable "lakehouse_bucket_name" {
  description = "Base S3 bucket name. The S3 module derives raw/processed/archive/quarantine prefixes inside this one bucket. Multi-bucket would also work; the prefix-per-zone layout is simpler for our scale."
  type        = string
}

variable "alert_email" {
  description = "Email address subscribed to the SNS pipeline-alerts topic. The first alert post-apply triggers a confirmation email — operator must click the link to activate."
  type        = string
}

variable "kms_key_deletion_window_days" {
  description = "Deletion window for the S3 KMS key. Min 7, max 30. Production should use 30 (extra forgive-time); dev can drop to 7."
  type        = number
  default     = 30
  validation {
    condition     = var.kms_key_deletion_window_days >= 7 && var.kms_key_deletion_window_days <= 30
    error_message = "kms_key_deletion_window_days must be between 7 and 30."
  }
}

variable "cloudwatch_log_retention_days" {
  description = "Retention for Lambda + Step Functions CloudWatch log groups. AWS billing scales with retained log volume; 30d is the sweet spot for ops-debugging visibility."
  type        = number
  default     = 30
}

variable "lambda_validator_image_uri" {
  description = "ECR image URI for the file_validator Lambda (Container Image deployment). For code-only validate, this is a placeholder; a real apply needs `aws ecr` build/push first. Format: <account>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>"
  type        = string
  default     = "placeholder.dkr.ecr.us-east-1.amazonaws.com/lakehouse-file-validator:latest"
}

variable "raw_lifecycle_archive_after_days" {
  description = "Days after which raw S3 objects transition to Glacier Flexible Retrieval. 90d default per spec; cheaper at the cost of restore latency."
  type        = number
  default     = 90
}

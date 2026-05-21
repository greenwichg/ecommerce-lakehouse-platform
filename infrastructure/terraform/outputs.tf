# Outputs consumed by external configuration:
# - Airflow Variables (S3 bucket, SNS topic ARN, Databricks job IDs (in their own state), etc.)
# - Snowflake stages (STORAGE_AWS_ROLE_ARN, LAKEHOUSE_BUCKET)
# - Operator runbooks (which secret name holds what)

output "bucket_name" {
  description = "Top-level S3 bucket name. Use as Airflow Variable lakehouse_s3_bucket."
  value       = module.s3.bucket_name
}

output "bucket_arn" {
  description = "Bucket ARN, for IAM policies in downstream stacks."
  value       = module.s3.bucket_arn
}

output "kms_key_id" {
  description = "KMS key ID for bucket encryption. Databricks instance profile needs Decrypt access."
  value       = module.s3.kms_key_id
}

output "alerts_topic_arn" {
  description = "SNS topic ARN. Use as Airflow Variable sns_alert_topic_arn."
  value       = module.sns.alerts_topic_arn
}

output "databricks_instance_profile_arn" {
  description = "IAM role ARN that Databricks job clusters assume."
  value       = module.iam.databricks_instance_profile_arn
}

output "airflow_execution_role_arn" {
  description = "IAM role ARN for MWAA / self-hosted Airflow."
  value       = module.iam.airflow_execution_role_arn
}

output "snowflake_storage_integration_role_arn" {
  description = "IAM role ARN used by the Snowflake STORAGE INTEGRATION. Pass into the Snowflake DDL as the S3_INTEGRATION_ROLE_ARN parameter."
  value       = module.iam.snowflake_storage_integration_role_arn
}

output "lambda_validator_function_arn" {
  description = "ARN of the file_validator Lambda."
  value       = module.lambda_validator.function_arn
}

output "step_functions_quarantine_replay_arn" {
  description = "ARN of the quarantine-review-replay state machine. Operators invoke this manually after a quarantine event."
  value       = module.step_functions.state_machine_arn
}

output "secret_arns" {
  description = "Secrets Manager secret ARNs by name. Populated with placeholders at apply time; real values updated post-apply via the AWS console / CLI."
  value       = module.secrets.secret_arns
}

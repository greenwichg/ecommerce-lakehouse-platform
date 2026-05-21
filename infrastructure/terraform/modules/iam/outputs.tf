output "databricks_instance_profile_arn" {
  description = "Instance profile ARN to attach to Databricks job clusters."
  value       = aws_iam_instance_profile.databricks.arn
}

output "databricks_role_arn" {
  description = "Underlying IAM role ARN (for trust-relationship audits)."
  value       = aws_iam_role.databricks.arn
}

output "airflow_execution_role_arn" {
  description = "IAM role ARN for MWAA / self-hosted Airflow execution."
  value       = aws_iam_role.airflow.arn
}

output "lambda_role_arn" {
  description = "IAM role ARN for the file validator Lambda."
  value       = aws_iam_role.lambda.arn
}

output "sfn_role_arn" {
  description = "IAM role ARN for the Step Functions state machine."
  value       = aws_iam_role.sfn.arn
}

output "snowflake_storage_integration_role_arn" {
  description = "IAM role ARN to plug into the Snowflake STORAGE INTEGRATION's STORAGE_AWS_ROLE_ARN."
  value       = aws_iam_role.snowflake_storage_integration.arn
}

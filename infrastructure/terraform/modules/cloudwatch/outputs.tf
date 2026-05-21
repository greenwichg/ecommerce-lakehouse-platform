output "lambda_log_group_arn" {
  description = "ARN of the Lambda validator log group."
  value       = aws_cloudwatch_log_group.lambda_validator.arn
}

output "lambda_log_group_name" {
  description = "Name of the Lambda validator log group (used by the Lambda function config)."
  value       = aws_cloudwatch_log_group.lambda_validator.name
}

output "sfn_log_group_arn" {
  description = "ARN of the Step Functions log group."
  value       = aws_cloudwatch_log_group.step_functions.arn
}

output "dashboard_name" {
  description = "Name of the pipeline dashboard."
  value       = aws_cloudwatch_dashboard.pipeline.dashboard_name
}

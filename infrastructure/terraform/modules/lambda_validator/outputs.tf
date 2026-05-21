output "function_name" {
  description = "Lambda function name (used by Airflow tasks that invoke it manually for replays)."
  value       = aws_lambda_function.validator.function_name
}

output "function_arn" {
  description = "Lambda function ARN."
  value       = aws_lambda_function.validator.arn
}

output "queue_arn" {
  description = "Main SQS queue ARN. Wired into the S3 bucket notification at the top-level main.tf."
  value       = aws_sqs_queue.validator.arn
}

output "queue_url" {
  description = "Main SQS queue URL (operators use this for inspection)."
  value       = aws_sqs_queue.validator.id
}

output "dlq_arn" {
  description = "Dead-letter queue ARN. CloudWatch alarm-on-non-zero-depth lives in modules/cloudwatch (Slice 6+ enhancement; for now ops checks manually)."
  value       = aws_sqs_queue.validator_dlq.arn
}

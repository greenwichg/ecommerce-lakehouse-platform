output "state_machine_arn" {
  description = "ARN of the quarantine-review-replay state machine. Operators invoke via aws stepfunctions start-execution."
  value       = aws_sfn_state_machine.quarantine_replay.arn
}

output "state_machine_name" {
  description = "State machine name (used in CloudWatch metrics + dashboard widgets)."
  value       = aws_sfn_state_machine.quarantine_replay.name
}

output "helper_function_arn" {
  description = "Helper Lambda ARN. Internal — not consumed outside this module."
  value       = aws_lambda_function.quarantine_helper.arn
}

# Step Functions state machine + supporting Lambda(s).
#
# Workflow: quarantine review + replay (Slice 5 design proposal Q3).
# Three branches after the WaitForOperatorDecision pause:
#   - replay        → move quarantine→raw → trigger Airflow DAG
#   - discard       → delete + audit log + notify
#   - fix-and-replay → notify operator of the exact _fixed/ upload key →
#                      poll for fix → replay the FIXED file to the
#                      original raw/ key → trigger Airflow DAG
#
# Why Step Functions (and not Airflow):
#   - WaitForOperatorDecision uses ``waitForTaskToken``, which holds the
#     execution open up to 7 days at zero compute cost while the
#     operator decides. Airflow sensors would hold a worker slot the
#     whole time.
#   - Standard SFN bills per state transition (~$25/million); a typical
#     quarantine review = ~10 transitions = $0.00025. The workflow can
#     wait literally days for cents.
#   - Pay-per-transition matches the access pattern: infrequent (one
#     per quarantine event) but important when it fires.

# Supporting helper Lambda (separate from the validator because it has
# different IAM needs — delete from quarantine, Airflow API key, etc.).
resource "aws_lambda_function" "quarantine_helper" {
  function_name = "${var.name_prefix}-quarantine-helper"
  role          = var.helper_lambda_role_arn
  package_type  = "Image"
  image_uri     = "placeholder.dkr.ecr.us-east-1.amazonaws.com/lakehouse-quarantine-helper:latest"

  timeout     = 30
  memory_size = 512

  environment {
    variables = {
      LAKEHOUSE_BUCKET    = var.bucket_id
      AIRFLOW_SECRET_NAME = "${var.name_prefix}/airflow-api-creds"
    }
  }

  tags = var.tags
}

# Resource-based policy: explicitly allow the state machine to invoke
# the helper. The role policy on the SFN side already grants this, but
# the resource-based policy makes the relationship discoverable from
# the Lambda's side (and required if we later attach a service control
# policy that blocks role-based invocation across services).
resource "aws_lambda_permission" "sfn_invokes_helper" {
  statement_id  = "AllowStepFunctionsInvokeHelper"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.quarantine_helper.function_name
  principal     = "states.amazonaws.com"
  source_arn    = aws_sfn_state_machine.quarantine_replay.arn
}

# Same for the validator Lambda — needed for the WaitForOperatorDecision
# activity-task pattern where SFN passes a task token into a Lambda
# invocation and the Lambda's response (or send-task-success from the
# operator) completes the state.
resource "aws_lambda_permission" "sfn_invokes_validator" {
  statement_id  = "AllowStepFunctionsInvokeValidator"
  action        = "lambda:InvokeFunction"
  function_name = element(split(":", var.validator_function_arn), 6)
  principal     = "states.amazonaws.com"
  source_arn    = aws_sfn_state_machine.quarantine_replay.arn
}

resource "aws_sfn_state_machine" "quarantine_replay" {
  name     = "${var.name_prefix}-quarantine-review-replay"
  role_arn = var.sfn_role_arn

  definition = templatefile("${path.module}/definition.asl.json", {
    alerts_topic_arn       = var.alerts_topic_arn
    validator_function_arn = var.validator_function_arn
    helper_function_arn    = aws_lambda_function.quarantine_helper.arn
    bucket_id              = var.bucket_id
  })

  logging_configuration {
    include_execution_data = true
    level                  = "ALL"
    log_destination        = "${var.log_group_arn}:*"
  }

  type = "STANDARD" # Express incompatible with 7-day human wait

  tags = var.tags
}

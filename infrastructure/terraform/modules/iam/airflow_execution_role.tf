# Airflow execution role (MWAA-default trust; self-hosted edits inline).
#
# Permissions:
#   - S3: ListBucket scoped to raw/* (for S3KeySensor); GetObject on raw
#         (sensor file existence check); NO write to bronze/silver/gold
#         (that's Databricks' job)
#   - Secrets Manager: GetSecretValue on all lakehouse secrets (Airflow
#         needs Snowflake creds, Databricks PAT, exchangerate API key
#         indirectly via dq_gates → libs.config)
#   - Lambda InvokeFunction on the validator (for manual replay tasks)
#   - Step Functions StartExecution on the quarantine state machine
#   - SNS Publish on the alerts topic (sns_failure_callback)
#   - CloudWatch Logs: write Airflow task logs

resource "aws_iam_role" "airflow" {
  name               = "${var.name_prefix}-airflow-execution"
  assume_role_policy = data.aws_iam_policy_document.assume_role_airflow_mwaa.json
  description        = "Airflow execution role. Sensors raw/, invokes Lambda+SFN, publishes SNS, reads all secrets."
  tags               = var.tags
}

data "aws_iam_policy_document" "airflow_inline" {
  statement {
    sid       = "ListBucketForS3Sensors"
    actions   = ["s3:ListBucket"]
    resources = [var.bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*"]
    }
  }

  statement {
    sid       = "ReadRawForSensors"
    actions   = ["s3:GetObject", "s3:GetObjectAttributes"]
    resources = [local.raw_prefix]
  }

  statement {
    sid     = "ReadAllLakehouseSecrets"
    actions = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
    resources = [
      for arn in var.secrets_arns : arn
    ]
  }

  statement {
    sid       = "InvokeValidatorLambda"
    actions   = ["lambda:InvokeFunction"]
    resources = ["arn:aws:lambda:${local.region}:${local.account_id}:function:${var.name_prefix}-file-validator"]
  }

  statement {
    sid     = "StartStepFunctionsExecution"
    actions = ["states:StartExecution", "states:DescribeExecution"]
    resources = [
      "arn:aws:states:${local.region}:${local.account_id}:stateMachine:${var.name_prefix}-quarantine-review-replay",
      "arn:aws:states:${local.region}:${local.account_id}:execution:${var.name_prefix}-quarantine-review-replay:*",
    ]
  }

  statement {
    sid       = "PublishAlerts"
    actions   = ["sns:Publish"]
    resources = [var.alerts_topic_arn]
  }

  statement {
    sid = "WriteAirflowLogs"
    actions = [
      "logs:CreateLogGroup", "logs:CreateLogStream",
      "logs:PutLogEvents", "logs:DescribeLogGroups",
    ]
    resources = ["arn:aws:logs:${local.region}:${local.account_id}:log-group:airflow-*"]
  }

  # KMS Decrypt for any secret encrypted with our customer-managed key
  # (Secrets Manager uses the default AWS-managed key for our secrets,
  # but if we switch them to the customer key in future, the role needs
  # this — preemptive).
  statement {
    sid       = "KMSDecryptForSecrets"
    actions   = ["kms:Decrypt"]
    resources = [var.kms_key_arn]
  }
}

resource "aws_iam_role_policy" "airflow_inline" {
  name   = "${var.name_prefix}-airflow-execution-policy"
  role   = aws_iam_role.airflow.id
  policy = data.aws_iam_policy_document.airflow_inline.json
}

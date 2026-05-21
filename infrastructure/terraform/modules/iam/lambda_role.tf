# Lambda validator role.
#
# Permissions:
#   - SQS: ReceiveMessage / DeleteMessage / GetQueueAttributes on the
#         validator queue (created in modules/lambda_validator)
#   - S3: GetObject on raw/* (reads incoming files); CopyObject +
#         DeleteObject for the move to quarantine/; PutObject on
#         quarantine/ + processed/_manifests/
#   - SNS: Publish on alerts topic (warn-tier and quarantine notifications)
#   - CloudWatch: PutMetricData (custom Lakehouse/Validator namespace),
#         Logs write to its own log group
#
# Explicit deny by omission: Databricks, Snowflake creds (Lambda doesn't
# talk to either), IAM, write to bronze/silver/gold.

resource "aws_iam_role" "lambda" {
  name               = "${var.name_prefix}-lambda-validator"
  assume_role_policy = data.aws_iam_policy_document.assume_role_lambda.json
  description        = "File validator Lambda role. Read raw, write quarantine + manifests, emit metrics + SNS."
  tags               = var.tags
}

data "aws_iam_policy_document" "lambda_inline" {
  statement {
    sid       = "ListBucketForRawFiles"
    actions   = ["s3:ListBucket"]
    resources = [var.bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*"]
    }
  }

  statement {
    sid       = "ReadRawFiles"
    actions   = ["s3:GetObject", "s3:GetObjectAttributes"]
    resources = [local.raw_prefix]
  }

  statement {
    sid       = "MoveToQuarantine"
    actions   = ["s3:DeleteObject"]
    resources = [local.raw_prefix]
  }

  statement {
    sid       = "WriteQuarantineAndManifests"
    actions   = ["s3:PutObject", "s3:PutObjectAcl"]
    resources = [local.quarantine_prefix, local.manifests_prefix]
  }

  statement {
    sid       = "KMSDecryptForReads"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = [var.kms_key_arn]
  }

  statement {
    sid = "ConsumeValidatorQueue"
    actions = [
      "sqs:ReceiveMessage", "sqs:DeleteMessage",
      "sqs:GetQueueAttributes", "sqs:GetQueueUrl",
    ]
    # Queue ARN is fixed via naming convention (constructed in the
    # lambda_validator module) so we can reference it here without a
    # cross-module dependency.
    resources = [
      "arn:aws:sqs:${local.region}:${local.account_id}:${var.name_prefix}-validator-queue",
      "arn:aws:sqs:${local.region}:${local.account_id}:${var.name_prefix}-validator-queue-dlq",
    ]
  }

  statement {
    sid       = "PublishAlerts"
    actions   = ["sns:Publish"]
    resources = [var.alerts_topic_arn]
  }

  statement {
    sid = "EmitCloudWatchMetrics"
    # Note: cloudwatch:PutMetricData can't be scoped by namespace via
    # IAM — it's a service limitation. Documented in README.md.
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["Lakehouse/Validator", "Lakehouse/Pipeline"]
    }
  }

  statement {
    sid       = "WriteLambdaLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${var.lambda_log_group_arn}:*"]
  }
}

resource "aws_iam_role_policy" "lambda_inline" {
  name   = "${var.name_prefix}-lambda-validator-policy"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_inline.json
}

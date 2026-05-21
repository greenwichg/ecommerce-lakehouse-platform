# Step Functions execution role for the quarantine-review-replay state
# machine.
#
# Permissions:
#   - Lambda InvokeFunction on the validator (re-validation step) and on
#     supporting Lambdas defined in modules/step_functions
#   - S3 CopyObject + DeleteObject scoped to quarantine/ (replay flow
#     moves fixed files from quarantine back to raw/)
#   - SNS Publish on alerts topic (operator notifications)
#   - CloudWatch Logs to its log group
#
# Explicit deny by omission: Databricks, Snowflake, any S3 write outside
# quarantine/.

resource "aws_iam_role" "sfn" {
  name               = "${var.name_prefix}-stepfn-quarantine-replay"
  assume_role_policy = data.aws_iam_policy_document.assume_role_step_functions.json
  description        = "Step Functions role for quarantine-review-replay. Invokes Lambdas, moves quarantine files, publishes SNS."
  tags               = var.tags
}

data "aws_iam_policy_document" "sfn_inline" {
  statement {
    sid     = "InvokeQuarantineLambdas"
    actions = ["lambda:InvokeFunction"]
    resources = [
      "arn:aws:lambda:${local.region}:${local.account_id}:function:${var.name_prefix}-file-validator",
      "arn:aws:lambda:${local.region}:${local.account_id}:function:${var.name_prefix}-quarantine-helper",
    ]
  }

  statement {
    sid       = "ListBucketForQuarantine"
    actions   = ["s3:ListBucket"]
    resources = [var.bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["quarantine/*", "raw/*"]
    }
  }

  statement {
    sid     = "MoveQuarantineFilesBackToRaw"
    actions = [
      "s3:GetObject", "s3:GetObjectAttributes",
      "s3:PutObject", "s3:DeleteObject",
    ]
    resources = [local.quarantine_prefix, local.raw_prefix]
  }

  statement {
    sid       = "KMSDecryptForReads"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = [var.kms_key_arn]
  }

  statement {
    sid       = "PublishOperatorAlerts"
    actions   = ["sns:Publish"]
    resources = [var.alerts_topic_arn]
  }

  # CloudWatch Logs delivery — SFN logs full execution history when
  # configured with logging_configuration in the state machine resource.
  statement {
    sid = "WriteSfnExecutionLogs"
    actions = [
      "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery", "logs:ListLogDeliveries",
      "logs:PutResourcePolicy", "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups", "logs:CreateLogStream", "logs:PutLogEvents",
    ]
    # SFN's log-delivery API is account-wide; can't be scoped to a single
    # log group via IAM. Constrained later via the state machine's
    # logging_configuration target. Documented in README.md.
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "sfn_inline" {
  name   = "${var.name_prefix}-stepfn-quarantine-replay-policy"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn_inline.json
}

# Quarantine-helper Lambda role.
#
# Smaller and more permissive than the validator role on writes —
# moves files OUT of quarantine/ back to raw/, and writes an audit log
# under processed/_quarantine_audit/. Also reads the Airflow API
# credentials secret so it can trigger a DAG run.
#
# Permissions:
#   - S3: GetObject on quarantine/*; PutObject on raw/* and the audit
#         prefix; DeleteObject on quarantine/* (after a successful copy)
#   - Secrets Manager: GetSecretValue on the Airflow API creds secret
#   - KMS: Decrypt + GenerateDataKey (S3 reads + writes go through KMS)
#   - CloudWatch Logs: write to the shared Lambda log group
#
# Explicit deny by omission: SQS (this Lambda isn't queue-driven),
# Databricks, Snowflake creds, writes to bronze/silver/gold.

resource "aws_iam_role" "lambda_helper" {
  name               = "${var.name_prefix}-lambda-quarantine-helper"
  assume_role_policy = data.aws_iam_policy_document.assume_role_lambda.json
  description        = "Quarantine-helper Lambda role. Moves quarantine→raw, writes audit logs, reads Airflow creds."
  tags               = var.tags
}

data "aws_iam_policy_document" "lambda_helper_inline" {
  statement {
    sid       = "ListBucketForQuarantineAndFix"
    actions   = ["s3:ListBucket"]
    resources = [var.bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      # _fixed/* lives under quarantine/, so quarantine/* covers both
      # the original file and the operator-uploaded fix.
      values = ["quarantine/*", "raw/*"]
    }
  }

  statement {
    sid       = "ReadQuarantineAndFixedFiles"
    actions   = ["s3:GetObject", "s3:GetObjectAttributes"]
    resources = [local.quarantine_prefix]
  }

  statement {
    sid       = "MoveBackToRawAndDeleteFromQuarantine"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = [local.raw_prefix, local.quarantine_prefix]
  }

  statement {
    sid     = "WriteAuditLog"
    actions = ["s3:PutObject", "s3:GetObject"]
    # Append-style audit: handler reads existing line(s), appends new line,
    # writes back. Same audit-file key per day.
    resources = ["${var.bucket_arn}/processed/_quarantine_audit/*"]
  }

  statement {
    sid     = "ReadAirflowApiCreds"
    actions = ["secretsmanager:GetSecretValue"]
    # Airflow creds are one of three secrets created in modules/secrets.
    # Scoped to that specific secret rather than all of var.secrets_arns
    # since this Lambda doesn't need Databricks PAT or Snowflake creds.
    resources = [var.secrets_arns["airflow-api-creds"]]
  }

  statement {
    sid       = "KMSAccessForS3AndSecrets"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = [var.kms_key_arn]
  }

  statement {
    sid       = "WriteLambdaLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${var.lambda_log_group_arn}:*"]
  }
}

resource "aws_iam_role_policy" "lambda_helper_inline" {
  name   = "${var.name_prefix}-lambda-quarantine-helper-policy"
  role   = aws_iam_role.lambda_helper.id
  policy = data.aws_iam_policy_document.lambda_helper_inline.json
}

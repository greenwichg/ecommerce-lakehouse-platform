# Databricks job cluster role.
#
# Permissions:
#   - Read: raw, _checkpoints (Auto Loader state)
#   - Write: bronze, silver, gold, quarantine, _checkpoints
#   - KMS: Decrypt + GenerateDataKey on the bucket key
#   - Secrets Manager: GetSecretValue on snowflake-creds (for the
#     Snowflake JDBC connector) and databricks-pat (rare; usually NOT
#     needed, but listed for parity with Airflow if any job needs to
#     self-reference)
#
# Explicit deny by omission: any S3 action outside the bucket, any
# EC2/IAM/Lambda/Step Functions action.

resource "aws_iam_role" "databricks" {
  name               = "${var.name_prefix}-databricks-jobs"
  assume_role_policy = data.aws_iam_policy_document.assume_role_databricks_ec2.json
  description        = "Databricks job cluster role. Read raw/_checkpoints; write bronze/silver/gold/quarantine/_checkpoints."
  tags               = var.tags
}

data "aws_iam_policy_document" "databricks_inline" {
  # Listing the bucket — required for any prefix-based read. Scoped via
  # the s3:prefix condition so list operations can't enumerate keys
  # outside the layers we own.
  statement {
    sid       = "ListBucketScopedByPrefix"
    actions   = ["s3:ListBucket"]
    resources = [var.bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "raw/*", "bronze/*", "silver/*", "gold/*",
        "quarantine/*", "_checkpoints/*",
      ]
    }
  }

  statement {
    sid       = "ReadRawAndCheckpoints"
    actions   = ["s3:GetObject", "s3:GetObjectAttributes"]
    resources = [local.raw_prefix, local.checkpoints_prefix]
  }

  statement {
    sid = "WriteBronzeSilverGoldQuarantineCheckpoints"
    actions = [
      "s3:PutObject", "s3:DeleteObject", "s3:GetObject",
      "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
    ]
    resources = [
      local.bronze_prefix, local.silver_prefix, local.gold_prefix,
      local.quarantine_prefix, local.checkpoints_prefix,
    ]
  }

  statement {
    sid       = "KMSEncryptDecryptForBucketKey"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = [var.kms_key_arn]
  }

  statement {
    sid     = "ReadSnowflakeCreds"
    actions = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
    resources = [
      var.secrets_arns["snowflake-creds"],
    ]
  }
}

resource "aws_iam_role_policy" "databricks_inline" {
  name   = "${var.name_prefix}-databricks-jobs-policy"
  role   = aws_iam_role.databricks.id
  policy = data.aws_iam_policy_document.databricks_inline.json
}

# Instance profile wraps the role so EC2 can use it.
resource "aws_iam_instance_profile" "databricks" {
  name = "${var.name_prefix}-databricks-jobs"
  role = aws_iam_role.databricks.name
  tags = var.tags
}

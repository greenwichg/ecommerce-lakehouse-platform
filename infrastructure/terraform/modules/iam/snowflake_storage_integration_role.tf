# Snowflake Storage Integration role.
#
# Trust: Snowflake-managed AWS account + ExternalId (both placeholders;
# rotate post-apply per the README "Snowflake STORAGE INTEGRATION
# bootstrap" walkthrough).
#
# Permissions:
#   - List on the bucket (Snowflake needs ListObjectsV2 to discover
#     stage contents)
#   - GetObject on gold/* (the stages all live under gold/)
#   - KMS Decrypt for SSE-KMS objects
#
# No write — Snowflake reads from the lake, never writes.

resource "aws_iam_role" "snowflake_storage_integration" {
  name               = "${var.name_prefix}-snowflake-storage-integration"
  assume_role_policy = data.aws_iam_policy_document.assume_role_snowflake.json
  description        = "Role that Snowflake assumes via STORAGE INTEGRATION to read gold Parquet from S3."
  tags               = var.tags
}

data "aws_iam_policy_document" "snowflake_inline" {
  statement {
    sid       = "ListGoldPrefix"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["gold/*"]
    }
  }

  statement {
    sid       = "ReadGoldObjects"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${var.bucket_arn}/gold/*"]
  }

  statement {
    sid       = "KMSDecryptForSSEKMS"
    actions   = ["kms:Decrypt", "kms:DescribeKey"]
    resources = [var.kms_key_arn]
  }
}

resource "aws_iam_role_policy" "snowflake_inline" {
  name   = "${var.name_prefix}-snowflake-storage-integration-policy"
  role   = aws_iam_role.snowflake_storage_integration.id
  policy = data.aws_iam_policy_document.snowflake_inline.json
}

# Single bucket with prefix-per-zone layout.
#
# Why one bucket rather than four (raw/processed/archive/quarantine):
#  - S3 bucket count is account-scoped, low limit (~100 buckets) by default
#  - Cross-zone moves stay within one bucket = cheaper + faster (no replication)
#  - IAM policies can scope by prefix just as effectively as by bucket
#  - Lifecycle policies attach to a bucket and filter by prefix — clean fit
#
# Prefixes used:
#   raw/<source>/year=YYYY/month=MM/day=DD/      raw source files
#   bronze/<source>/                              Auto Loader output
#   silver/<source>/                              dedup'd current state
#   gold/<fact_or_dim>/                           star schema
#   quarantine/<source>/                          Lambda-quarantined bad files
#   processed/_manifests/                         Lambda validator manifests
#   _checkpoints/<layer>/<source>/                Auto Loader / Streaming state
#   archive/                                      reserved (lifecycle target)

resource "aws_kms_key" "lakehouse" {
  description             = "Encryption key for the ${var.env} lakehouse bucket."
  deletion_window_in_days = var.kms_key_deletion_window_days
  enable_key_rotation     = true
  tags                    = var.tags
}

resource "aws_kms_alias" "lakehouse" {
  name          = "alias/lakehouse-${var.env}"
  target_key_id = aws_kms_key.lakehouse.key_id
}

resource "aws_s3_bucket" "lakehouse" {
  bucket = var.bucket_name
  tags   = var.tags
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lakehouse.arn
    }
    # Use S3 Bucket Keys to drop KMS request volume by ~99% for high-write
    # workloads (every Bronze write would otherwise hit KMS once per object).
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    # MFA-delete is not enableable via Terraform (requires root + console),
    # so we don't try; non-current versioning is enough to undo accidental
    # overwrites/deletes.
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  # Raw zone: transition to Glacier Flexible Retrieval after N days (spec
  # default 90). Object content stops getting hot-tier IO costs while
  # remaining queryable via expedited restore (~1-5 min, ~$0.03/GB).
  rule {
    id     = "raw-to-glacier"
    status = "Enabled"
    filter { prefix = "raw/" }

    transition {
      days          = var.raw_lifecycle_archive_after_days
      storage_class = "GLACIER"
    }

    # Old non-current versions get cleaned up after 30 days — versioning
    # is for accident-recovery, not long-term history.
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  # Quarantine zone: keep for 90 days, then expire. If ops hasn't reviewed
  # a quarantined file within 90 days, the operational record is the
  # manifest in processed/_manifests — the file itself can go.
  rule {
    id     = "quarantine-expire-90d"
    status = "Enabled"
    filter { prefix = "quarantine/" }

    expiration {
      days = 90
    }
  }

  # Manifests: keep indefinitely. They're tiny (KB-scale) and operational
  # gold — answers "what arrived when" without re-reading raw.
  rule {
    id     = "manifests-keep-current"
    status = "Enabled"
    filter { prefix = "processed/_manifests/" }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  # Checkpoints: never archive. Auto Loader needs them readable on every
  # bronze run. Cleanup happens in the streaming framework, not via
  # lifecycle.
  rule {
    id     = "checkpoints-no-transition"
    status = "Enabled"
    filter { prefix = "_checkpoints/" }

    # Just expire abandoned multipart uploads — cheap insurance.
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# Note: aws_s3_bucket_notification lives at the top-level main.tf, not
# here. The notification needs the SQS queue ARN from
# modules/lambda_validator, and that queue's access policy needs this
# bucket's ARN — circular if both lived in the same module. Top-level
# wiring breaks the cycle cleanly.

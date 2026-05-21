# Top-level wiring. Each module gets its inputs from variables.tf and
# from other modules' outputs. Keep this file thin — the per-module
# detail lives in modules/<name>/main.tf.

locals {
  name_prefix = "lakehouse-${var.env}"

  common_tags = {
    Project     = "ecommerce-lakehouse-platform"
    Environment = var.env
    ManagedBy   = "terraform"
  }
}

# -----------------------------------------------------------------------------
# Storage: buckets + lifecycle + KMS
# -----------------------------------------------------------------------------
module "s3" {
  source                           = "./modules/s3"
  bucket_name                      = var.lakehouse_bucket_name
  env                              = var.env
  kms_key_deletion_window_days     = var.kms_key_deletion_window_days
  raw_lifecycle_archive_after_days = var.raw_lifecycle_archive_after_days
  tags                             = local.common_tags
}

# -----------------------------------------------------------------------------
# Alerting + observability
# -----------------------------------------------------------------------------
module "sns" {
  source      = "./modules/sns"
  name_prefix = local.name_prefix
  alert_email = var.alert_email
  tags        = local.common_tags
}

module "cloudwatch" {
  source               = "./modules/cloudwatch"
  name_prefix          = local.name_prefix
  log_retention_days   = var.cloudwatch_log_retention_days
  sns_alerts_topic_arn = module.sns.alerts_topic_arn
  bucket_name          = module.s3.bucket_name
  tags                 = local.common_tags
}

# -----------------------------------------------------------------------------
# Secrets — placeholder values; real ones populated post-apply via AWS console
# -----------------------------------------------------------------------------
module "secrets" {
  source      = "./modules/secrets"
  name_prefix = local.name_prefix
  tags        = local.common_tags
}

# -----------------------------------------------------------------------------
# IAM: four roles, one per consumer. Each module above can read the role ARNs
# from outputs without duplicating policy documents.
# -----------------------------------------------------------------------------
module "iam" {
  source               = "./modules/iam"
  name_prefix          = local.name_prefix
  bucket_arn           = module.s3.bucket_arn
  kms_key_arn          = module.s3.kms_key_arn
  secrets_arns         = module.secrets.secret_arns
  alerts_topic_arn     = module.sns.alerts_topic_arn
  lambda_log_group_arn = module.cloudwatch.lambda_log_group_arn
  sfn_log_group_arn    = module.cloudwatch.sfn_log_group_arn
  tags                 = local.common_tags
}

# -----------------------------------------------------------------------------
# File validator: Container Image Lambda triggered by S3 → SQS
# -----------------------------------------------------------------------------
module "lambda_validator" {
  source           = "./modules/lambda_validator"
  name_prefix      = local.name_prefix
  bucket_id        = module.s3.bucket_id
  bucket_arn       = module.s3.bucket_arn
  image_uri        = var.lambda_validator_image_uri
  lambda_role_arn  = module.iam.lambda_role_arn
  log_group_name   = module.cloudwatch.lambda_log_group_name
  alerts_topic_arn = module.sns.alerts_topic_arn
  tags             = local.common_tags
}

# S3 → SQS event notification wired here at top level to break the
# circular dependency (queue's access policy needs the bucket ARN; the
# bucket notification needs the queue ARN).
resource "aws_s3_bucket_notification" "raw_to_validator" {
  bucket = module.s3.bucket_id

  queue {
    queue_arn     = module.lambda_validator.queue_arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "raw/"
  }

  # The queue policy in modules/lambda_validator must be in place before
  # S3 tries to publish, otherwise the first put fails. Explicit
  # depends_on makes the apply order obvious.
  depends_on = [module.lambda_validator]
}

# -----------------------------------------------------------------------------
# Step Functions: human-in-the-loop quarantine review + replay
# -----------------------------------------------------------------------------
module "step_functions" {
  source                 = "./modules/step_functions"
  name_prefix            = local.name_prefix
  sfn_role_arn           = module.iam.sfn_role_arn
  helper_lambda_role_arn = module.iam.lambda_helper_role_arn
  log_group_arn          = module.cloudwatch.sfn_log_group_arn
  validator_function_arn = module.lambda_validator.function_arn
  alerts_topic_arn       = module.sns.alerts_topic_arn
  bucket_id              = module.s3.bucket_id
  tags                   = local.common_tags
}

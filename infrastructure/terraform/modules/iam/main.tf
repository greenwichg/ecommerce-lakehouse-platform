# IAM least-privilege roles, four total + one bonus (Snowflake storage
# integration). Each role's policy lives in its own file in this module
# for readability:
#
#   databricks_instance_profile.tf
#   airflow_execution_role.tf
#   lambda_role.tf
#   step_functions_role.tf
#   snowflake_storage_integration_role.tf
#
# Shared policy fragments (assume-role docs that get reused) live in
# policy_documents.tf.
#
# See README.md for the principle-of-least-privilege walkthrough and
# documented deviations.

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  # Common attributes used in policy statements
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name

  # Bucket prefix → action scoping. Easier to read inline than re-typing
  # the bucket ARN concatenation each time.
  raw_prefix         = "${var.bucket_arn}/raw/*"
  bronze_prefix      = "${var.bucket_arn}/bronze/*"
  silver_prefix      = "${var.bucket_arn}/silver/*"
  gold_prefix        = "${var.bucket_arn}/gold/*"
  quarantine_prefix  = "${var.bucket_arn}/quarantine/*"
  manifests_prefix   = "${var.bucket_arn}/processed/_manifests/*"
  checkpoints_prefix = "${var.bucket_arn}/_checkpoints/*"
}

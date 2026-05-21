# Secrets Manager entries with placeholder values.
#
# Terraform creates the secret resources + initial placeholder JSON;
# operators rotate to real values post-apply via:
#
#   aws secretsmanager put-secret-value \
#       --secret-id <name> --secret-string file://real-creds.json
#
# Why placeholders rather than no `secret_string`: AWS Secrets Manager
# bills per secret-version, and an unversioned secret can confuse some
# clients. Placeholders are safe (the values document the JSON shape)
# and don't accidentally reveal anything.

locals {
  secrets = {
    "databricks-pat" = jsonencode({
      host  = "https://<workspace>.cloud.databricks.com"
      token = "PLACEHOLDER_REPLACE_VIA_PUT_SECRET_VALUE"
    })
    "snowflake-creds" = jsonencode({
      account    = "PLACEHOLDER"
      user       = "lakehouse_engineer"
      role       = "lakehouse_engineer"
      warehouse  = "prod_etl_wh"
      database   = "prod_lakehouse"
      # Snowflake prod auth uses key-pair, not password. PEM placeholder
      # gets put_secret_value'd to the real RSA private key post-apply.
      private_key_pem = "PLACEHOLDER_REPLACE_VIA_PUT_SECRET_VALUE"
    })
    "exchangerate-api-key" = jsonencode({
      # exchangerate.host doesn't actually need a key on the free tier,
      # but if we switch to a paid provider in Slice 6+ the secret exists.
      api_key = "PLACEHOLDER_NOT_CURRENTLY_USED"
    })
    "airflow-api-creds" = jsonencode({
      # Quarantine-replay state machine's replay branch hits the Airflow
      # REST API to kick off the daily DAG against the recovered file.
      # MWAA exposes this on https://<env>.<region>.airflow.amazonaws.com.
      base_url = "https://PLACEHOLDER.airflow.amazonaws.com"
      username = "lakehouse_replay"
      password = "PLACEHOLDER_REPLACE_VIA_PUT_SECRET_VALUE"
    })
  }
}

resource "aws_secretsmanager_secret" "lakehouse" {
  for_each = local.secrets

  name        = "${var.name_prefix}/${each.key}"
  description = "Lakehouse secret: ${each.key}. Replace placeholder via put-secret-value post-apply."

  # Recovery window of 7 days lets ops undo accidental deletions; safer
  # than the default 30 because we may iterate on these often early on.
  recovery_window_in_days = 7

  tags = var.tags
}

resource "aws_secretsmanager_secret_version" "placeholder" {
  for_each = local.secrets

  secret_id     = aws_secretsmanager_secret.lakehouse[each.key].id
  secret_string = each.value
}

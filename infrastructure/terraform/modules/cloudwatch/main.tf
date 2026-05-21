# Log groups: one per consumer that writes logs (Lambda validator + Step
# Functions). Airflow's logs live wherever Airflow is hosted (MWAA writes
# them itself; self-hosted goes to the EC2 instance) — not configured
# here.

resource "aws_cloudwatch_log_group" "lambda_validator" {
  name              = "/aws/lambda/${var.name_prefix}-file-validator"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/states/${var.name_prefix}-quarantine-review-replay"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# -----------------------------------------------------------------------------
# Alarms — pre-defined for the metrics every Slice 1–4 component already
# emits or that the Lambda validator publishes.
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "lambda_error_rate" {
  alarm_name          = "${var.name_prefix}-lambda-validator-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "More than 5 validator Lambda errors in 10 minutes — investigate"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.sns_alerts_topic_arn]
  dimensions = {
    FunctionName = "${var.name_prefix}-file-validator"
  }
  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "quarantine_depth" {
  alarm_name          = "${var.name_prefix}-quarantine-queue-depth"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "QuarantineCount"
  namespace           = "Lakehouse/Validator"
  period              = 3600
  statistic           = "Sum"
  threshold           = 10
  alarm_description   = "More than 10 quarantines in the last hour — likely upstream schema break"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.sns_alerts_topic_arn]
  tags                = var.tags
}

resource "aws_cloudwatch_metric_alarm" "currency_fallback_rate" {
  alarm_name          = "${var.name_prefix}-currency-fallback-rate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  # The Slice 4 dq_currency_freshness gate publishes this metric on each
  # daily DAG run. Threshold = 50% matches the gate's hard-fail boundary;
  # the alarm gives us advance notice via SNS even when the gate hasn't
  # fired yet.
  metric_name        = "CurrencySimulatedPct"
  namespace          = "Lakehouse/Pipeline"
  period             = 86400
  statistic          = "Maximum"
  threshold          = 50
  alarm_description  = "Currency API fallback rate > 50% — investigate exchangerate.host availability"
  treat_missing_data = "notBreaching"
  alarm_actions      = [var.sns_alerts_topic_arn]
  tags               = var.tags
}

# -----------------------------------------------------------------------------
# Dashboard — consolidated pipeline view. Body lives in a separate file
# so the JSON stays readable and we can syntax-check it.
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = "${var.name_prefix}-pipeline"
  dashboard_body = templatefile("${path.module}/dashboard_body.json", {
    region      = data.aws_region.current.name
    name_prefix = var.name_prefix
    bucket_name = var.bucket_name
  })
}

data "aws_region" "current" {}

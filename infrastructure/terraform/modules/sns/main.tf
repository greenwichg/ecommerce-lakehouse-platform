# Pipeline alerts topic. Subscribers receive failure notifications from
# Airflow DAGs (via sns_failure_callback), Lambda quarantines, Step
# Functions transitions, and CloudWatch alarms.
#
# Email-only for Slice 5. Slack/PagerDuty/etc. are documented as a
# production upgrade path in docs/runbook.md — webhook subscription
# requires either Lambda-bridged SubscriptionConfirmation or an HTTPS
# endpoint, both of which are out of scope for code-only validate.

resource "aws_sns_topic" "alerts" {
  name = "${var.name_prefix}-pipeline-alerts"

  # KMS encrypt the topic at rest. Uses the AWS-managed SNS key by
  # default (good enough for ops alerts; not customer data).
  kms_master_key_id = "alias/aws/sns"

  tags = var.tags
}

resource "aws_sns_topic_subscription" "alert_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Allow CloudWatch Alarms to publish into this topic. The alarms
# themselves are defined in modules/cloudwatch; this policy is just the
# permission glue.
data "aws_iam_policy_document" "alerts_topic_policy" {
  statement {
    sid    = "AllowCloudWatchPublish"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }

    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.alerts_topic_policy.json
}

# File validator pipeline: S3 → SQS → Lambda.
#
# Why SQS in the middle (rather than direct S3 → Lambda):
#  - Built-in retry with visibility timeout. A transient pyarrow failure
#    or KMS throttle gets retried automatically.
#  - DLQ for repeated failures — easy to inspect what's going wrong
#    without writing custom dead-letter logic in the Lambda.
#  - Decouples bursts. S3 can fire dozens of events per second during a
#    backfill; SQS absorbs the burst and Lambda concurrency draws from
#    it at its own rate.
#  - Lambda failure doesn't lose the event (S3 notification → Lambda
#    direct invocation has no replay if Lambda throttles/errors).

# Dead letter queue: messages that fail validation max-receives times
# end up here. Cloudwatch alarm on ApproximateNumberOfMessages > 0
# tells ops to investigate.
resource "aws_sqs_queue" "validator_dlq" {
  name                       = "${var.name_prefix}-validator-queue-dlq"
  message_retention_seconds  = 14 * 24 * 3600  # 14 days
  visibility_timeout_seconds = var.visibility_timeout_seconds
  tags                       = var.tags
}

resource "aws_sqs_queue" "validator" {
  name                       = "${var.name_prefix}-validator-queue"
  visibility_timeout_seconds = var.visibility_timeout_seconds
  message_retention_seconds  = 4 * 24 * 3600   # 4 days
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.validator_dlq.arn
    maxReceiveCount     = 3
  })
  tags = var.tags
}

# Queue policy: allow S3 to publish only from our bucket.
data "aws_iam_policy_document" "queue_policy" {
  statement {
    sid    = "AllowS3Publish"
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.validator.arn]
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [var.bucket_arn]
    }
  }
}

resource "aws_sqs_queue_policy" "validator" {
  queue_url = aws_sqs_queue.validator.id
  policy    = data.aws_iam_policy_document.queue_policy.json
}

# Lambda function — Container Image deployment. The image URI points at
# ECR; build/push is documented in the top-level README.
resource "aws_lambda_function" "validator" {
  function_name = "${var.name_prefix}-file-validator"
  role          = var.lambda_role_arn
  package_type  = "Image"
  image_uri     = var.image_uri

  timeout     = var.lambda_timeout_seconds
  memory_size = var.lambda_memory_mb

  environment {
    variables = {
      LAKEHOUSE_BUCKET = var.bucket_id
      ALERTS_TOPIC_ARN = var.alerts_topic_arn
    }
  }

  # Logs go to the pre-created log group from modules/cloudwatch.
  # Naming: /aws/lambda/<function-name> is the convention.
  logging_config {
    log_group  = var.log_group_name
    log_format = "JSON"
  }

  tags = var.tags
}

# SQS → Lambda event source mapping. Batches up to 10 records per
# invocation; partial-batch reporting lets one bad message not poison
# the whole batch.
resource "aws_lambda_event_source_mapping" "validator" {
  event_source_arn = aws_sqs_queue.validator.arn
  function_name    = aws_lambda_function.validator.arn
  batch_size       = 10
  maximum_batching_window_in_seconds = 5
  function_response_types            = ["ReportBatchItemFailures"]
}

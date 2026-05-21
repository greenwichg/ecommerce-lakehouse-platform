output "alerts_topic_arn" {
  description = "ARN of the alerts topic. Use as Airflow Variable sns_alert_topic_arn."
  value       = aws_sns_topic.alerts.arn
}

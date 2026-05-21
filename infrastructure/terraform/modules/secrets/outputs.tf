output "secret_arns" {
  description = "Map of logical secret name to ARN. Consumed by the IAM module to scope GetSecretValue permissions."
  value       = { for k, v in aws_secretsmanager_secret.lakehouse : k => v.arn }
}

output "secret_names" {
  description = "Map of logical secret name to full Secrets Manager name (used by Airflow's SecretsManagerBackend)."
  value       = { for k, v in aws_secretsmanager_secret.lakehouse : k => v.name }
}

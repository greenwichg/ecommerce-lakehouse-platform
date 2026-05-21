output "bucket_id" {
  description = "Bucket name (not ARN)."
  value       = aws_s3_bucket.lakehouse.id
}

output "bucket_name" {
  description = "Alias for bucket_id."
  value       = aws_s3_bucket.lakehouse.id
}

output "bucket_arn" {
  description = "Bucket ARN, for IAM policy scoping."
  value       = aws_s3_bucket.lakehouse.arn
}

output "kms_key_id" {
  description = "KMS key ID."
  value       = aws_kms_key.lakehouse.key_id
}

output "kms_key_arn" {
  description = "KMS key ARN, for IAM policy scoping."
  value       = aws_kms_key.lakehouse.arn
}

env                            = "prod"
region                         = "us-east-1"
account_id                     = "210987654321"          # replace with real prod account
lakehouse_bucket_name          = "ecommerce-lakehouse-prod"
alert_email                    = "data-platform-oncall@example.com"
kms_key_deletion_window_days   = 30
cloudwatch_log_retention_days  = 90
raw_lifecycle_archive_after_days = 90

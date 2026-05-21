env                              = "dev"
region                           = "us-east-1"
account_id                       = "123456789012" # replace with real dev account
lakehouse_bucket_name            = "ecommerce-lakehouse-dev"
alert_email                      = "data-platform-dev@example.com"
kms_key_deletion_window_days     = 7 # short window OK for dev
cloudwatch_log_retention_days    = 14
raw_lifecycle_archive_after_days = 30 # cheaper dev cycle

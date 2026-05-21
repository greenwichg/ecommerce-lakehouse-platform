# Remote state backend.
#
# Commented out for code-only validation (no AWS account to back into).
# Uncomment and configure before the first `terraform apply` in any real
# environment — running with local state in prod is a bad time waiting to
# happen.
#
# The state-bucket + lock-table need to exist BEFORE this is uncommented;
# they're usually created by a tiny separate "bootstrap" terraform run or
# by hand once.
#
# terraform {
#   backend "s3" {
#     bucket         = "ecommerce-lakehouse-tfstate"
#     key            = "infrastructure/${var.env}/terraform.tfstate"
#     region         = "us-east-1"
#     dynamodb_table = "ecommerce-lakehouse-tfstate-lock"
#     encrypt        = true
#   }
# }

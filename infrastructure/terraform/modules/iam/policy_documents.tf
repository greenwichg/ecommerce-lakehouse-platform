# Shared assume-role policies. Putting these here means each role's file
# stays focused on its data policy rather than mixing trust + permission
# JSON.

data "aws_iam_policy_document" "assume_role_lambda" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "assume_role_step_functions" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

# Databricks instance profile: an EC2 role that Databricks attaches to
# job clusters. The trust policy allows the EC2 service plus the
# Databricks-managed AWS account that runs the cluster ec2-assume-role
# flow (account ID below is a placeholder; the real one comes from your
# Databricks workspace's "Instance Profiles" page).
data "aws_iam_policy_document" "assume_role_databricks_ec2" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

# Airflow execution role trust depends on hosting:
#   - MWAA: principal = airflow.amazonaws.com
#   - Self-hosted on EC2: principal = ec2.amazonaws.com (attach as
#     instance profile)
#   - Self-hosted on EKS: configure IRSA
# We provision for MWAA by default; self-hosted shops can edit the trust.
data "aws_iam_policy_document" "assume_role_airflow_mwaa" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
    }
  }
}

# Snowflake storage integration trust. Snowflake-managed AWS account
# assumes this role via STS. ExternalId is supplied by Snowflake on
# `DESC INTEGRATION lakehouse_s3_integration`; we use a placeholder here
# and document the rotation step in modules/iam/README.md.
data "aws_iam_policy_document" "assume_role_snowflake" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::PLACEHOLDER_SNOWFLAKE_ACCOUNT:root"]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = ["PLACEHOLDER_EXTERNAL_ID"]
    }
  }
}

# IAM module — least-privilege walkthrough

Five roles, each in its own file for readability:

| Role | File | Trusted by | Why exists |
|---|---|---|---|
| `databricks-jobs` (+ instance profile) | `databricks_instance_profile.tf` | `ec2.amazonaws.com` | Job clusters need bucket R/W |
| `airflow-execution` | `airflow_execution_role.tf` | `airflow.amazonaws.com` (MWAA) | Sensor + secrets + Lambda/SFN invoke |
| `lambda-validator` | `lambda_role.tf` | `lambda.amazonaws.com` | Validator function |
| `stepfn-quarantine-replay` | `step_functions_role.tf` | `states.amazonaws.com` | Quarantine workflow |
| `snowflake-storage-integration` | `snowflake_storage_integration_role.tf` | Snowflake AWS account (via ExternalId) | `STORAGE INTEGRATION` source |

## Principle of least privilege

Each role has the narrowest action × resource scope that lets it do its
job and nothing more. Specifically:

- **No `Action: *` anywhere**. Every statement names actions explicitly.
- **No `Resource: *`** except where AWS limits force it (documented
  deviations below).
- **Bucket access scoped by prefix**. Every role's `s3:ListBucket` uses a
  `Condition` block with `s3:prefix` to restrict what they can
  enumerate. `s3:GetObject` and write actions go to specific
  `<bucket>/<prefix>/*` ARNs.
- **Secrets read scoped per role**. Databricks only gets `snowflake-creds`
  (its only legitimate use); Airflow gets all of them (legitimately
  needs each for different connection types).
- **KMS Decrypt scoped to the bucket key** only.

## Deviations from strict least-privilege (documented)

These would ideally be tighter but aren't, for service-side reasons:

### 1. `cloudwatch:PutMetricData` resource = `"*"`

CloudWatch's `PutMetricData` doesn't accept a per-resource ARN — the
metric is identified by namespace + dimensions, not an ARN. We mitigate
by adding a `Condition: cloudwatch:namespace IN ["Lakehouse/Validator",
"Lakehouse/Pipeline"]` which is the most we can do.

### 2. Step Functions `logs:*` resource = `"*"`

`logs:CreateLogDelivery` and related actions are account-wide; they
configure log routing globally, not per-log-group. The actual log
delivery target is constrained in the state machine's
`logging_configuration` block, so the *effective* scope is one log
group even though the IAM is unscoped.

### 3. `s3:ListBucket` requires the bucket-level ARN

You can't `ListBucket` a prefix directly — IAM requires the bucket
ARN. The `s3:prefix` condition makes this effectively prefix-scoped.

### 4. KMS Decrypt is full-key, not per-encryption-context

KMS doesn't support encryption-context-aware policy scoping at the
key-policy level (encryption-context can constrain on a per-call basis
but not via IAM). The bucket key is single-purpose (encrypts only the
lakehouse bucket), so this is acceptable.

## What each role CAN'T do (validated by omission)

- Databricks: write to S3 outside the bucket; read any other secret;
  invoke Lambda; touch Step Functions; modify IAM; modify VPC; access
  EC2 control plane.
- Airflow: write to bronze/silver/gold (that's Databricks' job); modify
  IAM; modify VPC; access RDS; cross-account anything.
- Lambda: write to bronze/silver/gold; access Databricks; access
  Snowflake; modify IAM.
- Step Functions: write to bronze/silver/gold; access Databricks;
  access Snowflake; modify IAM.
- Snowflake storage integration: write to S3; access any prefix outside
  `gold/`; access KMS keys other than the bucket key; access Secrets
  Manager; access any other AWS service.

## Snowflake STORAGE INTEGRATION bootstrap

The trust policy in `policy_documents.tf` references
`PLACEHOLDER_SNOWFLAKE_ACCOUNT` and `PLACEHOLDER_EXTERNAL_ID`. The real
values come from Snowflake post-apply:

```sql
-- 1. Create the Snowflake side referencing this role's ARN
CREATE STORAGE INTEGRATION lakehouse_s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    STORAGE_AWS_ROLE_ARN = '<snowflake_storage_integration_role_arn from outputs>'
    STORAGE_ALLOWED_LOCATIONS = ('s3://<bucket>/gold/')
    ENABLED = TRUE;

-- 2. Snowflake assigns it an external ID. Read both:
DESC INTEGRATION lakehouse_s3_integration;
-- Look for STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID.

-- 3. Update the Terraform variables for the IAM trust policy:
#    edit modules/iam/policy_documents.tf assume_role_snowflake
#    identifiers = ["<STORAGE_AWS_IAM_USER_ARN>"]
#    values      = ["<STORAGE_AWS_EXTERNAL_ID>"]
#    terraform apply
```

This is the standard Snowflake STORAGE INTEGRATION dance — there's no
way to know Snowflake's assigned identifiers until after the Snowflake-
side object exists, so you `apply` twice.

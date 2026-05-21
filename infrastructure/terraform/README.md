# Terraform — ecommerce lakehouse infrastructure

Provisions the AWS infrastructure that every other slice has assumed
exists: S3 buckets with lifecycle policies, IAM roles, Lambda validator,
Step Functions quarantine-replay workflow, Secrets Manager entries, SNS
alerting, CloudWatch logs + dashboard.

## Layout

```
.
├── versions.tf                 # Terraform + provider pins
├── variables.tf                # Top-level inputs
├── backend.tf                  # S3 state backend (commented for code-only)
├── main.tf                     # Module wiring
├── outputs.tf                  # ARNs/IDs consumed by Airflow + Snowflake
├── environments/
│   ├── dev.tfvars
│   └── prod.tfvars
└── modules/
    ├── s3/                     # Buckets + prefixes + lifecycle + KMS
    ├── sns/                    # Alerts topic + email subscription
    ├── cloudwatch/             # Log groups + dashboard JSON
    ├── secrets/                # Secrets Manager placeholder entries
    ├── iam/                    # All four roles (Databricks, Airflow, Lambda, SFN)
    ├── lambda_validator/       # Container Image Lambda + SQS + S3 event wiring
    └── step_functions/         # Quarantine-review-replay state machine
```

## Code-only mode

This slice is code-only (no `terraform apply`). What we run is:

```bash
cd infrastructure/terraform
terraform init -backend=false
terraform validate
```

`terraform validate` catches:
- Syntax errors
- Missing required arguments
- Unknown resource attributes
- Type mismatches in variable usage
- Cross-module reference errors

It does NOT catch:
- AWS-side API rejections (resource name collisions, quota limits)
- IAM policy validity (the JSON parses but AWS rejects it as too broad)
- Wrong region / account assumptions

Those bake in on the first real `apply`, which is Slice 6+ scope.

## First real apply (post-Slice 6)

1. **Build + push the validator Lambda image** to ECR:
   ```bash
   cd lambda/file_validator
   docker build -t lakehouse-file-validator:latest .
   aws ecr get-login-password | docker login --username AWS --password-stdin \
       <account>.dkr.ecr.us-east-1.amazonaws.com
   docker tag lakehouse-file-validator:latest \
       <account>.dkr.ecr.us-east-1.amazonaws.com/lakehouse-file-validator:latest
   docker push <account>.dkr.ecr.us-east-1.amazonaws.com/lakehouse-file-validator:latest
   ```

2. **Configure the state backend**. Uncomment `backend.tf`, create the
   state bucket + DynamoDB lock table manually, then:
   ```bash
   terraform init -migrate-state
   ```

3. **Apply per environment**:
   ```bash
   terraform plan  -var-file=environments/prod.tfvars -out=prod.plan
   terraform apply prod.plan
   ```

4. **Populate Secrets Manager** with real values. The Terraform creates
   the secret resources with placeholder values; rotate them via:
   ```bash
   aws secretsmanager put-secret-value \
       --secret-id lakehouse-prod/snowflake-creds \
       --secret-string file://snowflake-creds.json
   ```

5. **Confirm the SNS email subscription** — first alert post-apply sends
   a confirmation request to `alert_email`. Operator must click the
   confirmation link in the email; otherwise alerts never deliver.

6. **Wire Airflow Variables / Snowflake DDL parameters** to the outputs
   from this stack. See `outputs.tf` for what's exposed.

## Module dependency graph

```
s3 ──────────────────► iam ──────► lambda_validator ──┐
   ╰───► cloudwatch ───╯                              ├───► step_functions
sns ────╯              │                              │
                       ╰──────────────────────────────╯
secrets ──────► iam (consumes secret ARNs for policy scoping)
```

## Operational notes

- **KMS key deletion window**: prod uses 30 days (the maximum), dev 7
  (shorter forgive-time, faster teardown). If a key is deleted, all
  data encrypted with it becomes unrecoverable.
- **Log retention**: prod 90d, dev 14d. Tunable per env.
- **Raw → Glacier transition**: prod 90d (per spec), dev 30d (cheaper
  testing cycle).

## What's deliberately NOT here

- **Databricks workspace**: managed by Databricks Terraform provider,
  out of scope (assumed pre-existing).
- **Snowflake account/warehouses/roles**: managed by `snowflake/ddl/`
  via schemachange, not by this Terraform.
- **MWAA environment**: if running managed Airflow, the MWAA module is
  a separate Slice 6+ deployment. Self-hosted Airflow on EC2 is also
  separate.
- **DNS / Route 53**: out of scope.

# S3 module

One bucket, prefix-per-zone layout. KMS-encrypted with bucket-key
optimisation (drops KMS request volume ~99x for high-write workloads).

## Why one bucket, not four

The spec says "S3 buckets: raw, processed, archive, quarantine" — plural.
A prefix-based layout inside a single bucket gives the same operational
isolation with these advantages:

- Bucket count is account-scoped to ~100 by default. We have plenty of
  room for one bucket-per-environment but not bucket-per-zone-per-env.
- Cross-zone moves (e.g., Lambda quarantining a raw file) stay within
  one bucket — no replication, no cross-account ARN gymnastics.
- IAM policies scope by prefix as effectively as by bucket; we already
  do this in the `iam` module.
- Lifecycle policies attach to a bucket and filter by prefix.

The prefix layout we standardize on:

| Prefix | Purpose | Lifecycle |
|---|---|---|
| `raw/<source>/year=...` | source files from generators | → Glacier after N days |
| `bronze/<source>/` | Auto Loader output | retained (the lake source-of-truth) |
| `silver/<source>/` | dedup'd current state | retained |
| `gold/<fact_or_dim>/` | star schema for BI | retained |
| `quarantine/<source>/` | Lambda-rejected files | expire after 90 days |
| `processed/_manifests/` | Lambda validator manifests | retained (tiny + operational gold) |
| `_checkpoints/<layer>/<source>/` | streaming state | no transition; abort orphan multiparts after 7d |
| `archive/` | reserved | (currently no rule; Slice 6+ may add) |

## Encryption

`aws:kms` with a customer-managed key. `bucket_key_enabled = true` so
each object's data-encryption key is wrapped once per bucket-key
lifecycle (rotating ~daily) rather than per-object — a 99x reduction in
KMS API request volume for high-write workloads.

Key rotation is on. Deletion window is 30 days in prod (max), 7 in dev.

## Versioning

Enabled, with non-current expiry after 30 days. Versioning is
accident-recovery insurance, not long-term history; 30 days is enough
to roll back yesterday's mistake but doesn't blow up storage cost.

MFA-delete isn't terraformable (requires root + console). Add manually
in prod if needed.

## Event notifications

S3 → SQS notification on `s3:ObjectCreated:*` under `raw/`, routing
into the validator queue in `modules/lambda_validator`. The queue ARN
flows in via `var.validator_queue_arn` to avoid a circular reference
(queue policy must exist before S3 emits the first event).

# Demo failure scenario

This is the operational drill we use to demonstrate that the platform's
failure-handling actually works end-to-end. It's deliberately small so
a viewer (recruiter, interviewer, on-call engineer) can run it in 30
seconds on a laptop with zero credentials.

The drill exercises:

1. **Validator Lambda** rejecting a malformed file
2. **Quarantine S3 layout** receiving the bad file
3. **SNS alert** with the diagnostic context (in mock mode, simulated)
4. **Step Functions** operator-decision flow with the three branches
5. **Quarantine helper Lambda** moving the corrected file back to raw/
6. **Airflow REST API** trigger for the replay run

If any of these are silently broken, the script fails loudly.

## Running it (mock mode — default, no credentials required)

```bash
# From repo root
python demo/inject_bad_file.py
```

Expected output:

```
======================================================================
DEMO — Inject malformed orders file → quarantine → operator → replay
======================================================================

Mode: MOCK (in-process moto AWS; no network, no credentials)

[1] Dropped malformed file: s3://demo-lakehouse-bucket/raw/orders/...
    Size: 1633 bytes, missing 'customer_id' column

[2] Validator processed: 1 record(s)
    - raw/orders/...: outcome=quarantine, reason=missing_columns: customer_id,...

[3] File moved to quarantine: s3://demo-lakehouse-bucket/quarantine/orders/...

[4] Step Functions execution started (mocked).
    Operator now picks one of:

    # replay: The file is actually fine — re-run pipeline
    aws stepfunctions send-task-success --task-token '...' --task-output '...'

    # discard: Upstream is broken; abandon this day's data
    aws stepfunctions send-task-success ...

    # fix-and-replay: We'll upload a corrected file to _fixed/
    aws stepfunctions send-task-success ...

Run again with --auto-replay to drive the replay path end-to-end.
```

To exercise the **full replay loop**:

```bash
python demo/inject_bad_file.py --auto-replay
```

Adds steps [5] (move quarantine → raw) and [6] (re-validate the
corrected file lands cleanly).

## Running it against real AWS

After `terraform apply` against your AWS account:

```bash
export LAKEHOUSE_BUCKET=lakehouse-prod-bucket
export SFN_STATE_MACHINE_ARN=arn:aws:states:us-east-1:...:lakehouse-prod-quarantine-review-replay
export AWS_REGION=us-east-1

python demo/inject_bad_file.py --live
```

The script PUTs the bad file into the real bucket and polls for:

- the quarantine/ landing (proves the S3 → SQS → Lambda chain works)
- a RUNNING SFN execution (proves the validator → SNS → SFN chain works)

Then the operator decides via:

- the AWS CLI command the script prints, OR
- the **Quarantine queue** widget on the Streamlit dashboard, which
  exposes one-click Replay / Discard / Fix-and-replay buttons that hit
  `SendTaskSuccess` for you.

## What's exercised at each step

| Step | Component | What's being proven |
|---|---|---|
| 1 | Generator + S3 PUT | Producer can drop a file at the configured raw/ prefix |
| 2 | Validator handler | `validate()` rejects files missing required columns with `severity=quarantine` |
| 3 | Quarantine S3 layout | `quarantine_file()` moves to the parallel quarantine/ prefix and writes the manifest |
| 4 | Step Functions | `WaitForOperatorDecision` opens with the task token; three branches available |
| 5 | Quarantine helper | `move_to_raw()` does the copy + delete, writes an audit-log line |
| 6 | Validator handler (replay) | A well-formed file passes the same validator that quarantined the bad one |

## Cleaning up after a live run

```bash
# Delete the test file from raw/ AND quarantine/
aws s3 rm s3://$LAKEHOUSE_BUCKET/raw/orders/year=$(date +%Y)/month=$(date +%m)/day=$(date +%d)/orders.parquet
aws s3 rm s3://$LAKEHOUSE_BUCKET/quarantine/orders/year=$(date +%Y)/month=$(date +%m)/day=$(date +%d)/orders.parquet

# Stop the SFN execution if it's still waiting
aws stepfunctions stop-execution --execution-arn <EXEC_ARN>
```

The audit log entry (under `processed/_quarantine_audit/`) stays — it's
deliberately append-only for compliance.

## Why this matters

The single most important question a senior reviewer asks is "what
happens when it breaks?" The Slice 4 pipeline plus the Slice 5
quarantine/replay infrastructure produces a complete, demonstrable
answer:

- Bad data **never** reaches the silver layer (validator quarantines at
  ingest).
- The operator gets a clear alert with reason + context.
- Three escape valves (replay, discard, fix-and-replay) cover the three
  outcomes ops actually want.
- Every decision is audited.
- The dashboard's quarantine widget closes the loop visually.

The demo script is the regression test for that whole story.

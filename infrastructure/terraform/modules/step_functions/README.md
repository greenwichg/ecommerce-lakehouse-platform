# Step Functions — quarantine review + replay

## The scenario

The validator Lambda quarantines a file because (say) a required column
went missing. An operator gets the SNS alert with diagnostic context
(reason, source, key). They investigate — maybe ping upstream, maybe
look at the actual file — and decide one of:

1. **replay** — the file is actually fine, re-run the pipeline against
   it. (Operator decides the quarantine was a false positive.)
2. **discard** — abandon the file. Upstream's broken, this day's data
   is partial, move on. Audit log records the decision + operator name.
3. **fix-and-replay** — the file is broken but ops will produce a
   corrected version. State machine notifies the operator of the
   expected upload path, then waits for that file to appear and
   replays once it does.

The operator's decision arrives back via
`aws stepfunctions send-task-success --task-token <token> --task-output
<json>` where the token came from the SNS notification.

## Why Step Functions, not Airflow

The defining constraint is the **wait for human input**, which can take
hours to days. Airflow sensors hold a worker slot the entire time —
crippling at scale and an awkward pattern. SFN's `waitForTaskToken`
holds the execution open at zero compute cost up to 1 year.

Cost comparison for a typical quarantine review (10 state transitions
over 3 days):

| | Step Functions Standard | Airflow worker slot 3d |
|---|---|---|
| Compute | $0.000025 × 10 = $0.00025 | 1 slot × 72h × ($X / slot-hour) |
| Wait support | Up to 1 year | Limited; reschedule-mode sensor wakes periodically |
| External completion API | Native `SendTaskSuccess` | Custom REST endpoint to a DAG run trigger |
| Failure surface | State persisted by SFN | Worker crash = lost task |

## State graph

```
NotifyOpsForReview
        │
        ▼
WaitForOperatorDecision  ←─── operator calls SendTaskSuccess(decision=...)
        │
        ▼
   ┌──RouteOnDecision──┐
   │         │         │
   ▼         ▼         ▼
replay   discard   fix-and-replay
   │         │         │
   │         │         ▼
   │         │   NotifyFixPath
   │         │         │
   │         │         ▼
   │         │   WaitForFixedFile (300s)
   │         │         │
   │         │         ▼
   │         │   PollForFixedFile
   │         │         │
   │         │         ▼
   │         │   FixedFilePresentChoice
   │         │      │      └─false─→ back to WaitForFixedFile
   │         │      │
   │         │      true
   │         │      ▼
   ▼         ▼      ▼
MoveBackToRaw      DiscardFile
   │
   ▼
TriggerAirflowDAG
   │
   ▼
ReplaySuccess
```

Plus failure / timeout terminals:

- `OperatorTimedOut`: 7-day waitForTaskToken expiry → SNS notify
- `OperatorDecisionInvalid`: malformed decision JSON → SNS notify
- `ReplayTriggerFailed`: Airflow API call failed after retries → SNS notify

All terminal states publish to the alerts topic so operators see the
outcome regardless of branch.

## Invoking it

```bash
# Start an execution after a quarantine event
aws stepfunctions start-execution \
    --state-machine-arn <arn-from-tf-output> \
    --input '{
        "quarantine_key": "quarantine/orders/year=2025/month=05/day=01/orders.parquet",
        "reason": "missing_columns: customer_id",
        "source": "orders"
    }'

# Once the operator decides, completion is one of:
aws stepfunctions send-task-success \
    --task-token "$TOKEN" \
    --task-output '{"decision": "replay", "operator": "alice@example.com", "logical_date": "2025-05-01"}'

aws stepfunctions send-task-success \
    --task-token "$TOKEN" \
    --task-output '{"decision": "discard", "operator": "alice@example.com", "reason": "upstream cleanup, batch abandoned"}'

aws stepfunctions send-task-success \
    --task-token "$TOKEN" \
    --task-output '{"decision": "fix-and-replay", "operator": "alice@example.com", "logical_date": "2025-05-01"}'
```

## Slice 6 enhancement: operator UI

The Streamlit dashboard (Slice 6) will get a "Quarantine queue" widget
that:
- Lists active SFN executions in the `WaitForOperatorDecision` state
- Shows the task token + diagnostic context for each
- One-click buttons for replay / discard / fix-and-replay that POST to
  the SFN SendTaskSuccess API

For Slice 5 the CLI form above is the documented manual path.

## Supporting Lambda

The `quarantine_helper` Lambda (small; pure stdlib + boto3) handles
the discrete S3 / Airflow operations the state machine needs:

- `discard`: delete quarantined object + write audit log
- `move_to_raw`: copy back to raw/ (triggers the S3 → validator chain)
- `poll_for_fix`: check whether `_fixed/<filename>` exists in S3
- `trigger_airflow`: POST to Airflow REST API to launch the DAG

Each action is a clean unit-testable function; tests live in
`tests/lambda_quarantine_helper/test_handler.py`.

## Configuration

| Variable | Where | Why |
|---|---|---|
| `alerts_topic_arn` | SNS module | Operator notifications |
| `validator_function_arn` | Lambda validator module | Activity-task target for WaitForOperatorDecision |
| `helper_function_arn` | Computed inside this module | Branch actions |
| `log_group_arn` | CloudWatch module | Execution logging |
| `bucket_id` | S3 module | Referenced in notification messages |

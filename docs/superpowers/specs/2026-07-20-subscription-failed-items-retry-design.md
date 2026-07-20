# Subscription Failed-Item Full-Pipeline Retry Design

## Goal

Add a manual "retry failed items" action to the subscription detail card. The action must retry every failed item in that subscription through the complete pipeline instead of only retrying the final transfer stage.

## Scope

- Backend endpoint to reset failed subscription items and start a full subscription run.
- Standalone and cluster execution paths.
- Frontend detail modal button, loading state, success/error feedback, and refresh.
- Regression tests for item reset, job regeneration, endpoint behavior, and UI request handling.

Out of scope:

- Automatic retry scheduling.
- Retry count policy changes for unrelated task types.
- Retrying successful, pending, transferring, or skipped subscription items.

## Current Flow and Failure Boundary

The existing manual check endpoint calls `subscription.RunForRole`. A normal run discovers source files, builds subscription items, and either performs local transfer or dispatches cluster work. Failed items can retain terminal state and old task/job associations, so a subsequent normal check may treat them as already handled or leave the failed cluster job as the active association.

The retry action must clear only retryable terminal state before entering the existing run path.

## API Design

Add a subscription-scoped endpoint:

```text
POST /admin/subscription/retry_failed
```

Request:

```json
{"id": 41}
```

The handler validates the ID, invokes a retry preparation operation, then calls the same role-aware execution path used by manual checks. The response uses the existing subscription run result shape so the frontend can refresh using existing parsing logic.

If no failed items exist, return a successful no-op result or a clear 4xx validation response; the preferred behavior is a successful no-op with zero reset count so the button remains safe under concurrent refreshes.

## Retry Preparation

Implement a transactional database operation that:

1. Selects subscription items belonging to the requested subscription whose effective terminal status is `failed`.
2. Resets them to `pending` or the existing re-plannable state used by discovery.
3. Clears `LastError`, terminal stage status, and stale transfer/job associations that would prevent a new dispatch.
4. Preserves source identity, file metadata, target naming, and selection timestamps.
5. Updates related cluster notification/job state to pending only when the failed item has a cluster association.
6. Returns the reset item count.

The operation must not mutate successful, skipped, currently transferring, or currently pending items. It must run in a transaction so partial resets cannot be exposed to a concurrent subscription run.

## Full Execution

After the reset transaction succeeds, call `RunForRole(subscriptionID, true, conf.Conf.Cluster.Role)`:

- Standalone mode reruns discovery and local transfer.
- Coordinator mode reruns discovery and dispatches new cluster work.
- A new run record is created through the existing run finalization path.
- New cluster jobs must not reuse terminal failed job IDs.

The subscription run lock remains the concurrency boundary. If a run is already active, return the existing conflict/error behavior rather than starting a second run.

## Frontend Design

Add a button in the subscription detail modal header or action row:

- Label: `重试失败项` / localized equivalent.
- Visible only when the detail contains at least one failed effective item.
- Disabled or loading while the request is active.
- On success, show the existing success notification and reload the detail data.
- On failure, show the backend error and keep the modal open.
- Do not close the modal automatically.

The detail table continues to show the individual failure reason. After refresh, reset items should display their current pending/transferring state and new job IDs when dispatched.

## Error Handling

- Invalid or missing subscription ID: 400.
- Subscription not found: 404.
- Database reset failure: 500 with no partial mutation.
- Active run conflict: preserve the existing manual-run error response.
- Execution failure after reset: return the normal run error while leaving the new terminal state and error visible for another manual retry.

## Testing Strategy

Backend tests:

- Reset only failed items.
- Preserve successful, skipped, pending, and transferring items.
- Clear stale job/task fields and failure metadata.
- Transaction rollback on reset failure.
- Retry endpoint invokes full role-aware execution.
- Cluster retry creates/reuses no terminal failed job association.
- No failed items produces a safe no-op.

Frontend tests or type/build verification:

- Button visibility tracks failed detail items.
- Button sends the subscription ID once.
- Loading prevents duplicate clicks.
- Successful retry refreshes detail data.
- Failed request keeps the modal open and displays an error.

## Acceptance Criteria

- A user can open a subscription card containing failed episodes and click one button to retry all failed episodes.
- The retry reruns source discovery, ETF handling, target subscription checking, dispatch, and transfer according to the deployment role.
- Successful items are not duplicated or reset.
- Failed items receive fresh execution state and can reach the normal transferred/succeeded terminal state.
- The action is safe to click repeatedly and does not create concurrent duplicate runs.

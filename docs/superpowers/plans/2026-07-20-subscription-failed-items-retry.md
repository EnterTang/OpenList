# Subscription Failed-Item Full-Pipeline Retry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a manual detail-card action that resets all failed subscription items and reruns the complete subscription pipeline, including ETF handling, source discovery, dispatch, and transfer.

**Architecture:** Add a transactional backend retry-preparation operation in the subscription database layer, expose it through a role-aware HTTP handler that calls the existing `RunForRole` execution path, and add a frontend API/action in the existing subscription detail modal. Failed items are reset only after validating the subscription and are then processed through the normal run lock and cluster/standalone routing.

**Tech Stack:** Go, Gin, GORM, SQLite-compatible transactions, SolidJS, TypeScript, Hope UI, existing `useFetch` and subscription API helpers.

## Global Constraints

- Retry is manual only; do not add automatic retry scheduling.
- Retry only items whose effective terminal status is `failed`.
- Do not mutate successful, skipped, pending, or transferring items.
- Reuse `RunForRole(subscriptionID, true, role)` for full execution.
- Preserve standalone and cluster execution semantics.
- Avoid reusing terminal failed cluster job IDs.
- Keep the detail modal open after success or failure and refresh it after success.

---

### Task 1: Add transactional failed-item reset primitives

**Files:**
- Modify: `/Volumes/extend Disk/Github/OpenList/.worktrees/cluster-stale-node-cleanup/internal/db/subscription.go`
- Modify: `/Volumes/extend Disk/Github/OpenList/.worktrees/cluster-stale-node-cleanup/internal/model/subscription.go` only if a response/count type is needed
- Test: `/Volumes/extend Disk/Github/OpenList/.worktrees/cluster-stale-node-cleanup/internal/db/subscription_test.go`

**Interfaces:**
- Produces `ResetFailedSubscriptionItems(ctx context.Context, subscriptionID uint) (int, error)` in `internal/db`.
- The operation runs inside a single GORM transaction and returns the number of reset items.

- [ ] **Step 1: Add failing database tests**

Create fixtures with one subscription containing failed, successful, skipped, pending, and transferring items. Assert that only failed items are changed, errors are cleared, stale cluster job IDs are cleared, and all other rows remain byte-for-byte unchanged in relevant fields.

Add a rollback test using an invalid update condition or an injected transaction failure path available in the existing DB test helpers. Assert no failed item is partially reset.

- [ ] **Step 2: Run the focused database tests and verify failure**

Run:

```bash
go test ./internal/db -run 'TestResetFailedSubscriptionItems' -count=1
```

Expected: FAIL because `ResetFailedSubscriptionItems` does not exist yet.

- [ ] **Step 3: Implement the transactional reset**

Use `db.WithContext(ctx).Transaction`. Select rows by `subscription_id` and `status = model.SubscriptionItemStatusFailed`. Update only retry state fields: set status to `model.SubscriptionItemStatusPending`, clear `last_error`, and clear `cluster_job_id`. Preserve source keys, file metadata, target fields, and timestamps unless the existing persistence contract requires `updated_at` to change.

Return `RowsAffected`. If the subscription does not exist, return the existing not-found error behavior used by the DB layer.

- [ ] **Step 4: Run focused tests and the related package suite**

Run:

```bash
go test ./internal/db -run 'TestResetFailedSubscriptionItems' -count=1
go test ./internal/db -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit the database primitive**

```bash
git add internal/db/subscription.go internal/db/subscription_test.go internal/model/subscription.go
git commit -m "feat(subscription): reset failed items for retry"
```

---

### Task 2: Expose a role-aware full-pipeline retry endpoint

**Files:**
- Modify: `/Volumes/extend Disk/Github/OpenList/.worktrees/cluster-stale-node-cleanup/server/handles/subscription.go`
- Modify: `/Volumes/extend Disk/Github/OpenList/.worktrees/cluster-stale-node-cleanup/server/router.go`
- Test: `/Volumes/extend Disk/Github/OpenList/.worktrees/cluster-stale-node-cleanup/server/handles/subscription_test.go`

**Interfaces:**
- Add `RetryFailedSubscription(c *gin.Context)`.
- Endpoint: `POST /admin/subscription/retry_failed`.
- Request JSON: `{"id": <subscription id>}`.
- Response: existing subscription run result plus a reset count if the current response model can carry it without breaking clients; otherwise return the existing run result unchanged.

- [ ] **Step 1: Add failing handler tests**

Cover missing/invalid ID, unknown subscription, no failed items, successful reset followed by `RunForRole`, and active-run/error propagation. Use the existing handler test setup and mock/in-process execution conventions rather than a real external worker.

- [ ] **Step 2: Run the focused handler tests and verify failure**

Run:

```bash
go test ./server/handles -run 'TestRetryFailedSubscription' -count=1
```

Expected: FAIL because the handler and route are not implemented.

- [ ] **Step 3: Implement the handler**

Parse a request containing `ID`, validate it, call `db.ResetFailedSubscriptionItems(c.Request.Context(), req.ID)`, then call `subscription.RunForRole(c.Request.Context(), req.ID, true, conf.Conf.Cluster.Role)`. Preserve normal `common.ErrorResp` status behavior. Treat zero reset count as a safe no-op response without creating a duplicate run; if the chosen implementation still runs the full scan for zero count, document and test that behavior explicitly.

Register the route beside the existing subscription create/update/check routes.

- [ ] **Step 4: Add cluster-specific assertions**

Verify the handler passes the configured cluster role and `transfer=true`, so coordinator mode enters `RunCluster` and standalone mode enters local transfer execution. Verify the reset happens before execution and that a failed old job ID is not sent as an active retry job.

- [ ] **Step 5: Run backend verification**

Run:

```bash
go test ./server/handles ./internal/db ./internal/subscription ./internal/cluster/... -count=1
git diff --check
```

Expected: PASS for the affected packages.

- [ ] **Step 6: Commit the backend endpoint**

```bash
git add server/handles/subscription.go server/handles/subscription_test.go server/router.go
git commit -m "feat(subscription): add failed item retry endpoint"
```

---

### Task 3: Add frontend API and detail-modal retry action

**Files:**
- Modify: `/Volumes/extend Disk/Github/OpenList-Frontend/src/utils/api.ts`
- Modify: `/Volumes/extend Disk/Github/OpenList-Frontend/src/pages/home/SubscriptionManagement.tsx`
- Modify: `/Volumes/extend Disk/Github/OpenList-Frontend/src/types/subscription.ts` only if the backend response adds a typed reset count
- Modify: frontend locale files containing subscription strings

**Interfaces:**
- Produces `subscriptionRetryFailed(id: number): PResp<SubscriptionRunResult>` in `src/utils/api.ts`.
- The detail modal receives `onRetryFailed: () => Promise<void>` and `retryFailedLoading: boolean | undefined`.
- Failure visibility is derived from `effective_status || status === "failed"` over the detail items.

- [ ] **Step 1: Add the API helper and wire loading state**

Implement:

```ts
export const subscriptionRetryFailed = (
  id: number,
): PResp<SubscriptionRunResult> =>
  r.post("/admin/subscription/retry_failed", { id })
```

Create a `useFetch(subscriptionRetryFailed)` instance in `SubscriptionManagement` and add a per-subscription or detail-ID loading signal so duplicate clicks are prevented.

- [ ] **Step 2: Add failure predicate and action handler**

Use the same effective status fields rendered by the detail table. The handler must call the new API exactly once, pass the open subscription ID, show the existing notification/error mechanism, and call the existing detail reload callback after success without closing the modal.

- [ ] **Step 3: Add the button to the detail modal**

Place a button in the modal header/action row. Render it only when at least one detail item is failed. Use the existing Hope UI `Button`, loading prop, icon conventions, and localized text. Keep the table’s individual errors unchanged.

- [ ] **Step 4: Add frontend tests or the repository’s supported verification**

If component tests are not configured, add a focused pure helper test for the failure predicate and run type/build verification. Confirm the button disappears after a successful refresh when no failed items remain.

Run:

```bash
pnpm exec prettier --write src/utils/api.ts src/pages/home/SubscriptionManagement.tsx src/types/subscription.ts
pnpm exec tsc -p tsconfig.json --noEmit
pnpm run build
```

Expected: PASS.

- [ ] **Step 5: Commit the frontend action**

```bash
git add src/utils/api.ts src/pages/home/SubscriptionManagement.tsx src/types/subscription.ts src/lang src/lang-overrides
git commit -m "feat(subscription): add retry failed items action"
```

---

### Task 4: End-to-end regression verification

**Files:**
- Test: affected backend and frontend test files from Tasks 1–3
- Modify: no production files unless verification exposes a concrete defect

- [ ] **Step 1: Verify the database state transition**

Run the focused reset tests and inspect that failed rows become pending with cleared errors/job IDs while successful rows remain unchanged.

- [ ] **Step 2: Verify the HTTP contract**

Run the handler tests and confirm invalid IDs, no-op retries, database failures, active-run conflicts, and execution failures have deterministic responses.

- [ ] **Step 3: Verify standalone and cluster paths**

Run the affected Go package tests and confirm both role branches use the full execution path with transfer enabled where supported.

- [ ] **Step 4: Verify the frontend interaction**

Open the subscription detail modal in the development frontend, confirm the button appears only for failed items, click it once, confirm loading prevents a second request, and confirm the detail refresh shows new pending/transferring state or the new failure.

- [ ] **Step 5: Run final checks**

Backend:

```bash
go test ./server/handles ./internal/db ./internal/subscription ./internal/cluster/... -count=1
git diff --check
```

Frontend:

```bash
pnpm exec tsc -p tsconfig.json --noEmit
pnpm run build
```

Record any unrelated pre-existing failures without claiming full-suite success.

- [ ] **Step 6: Review diffs and summarize**

Inspect both repository worktrees, confirm no generated artifacts or unrelated changes are included, and report the exact tests run and any remaining environment limitations.

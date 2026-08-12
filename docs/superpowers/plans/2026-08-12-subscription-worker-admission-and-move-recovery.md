# Subscription Worker Admission and Lossless Move Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent subscription media-transfer jobs from becoming permanently `running` before a Worker creates the native move task, and guarantee that capacity pressure, Worker stalls, provider failures, and reconnects result in visible queue state, bounded retry, or durable terminal compensation.

**Architecture:** Keep the existing Coordinator-owned `x_cluster_jobs` model and the Worker-native `fs.Move` task model, but make admission explicit. A Worker must reserve a media execution slot before acknowledging `job.offer`; when no slot is available it sends the existing `job.reject` protocol message with a retryable capacity code, allowing the Coordinator to return the job to `queued`. A Coordinator watchdog repairs pre-fix attempts that were accepted without any stage, while subscription reconciliation remains the business-state safety net. Capacity is also advertised in inventory so new dispatches are spread without flooding a Worker.

**Tech Stack:** Go, GORM, existing cluster protocol v1, existing Worker `limitGate`, SQLite/Postgres-compatible persistence through GORM, Redis result queue, Go test, existing admin cluster-job API.

## Global Constraints

- Do not add a new queue or dependency; reuse `x_cluster_jobs`, `x_cluster_job_attempts`, `x_cluster_job_stages`, the existing `job.reject` protocol payload, and the existing Redis result queue.
- `job.accept` must mean “the Worker has reserved execution capacity,” not merely “the WebSocket handler received the offer.”
- Never create a native `fs.Move` task before the Worker has a durable cluster attempt and a valid execution slot.
- Rejection caused by capacity, a stale Worker start, lease expiry, timeout, 429/5xx/network failure, or transient 115/123/光鸭 errors must remain retryable and must not be converted to a permanent subscription failure.
- Credential invalidation, invalid signatures, 405 HTML responses, invalid/expired shares, malformed multi-episode identity, and unsafe target mismatches remain non-retryable or blocked according to the existing provider error policy.
- Preserve idempotency and fencing: a stale attempt must never report success, delete staged media, or overwrite a newer attempt.
- Do not perform direct SQL state edits on production remotes. Historical recovery must run through Coordinator code and existing/admin APIs.
- Keep provider credentials, cookies, tokens, response bodies, and account fingerprints out of logs and test output.
- Existing direct-download-first/fallback behavior, 115 classification, 123/光鸭 share-save behavior, ETF upload, and cleanup semantics must remain backward compatible.
- Each implementation task must add or update focused tests before changing production behavior, then pass the focused package tests before moving to the next task.

## Evidence and scope

The host snapshot for subscription `160` showed six affected episodes (`13`, `15`, `16`, `17`, `18`, `19`) with `media.transfer` jobs assigned to `etflix-oplist-master`. Each job was accepted but had no stage, no result, no notification, and no Worker cleanup record. The same Worker had `89` running media jobs, only `10` with stages, and `79` without stages; host `move_task_threads_num` was `10`. The six jobs therefore existed in the Coordinator queue but had not reached `fs.Move`, so their absence from the Worker-native pending move-task queue was expected under the current implementation.

The local source confirms the causal path:

```text
acceptJob -> send job.accept -> start lease renewal
         -> executeMediaTransfer -> acquireDownloadCapacity
         -> stage permits/status -> fs.Move
```

The current code starts lease renewal before `limitGate.Acquire` returns, so a goroutine waiting for capacity can keep its lease alive indefinitely. The plan below fixes this behavior and repairs already-stuck jobs.

## File map

| Area | Files | Responsibility |
|---|---|---|
| Worker admission | `internal/cluster/worker/control.go`, `internal/cluster/worker/service.go` | Non-blocking capacity reservation, release-on-all-paths, acceptance semantics, bounded logs. |
| Protocol/coordinator | `internal/cluster/protocol/payloads.go`, `internal/cluster/coordinator/service.go`, `internal/model/cluster_job.go` | Job rejection handling, attempt fencing, retryable requeue, attempt start timestamps, stale-attempt recovery. `JobReject` and `rejected` already exist and should be wired rather than replaced. |
| Runtime scheduling | `internal/cluster/runtime.go`, `internal/cluster/worker/inventory.go`, `internal/cluster/worker/control.go`, `internal/cluster/runtime_inventory_support.go`, `internal/cluster/subscription_dispatcher.go` | Sweep cadence, capacity advertisement, dispatch load/capacity selection. |
| Subscription consistency | `internal/subscription/reconcile.go`, `internal/subscription/cluster_dispatch.go`, `internal/subscription/execution_status_test.go` | Keep item/run status aligned with queued, active, retryable, blocked, and terminal cluster states. |
| API/observability | `server/handles/cluster.go`, `internal/cluster/coordinator/service.go` | Ensure the existing `/api/admin/cluster/jobs` response exposes rejection/stall fields and stages; add only missing fields/filters. |
| Tests | `internal/cluster/worker/*_test.go`, `internal/cluster/coordinator/*_test.go`, `internal/cluster/*_test.go`, `internal/subscription/*_test.go`, `internal/repair/*_test.go` | Unit, protocol, integration, reconciliation, and historical-recovery coverage. |

---

### Task 1: Lock the failure model with regression tests

**Files:**

- Modify: `internal/cluster/worker/control_test.go`
- Modify: `internal/cluster/worker/service_test.go`
- Modify: `internal/cluster/coordinator/service_test.go`
- Modify: `internal/cluster/coordinator/lease_recovery_test.go`
- Modify: `internal/subscription/execution_status_test.go`
- Modify: `internal/subscription/cluster_dispatch_test.go`

**Interfaces:**

- Consumes: current `limitGate`, `Service.acceptJob`, `Coordinator.handleJobResult`, `SweepExpiredLeases`, and subscription reconciliation behavior.
- Produces: failing tests that define the new admission, rejection, stale-start, and subscription-state contracts for later tasks.

- [ ] **Step 1: Add a full-capacity gate test.**

  Add a test that creates `newLimitGate(1)`, acquires its only slot, calls the future non-blocking admission method, and asserts that it returns `ok == false` immediately. Assert that releasing the first slot makes a second admission succeed.

- [ ] **Step 2: Add a Worker offer test.**

  Construct a valid `media.transfer` offer with a fake sender and a fake result queue. Saturate the Worker gate, call `Service.HandleMessage` with `job.offer`, and assert:

  ```text
  sent message types: job.reject, not job.accept
  rejection code: worker_capacity_unavailable
  retryable: true
  active task count: 0
  no lease-renew message emitted
  ```

- [ ] **Step 3: Add Coordinator rejection tests.**

  Add one test for retryable `JobReject` asserting the attempt becomes `rejected`, the job becomes `queued`, assignment/current attempt are cleared, `available_at` is in the future, and the subscription item is not changed to terminal `failed`. Add one test for non-retryable rejection asserting terminal failure is persisted and reconciliation classifies the item correctly.

- [ ] **Step 4: Add stale-start recovery tests.**

  Seed an accepted/running media attempt with a lease still in the future and no stage, advance `now` beyond the admission grace period, call the future `SweepStalledAttempts`, and assert the job is queued for a new generation. Seed a second attempt with `uploading_mobile/running` and assert it is untouched.

- [ ] **Step 5: Add subscription-state tests.**

  Assert that a subscription with queued/running jobs remains `running`, never `success`; a terminal provider failure becomes `failed` or `blocked`; a successful job repairs a stale `transferring` item to `transferred`; and a `transferring` item with no durable job returns to `pending` for compensation.

- [ ] **Step 6: Run the new tests before implementation.**

  Run:

  ```bash
  go test ./internal/cluster/worker ./internal/cluster/coordinator ./internal/cluster ./internal/subscription
  ```

  Expected: the newly added admission/rejection/stall tests fail because the production paths do not yet implement the contract. Existing tests must continue to pass except for tests whose assertions intentionally encode the old acceptance semantics; update those assertions only when Task 2 changes the behavior.

- [ ] **Step 7: Commit the test contract.**

  Commit with a Conventional Commit title such as `test(cluster): define worker admission recovery contract`, including the repository’s required decision trailers when the implementation commit is created.

---

### Task 2: Make Worker admission capacity-aware and non-blocking

**Files:**

- Modify: `internal/cluster/worker/control.go:48-94,464-483`
- Modify: `internal/cluster/worker/service.go:89-126,514-589,951-963`
- Modify: `internal/cluster/worker/direct_download.go:178-207`
- Test: `internal/cluster/worker/control_test.go`
- Test: `internal/cluster/worker/service_test.go`
- Test: `internal/cluster/worker/direct_download_test.go`

**Interfaces:**

- Consumes: `limitGate`, `JobReject`, `AttemptRef`, `resultQueue.ClaimAttempt/ReleaseAttempt`, and all current media execution modes.
- Produces: `limitGate.TryAcquire() (release func(), ok bool)` and a Worker rule that `job.accept` is sent only after a media execution slot is reserved.

- [ ] **Step 1: Implement non-blocking capacity reservation.**

  Add the following behavior to `limitGate` without changing blocking callers. Refactor the current inline release closure into a private `releaseOne` helper so both blocking and non-blocking acquisition use the same wake-up path:

  ```go
  func (g *limitGate) TryAcquire() (func(), bool) {
      g.mu.Lock()
      if g.limit > 0 && g.active >= g.limit {
          g.mu.Unlock()
          return nil, false
      }
      g.active++
      g.mu.Unlock()
      return g.releaseOne, true
  }

  func (g *limitGate) releaseOne() {
      g.mu.Lock()
      if g.active > 0 {
          g.active--
      }
      close(g.wake)
      g.wake = make(chan struct{})
      g.mu.Unlock()
  }
  ```

  Make `Acquire` return `g.releaseOne` as well. Keep `limit == 0` as unlimited, consistent with `Acquire`.

- [ ] **Step 2: Reserve before accepting a media offer.**

  In `acceptJob`, preserve duplicate-attempt handling, then for `media.transfer` call `TryAcquire` before `sendJobAccept`. If no slot is available:

  1. release the durable attempt claim;
  2. send `protocol.MessageJobReject` with `Code: "worker_capacity_unavailable"`, `Reason: "worker media execution capacity is full"`, and `Retryable: true`;
  3. do not insert an active task;
  4. do not start `maintainLease`;
  5. return `nil` so the transport acknowledges receipt of the offer and keeps the WebSocket alive.

  Rejection must be idempotent for a replayed offer and must never release another attempt’s claim.

- [ ] **Step 3: Carry the reserved slot through execution.**

  Add a `capacityRelease func()` field to `activeTask`. Release it exactly once from the goroutine defer, including execution error, context cancellation, result-send failure, and stale lease cancellation. Remove the second blocking `acquireDownloadCapacity` call from `executeMediaTransfer`; the caller now owns the slot.

- [ ] **Step 4: Apply the same admission rule to direct-download-first.**

  A `direct_download` task and its transfer fallback must reserve one media slot before acceptance. The fallback must reuse the same slot and must not acquire a second slot. Ensure a failed direct attempt does not release the slot before the transfer fallback finishes.

- [ ] **Step 5: Add bounded execution logs.**

  Log only `job_id`, `attempt_id`, generation, active count, limit, admission outcome, and stable error code. Emit one message when a job is rejected for capacity and one when it starts a stage. Do not log `TaskContextJSON`, share URLs, passcodes, or provider credentials.

- [ ] **Step 6: Run Worker tests.**

  Run:

  ```bash
  go test ./internal/cluster/worker -run 'Test(LimitGate|AcceptJob|Media|Direct)' -count=1
  go test ./internal/cluster/worker -race -count=1
  ```

  Expected: full-capacity offers reject immediately; admitted offers reach stage execution; capacity is released after success and failure; direct-download fallback never deadlocks or consumes two slots.

- [ ] **Step 7: Commit the Worker admission change.**

  Use a message such as `fix(cluster-worker): reserve capacity before accepting media jobs`.

---

### Task 3: Wire `job.reject` into Coordinator retry and fencing

**Files:**

- Modify: `internal/cluster/coordinator/service.go:493-565,816-856`
- Modify: `internal/cluster/runtime.go:720-770,1380-1460` only if redispatch filtering needs the new code
- Modify: `internal/model/cluster_job.go` only if JSON/API annotations for rejection metadata are missing; do not add a duplicate status
- Test: `internal/cluster/coordinator/service_test.go`
- Test: `internal/cluster/coordinator/retry_policy_test.go`
- Test: `internal/cluster/protocol/protocol_test.go`

**Interfaces:**

- Consumes: existing `protocol.MessageJobReject`, `protocol.JobReject`, `ClusterAttemptStatusRejected`, `ClusterJobStatusQueued`, and `mediaJobLeaseDuration`.
- Produces: `Coordinator.handleJobReject(ctx, peer, message, rejected) error`, wired from `Coordinator.HandleMessage`.

- [ ] **Step 1: Validate the rejection envelope and attempt reference.**

  Decode `JobReject`, require a non-empty job/attempt/generation/code, verify the current job assignment and lease token using `loadAndValidateAttempt`, and reject stale messages without mutating the current generation.

- [ ] **Step 2: Persist a retryable capacity rejection atomically.**

  For `Retryable == true` and `Code == "worker_capacity_unavailable"`:

  ```text
  attempt.status          = rejected
  attempt.finished_at     = now
  attempt.error_code      = worker_capacity_unavailable
  job.status              = queued
  job.assigned_node_id    = ""
  job.current_attempt_id  = ""
  job.available_at        = now + bounded capacity backoff
  job.last_error_code     = worker_capacity_unavailable
  job.last_error          = sanitized rejection reason
  ```

  Keep `current_generation` unchanged; the next redispatch increments it. Keep the subscription item linked to the durable job and non-terminal.

- [ ] **Step 3: Handle non-retryable rejection.**

  Persist the rejected attempt and mark the job terminal with the supplied code. Let the existing subscription reconciliation classify the item as `failed` or `blocked`; never silently drop the job.

- [ ] **Step 4: Add retry bounds.**

  Apply the existing maximum automatic media-transfer generation limit. If capacity rejection repeats beyond the limit, mark the job `dead_letter`, set the item’s stable error code to `retry_limit_exceeded`, and expose a manual retry path through the existing `/api/admin/cluster/jobs/:id/retry` endpoint.

- [ ] **Step 5: Test ACK/NACK interaction.**

  Confirm that the Worker’s `job.reject` is a normal protocol message, not a transport NACK. A transport NACK remains reserved for malformed/handler failures; capacity rejection must be durably handled by `handleJobReject`.

- [ ] **Step 6: Run Coordinator tests.**

  ```bash
  go test ./internal/cluster/coordinator ./internal/cluster -run 'Test.*(Reject|Retry|Lease|Stale)' -count=1
  go test ./internal/cluster/protocol -count=1
  ```

  Expected: retryable rejection returns jobs to `queued`; stale rejection cannot alter a newer generation; exhausted attempts become dead-lettered; protocol messages are accepted and fenced correctly.

- [ ] **Step 7: Commit the protocol lifecycle change.**

  Use a message such as `fix(cluster): requeue retryable worker job rejections`.

---

### Task 4: Recover pre-fix accepted jobs that have no execution stage

**Files:**

- Modify: `internal/cluster/coordinator/service.go` near `SweepExpiredLeases`
- Modify: `internal/cluster/runtime.go:553-569`
- Modify: `internal/subscription/reconcile.go:287-399`
- Test: `internal/cluster/coordinator/lease_recovery_test.go`
- Test: `internal/cluster/coordinator/service_test.go`
- Test: `internal/subscription/execution_status_test.go`

**Interfaces:**

- Consumes: `ClusterJobAttempt.AcceptedAt`, `StartedAt`, `LeaseUntil`, `ClusterJobStage`, existing retry limits, and `subscriptionJobActive`.
- Produces: `Coordinator.SweepStalledAttempts(ctx, now, grace) (int64, error)` and a periodic runtime invocation.

- [ ] **Step 1: Define the stalled-start predicate.**

  A media attempt is stalled only when all conditions hold:

  ```text
  job.type == media.transfer
  attempt is the current attempt
  attempt.status in {accepted, running}
  attempt has no stage row for the current attempt
  now - accepted_at (or updated_at when accepted_at is absent) >= 10 minutes
  ```

  Do not requeue a job with `saving_share`, `uploading_mobile`, or `worker_media_cleanup` stage activity; those are real executions and must be governed by stage/lease rules.

- [ ] **Step 2: Requeue stalled attempts transactionally.**

  Mark the attempt `lost` with `error_code = "worker_start_timeout"`, set `finished_at`, clear the job assignment/current attempt, set `status = queued`, set `available_at = now`, and retain the job’s immutable task context. If the generation reaches the automatic limit, dead-letter the job and update the linked item through the existing reconciliation path.

- [ ] **Step 3: Set attempt start timestamps correctly.**

  In `handleStagePermitRequest` and/or the first accepted stage status transition, set `attempt.started_at` once. Do not use `job.accept` as execution start. This makes future diagnostics distinguish admitted, queued, and actually started work.

- [ ] **Step 4: Run the sweep before redispatch.**

  In `processManifestProcessorTick`, call `SweepStalledAttempts` before `SweepExpiredLeases` and `redispatchQueuedJobs`. Keep the operation idempotent so repeated 5-second ticks cannot create two replacements for one attempt.

- [ ] **Step 5: Test the historical cohort shape.**

  Seed `89` running attempts with `10` stage-bearing attempts and `79` no-stage attempts. Assert exactly `79` are requeued, the `10` active transfers are unchanged, and a second sweep returns zero additional changes.

- [ ] **Step 6: Run recovery tests.**

  ```bash
  go test ./internal/cluster/coordinator ./internal/cluster -run 'Test(Sweep|Requeue|LeaseRecovery|Stalled)' -count=1
  go test ./internal/subscription -run 'Test.*(Reconcile|ExecutionStatus)' -count=1
  ```

  Expected: stale accepted jobs become retryable queued jobs; real stage work is preserved; successful/failed late callbacks from the old attempt are fenced and cannot overwrite the replacement.

- [ ] **Step 7: Commit historical recovery.**

  Use a message such as `fix(cluster): recover accepted jobs without execution stages`.

---

### Task 5: Advertise and use Worker capacity during dispatch

**Files:**

- Modify: `internal/cluster/worker/inventory.go:63-83`
- Modify: `internal/cluster/worker/control.go:136-156`
- Modify: `internal/cluster/runtime_inventory_support.go:19-28,140-149`
- Modify: `internal/cluster/subscription_dispatcher.go:32-44,474-607`
- Test: `internal/cluster/runtime_inventory_test.go`
- Test: `internal/cluster/subscription_target_selection_test.go`
- Test: `internal/cluster/subscription_dispatcher_test.go`

**Interfaces:**

- Consumes: `NodeCapabilities.DownloadConcurrency`, `UploadConcurrency`, provider account load, and Coordinator-side active-job counts.
- Produces: capacity-aware target selection that treats a full Worker as temporarily unavailable instead of assigning unbounded offers to it.

- [ ] **Step 1: Populate inventory concurrency fields.**

  Set `InventoryReport.Capabilities.DownloadConcurrency` and `UploadConcurrency` from the Worker’s effective configured limits. If the configured limit is zero, report the resolved default rather than zero so the Coordinator can make a safe decision.

- [ ] **Step 2: Extend target matching with capacity metadata.**

  Add download/upload capacity and a `CapacityKnown` flag to the internal `nodeProviderAccountMatch`. Keep the wire protocol unchanged. Calculate media capacity as the minimum relevant staging/download and delivery/upload limit.

- [ ] **Step 3: Account for current and same-batch assignments.**

  In `chooseDispatchTarget`, include current `leased`, `running`, and `cancel_requested` jobs plus `target.pendingAssignments`. Do not count terminal jobs. A full target must not be selected for a new item; the caller must leave that item pending for a later scheduler pass.

- [ ] **Step 4: Preserve preferred-worker fallback.**

  A preferred Worker that is full is not an error if another compatible Worker has capacity. If all compatible Workers are full, return the existing retryable Worker-unavailable error with a stable `worker_capacity_unavailable` code/message and no partial subscription-item claim.

- [ ] **Step 5: Test dispatch saturation.**

  With one Worker capacity of `2` and four compatible tasks, assert the first two are planned for that Worker, remaining tasks stay pending, no extra `media.transfer` rows are created, and a second dispatch after capacity is released can select the remaining tasks. With two Workers, assert spreading and preferred-worker fallback.

- [ ] **Step 6: Run dispatch tests.**

  ```bash
  go test ./internal/cluster -run 'Test.*(Target|Dispatch|Capacity|Inventory)' -count=1
  ```

  Expected: no dispatch batch creates more assigned jobs than the advertised capacity; no item is changed to `transferring` without a durable job ID.

- [ ] **Step 7: Commit capacity-aware dispatch.**

  Use a message such as `perf(cluster): schedule media jobs within worker capacity`.

---

### Task 6: Align subscription item/run status and retry compensation

**Files:**

- Modify: `internal/subscription/reconcile.go:204-258,287-417,469-485`
- Modify: `internal/subscription/cluster_dispatch.go:26-44,203-257,837-930`
- Modify: `internal/subscription/service.go:200-300` only if run aggregation still treats queued work as success
- Test: `internal/subscription/execution_status_test.go`
- Test: `internal/subscription/cluster_dispatch_test.go`
- Test: `internal/subscription/service_test.go`

**Interfaces:**

- Consumes: queued/retry-wait/rejected/stalled job outcomes and existing state-version optimistic writes.
- Produces: an idempotent reconciliation projection in which `transferring` means a durable active/queued execution exists, `pending` means compensation is required, and `success` is impossible while any required work remains non-terminal. Extend `ClusterWorkerUnavailableError` with a stable `Code` field while preserving `errors.Is(err, ErrClusterWorkerUnavailable)` for existing callers.

- [ ] **Step 1: Distinguish queued execution from missing execution.**

  If an item is `transferring` and its linked job is `queued`, `leased`, `running`, or `retry_wait`, keep the item linked and non-terminal. Expose the cluster job’s `last_error_code` and `available_at` through the existing job/API response so operators can see capacity waiting instead of an empty Worker move queue.

- [ ] **Step 2: Repair missing durable jobs.**

  If an item is `transferring` or `notifying` with no durable cluster job, return it to `pending`, clear the stale job ID, preserve a bounded diagnostic error, and allow the existing retry dispatcher to rebuild from persisted source metadata.

- [ ] **Step 3: Add capacity/start-timeout retry classification.**

  Classify `worker_capacity_unavailable`, `worker_start_timeout`, `lease_expired`, and `worker_unavailable` as retryable or blocked according to whether a compatible Worker is currently available. Do not classify them as provider-terminal failures.

- [ ] **Step 4: Preserve state-version fencing on retry.**

  Retry/reconciliation updates must continue using `WHERE id = ? AND state_version = ?`. If the item changed during reconciliation, reload and re-evaluate instead of overwriting a newer source observation. Add a test that simulates a concurrent source update and asserts the operation returns a retryable conflict without deleting the new source/job relationship.

- [ ] **Step 5: Correct run aggregation.**

  Keep scan/discovery/dispatch counts separate from final transfer counts. A run with queued, retry-wait, blocked, unknown, transferring, or notifying items must remain `running` or an explicit incomplete state; only all required items being transferred/skipped may become success.

- [ ] **Step 6: Run subscription tests.**

  ```bash
  go test ./internal/subscription -run 'Test.*(Execution|Reconcile|Retry|Cluster)' -count=1
  go test ./internal/subscription -race -count=1
  ```

  Expected: no `Subscription item changed during reconciliation` is emitted for an ordinary retry race that can be safely reloaded; successful items remain successful while only failed/missing items are rebuilt; queued jobs keep the run incomplete.

- [ ] **Step 7: Commit subscription compensation.**

  Use a message such as `fix(subscription): reconcile queued transfers without losing items`.

---

### Task 7: Close provider retry and direct-download regressions

**Files:**

- Review/modify only where tests expose a regression: `internal/subscription/share_115.go`, `internal/subscription/share_123.go`, `internal/subscription/share_guangyapan.go`, `internal/subscription/share_save.go`
- Test: `internal/subscription/share_115_test.go`
- Test: `internal/subscription/share_123_test.go`
- Test: `internal/subscription/share_guangyapan_test.go`
- Test: `internal/subscription/share_save_batch_test.go`
- Test: `internal/cluster/worker/direct_download_test.go`

**Interfaces:**

- Consumes: existing provider-specific stable error codes and direct-download-first/fallback implementation.
- Produces: regression evidence that the queue fix does not reintroduce provider blind retries or delete-before-result behavior.

- [ ] **Step 1: Verify 115 classifications.**

  Assert the following behavior remains stable:

  | Response | Expected |
  |---|---|
  | 429/rate limit | bounded retry with backoff and `share_save_rate_limited` |
  | timeout/5xx | retryable transient category |
  | 401/refresh-token/signature invalid | blocked credential category, no blind retry |
  | 405 HTML | `share_save_method_not_allowed`/gateway category, preserve status and content type, no repeated identical request |
  | invalid/expired share | terminal source-invalid category |

- [ ] **Step 2: Verify 123 and 光鸭 behavior.**

  Keep share-save result-unknown semantics distinct from retryable transport failures, preserve idempotency keys, and verify a failed upload cannot remove the staging source before the result/cleanup receipt is durable.

- [ ] **Step 3: Verify direct link fallback.**

  Assert direct share URL success creates no unnecessary share-save operation; a retryable direct-link failure falls back to the same durable transfer job; a credential/permission failure is blocked without blind fallback; and the reserved Worker slot is reused across fallback.

- [ ] **Step 4: Run provider regression tests.**

  ```bash
  go test ./internal/subscription -run 'Test.*(115|123|Guang|ShareSave)' -count=1
  go test ./internal/cluster/worker -run 'Test.*Direct' -count=1
  ```

  Expected: provider-specific retries remain bounded and classified, while task admission/retry state remains visible in cluster jobs.

- [ ] **Step 5: Commit only if provider code changed.**

  Use a message such as `test(storage): preserve provider retry semantics during admission recovery` when the task changes tests only, or a scoped `fix(...)` title when production behavior changes.

---

### Task 8: Add admin observability and repair verification

**Files:**

- Review/modify: `server/handles/cluster.go:89-115`
- Review/modify: `internal/cluster/coordinator/service.go` job-list projection
- Test: server handler tests if present; otherwise add focused Coordinator list tests under `internal/cluster/coordinator/service_test.go`

**Interfaces:**

- Consumes: `ClusterJob.Status`, `CurrentAttemptID`, `LastErrorCode`, `AvailableAt`, attempts, and stages.
- Produces: an operator-visible distinction between queued capacity, accepted, started stage, retry-wait, failed, and dead-letter jobs through the existing admin cluster-job API.

- [ ] **Step 1: Verify `/api/admin/cluster/jobs` fields.**

  Ensure each returned job includes `status`, `assigned_node_id`, `current_attempt_id`, `current_generation`, `available_at`, `last_error_code`, `last_error`, and current-attempt stages. If the current list method omits attempts/stages, extend only its response projection; do not add a second task endpoint.

- [ ] **Step 2: Add status filtering tests.**

  Assert operators can query `queued`, `running`, `failed`, and `dead_letter` independently and can identify a capacity-rejected job without inspecting Worker-local native tasks.

- [ ] **Step 3: Add stable operational log assertions.**

  For a capacity reject, stale-start recovery, result success, and terminal failure, assert logs contain only the job/attempt IDs and stable code. Assert no share URL/passcode/token appears.

- [ ] **Step 4: Run API/Coordinator tests.**

  ```bash
  go test ./internal/cluster/coordinator ./server/handles -count=1
  ```

  Expected: the admin job view is sufficient to explain why a Worker-native move task is absent.

---

### Task 9: Full local verification and release gate

**Files:**

- No production files; review generated test artifacts and the final diff.

- [ ] **Step 1: Run focused suites in dependency order.**

  ```bash
  go test ./internal/cluster/worker ./internal/cluster/coordinator ./internal/cluster/protocol -count=1
  go test ./internal/cluster ./internal/subscription ./internal/repair -count=1
  ```

- [ ] **Step 2: Run race-sensitive suites.**

  ```bash
  go test -race ./internal/cluster/worker ./internal/cluster/coordinator ./internal/subscription -count=1
  ```

- [ ] **Step 3: Run repository checks available in the worktree.**

  ```bash
  go test ./...
  go vet ./...
  gofmt -l internal/cluster internal/subscription internal/model server/handles
  ```

  Expected: no test failures, no vet diagnostics, and no files reported by `gofmt -l`.

- [ ] **Step 4: Review invariants manually.**

  Confirm:

  ```text
  no accepted job can wait on a full gate while renewing a lease
  no native move task exists without an admitted cluster execution
  every rejected/stalled/expired attempt has a durable next state
  late old-attempt results are fenced
  successful uploads remain idempotent after retry/restart
  subscription success requires terminal completion of all required items
  ```

- [ ] **Step 5: Commit the integrated change set.**

  Use a final Conventional Commit such as `fix(subscription): make worker transfers lossless`, with Lore trailers documenting constraints, rejected alternatives, tested suites, and known remote gaps.

---

### Task 10: Remote rollout and acceptance test

**Files:**

- No source files; use the deployed image, existing admin API, host database snapshot, and log files.

- [ ] **Step 1: Verify access and capture a baseline.**

  Before deployment, record for subscription `160`:

  ```sql
  select episode, id, status, cluster_job_id
  from x_subscription_items
  where subscription_id = 160 and episode in (13,15,16,17,18,19);

  select status, assigned_node_id, count(*)
  from x_cluster_jobs
  where assigned_node_id = 'etflix-oplist-master'
  group by status, assigned_node_id;
  ```

  Back up the host database and preserve the current host/Worker logs. Do not edit the database manually.

- [ ] **Step 2: Roll out in compatibility order.**

  Deploy the Coordinator/host image first so it understands `job.reject` and stale-start recovery, then deploy the Worker image to every media-capable node. Confirm each Worker reconnects, reports inventory, and advertises non-zero effective concurrency.

- [ ] **Step 3: Verify historical repair without manual SQL.**

  Wait for the Coordinator processor tick or invoke the existing admin retry/reconciliation endpoint. Confirm the six affected jobs leave the pre-fix accepted/no-stage state, old attempts become `lost` with `worker_start_timeout` or `rejected` with `worker_capacity_unavailable`, and new generations are queued/assigned.

- [ ] **Step 4: Test “天才，女友” episodes 13 and 15–19.**

  Monitor every 5–10 seconds through the admin job API and host DB:

  ```text
  queued -> leased/offered -> accepted -> saving_share -> uploading_mobile -> succeeded
  ```

  Verify that once `uploading_mobile` starts, a Worker-native move task appears; after completion the 139 target contains the expected file with correct name and size/hash, the staging object is cleaned only after the result is durable, and the subscription item becomes `transferred`.

- [ ] **Step 5: Test saturation and release.**

  Dispatch more media items than the Worker’s configured capacity. Verify no more than the configured number are accepted, excess work remains visible as Coordinator `queued` or receives retryable `worker_capacity_unavailable`, and the next job starts after a slot is released. Verify no job remains indefinitely `running` with zero stages.

- [ ] **Step 6: Test retry compensation.**

  Use a controlled retryable failure or an existing safe test fixture to verify:

  ```text
  first attempt failed/retry-wait
  only failed item is requeued
  successful sibling items are not duplicated
  new generation is fenced from the old attempt
  final item/run state reflects actual result
  ```

- [ ] **Step 7: Run provider smoke tests.**

  On the deployed environment, test one known-good 115 share-save, one 115 direct-link-first download, one 123 share-save, one 光鸭 share-save, and one 139 ETF upload. For 115, verify at least one 429/405/credential-classification path from logs or controlled fixtures without exposing credentials.

- [ ] **Step 8: Test restart/reconnect behavior.**

  During a controlled test job, reconnect the Worker or restart only the test Worker process. Verify an active stage either resumes under the same fenced attempt or is requeued after lease expiry; verify no duplicate native move/upload is created and no staged source is deleted by an old attempt.

- [ ] **Step 9: Acceptance criteria.**

  The release is accepted only when all of the following hold:

  - The six “天才，女友” episodes complete or receive a clearly classified, retryable/blocked terminal state.
  - No required item remains `transferring` without a durable cluster job.
  - No cluster job remains `running` beyond the start grace period with zero stages.
  - Native Worker move tasks are created only after cluster admission and stage start.
  - Capacity saturation never causes unbounded accepted goroutines or lease renewal without execution.
  - Retry does not duplicate already successful files or delete source media prematurely.
  - Subscription run status is not `success` while any required work is queued, active, blocked, unknown, or failed.
  - 115/123/光鸭/139 provider regression tests and smoke tests pass.

- [ ] **Step 10: Record remote gaps honestly.**

  If Worker SSH remains unavailable, record that Worker-local filesystem/task logs could not be independently inspected. Use Coordinator database state, cluster inbox/outbox records, admin API responses, host logs, and target-storage verification as the available evidence; do not claim Worker-local log validation.

## Self-review and coverage checklist

- Worker capacity starvation: covered by Tasks 1–5.
- Missing native move task: covered by admission semantics, stage gating, and remote acceptance in Tasks 2 and 10.
- Lease renewal masking blocked work: covered by Tasks 2 and 4.
- Existing pre-fix 89-job cohort: covered by Task 4 and remote Task 10.
- Subscription item/run state mismatch: covered by Task 6.
- Retry and compensation: covered by Tasks 3, 4, 6, and 10.
- 115 405/HTML, credential/signature, rate-limit classification: covered by Task 7.
- 123/光鸭 share-save and 139 ETF behavior: covered by Task 7 and remote smoke tests.
- Direct-link-first/fallback: covered by Tasks 2 and 7.
- Batch/idempotency and no duplicate success: covered by existing reliable-execution plan plus Tasks 6 and 10.
- Multi-episode parsing: remains covered by the existing `2026-08-11-subscription-reliable-execution.md` parser task and must be included in the release gate.

No production implementation should begin until Task 1’s regression tests are present and the current acceptance semantics are explicitly captured.

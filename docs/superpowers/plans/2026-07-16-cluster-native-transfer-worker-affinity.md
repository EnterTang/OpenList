# Cluster Native Transfer and Worker Affinity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make clustered subscription execution use preferred-worker soft affinity, OpenList-native asynchronous move tasks, and the same genre/TMDB target paths and concurrency model as standalone execution.

**Architecture:** The coordinator persists an optional preferred worker hint on subscriptions and applies it only after normal eligibility filtering, falling back automatically when unavailable. Workers map the coordinator-planned logical target path onto their local delivery root, enqueue a durable native move task, and reconcile its terminal state into the cluster manifest/result flow without relying on transient context values. Cluster-specific target/download/upload gates are removed from media execution so OpenList task managers own concurrency.

**Tech Stack:** Go, GORM, OpenList `fs`/`tache` task managers, React/Solid-style frontend signals, TypeScript, Go tests, frontend lint/build.

---

### Task 1: Preferred worker persistence and soft scheduling

**Files:**
- Modify: `internal/model/subscription.go`
- Modify: `server/handles/subscription.go`
- Modify: `internal/subscription/cluster_dispatch.go`
- Modify: `internal/cluster/protocol/payloads.go`
- Modify: `internal/cluster/subscription_dispatcher.go`
- Modify: `internal/cluster/runtime.go`
- Test: `internal/cluster/subscription_target_selection_test.go`
- Test: `internal/cluster/subscription_dispatcher_test.go`

- [ ] Write failing tests proving an eligible preferred worker wins, an offline/draining/incompatible preferred worker falls back to normal scoring, automatic mode preserves current scoring, and redispatch retains the preference.
- [ ] Run the focused tests and confirm they fail because the preference field/selection behavior does not exist.
- [ ] Add `preferred_worker_node_id` to `model.Subscription`, normalize whitespace on save, propagate it into immutable task context, and apply it after eligibility filtering for inspect, initial media dispatch, and redispatch.
- [ ] Run focused tests and the full `go test ./internal/cluster/... ./internal/subscription/... ./server/handles/...` suite.

### Task 2: Standalone-equivalent worker target path mapping

**Files:**
- Modify: `internal/cluster/worker/service.go`
- Modify: `internal/cluster/resultqueue/queue.go`
- Test: `internal/cluster/worker/service_test.go`
- Test: `internal/cluster/resultqueue/queue_test.go`

- [ ] Write failing tests mapping `LogicalMediaRoot` and `LogicalTargetPath` to the worker-local delivery root, rejecting traversal/out-of-root paths, using the planned target filename, and accepting cleanup only for the exact mapped path and remote file ID.
- [ ] Run focused tests and confirm the current `.openlist-cluster/<job>/<media>` behavior fails them.
- [ ] Replace the physical namespace path with a safe relative logical-path mapper. Keep job/media IDs only as database/task identity, not user-visible storage directories.
- [ ] Change reconciliation and cleanup ownership checks to use final path, name, size, SHA256, storage mount, and exact remote file ID; never remove parent media directories.
- [ ] Run focused worker/resultqueue tests.

### Task 3: Durable native asynchronous move execution

**Files:**
- Modify: `internal/fs/copy_move.go`
- Modify: `internal/task/base.go`
- Modify: `internal/bootstrap/task.go`
- Modify: `internal/cluster/worker/service.go`
- Modify: `internal/cluster/worker/upload_context.go`
- Create: `internal/cluster/worker/native_transfer.go`
- Modify: `internal/model/cluster_job.go` or create a focused native-transfer binding model
- Test: `internal/cluster/worker/native_transfer_test.go`
- Test: `internal/cluster/worker/provider_pipeline_integration_test.go`
- Test: `internal/fs/copy_move_test.go`

- [ ] Write failing tests proving cluster execution enqueues `MoveTaskManager` and returns immediately, task metadata survives context replacement/restart, terminal success emits exactly one upload manifest, failure maps to the cluster attempt, and source cleanup is not duplicated after move success.
- [ ] Run focused tests and confirm they fail with the current `NoTaskKey` synchronous path.
- [ ] Add a persistent cluster/native-task binding created before task scheduling, attach only a stable binding ID to the task, and reconstruct manifest/finalization data from durable storage rather than context-only values.
- [ ] Remove `NoTaskKey`, call native asynchronous `fs.Move`, assign a safe system/admin task creator, and reconcile native task terminal state into manifest enqueue and exact cleanup.
- [ ] Ensure native move task persistence is enabled for cluster-created tasks without changing unrelated user task behavior; recovery must be idempotent if a task or worker restarts.
- [ ] Run focused tests and restart-recovery tests.

### Task 4: Remove cluster-owned media concurrency gates and improve stage truth

**Files:**
- Modify: `internal/cluster/worker/service.go`
- Modify: `internal/cluster/worker/control.go`
- Modify: `internal/cluster/coordinator/service.go`
- Modify: `internal/model/cluster_job.go`
- Modify: `internal/cluster/protocol/payloads.go`
- Test: `internal/cluster/worker/service_test.go`
- Test: `internal/cluster/coordinator/service_test.go`

- [ ] Write failing tests proving multiple media offers enqueue native tasks without waiting for a target gate and stage state advances from permitted to running and terminal status based on native task reconciliation.
- [ ] Run focused tests and confirm the current target gate and permit-only stages fail them.
- [ ] Remove media execution's target/download/upload gate acquisition. Retain provider/account eligibility and scheduler capacity checks, while delegating actual transfer concurrency to OpenList task-manager worker counts.
- [ ] Add idempotent stage progress/result messages or coordinator updates for saving-share and uploading-mobile/native-transfer stages.
- [ ] Run focused cluster tests and race-sensitive tests.

### Task 5: Subscription worker selector in the frontend

**Files:**
- Modify: `/Volumes/extend Disk/Github/OpenList-Frontend/src/types/subscription.ts`
- Modify: `/Volumes/extend Disk/Github/OpenList-Frontend/src/pages/home/SubscriptionManagement.tsx`
- Modify: `/Volumes/extend Disk/Github/OpenList-Frontend/src/lang/en/subscription.json`
- Modify: `/Volumes/extend Disk/Github/OpenList-Frontend/src/lang-overrides/zh-CN/subscription.json`
- Modify: `/Volumes/extend Disk/Github/OpenList-Frontend/src/lang-overrides/zh-TW/subscription.json`

- [ ] Add a failing component/type-level test if the repository has an established harness; otherwise use TypeScript compilation as the red check after referencing the missing field.
- [ ] Add an “Automatic” default and selectable online non-draining workers, while preserving a saved unavailable worker as a disabled option.
- [ ] Explain that offline, draining, or incompatible preferred workers fall back automatically and that changes affect newly created jobs only.
- [ ] Run frontend lint and production build.

### Task 6: Integrated regression and acceptance verification

**Files:**
- Modify tests only if an uncovered integration boundary is found.

- [ ] Run `gofmt` on modified Go files.
- [ ] Run focused red/green regression suites for scheduling, path mapping, native move lifecycle, cleanup, and stage transitions.
- [ ] Run `go test ./internal/cluster/... ./internal/subscription/... ./internal/fs/... ./server/handles/...`.
- [ ] Run `go test ./...` and record any unrelated pre-existing failures separately.
- [ ] Run frontend lint and build.
- [ ] Inspect `git diff --check`, backend diff, and frontend diff; confirm no unrelated untracked files were modified.
- [ ] If remote deployment is in scope after local verification, build/deploy the image and verify a new subscription run distributes to an eligible worker, creates native move tasks, writes the genre/TMDB/season path, and creates no new `.openlist-cluster` directory.

## Self-review

- Coverage: preferred-worker automatic fallback, native asynchronous move, OpenList-owned concurrency, standalone path parity, exact cleanup, task-panel visibility, recovery, frontend selection, and TDD verification are all assigned explicit tasks.
- No new external dependencies are required.
- Existing `.openlist-cluster` data is intentionally left untouched; only new executions stop creating it.
- Preference is a scheduling hint and is excluded from media identity/idempotency semantics.

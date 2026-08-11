# Subscription Database Performance and PostgreSQL Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make subscription management return promptly under the current million-row event workload, stop durable event tables from growing without bound, and provide a measured, reversible path from SQLite to PostgreSQL for the cluster deployment.

**Architecture:** Keep the existing GORM database abstraction and public subscription API. First replace the correlated latest-event query with a portable aggregate query and add regression/performance coverage. Then add bounded retention for terminal Telegram events and processed cluster inbox records, preserving active/retryable records. Finally add an explicit SQLite-to-PostgreSQL migration command, compatibility tests, validation reports, and a stop-the-world cutover/rollback procedure. PostgreSQL is the target for the high-write cluster deployment, but the immediate page fix remains database-neutral.

**Tech Stack:** Go, GORM, SQLite/glebarez SQLite, PostgreSQL/GORM driver, SQLite read-only inspection, Docker Compose for integration tests, `go test`, `go test -bench`, and authenticated HTTP smoke tests.

## Global Constraints

- Preserve the existing `ListSubscriptions` response shape and the latest-event tie-break rule: greatest `created_at`, then greatest `id`.
- Do not add a runtime dependency for the query fix or retention worker.
- Never delete `pending`, `processing`, `retry_wait`, or otherwise active records during retention.
- Do not delete remote production data during development or testing; destructive retention runs must support dry-run, bounded batches, and an explicit production invocation.
- Preserve the existing user-owned change in `internal/115sy/files_test.go`.
- Keep SQLite support for development and single-node installations after PostgreSQL support is delivered.
- PostgreSQL cutover must have a complete backup, row-count validation, integrity validation, smoke test, and rollback path before production writes are enabled.
- Do not claim a performance improvement until a test has recorded query latency, endpoint latency, errors, CPU, and SQLite WAL/database size or PostgreSQL connection/lock metrics.

---

## Current Findings and Acceptance Criteria

The current remote instance has 151 subscriptions, 3,708 subscription items, 1,052,270 Telegram events, and 641,318 processed cluster inbox records. The Telegram event table occupies approximately 3.64 GiB, while the SQLite database and WAL are approximately 4.5 GiB and 5.1 GiB. The current `NOT EXISTS` latest-event query did not finish within 15 seconds on a read-only snapshot; an aggregate equivalent completed in approximately 0.4 seconds.

The implementation is complete only when all of these are true:

1. The latest-event query returns exactly one event per subscription and preserves the `created_at DESC, id DESC` rule, including equal timestamps.
2. A seeded dataset with at least 1,000,000 Telegram events completes the latest-event query within 1 second on the remote-like SQLite benchmark fixture and has no quadratic query plan.
3. The subscription list endpoint has p95 latency below 2 seconds with 20 concurrent readers on the remote-like fixture, with zero database-lock errors.
4. Retention deletes only records past the configured terminal-state horizon, is idempotent, uses bounded batches, and leaves active/retryable records untouched.
5. The migration validator proves row counts, key uniqueness, required field preservation, selected payload hashes, and sequence correctness for every migrated table.
6. PostgreSQL integration tests pass for subscription listing, event enqueue/claim/complete, cluster inbox deduplication, subscriptions, and existing admin handlers.
7. The production runbook can restore the pre-cutover SQLite snapshot and restart the service without requiring a reverse data migration.

## File Map

- Modify `internal/db/subscription_realtime.go`: replace the correlated latest-event query and add bounded event-retention primitives.
- Modify `internal/db/subscription.go`: add bounded subscription-event cleanup support only if the retention query is kept with subscription DB operations.
- Modify `internal/model/subscription_realtime.go`: add retention-related model constants or indexes only when required by the query plan.
- Modify `internal/model/cluster_job.go`: add or adjust inbox-retention indexes only when required by measured plans.
- Modify `internal/cluster/coordinator/service.go`: expose the existing inbox storage semantics to a cleanup service without changing message deduplication behavior.
- Create `internal/subscription/retention.go`: coordinate terminal event and processed inbox cleanup, with dry-run and batch limits.
- Create `internal/subscription/retention_test.go`: unit and integration tests for retention safety and idempotence.
- Modify `internal/subscription/list_test.go` and `internal/db/subscription_test.go`: add latest-event correctness and regression tests.
- Create `internal/db/subscription_realtime_benchmark_test.go`: benchmark the old failure shape and the replacement at realistic row counts without retaining the old production query in runtime code.
- Modify `internal/conf/config.go`: add explicit database pool and retention settings only after the migration/retention behavior is defined.
- Modify `internal/bootstrap/db.go`: configure PostgreSQL connection pool limits and expose the selected driver in diagnostics.
- Create `cmd/migrate_database/main.go`: run an offline SQLite-to-PostgreSQL migration with dry-run, copy, validate, and report modes.
- Create `internal/db/migration.go`: define migration table order, row-copy helpers, validation counts, hashes, and sequence reset logic.
- Create `internal/db/migration_test.go`: test migration ordering, validation failures, and sequence behavior with test databases.
- Modify `docker-compose.yml`: add an optional PostgreSQL service/profile for local integration testing; do not make it mandatory for SQLite users.
- Create `docs/operations/subscription-database-migration.md`: production migration, retention, backup, cutover, monitoring, and rollback runbook.

---

## Task 1: Lock down latest-event behavior with failing tests

**Files:**

- Modify: `internal/db/subscription_test.go`
- Modify: `internal/subscription/list_test.go`
- Test fixture: existing in-memory SQLite setup used by the subscription tests

**Interfaces:**

- Consumes: `db.ListLatestSubscriptionTelegramEventsBySubscriptionIDs([]uint)`
- Produces: explicit tests for one-row-per-subscription behavior and tie-breaking

- [ ] **Step 1: Add the equal-timestamp regression test**

Create two events for one subscription with identical `CreatedAt` and different IDs, plus an older event. Assert that only the event with the greatest ID is returned.

```go
func TestListLatestSubscriptionTelegramEventsUsesIDAsTieBreaker(t *testing.T) {
	createdAt := time.Date(2026, 8, 10, 9, 0, 0, 0, time.UTC)
	older := model.SubscriptionTelegramEvent{SubscriptionID: subscription.ID, Channel: "c", MessageID: "older", CreatedAt: createdAt.Add(-time.Minute)}
	sameTimeLow := model.SubscriptionTelegramEvent{SubscriptionID: subscription.ID, Channel: "c", MessageID: "same-low", CreatedAt: createdAt}
	sameTimeHigh := model.SubscriptionTelegramEvent{SubscriptionID: subscription.ID, Channel: "c", MessageID: "same-high", CreatedAt: createdAt}
	for _, event := range []*model.SubscriptionTelegramEvent{&older, &sameTimeLow, &sameTimeHigh} {
		if err := database.Create(event).Error; err != nil {
			t.Fatal(err)
		}
	}

	got, err := ListLatestSubscriptionTelegramEventsBySubscriptionIDs([]uint{subscription.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].MessageID != "same-high" {
		t.Fatalf("latest event = %#v, want only same-high", got)
	}
}
```

- [ ] **Step 2: Add empty, multi-subscription, and no-event cases**

Cover an empty ID list, two subscriptions with independent latest events, and a subscription with no events. Assert that no event from one subscription is returned for another.

- [ ] **Step 3: Run the focused tests and verify the baseline**

Run:

```bash
go test ./internal/db ./internal/subscription -run 'TestListLatestSubscriptionTelegramEvents|TestListSubscriptionsWithProgress' -count=1
```

Expected: existing tests pass and the new tie-break test passes against the current implementation. If the current implementation fails a new test, record the failure as the regression contract before changing production code.

- [ ] **Step 4: Commit the test contract**

Use a Conventional Commit title such as `test(subscription): lock down latest event selection`; include the repository Lore trailers required by `AGENTS.md`.

---

## Task 2: Replace the quadratic latest-event query

**Files:**

- Modify: `internal/db/subscription_realtime.go:168-197`
- Test: `internal/db/subscription_test.go`
- Test: `internal/subscription/list_test.go`

**Interfaces:**

- Consumes: the Task 1 tests and existing `SubscriptionTelegramEvent` index `idx_subscription_telegram_events_latest`.
- Produces: the unchanged `ListLatestSubscriptionTelegramEventsBySubscriptionIDs` signature and one latest row per requested subscription.

- [ ] **Step 1: Replace `NOT EXISTS` with a portable two-stage aggregate query**

Build both subqueries with GORM so the existing table prefix and SQLite/PostgreSQL quoting continue to work:

```go
latestCreatedAt := db.Table(table).
	Select("subscription_id, MAX(created_at) AS created_at").
	Where("subscription_id IN ?", subscriptionIDs).
	Group("subscription_id")

latestID := db.Table(table).
	Select("subscription_id, created_at, MAX(id) AS id").
	Where("subscription_id IN ?", subscriptionIDs).
	Group("subscription_id, created_at")

query := db.Table(table + " AS e").
	Select("e.*").
	Joins("JOIN (?) AS latest_time ON latest_time.subscription_id = e.subscription_id AND latest_time.created_at = e.created_at", latestCreatedAt).
	Joins("JOIN (?) AS latest_id ON latest_id.subscription_id = e.subscription_id AND latest_id.created_at = e.created_at AND latest_id.id = e.id", latestID).
	Where("e.subscription_id IN ?", subscriptionIDs)
```

The final query must return one row per subscription even when multiple rows share a timestamp. Do not load the complete event history into Go.

- [ ] **Step 2: Verify the generated SQL on SQLite and PostgreSQL**

Run the focused tests on SQLite, then run the same query against a disposable PostgreSQL database. Inspect `EXPLAIN QUERY PLAN` on SQLite and `EXPLAIN (ANALYZE, BUFFERS)` on PostgreSQL. The plan must use the subscription/time/id indexes or equivalent grouped index access and must not contain a correlated scan for every candidate event.

- [ ] **Step 3: Add a query-shape regression guard**

Add a test helper that captures the generated SQL in debug mode and rejects the old `NOT EXISTS` pattern. Keep the guard narrow: it should protect this query only and must not assert a driver-specific placeholder format.

- [ ] **Step 4: Run the focused test suite**

Run:

```bash
go test ./internal/db ./internal/subscription -run 'TestListLatestSubscriptionTelegramEvents|TestListSubscriptionsWithProgress' -count=1
```

Expected: PASS with one latest event per subscription and no response-shape changes.

- [ ] **Step 5: Commit the query fix**

Use a title such as `fix(subscription): avoid quadratic latest event lookup`, with tested and not-tested Lore trailers.

---

## Task 3: Add realistic performance benchmarks and endpoint verification

**Files:**

- Create: `internal/db/subscription_realtime_benchmark_test.go`
- Modify: `internal/subscription/list_test.go`
- Create: `scripts/benchmark_subscription_list.sh`

**Interfaces:**

- Consumes: the unchanged latest-event API and a temporary database fixture.
- Produces: reproducible benchmark output for 151 subscriptions and at least 1,000,000 events.

- [ ] **Step 1: Create a deterministic fixture generator**

Seed 151 subscriptions, 3,708 subscription items, and 1,000,000 events distributed across subscriptions. Use deterministic timestamps and message IDs so the expected latest event is known. Store the fixture under a temporary directory and never under the production data directory.

- [ ] **Step 2: Add a benchmark for the latest-event query**

Benchmark only the database call, report allocations, and fail a dedicated acceptance script when the p95 exceeds the agreed threshold. The Go benchmark itself should remain diagnostic and should not make machine-dependent assertions.

- [ ] **Step 3: Add concurrent endpoint measurement**

The script must issue 20 concurrent authenticated list requests and record p50, p95, p99, HTTP errors, and database-lock errors. It must not print credentials or store tokens in the repository.

- [ ] **Step 4: Define the performance gate**

For the remote-like SQLite fixture, require latest-event query p95 under 1 second and list endpoint p95 under 2 seconds with zero lock errors. For PostgreSQL, run the same fixture and record the result; use the SQLite gate as the minimum functional target and report the PG result separately.

- [ ] **Step 5: Run the benchmark before and after the query change**

Run:

```bash
go test ./internal/db -run '^$' -bench 'BenchmarkListLatestSubscriptionTelegramEvents' -benchmem -count=5
```

Expected: the replacement query completes within the gate; the old implementation, when tested only in an isolated comparison fixture, demonstrates the timeout/scale failure that motivated the change.

- [ ] **Step 6: Commit benchmark coverage**

Use a title such as `test(subscription): benchmark large realtime event history`.

---

## Task 4: Implement bounded terminal-event and cluster-inbox retention

**Files:**

- Create: `internal/subscription/retention.go`
- Create: `internal/subscription/retention_test.go`
- Modify: `internal/db/subscription_realtime.go`
- Modify: `internal/model/cluster_job.go` only if the measured retention plan requires a composite index
- Modify: `internal/conf/config.go`
- Modify: `internal/bootstrap/run.go:111-115` to start the coordinator-owned retention loop next to the existing subscription scheduler and Telegram realtime listener

**Interfaces:**

- Consumes: `RetentionOptions{DryRun bool, BatchSize int, EventTerminalAge time.Duration, InboxProcessedAge time.Duration}`.
- Produces: `RunSubscriptionRetention(ctx context.Context, options RetentionOptions) (RetentionReport, error)` where `RetentionReport` contains scanned, eligible, deleted, skipped-active, and error counts.

- [ ] **Step 1: Add retention safety tests first**

Create rows for every Telegram event status and cluster inbox status at both old and recent timestamps. Assert that only old terminal rows are eligible; pending, processing, retry-wait, and recent rows remain untouched.

```go
func TestRunSubscriptionRetentionKeepsActiveEvents(t *testing.T) {
	old := time.Now().UTC().Add(-30 * 24 * time.Hour)
	for _, status := range []string{
		model.SubscriptionTelegramEventStatusPending,
		model.SubscriptionTelegramEventStatusProcessing,
		model.SubscriptionTelegramEventStatusRetryWait,
	} {
		createEvent(t, status, old)
	}

	report, err := RunSubscriptionRetention(context.Background(), RetentionOptions{
		DryRun:             false,
		BatchSize:          100,
		EventTerminalAge:   7 * 24 * time.Hour,
		InboxProcessedAge:  14 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Deleted != 0 {
		t.Fatalf("deleted %d active records", report.Deleted)
	}
}
```

- [ ] **Step 2: Implement bounded deletion predicates**

Use primary-key or indexed timestamp batches. Delete only:

```sql
-- Telegram events: terminal records older than the configured horizon.
status IN ('processed', 'dead_letter')
AND COALESCE(processed_at, updated_at) < ?

-- Cluster inbox: processed records older than the configured horizon.
status = 'processed'
AND processed_at < ?
```

Never issue an unbounded `DELETE`. Select at most `BatchSize` IDs, delete by those IDs, commit, and repeat until fewer than `BatchSize` rows are returned.

- [ ] **Step 3: Add dry-run and idempotence behavior**

Dry-run must return eligible counts without deleting. A second real run immediately after a successful run must delete zero additional rows. Every batch must be cancellable through `context.Context`.

- [ ] **Step 4: Add configuration with conservative defaults**

Add explicit settings for event and inbox retention and batch size. The defaults must be documented as operational policy, and active/retryable rows must never be governed by the terminal cleanup setting. Set the initial production values only after confirming the Telegram replay/catch-up window and cluster duplicate-message window in the deployment runbook.

- [ ] **Step 5: Schedule cleanup away from request handling**

Run retention from a single coordinator-owned background loop with a long interval and one in-flight run. The subscription list request must never perform cleanup synchronously.

- [ ] **Step 6: Verify retention on a copied remote snapshot**

Run dry-run first and compare eligible counts with the status distribution. Run a bounded test cleanup on a disposable copy, then confirm active counts and latest-event results are unchanged.

- [ ] **Step 7: Commit retention support**

Use a title such as `feat(subscription): bound terminal event retention`.

---

## Task 5: Add PostgreSQL compatibility and connection-pool support

**Files:**

- Modify: `internal/conf/config.go`
- Modify: `internal/bootstrap/db.go`
- Modify: `internal/db/util.go` only for confirmed driver-specific quoting gaps
- Create: `internal/db/postgres_compatibility_test.go`
- Modify: `docker-compose.yml` with an optional PostgreSQL integration-test profile

**Interfaces:**

- Consumes: existing `Database` fields and the existing GORM PostgreSQL driver.
- Produces: tested PostgreSQL startup, migration, pool configuration, and subscription/cluster query compatibility.

- [ ] **Step 1: Inventory SQLite-specific SQL before changing behavior**

Search all production database code for SQLite-only syntax, backtick-quoted identifiers, date functions, `INSERT OR`, `ON CONFLICT` assumptions, and raw queries. Record each finding and either add a driver branch or prove the SQL is portable.

- [ ] **Step 2: Add pool settings without changing SQLite defaults**

After `gorm.Open`, call `sqlDB, err := dB.DB()` and configure PostgreSQL `SetMaxOpenConns`, `SetMaxIdleConns`, and `SetConnMaxLifetime` from explicit config values. Leave SQLite at a single-writer-safe configuration unless measured tests justify a change.

- [ ] **Step 3: Add a disposable PostgreSQL test service**

Use a Compose profile with a pinned PostgreSQL major version. Do not put production credentials in the repository. Tests must obtain the DSN from an environment variable and skip with a clear message when it is absent.

- [ ] **Step 4: Run schema and behavior compatibility tests**

Run `AutoMigrate` against PostgreSQL and exercise subscription creation, list/progress calculation, event enqueue/claim/complete, latest-event selection, cluster inbox deduplication, and retention. Verify table prefixes, timestamps, text payloads, unique constraints, and indexes.

- [ ] **Step 5: Commit compatibility support**

Use a title such as `test(db): verify PostgreSQL subscription compatibility`.

---

## Task 6: Build an explicit SQLite-to-PostgreSQL migration command

**Files:**

- Create: `cmd/migrate_database/main.go`
- Create: `internal/db/migration.go`
- Create: `internal/db/migration_test.go`
- Create: `docs/operations/subscription-database-migration.md`

**Interfaces:**

- Consumes: source SQLite path, target PostgreSQL DSN, `--dry-run`, `--validate-only`, `--batch-size`, and `--tables` flags.
- Produces: migrated schema/data, a machine-readable validation report, restored sequences, and a non-zero exit code for any mismatch.

- [ ] **Step 1: Define migration order and table inventory**

Declare the full table list from `internal/db/db.go`, grouped as base tables, subscription tables, cluster tables, and auxiliary tables. Copy base rows before dependent rows; keep source IDs for all tables whose IDs participate in references.

- [ ] **Step 2: Add dry-run inspection**

`--dry-run` must connect to both databases, verify connectivity, print table counts and estimated payload sizes, and perform no writes. `--validate-only` must compare an existing pair without copying.

- [ ] **Step 3: Implement batch copy with explicit transactions**

Read source rows by stable primary-key order, write target batches in transactions, and use PostgreSQL-native conflict handling only for resumable retries. Do not load the full 4+ GiB database into process memory. Preserve nullable timestamps, JSON/text payloads, status strings, and IDs.

- [ ] **Step 4: Reset PostgreSQL sequences**

For every auto-increment table, set the sequence to at least the maximum imported ID and validate that the next insert succeeds without a primary-key collision.

- [ ] **Step 5: Add validation report contents**

For every table, report source count, target count, source/target min and max primary key, and a deterministic sample hash over stable columns. For subscription events and cluster inboxes, additionally compare counts grouped by status and date bucket. Fail validation on any mismatch.

- [ ] **Step 6: Test interruption and resume**

Stop migration between batches, rerun it, and verify no duplicate rows and identical validation output. Test a deliberate mismatch and confirm a non-zero exit code with the affected table and column group named.

- [ ] **Step 7: Write the cutover and rollback runbook**

The runbook must require:

1. Stop OpenList and all writers.
2. Snapshot `data.db`, `data.db-wal`, and `data.db-shm` together while the service is stopped.
3. Run migration and validation.
4. Start a canary instance against PostgreSQL.
5. Verify login, subscription list, event processing, cluster coordinator health, and worker communication.
6. Enable production traffic only after smoke tests pass.
7. Roll back by stopping the PG instance, restoring the pre-cutover SQLite configuration/snapshot, and restarting OpenList; do not attempt reverse replication during the incident window.

- [ ] **Step 8: Commit migration tooling and runbook**

Use a title such as `feat(db): add validated SQLite to PostgreSQL migration`.

---

## Task 7: Production verification and staged rollout

**Files:**

- Modify: `docs/operations/subscription-database-migration.md`
- Create: `scripts/verify_subscription_database_rollout.sh`

- [ ] **Step 1: Capture the SQLite baseline**

Record latest-event query latency, list endpoint p50/p95/p99, HTTP 5xx count, database-lock errors, CPU, WAL size, event count by status, inbox count by status, and the repeated `staging failed` log rate.

- [ ] **Step 2: Deploy only the query fix first**

Verify that the subscription page returns before enabling retention or changing the database driver. This isolates the direct page regression from lifecycle and migration effects.

- [ ] **Step 3: Enable retention in dry-run mode**

Compare the report against the expected status distribution. Confirm no pending or retryable records are eligible.

- [ ] **Step 4: Enable bounded retention**

Run a small batch, watch CPU/IO/lock metrics, and increase the batch limit only if the database remains responsive. Confirm that the event table and WAL stop growing without bound.

- [ ] **Step 5: Run the PostgreSQL canary**

Use the migrated copy and run authenticated list requests, manual subscription checks, realtime event processing, cluster dispatch, worker acknowledgements, and admin detail views. Compare response counts and sampled records with the SQLite baseline.

- [ ] **Step 6: Decide cutover using explicit gates**

Proceed only if migration validation is exact, all focused/integration/load tests pass, p95 list latency meets the gate, no new lock/duplicate/sequence errors occur, and rollback has been exercised on the canary.

- [ ] **Step 7: Record post-rollout evidence**

Store only non-secret metrics and validation summaries in the operations record. Do not store credentials, JWTs, cookies, or API tokens.

---

## Test Plan Summary

### Unit and database behavior

- Latest event: empty input, one subscription, multiple subscriptions, equal timestamps, ID tie-break, missing events, duplicate invocation.
- Subscription list: progress calculation unchanged, archive filtering unchanged, pagination response unchanged, latest realtime status unchanged.
- Retention: terminal-only deletion, active-state preservation, age boundary, bounded batches, cancellation, dry-run, idempotence, partial failure reporting.
- Migration: table order, null/text/time preservation, unique constraints, sequence reset, resumability, mismatch detection.

### Integration

- SQLite current driver with WAL and busy timeout.
- PostgreSQL pinned test service using the same GORM models and table prefix.
- Event enqueue → claim → complete/retry/dead-letter flow.
- Cluster inbox duplicate message and sequence handling.
- Retention concurrent with list reads and event writes.

### Performance and load

- 1,000,000-event fixture, 151 subscriptions, 20 concurrent list readers.
- Latest-event query p95 under 1 second on the remote-like fixture.
- List endpoint p95 under 2 seconds and zero database-lock errors.
- Record allocation count, CPU, IO, database size, WAL size, PostgreSQL connections, locks, and error rate.
- Compare SQLite and PostgreSQL under the same read/write workload; report results rather than assuming PG is faster.

### Production smoke and rollback

- Login and current-user request.
- Subscription list, search, filter, archive status, pagination.
- Subscription detail and run history.
- Realtime event processing and latest status projection.
- Cluster coordinator lease, worker acknowledgement, job dispatch/result.
- Stop PG and restore the pre-cutover SQLite snapshot on the canary; verify service recovery.

## Risks and Mitigations

- **Query fix changes tie behavior:** lock equal-timestamp tests before implementation and compare sampled production results.
- **Retention breaks replay deduplication:** preserve active records and set the terminal horizon from observed Telegram/cluster replay guarantees; dry-run before deletion.
- **Migration loses payloads or sequence state:** batch copy, row counts, sample hashes, explicit sequence reset, and canary validation.
- **PostgreSQL compatibility gap:** run the same subscription/cluster tests against both drivers and audit raw SQL before cutover.
- **Background staging retries continue consuming CPU:** track and remediate the repeated `staging failed` loop as a separate work item; do not conflate it with the list-query acceptance gate.
- **Large SQLite snapshot is slow to copy:** stop writers, snapshot all three database files together, validate the snapshot, and perform migration offline or from a verified copy.

## Self-Review Checklist

- [x] The plan separates the direct page regression from the database migration.
- [x] Every implementation phase has exact files, interfaces, tests, commands, and acceptance criteria.
- [x] Active and retryable records are explicitly protected from retention.
- [x] PostgreSQL migration includes validation and rollback rather than a config-only switch.
- [x] The existing unrelated `internal/115sy/files_test.go` change is protected.
- [x] No production mutation is included in the development steps.

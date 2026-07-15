# Cluster SQLite Resilience Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent transient SQLite contention from disabling OpenList cluster coordinators and apply correct WAL settings to Hybrid and Worker nodes.

**Architecture:** Move SQLite DSN construction into the build-specific adapters, then isolate lease-renewal state transitions in a testable helper and synchronize coordinator access during manifest processing. Deploy only after focused tests, race tests, a remote backup, and image provenance checks.

**Tech Stack:** Go, GORM, glebarez/sqlite, gorm.io/driver/sqlite, Docker Compose, SQLite.

---

### Task 1: Lock SQLite driver behavior with failing tests

**Files:**
- Create: `internal/bootstrap/sqlite_driver_glebarez_test.go`
- Modify: `internal/bootstrap/sqlite_driver_glebarez.go`
- Modify: `internal/bootstrap/sqlite_driver_gorm.go`
- Modify: `internal/bootstrap/db.go`

- [ ] Write a test that opens a temporary database through the default adapter and asserts `PRAGMA journal_mode` is `wal` and `PRAGMA busy_timeout` is `5000`.
- [ ] Run `go test ./internal/bootstrap -run TestOpenSQLiteConfiguresWALAndBusyTimeout -count=1` and confirm it fails because the existing `_journal` option is ignored by glebarez.
- [ ] Add a build-specific `sqliteDSN` helper and make `InitDB` call `openSQLite(sqliteDSN(database.DBFile))`.
- [ ] Run the focused test and confirm it passes.
- [ ] Run `go test ./internal/bootstrap -count=1`.

### Task 2: Preserve coordinator ownership through transient database errors

**Files:**
- Modify: `internal/cluster/runtime.go`
- Modify: `internal/cluster/runtime_security_test.go`

- [ ] Write table tests for lease renewal decisions: success advances the deadline, zero affected rows fences immediately, transient error before deadline retries, and error at/after deadline fences.
- [ ] Run the focused tests and confirm they fail because no decision helper exists and the current code fences on every error.
- [ ] Implement the smallest lease-decision helper and update `runCoordinatorLease` to use it.
- [ ] Run focused tests and all `internal/cluster` tests.

### Task 3: Remove the manifest/fencing nil race

**Files:**
- Modify: `internal/cluster/runtime.go`
- Modify: `internal/cluster/runtime_security_test.go`

- [ ] Write a regression test that snapshots the coordinator while fencing clears the runtime field and confirms the valid snapshot remains callable.
- [ ] Run the focused test and confirm the unsynchronized access is detected by the test or `-race`.
- [ ] Snapshot the coordinator under `RLock` once per processor tick and invoke only the snapshot.
- [ ] Run `go test -race ./internal/cluster -count=1`.

### Task 4: Repository verification

**Files:**
- No production files beyond Tasks 1-3.

- [ ] Run `gofmt` on changed Go files.
- [ ] Run focused bootstrap and cluster tests.
- [ ] Run the repository lint, type/static analysis, and broader Go test commands supported by the project tooling.
- [ ] Inspect `git diff --check`, `git diff`, and `git status` to ensure unrelated user changes were preserved.

### Task 5: Back up, deploy, and verify remotely

**Files:**
- Remote backup under `/volume1/docker_dir/openlist_etf/.config-backups/<timestamp>-cluster-sqlite-resilience/`.
- Remote Compose file remains unchanged unless image provenance requires an explicit immutable tag.

- [ ] Record the current image digest, container inspect output, database size, and service state.
- [ ] Stop only the affected service and copy `data.db`, `data.db-wal`, `data.db-shm`, and `config.json` when present.
- [ ] Build and transfer/deploy the verified image without overwriting unrelated remote files.
- [ ] Start the service and assert persistent `journal_mode=wal`, inspect the application DSN for `busy_timeout=5000`, verify `/api/cluster/ws` no longer reports coordinator disabled, and confirm the worker reconnects.
- [ ] Monitor logs across multiple lease intervals for SQLite, lease, panic, and worker warnings.
- [ ] If verification fails, restore the prior image and backup before reporting.

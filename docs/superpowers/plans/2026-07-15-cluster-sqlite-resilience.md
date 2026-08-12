# Cluster SQLite Resilience Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent transient SQLite contention from disabling OpenList cluster coordinators and apply correct WAL settings to Hybrid and Worker nodes.

**Architecture:** Move SQLite DSN construction into the build-specific adapters, use immediate transactions to prevent WAL snapshot-upgrade failures, isolate lease-renewal state transitions in a testable helper, and synchronize coordinator lifecycle access. Deploy only after focused tests, race tests, a remote backup, and image provenance checks.

**Tech Stack:** Go, GORM, glebarez/sqlite, gorm.io/driver/sqlite, Docker Compose, SQLite.

---

### Task 1: Lock SQLite driver behavior with failing tests

**Files:**
- Create: `internal/bootstrap/sqlite_driver_glebarez_test.go`
- Modify: `internal/bootstrap/sqlite_driver_glebarez.go`
- Modify: `internal/bootstrap/sqlite_driver_gorm.go`
- Modify: `internal/bootstrap/db.go`

- [x] Write a test that opens a temporary database through the default adapter and asserts `PRAGMA journal_mode` is `wal` and `PRAGMA busy_timeout` is `5000`.
- [x] Run `go test ./internal/bootstrap -run TestOpenSQLiteConfiguresWALAndBusyTimeout -count=1` and confirm it fails because the existing `_journal` option is ignored by glebarez.
- [x] Add a build-specific `sqliteDSN` helper and make `InitDB` call `openSQLite(sqliteDSN(database.DBFile))`.
- [x] Run the focused test through both default and `sqlite_cgo_compat` adapters and confirm it passes.
- [x] Add a real two-connection regression that reproduces 517/deferred lock failure, then add `_txlock=immediate` to both adapters and confirm the test passes.
- [x] Run default and `sqlite_cgo_compat` `internal/bootstrap` package tests.

### Task 2: Preserve coordinator ownership through transient database errors

**Files:**
- Modify: `internal/cluster/runtime.go`
- Modify: `internal/cluster/runtime_security_test.go`

- [x] Write table tests for lease renewal decisions: success advances the deadline, zero affected rows fences immediately, transient error before deadline retries, and error at/after deadline fences.
- [x] Add cases proving a successful SQL result at/after the old deadline still fences and the local deadline exactly matches the value written to the database.
- [x] Run the focused tests and confirm they fail against the old unconditional fencing/deadline behavior.
- [x] Bound each renewal SQL context by the current lease deadline and re-evaluate wall time after every SQL completion.
- [x] Capture immutable runtime generation/owner state and order normal stop after the final in-flight renewal.
- [x] Add stale-generation, stop/renewal barrier, and worker-background shutdown tests.
- [x] Run focused tests and all `internal/cluster` tests.

### Task 3: Remove the manifest/fencing nil race

**Files:**
- Modify: `internal/cluster/runtime.go`
- Modify: `internal/cluster/runtime_security_test.go`

- [x] Write a regression test that snapshots the coordinator while fencing clears the runtime field and executes the real processor tick through the retained snapshot.
- [x] Capture the generation-specific service/context in Hub callbacks instead of rereading mutable runtime fields.
- [x] Snapshot the coordinator under `RLock` once per processor tick and invoke only the snapshot.
- [x] Run `go test -race ./internal/cluster -count=1`.

### Task 4: Repository verification

**Files:**
- No production files beyond Tasks 1-3.

- [x] Run `gofmt` on changed Go files.
- [x] Run focused bootstrap/default/CGO, cluster, race, affected-package, and pure-Go production-tag tests.
- [x] Run `go vet ./internal/bootstrap ./internal/cluster/...`.
- [x] Run `go test ./... -count=1`; record that it is not fully green because of existing Go 1.26 format diagnostics, missing macOS `fuse.h`, Codex network transport behavior, and the missing localhost aria2 service. Do not report this command as PASS.
- [x] Build the production Linux/amd64 `jsoniter` binary with Go 1.26.4 and verify the embedded pure-Go DSN string.
- [x] Inspect `git diff --check`, `git diff`, and `git status` to ensure unrelated user changes were preserved.

### Task 5: Build and preload an immutable candidate

**Files:**
- Local/remote Docker artifacts only.

- [x] Build from the exact remote runtime base image ID rather than the stale local `latest` cache.
- [x] Use unique tag `entergtang/openlist-etf:sqlite-resilience-33596f44-771bb07f4b98` without overwriting `latest`.
- [x] Verify image ID `sha256:8a84d2b4e00da2d4ac1052d57ac00d1c8899694b7f46481589e697e54f849f5e`, Linux/amd64 platform, and image/binary SHA-256 agreement.
- [x] Complete local `/ping`, WAL, restart-count, and log smoke checks.
- [x] Save, transfer, checksum, and `docker load` the candidate before stopping the remote service.
- [x] Preflight a separate candidate Compose override and confirm normalized resolved Compose differs only in `openlist-etf.image`.

### Task 6: Back up, deploy, and verify remotely

**Files:**
- Remote backup under `/volume1/docker_dir/openlist_etf/.config-backups/<timestamp>-cluster-sqlite-resilience-immediate/`.
- Separate immutable Compose overrides; the main Compose file remains unchanged.

- [x] Record the old image ID/ref, container/Redis inspect, resolved current/candidate Compose, data-file stat, and database mount source without printing secrets.
- [x] Create a 0700 backup directory and retain the old image archive under an immutable rollback tag.
- [x] Stop only `openlist-etf`; leave `openlist-worker-redis` running with the same ID/start time/restart count.
- [x] Back up the same-batch `data.db`, `data.db-wal`, `data.db-shm`, `data.db-journal`, `config.json`, Compose, override, and `.env` set when present.
- [x] Run `PRAGMA quick_check` against a separate verification copy and require `ok`.
- [x] Exercise the rollback path after the first candidate still reproduced the pre-existing 517 failure: save failed state, move the entire candidate DB group, restore the same-batch group, and restart old image ID `sha256:477d449e...` with `--no-deps --pull never`.
- [x] Deploy the final candidate with the separate immutable override, `--no-deps`, and `--pull never`.
- [x] Assert `/ping` 200, `/api/cluster/ws` 400 instead of disabled 404, exact image and binary provenance, persistent WAL, restart count 0, online nodes, and connected latest sessions.
- [x] Sample six times over 80 seconds: stable owner/container, advancing lease and heartbeats, Redis unchanged.
- [x] Confirm zero post-deployment log lines for 517, `SQLITE_BUSY`, `database is locked`, nested transaction, lease lost/expired, coordinator disabled, panic, and fatal patterns.

### Final Implementation Record

- Source hash: `771bb07f4b980c33bd886b87169f592e45b073c5b651a78e3993e3d436dbc192`
- Frontend/public hash: `2c62ba666b4427c5ab8c31b37775303a4d41d3ce2325809efa09ce4643360b66`
- Binary SHA-256: `2728c6d62d5ce01af3de2b4f5bf304715531dd5b03c2a9b5c34d934b6f032852`
- Image archive SHA-256: `677592fc3254cc0e2b6d01977b1e99510e312eb6a0b18ff3a410d688b9fa8401`
- Final remote backup: `/volume1/docker_dir/openlist_etf/.config-backups/20260715-234943-cluster-sqlite-resilience-immediate/`
- Sanitized runtime evidence: the backup's `post-deploy/` directory.
- Persistent deployment override: `/volume1/docker_dir/openlist_etf/docker-compose.sqlite-resilience.override.yaml`. Future Compose operations for this deployment must include this file until the immutable image is incorporated into the primary deployment configuration.

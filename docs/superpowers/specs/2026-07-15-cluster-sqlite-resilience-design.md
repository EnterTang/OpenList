# Cluster SQLite Resilience Design

## Context

The Hybrid node on `192.168.1.182` was configured correctly but lost its coordinator after a transient SQLite failure. The pure-Go SQLite build opened the database with mattn-style `_journal` and `_vacuum` query parameters, so the running database remained in `DELETE` journal mode and the intended per-connection tuning was not expressed for that driver. Under concurrent subscription and cluster writes, lease renewal failed, fencing cleared the coordinator service, and the manifest processor raced with that cleanup and dereferenced a nil service.

## Goals

- Make the SQLite configuration effective for both supported SQLite drivers.
- Let a coordinator survive transient SQLite write contention without violating its 45-second lease.
- Remove the coordinator-service data race during fencing.
- Cover Hybrid and ordinary Worker deployments because both use the same database bootstrap path.

## Non-goals

- Migrating existing installations to MySQL or PostgreSQL.
- Changing cluster role semantics or allowing two coordinators to share a lease.
- Refactoring unrelated subscription or transfer code.

## Options Considered

1. **Remote-only PRAGMA change and restart.** Fast recovery, but future containers still open SQLite with the wrong parameters and no per-connection timeout.
2. **Driver-aware SQLite initialization plus lease and race hardening.** Recommended. It fixes the shared root cause with a narrow code change and keeps existing deployments compatible.
3. **Move cluster installations to a client/server database.** Stronger write concurrency, but introduces an operational migration and is disproportionate to this defect.

## Design

### SQLite initialization

Each build-specific SQLite adapter will construct the DSN it understands:

- `glebarez/sqlite`: `_pragma=journal_mode(WAL)`, `_pragma=busy_timeout(5000)`, and `_pragma=auto_vacuum(incremental)`.
- `gorm.io/driver/sqlite`: `_journal=WAL`, `_busy_timeout=5000`, and `_vacuum=incremental`.

`InitDB` will pass the database path to that adapter instead of embedding driver-specific options in shared bootstrap code. This also applies to ordinary Workers.

### Coordinator lease renewal

The lease loop will distinguish an ownership loss from transient database contention:

- `RowsAffected == 0` with no database error still means the lease is no longer owned and fences immediately.
- A database error before the current 45-second lease deadline logs a warning and retries on the next 15-second tick.
- Repeated errors reaching the known lease deadline fence the coordinator.
- A successful renewal advances the local deadline.

This preserves split-brain safety while preventing a single `SQLITE_BUSY` from disabling the coordinator.

### Manifest processor synchronization

The processor will snapshot `coordinatorService` under the runtime read lock before invoking it. Fencing may clear the runtime field, but the current tick retains a valid service reference and observes the canceled context instead of dereferencing nil.

## Verification

- Unit-test both driver-specific DSN builders.
- Open a real pure-Go SQLite database and assert `journal_mode=wal` and `busy_timeout=5000`.
- Unit-test lease decisions for transient error, expired error, lost ownership, and successful renewal.
- Add a regression test that fences between the manifest processor snapshot and service use without panic.
- Run focused tests, package tests, `go test -race` for the cluster package, and the repository's available lint/static checks.
- After backing up the remote database, deploy and verify the persistent WAL mode, Hybrid WebSocket status, worker connectivity, restart count, and absence of new database-lock warnings. A separate `sqlite3` CLI connection cannot prove the application's connection-local busy timeout, so that setting is verified by the real-adapter test and startup DSN inspection.

## Rollback

Rollback uses the pre-deployment database/config backup and the previous Docker image digest. WAL sidecar files must remain with the database during rollback; the service will be stopped before copying database files.

# Cluster SQLite Resilience Design

## Context

The Hybrid node on `192.168.1.182` was configured correctly but lost its coordinator after a transient SQLite failure. The pure-Go SQLite build opened the database with mattn-style `_journal` and `_vacuum` query parameters, so the intended WAL and per-connection tuning was not expressed for that driver. Under concurrent subscription and cluster writes, lease renewal failed, fencing cleared the coordinator service, and downstream requests reported `Cluster coordinator is disabled`.

After WAL was made effective, the ETF auto worker also exposed a second SQLite-specific failure: `database is locked (517)`. Code 517 is `SQLITE_BUSY_SNAPSHOT`. A deferred transaction in `closeBatch` read a WAL snapshot, another connection committed a heartbeat or lease write, and the original transaction then failed immediately when it tried to upgrade the stale snapshot to a writer. `busy_timeout` cannot repair a stale snapshot, so transaction locking also has to be made explicit.

## Goals

- Make the SQLite configuration effective for both supported SQLite drivers.
- Prevent deferred read-to-write transaction upgrades from producing `SQLITE_BUSY_SNAPSHOT`.
- Let a coordinator survive transient SQLite write contention without violating its 45-second lease.
- Remove coordinator-service and runtime-generation races during fencing and restart.
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

Both adapters also set `_txlock=immediate`. Both underlying drivers support this option and issue `BEGIN IMMEDIATE` for non-read-only transactions. SQLite permits only one writer, so taking the writer reservation before the first transaction read converts an unsafe snapshot upgrade into bounded serialization at transaction entry. Competing writers wait under the 5000 ms busy timeout while WAL readers remain concurrent.

`InitDB` will pass the database path to that adapter instead of embedding driver-specific options in shared bootstrap code. This also applies to ordinary Workers.

### Coordinator lease renewal

The lease loop will distinguish an ownership loss from transient database contention:

- `RowsAffected == 0` with no database error still means the lease is no longer owned and fences immediately.
- A database error before the current 45-second lease deadline logs a warning and retries on the next 15-second tick.
- Repeated errors reaching the known lease deadline fence the coordinator.
- The renewal SQL context is bounded by the current lease deadline.
- Completion at or after the current deadline fences even if the SQL statement returned success.
- A successful renewal advances the local deadline to the exact timestamp written to the database.

This preserves split-brain safety while preventing a single `SQLITE_BUSY` from disabling the coordinator.

Each runtime start captures an immutable lease owner and generation. An old goroutine cannot renew or fence a newer runtime. Normal stop orders the final renewal before the lease-release update, and fencing waits for Hybrid worker background loops before closing Redis.

### Manifest processor synchronization

The processor will snapshot `coordinatorService` under the runtime read lock before invoking it. Fencing may clear the runtime field, but the current tick retains a valid service reference and observes the canceled context instead of dereferencing nil. Hub callbacks capture the service and context created by their own runtime generation instead of reading mutable runtime fields after fencing.

## Verification

- Open a real pure-Go SQLite database and assert `journal_mode=wal` and `busy_timeout=5000`.
- Run the same real-adapter assertions with `sqlite_cgo_compat`.
- Use two real SQLite connections to prove that a concurrent writer waits for an immediate transaction; the old deferred configuration reproduces code 517/default-driver lock failure.
- Unit-test lease decisions for transient error, expired error, lost ownership, and successful renewal.
- Test success completing at/after the lease deadline, stale-generation fencing rejection, stop/renewal ordering, worker shutdown ordering, and the processor snapshot path.
- Run focused tests, package tests, `go test -race` for the cluster package, and the repository's available lint/static checks.
- After backing up the remote database, deploy and verify persistent WAL mode, Hybrid WebSocket status, worker connectivity, restart count, multiple heartbeat/lease intervals, and absence of new database-lock warnings. A separate `sqlite3` CLI connection cannot prove the application's connection-local busy timeout or transaction mode. Those settings are verified by both real-adapter tests, the final binary string, and exact image provenance.

## Implementation and Verification Record

Final source/build provenance:

- Git HEAD: `33596f448d6e8533aeb9aa33dad37a2c340e7be3`
- Source-content hash: `771bb07f4b980c33bd886b87169f592e45b073c5b651a78e3993e3d436dbc192`
- Frontend/public dist hash: `2c62ba666b4427c5ab8c31b37775303a4d41d3ce2325809efa09ce4643360b66`
- Build: Go 1.26.4, `CGO_ENABLED=0`, `GOOS=linux`, `GOARCH=amd64`, `-tags=jsoniter`
- Previous image ID used as the exact runtime base: `sha256:477d449e09518199b1ae321f72695e348b5d1eaca8f22a20c602c48deb38dceb`
- Final tag: `entergtang/openlist-etf:sqlite-resilience-33596f44-771bb07f4b98`
- Final image ID: `sha256:8a84d2b4e00da2d4ac1052d57ac00d1c8899694b7f46481589e697e54f849f5e`
- Binary SHA-256: `2728c6d62d5ce01af3de2b4f5bf304715531dd5b03c2a9b5c34d934b6f032852`
- Image archive SHA-256: `677592fc3254cc0e2b6d01977b1e99510e312eb6a0b18ff3a410d688b9fa8401`

Local verification passed for the default and `sqlite_cgo_compat` bootstrap adapters, cluster package, cluster race detector, affected packages, pure-Go production tags, `go vet`, production Linux/amd64 build, `gofmt`, and `git diff --check`. `go test ./...` was also executed, but the repository-wide command is not fully green because of pre-existing/environmental failures: Go 1.26 non-constant format vet diagnostics, missing macOS `fuse.h`, the Codex network transport wrapper affecting `internal/net`, and aria2 tests requiring a service at localhost:6800. All packages affected by this change passed.

Remote deployment evidence:

- A first WAL/busy-timeout candidate proved that lease and heartbeat renewal survived contention, but ETF logs still reproduced the pre-existing code 517 pattern. It was rolled back immediately. Its failed state is retained under `/volume1/docker_dir/openlist_etf/.config-backups/20260715-232907-cluster-sqlite-resilience/failed-candidate-20260715-233355/`.
- The final pre-deployment backup is `/volume1/docker_dir/openlist_etf/.config-backups/20260715-234943-cluster-sqlite-resilience-immediate/`; its verification copy returned `PRAGMA quick_check = ok`.
- The main Compose file was not modified. `/volume1/docker_dir/openlist_etf/docker-compose.sqlite-resilience.override.yaml` pins the immutable final tag. Resolved current/candidate Compose hashes were identical after removing only the service image field.
- `/ping` returned HTTP 200 and `/api/cluster/ws` returned HTTP 400 rather than the disabled 404 path.
- Six samples from `2026-07-15T15:51:57Z` through `15:53:17Z` retained one container ID, restart count 0, stable lease owner, `journal_mode=wal`, strictly advancing coordinator lease, advancing node heartbeats, and connected latest sessions for both observed nodes.
- The Redis container ID, start time, restart count 0, and image remained unchanged across both deployments and rollback.
- Post-deployment application and container scans returned zero lines for `SQLITE_BUSY`, `database is locked`, nested transaction, lease lost/expired, coordinator disabled, panic, and fatal patterns.
- Sanitized evidence is retained in the final backup's `post-deploy/` directory; secrets and full resolved environment output are not copied into this document.

## Rollback

Rollback uses the pre-deployment database/config backup and the immutable previous image ID/tag. Stop only `openlist-etf`, preserve a failed-candidate snapshot, and move the current `data.db`, `data.db-wal`, `data.db-shm`, and `data.db-journal` set out of the live directory before restoring the same-batch set. If a sidecar did not exist in the backup, it must not remain after restore. Restore `config.json` and any used Compose override/.env from the same backup, then start the old image with `--no-deps --pull never`. Never overwrite a running database or combine an old main database with candidate sidecars. Redis remains running throughout rollback.

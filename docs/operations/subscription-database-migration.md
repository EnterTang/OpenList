# Subscription Database Performance and PostgreSQL Cutover Runbook

This runbook applies to the coordinator deployment that currently stores OpenList state in SQLite. It assumes the query fix, retention tests, PostgreSQL compatibility tests, and migration validation report have passed.

## Before the change

1. Confirm that no other process writes the SQLite files.
2. Record the current OpenList image/build, database file sizes, WAL size, event/inbox counts by status, list endpoint p50/p95/p99, CPU, and recent database-lock errors.
3. Confirm that the PostgreSQL instance has sufficient storage for the SQLite database plus indexes and at least 2x expected daily growth.
4. Create a private backup directory with restricted permissions. Do not put credentials, tokens, or cookies in the backup manifest.
5. Run the migration command in dry-run mode against the source snapshot and target PostgreSQL database.

Example:

```bash
go run ./cmd/migrate_database \
  -source /path/to/data.db \
  -target "$OPENLIST_POSTGRES_DSN" \
  -dry-run \
  -sample-size 100
```

## Stop-the-world snapshot

Stop OpenList and every local writer first. Copy `data.db`, `data.db-wal`, and `data.db-shm` together while the process is stopped. If SQLite was cleanly checkpointed and the `-wal` or `-shm` files do not exist, record that fact in the migration report; never mix files from different snapshots.

After copying, calculate a checksum for each file and run the migration from the verified copy. Keep the original snapshot unchanged until the PostgreSQL canary has passed.

## Migration and validation

Run the complete migration into an empty PostgreSQL database:

```bash
go run ./cmd/migrate_database \
  -source /verified/snapshot/data.db \
  -target "$OPENLIST_POSTGRES_DSN" \
  -batch-size 500 \
  -sample-size 100
```

The command must exit non-zero on any row-count, primary-key range, status/date-bucket, or sample-hash mismatch. Save its JSON output with the migration change record. For large tables, rerunning the same command is safe because inserts use conflict-ignore semantics and validation prevents silently accepting a partial or conflicting target.

If the migration is interrupted, rerun it with the same source snapshot and target DSN. Do not start OpenList against a partially migrated target until validation succeeds.

## PostgreSQL application configuration

Set the database configuration through the existing database fields:

```json
{
  "database": {
    "type": "postgres",
    "host": "postgres-host",
    "port": 5432,
    "user": "openlist",
    "name": "openlist",
    "ssl_mode": "require",
    "max_open_conns": 20,
    "max_idle_conns": 10,
    "conn_max_lifetime_minutes": 30
  }
}
```

Prefer a DSN or secret-injection mechanism that does not write the password to shell history or source control.

## Canary checks

Start one canary instance against PostgreSQL and verify:

- login and `/api/admin/subscription/list?page=1&per_page=20`;
- search, source/status/archive filters, and pagination;
- subscription detail and run-history endpoints;
- Telegram event enqueue, claim, retry, and completion;
- coordinator lease, worker acknowledgement, job dispatch, and job result;
- retention dry-run report and latest-event results;
- no PostgreSQL errors, duplicate-key errors, sequence errors, or lock timeouts.

Run `scripts/verify_subscription_database_rollout.sh` with an externally supplied URL and token. Never commit the token or copy its value into logs.

## Retention rollout

The default retention scheduler runs only on the coordinator and uses bounded batches. Enable it in dry-run mode first in the deployment configuration. Compare eligible counts with the expected terminal status distribution. Then enable deletion with a small batch and monitor CPU, IO, database locks, and endpoint p95. Increase the batch only after the service remains responsive.

The initial defaults are 7 days for terminal Telegram events and 14 days for processed cluster inbox records. Change these only after confirming the Telegram catch-up/deduplication window and cluster replay window for the deployment.

## Rollback

Rollback is a database endpoint rollback, not a reverse replication operation:

1. Stop the canary and all PostgreSQL writers.
2. Restore the pre-cutover SQLite configuration and verified snapshot.
3. Start one SQLite canary and run the same smoke checks.
4. Route traffic back only after the SQLite canary is healthy.
5. Preserve the PostgreSQL database and migration report for investigation; do not delete it during the incident window.

After rollback, any writes made to PostgreSQL after the cutover must be treated as a separate fork. Do not merge them back into SQLite without an explicit reconciliation plan.

## Post-cutover monitoring

For the first 24 hours record list endpoint latency, request errors, PostgreSQL active/idle connections, lock waits, transaction latency, event/inbox counts by status, retention deletion counts, and the repeated `staging failed` rate. Keep the SQLite snapshot until the observation window and backup policy both allow its removal.

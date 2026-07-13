# OpenList Cluster (Phases 1-3)

OpenList can run as `standalone`, `coordinator`, `worker`, or `hybrid`.
The cluster runtime provides authenticated WSS node connections, inventory
reporting, automatic subscription fan-out, durable result and cleanup queues,
Coordinator ETF materialization/notification, and an encrypted configuration
control plane.

## Coordinator configuration

```json
{
  "cluster": {
    "role": "coordinator",
    "node_id": "coordinator-1",
    "enrollment_token": "replace-with-a-long-random-secret",
    "websocket_path": "/api/cluster/ws",
    "etf_root_path": "/mobile/ETF",
    "target_base_url": "https://target.example/api/v1",
    "target_api_token": "replace-with-target-token",
    "target_supports_idempotency": true
  }
}
```

Set `CLUSTER_SECRET_MASTER_KEY` to a random 32-byte value encoded as hex or
base64. It is environment-only and encrypts storage credentials at rest;
losing it makes stored credentials unrecoverable.

The public Worker URL must use `wss://` unless the Coordinator is reached via
`localhost`, `127.0.0.1`, or `::1`. Run TLS on OpenList or terminate TLS at a
trusted reverse proxy. A Coordinator refuses to start without an enrollment
token and ETF root path.

Only one Coordinator may own a shared database. A renewable database lease
stops the subscription and ETF schedulers if another process owns the control
plane.

## Worker configuration

```json
{
  "cluster": {
    "role": "worker",
    "node_id": "worker-shanghai-1",
    "coordinator_url": "wss://coordinator.example/api/cluster/ws",
    "enrollment_token": "same-enrollment-token",
    "worker_key_file": "/data/openlist/cluster/worker-shanghai-1.x25519.key",
    "redis": {
      "address": "redis:6379",
      "result_stream": "cluster:upload-results:v1",
      "cleanup_stream": "cluster:local-cleanup:v1",
      "dead_letter_stream": "cluster:upload-results:dlq:v1",
      "consumer_group": "openlist-cluster-reporter",
      "require_aof": true
    }
  }
}
```

Redis must use persistent storage with:

```text
appendonly yes
appendfsync always
maxmemory-policy noeviction
```

The Worker refuses new uploads when Redis durability is unhealthy or an older
media cleanup is still pending. A 139 mount used by cluster jobs must enable
`cluster_dedicated_account`; cleanup may empty that account's recycle bin, so
the account must not contain unrelated data.

Keep `worker_key_file` on persistent storage. It is created with mode `0600`
beneath a `0700` directory. The Coordinator pins its X25519 public key on first
enrollment and rejects later connections that replace the key.

## Windows embedded Redis

Local Windows AMD64 artifacts built by `scripts/build-release-local.sh` with
the `windows-amd64`, `amd64`, or `all` target embed Redis while keeping the
distribution ZIP to one file: `openlist.exe`. On Windows, a worker or hybrid
configured with a loopback Redis address and exactly a blank or `default` ACL
username automatically extracts the versioned runtime beneath
`<data>/runtime/redis/7.2.14`. Persistent AOF data, the generated authentication
secret, and logs remain beneath `<data>/redis`.

External, remote, and custom-ACL Redis configurations remain authoritative.
The manager also reuses an existing compatible Redis service when it satisfies
the configured authentication and durability checks. A Redis process started
by OpenList binds only to loopback with protected mode enabled, uses
`appendonly yes`, `appendfsync always`, and `maxmemory-policy noeviction`, and
receives a generated password when none is configured. That generated password
is not written to `config.json`. OpenList gracefully stops only a Redis child it
owns (with a bounded forced-stop fallback); reused services are left running.

The embedded runtime is the Redis 7.2.14 community Windows MSYS2 port. Its
extracted runtime includes the BSD Redis and Apache Windows-port license
notices. For production or other high-assurance deployments, prefer an
external Redis on Linux or a vendor-supported platform. Windows ARM64,
32-bit, and legacy Windows support are not promised.

## Admin APIs

All endpoints are under `/api/admin/cluster` and require administrator auth.

- `GET /config` (secrets are returned only as `*_configured` flags)
- `POST /config` (persists runtime configuration and returns
  `restart_required: true`; empty secret fields preserve their current values,
  while `clear_enrollment_token`, `clear_target_api_token`, and Redis
  `clear_password` explicitly remove them; saving restricts `config.json` to
  mode `0600` because it may contain these credentials)
- `GET /nodes`
- `POST /nodes/:id/inventory/query`
- `POST /nodes/:id/state` (`online`, `draining`, `disabled`, `revoked`)
- `POST /nodes/:id/config`
- `GET /jobs`
- `POST /jobs/dispatch`
- `POST /jobs/dispatch_batch`
- `POST /jobs/:id/retry`
- `POST /jobs/clear_failed`
- `GET /results`
- `GET /result_queue/stats`
- `GET|POST /secrets`
- `POST /secrets/:id/revoke`
- `GET|POST /storage-profiles`
- `GET /audit`

Changing the local role, Coordinator URL, WebSocket path, identity, Redis, or
notification settings does not hot-switch the running process. Restart the
OpenList process or container after saving. Environment variables still take
precedence over `config.json`, so a value supplied with `OPENLIST_CLUSTER_*`
must be changed at the deployment layer rather than in the admin UI.

Uncertain target notifications can be inspected through
`GET /api/admin/etf_auto/jobs?status=unknown` and explicitly retried with
`POST /api/admin/etf_auto/jobs/:id/retry_unknown` after an operator verifies
the target service state.

A subscription first creates a metadata-only `share.inspect` job. A compatible
Worker reads the share with its local provider account and returns a sorted,
sealed manifest without saving or downloading files. The Coordinator diffs the
manifest and creates one `media.transfer` child per media work unit. The batch
scheduler distributes children by assigned bytes and capacity. Configure the
same logical target binding name (for example `mobile-primary`) on each Worker;
the Worker resolves that binding to its own physical mount path, so retries can
move between locations whose OpenList paths differ.

Different Telegram message IDs may inspect the same share URL again. Canonical
provider/share/file identity and the content fingerprint suppress unchanged
objects, while new or changed objects create transfer jobs. A Telegram cursor
advances only after all inspect jobs for that message are durably persisted.

## Delivery and cleanup guarantees

- Coordinator commands are persisted in an outbox before transmission.
- Worker execution attempts are journaled in Redis before external side effects.
- Leases renew only after Coordinator ACK; NACK or disconnect cancels work.
- Saving and mobile upload require generation-bound stage permits.
- ETF metadata and exact-path cleanup requests are written atomically to Redis.
- Media cleanup happens immediately and retries across restarts without
  repeating the upload.
- Target requests include `Idempotency-Key`. Network/5xx outcomes become
  `unknown` for manual reconciliation unless the configured target explicitly
  declares idempotency support; only then are uncertain responses retried with
  the same key.

Secrets are AES-256-GCM encrypted in the Coordinator database and re-encrypted
for one pinned Worker with X25519/HKDF/AES-GCM. Plaintext credentials never
enter inventory, task context, audit detail, or API responses. Desired Worker
configuration and storage profiles use monotonically increasing revisions;
observed state and hashes are persisted locally before the Worker replies, so
replayed control commands remain idempotent after restart.

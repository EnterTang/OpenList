# Windows Embedded Redis Design

## Goal

Produce a Windows AMD64 OpenList release whose archive contains only
`openlist.exe`, while allowing a `worker` or `hybrid` node to start with a
durable local Redis service without requiring Docker, WSL, a Windows service,
or a separately installed Redis package.

The executable may extract runtime files and persistent data beneath the
configured OpenList data directory after it starts.

## Scope

This design applies to Windows AMD64 artifacts built by
`scripts/build-release-local.sh`. Existing Linux, macOS, Docker, official
release, remote Redis, coordinator-only, and standalone behavior must remain
unchanged.

## Selected Redis Distribution

The local build downloads the MSYS2 archive from the
`redis-windows/redis-windows` Redis 7.2.14 release. The build pins both the URL
and SHA-256 digest. Redis 7.2 provides the Streams, consumer groups,
transactions, `XAUTOCLAIM`, and AOF configuration used by the cluster worker,
while remaining under the Redis BSD 3-Clause license.

Only the server executable and its required runtime DLLs are embedded. The
Redis and Windows-port license notices are included in the embedded bundle and
extracted beside the runtime files.

The embedded payload is a generated build input. It is downloaded and
assembled by the local release script and is not committed to the repository.

## Build Architecture

For the `windows-amd64` target, `scripts/build-release-local.sh` will:

1. Download the pinned Redis archive into a temporary build directory.
2. Verify its SHA-256 digest before reading it.
3. Extract the required Redis files and add the applicable license notices.
4. Create a deterministic embedded payload in the ignored generated-assets
   directory used by the Windows Go build.
5. Run the existing frontend and backend build pipeline.
6. Remove the generated payload on exit, including failed builds.
7. Verify that the produced Windows ZIP contains only `openlist.exe`.

Non-Windows targets do not download or embed Redis. A normal Windows build that
does not use the local release script remains valid but reports that an
embedded Redis payload is unavailable if automatic startup is needed.

## Runtime Activation

OpenList considers the embedded Redis manager only when all of these conditions
are true:

- The operating system is Windows.
- The cluster role is `worker` or `hybrid`.
- The configured Redis endpoint is local loopback (`127.0.0.1`, `localhost`,
  or `::1`).
- No usable Redis is already available through the configured endpoint and
  credentials.
- The executable contains a valid embedded Redis payload.

Remote Redis configuration always wins. If a local Redis is already reachable
and passes the worker's existing durability checks, OpenList reuses it and does
not extract or start another process.

## Files and Persistence

Runtime binaries are versioned beneath:

```text
<data>/runtime/redis/7.2.14/
```

Persistent Redis state is stored beneath:

```text
<data>/redis/
```

Extraction is atomic. OpenList writes into a temporary directory, verifies the
expected files, and renames it into place. A version marker and payload digest
allow later starts to reuse a complete extraction and replace an incomplete or
mismatched extraction.

The generated Redis configuration, log, authentication secret, and AOF files
remain under the OpenList data directory. Upgrading OpenList must not delete the
Redis data directory.

## Redis Configuration and Security

The managed Redis instance:

- Binds only to `127.0.0.1`.
- Enables protected mode.
- Uses a cryptographically random password persisted with restrictive file
  permissions where Windows permits them.
- Disables daemonization so OpenList can supervise the child process.
- Enables `appendonly yes`.
- Uses `appendfsync always`.
- Uses `maxmemory-policy noeviction`.
- Places its working directory, AOF, log, and PID information beneath
  `<data>/redis/`.

The generated password is applied to the in-memory Redis client configuration;
it is not copied into the user's `config.json`. Explicit remote or custom Redis
credentials are not modified.

## Startup and Conflict Handling

Before starting a child process, OpenList probes the configured endpoint:

- A compatible durable Redis is reused.
- A previously managed Redis that is still running is authenticated with the
  persisted secret and reused.
- A Redis instance that responds but fails required durability checks produces
  a clear error; OpenList does not replace or reconfigure it.
- A non-Redis listener or an occupied port produces a clear error; OpenList
  never terminates an unknown process.
- If the port is free, OpenList starts the extracted Redis process and waits
  for authenticated `PING` and the existing durability validation before
  starting the worker runtime.

Startup has a bounded timeout. Redis process exit, timeout, malformed assets,
unsafe archive paths, missing DLLs, and configuration failures are surfaced as
cluster startup errors with actionable paths and causes.

## Shutdown and Recovery

The cluster runtime owns the embedded Redis manager. Shutdown order is:

1. Stop worker message processing and background reporters.
2. Close normal worker Redis clients.
3. Request an authenticated Redis `SHUTDOWN`.
4. Wait for bounded graceful process exit.
5. Terminate only the child process created by the current OpenList process if
   graceful shutdown times out.

OpenList never kills a reused external Redis process. After an OpenList crash,
the next start may reuse a surviving managed Redis using the persisted secret,
or start a new process from the intact AOF data if no listener remains.

## Component Boundaries

The implementation introduces an internal embedded-Redis package with these
separate responsibilities:

- Embedded asset provider: Windows payload access and non-Windows stub.
- Extractor: safe, atomic, versioned runtime extraction.
- Configuration generator: deterministic Redis configuration.
- Endpoint classifier and probe: decide whether automatic management is
  allowed and distinguish reusable Redis from conflicts.
- Process supervisor: start, readiness-check, and stop only owned processes.
- Cluster integration: acquire the effective Redis options before constructing
  the existing result queue, then release the manager during cluster shutdown.

The existing result queue remains Redis-backed and keeps its current Streams
and durability validation behavior.

## Testing

Implementation follows test-first development. Automated tests cover:

- Loopback and remote endpoint classification.
- Activation for worker/hybrid roles and exclusion for other roles/platforms.
- Redis configuration generation and required durability/security settings.
- Safe extraction, traversal rejection, digest/version reuse, and incomplete
  extraction replacement.
- Existing durable Redis reuse.
- Managed Redis password injection without mutating persisted configuration.
- Port conflicts and incompatible Redis errors.
- Readiness timeout and early child-process exit.
- Owned-process shutdown versus reused-process preservation.
- Build helper digest rejection and generated-asset cleanup.

Verification includes Go unit tests, shell syntax checks, Windows cross-build,
inspection that the release ZIP contains only `openlist.exe`, and—when a
Windows execution environment is available—an end-to-end start, Redis
durability query, restart, and AOF persistence check.

## Non-goals

- Supporting Windows ARM64 or 32-bit Redis.
- Installing Redis as a Windows service.
- Exposing the managed Redis outside loopback.
- Replacing Redis Streams with an embedded Go database.
- Automatically modifying or terminating user-managed Redis instances.
- Changing coordinator or standalone runtime requirements.

## Known Risk

The selected Redis build is a community Windows port; its own documentation
recommends Linux for production deployments. Binding to loopback, using
authentication, pinning the artifact digest, retaining AOF durability checks,
and preserving support for an explicitly configured external Redis limit this
risk. Operators requiring vendor-supported Redis should configure a remote
Redis deployment, which bypasses the embedded manager entirely.

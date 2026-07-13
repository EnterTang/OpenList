# Windows Embedded Redis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Windows AMD64 `openlist.exe` that embeds, extracts, starts, validates, and shuts down a durable loopback Redis service for worker and hybrid roles.

**Architecture:** Add a focused `internal/embeddedredis` package for activation decisions, safe payload extraction, Redis configuration, probing, and owned-process supervision. The cluster runtime asks this package for effective Redis connection settings before creating its existing queue client. The local Windows release script prepares a pinned Redis 7.2.14 payload immediately before invoking the existing build pipeline and removes generated assets afterward.

**Tech Stack:** Go 1.26, `go:embed`, `os/exec`, `archive/zip`, `go-redis/v9`, Bash, Redis 7.2.14 Windows MSYS2 distribution.

---

## File Structure

- Create `internal/embeddedredis/options.go`: activation rules, options, effective connection result, constants.
- Create `internal/embeddedredis/options_test.go`: loopback, role, credential, and configuration tests.
- Create `internal/embeddedredis/extract.go`: safe atomic payload extraction and version/digest markers.
- Create `internal/embeddedredis/extract_test.go`: extraction, traversal, reuse, and replacement tests.
- Create `internal/embeddedredis/payload_windows.go`: Windows-only embedded generated asset provider.
- Create `internal/embeddedredis/payload_other.go`: non-Windows unavailable provider.
- Create `internal/embeddedredis/assets/generated/.gitkeep`: keeps the embed directory valid without committing a payload.
- Create `internal/embeddedredis/manager.go`: probe, start, readiness, effective credentials, and shutdown orchestration.
- Create `internal/embeddedredis/manager_test.go`: fake process/probe coverage for reuse, conflicts, startup, and shutdown ownership.
- Modify `internal/cluster/runtime.go`: acquire and release the embedded Redis manager around existing worker clients.
- Create `scripts/prepare-embedded-redis.sh`: pinned download, SHA-256 verification, minimal payload assembly, notices, and cleanup.
- Create `scripts/prepare-embedded-redis-test.sh`: shell regression tests for digest verification and payload contents.
- Modify `scripts/build-release-local.sh`: prepare embedded Redis only for Windows AMD64, clean it on exit, and inspect the final archive.
- Modify `.gitignore`: ignore generated embedded Redis payloads while retaining `.gitkeep`.
- Modify `docs/cluster.md`: document automatic Windows Redis behavior, paths, override rules, and the community-port risk.

### Task 1: Lock Activation and Configuration Behavior

**Files:**
- Create: `internal/embeddedredis/options.go`
- Create: `internal/embeddedredis/options_test.go`

- [x] **Step 1: Write failing tests for activation decisions**

Add table-driven tests that require management only for Windows worker/hybrid roles using loopback endpoints. Include `127.0.0.1:6379`, `localhost:6380`, `[::1]:6379`, remote IPs, malformed addresses, standalone/coordinator roles, non-Windows platforms, and custom ACL usernames.

```go
func TestShouldManage(t *testing.T) {
	tests := []struct {
		name, goos, role, address, username string
		want                             bool
	}{
		{name: "windows worker loopback", goos: "windows", role: "worker", address: "127.0.0.1:6379", want: true},
		{name: "windows hybrid localhost", goos: "windows", role: "hybrid", address: "localhost:6380", want: true},
		{name: "windows ipv6 loopback", goos: "windows", role: "worker", address: "[::1]:6379", want: true},
		{name: "remote redis", goos: "windows", role: "worker", address: "10.0.0.5:6379", want: false},
		{name: "custom ACL", goos: "windows", role: "worker", address: "127.0.0.1:6379", username: "worker", want: false},
		{name: "linux", goos: "linux", role: "worker", address: "127.0.0.1:6379", want: false},
		{name: "coordinator", goos: "windows", role: "coordinator", address: "127.0.0.1:6379", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ShouldManage(tt.goos, Options{Role: tt.role, Address: tt.address, Username: tt.username}))
		})
	}
}
```

- [x] **Step 2: Run the test and verify RED**

Run: `go test ./internal/embeddedredis -run 'TestShouldManage'`

Expected: FAIL because the package and `ShouldManage` do not exist.

- [x] **Step 3: Implement activation types and loopback parsing**

Define `Version = "7.2.14"`, `PayloadFilename = "redis-windows.zip"`, `Options`, `EffectiveOptions`, and `ShouldManage`. Parse endpoints with `net.SplitHostPort`, normalize the role, require `worker` or `hybrid`, require Windows, accept only loopback hostnames/addresses, and leave custom ACL usernames under user management.

- [x] **Step 4: Write failing tests for generated Redis configuration**

Assert the configuration contains the selected port, loopback binding, protected mode, foreground mode, password, forward-slash data paths, AOF requirements, and no-eviction policy. Assert quotes and newlines in a password are rejected rather than emitted into the configuration.

- [x] **Step 5: Run the configuration test and verify RED**

Run: `go test ./internal/embeddedredis -run 'TestRenderConfig'`

Expected: FAIL because `RenderConfig` is missing.

- [x] **Step 6: Implement minimal deterministic configuration rendering**

Add `RenderConfig(port int, dataDir, password string) ([]byte, error)` using explicit Redis directives:

```text
bind 127.0.0.1
protected-mode yes
port <port>
daemonize no
dir "<forward-slash-path>"
appendonly yes
appendfsync always
maxmemory-policy noeviction
requirepass "<escaped-password>"
```

- [x] **Step 7: Run package tests and verify GREEN**

Run: `go test ./internal/embeddedredis`

Expected: PASS.

- [x] **Step 8: Commit activation and configuration behavior**

Commit the two files with a Conventional Commit title and Lore trailers. Record `go test ./internal/embeddedredis` in `Tested:`.

### Task 2: Add Safe Versioned Payload Extraction

**Files:**
- Create: `internal/embeddedredis/extract.go`
- Create: `internal/embeddedredis/extract_test.go`
- Create: `internal/embeddedredis/payload_windows.go`
- Create: `internal/embeddedredis/payload_other.go`
- Create: `internal/embeddedredis/assets/generated/.gitkeep`
- Modify: `.gitignore`

- [x] **Step 1: Write failing extraction tests**

Build ZIP fixtures in memory. Require `ExtractPayload` to extract only expected basenames, reject `../escape`, reject missing `redis-server.exe` or DLLs, write a digest marker, reuse an intact matching directory, and atomically replace an incomplete directory.

```go
required := []string{
	"redis-server.exe",
	"msys-2.0.dll",
	"msys-crypto-3.dll",
	"msys-gcc_s-seh-1.dll",
	"msys-ssl-3.dll",
	"msys-stdc++-6.dll",
	"COPYING.redis",
	"LICENSE.redis-windows",
}
```

- [x] **Step 2: Run extraction tests and verify RED**

Run: `go test ./internal/embeddedredis -run 'TestExtractPayload'`

Expected: FAIL because extraction is not implemented.

- [x] **Step 3: Implement safe atomic extraction**

Calculate SHA-256 from the embedded ZIP, use `<data>/runtime/redis/7.2.14`, reject absolute paths and path traversal, allow only the required file set, write into a sibling temporary directory, verify every required file, write `.payload-sha256`, then rename into place. Preserve `<data>/redis` separately.

- [x] **Step 4: Add platform-specific payload providers**

Use a Windows build-tagged file with:

```go
//go:embed all:assets/generated
var generatedAssets embed.FS
```

Read `assets/generated/redis-windows.zip`; return unavailable when only `.gitkeep` exists. The non-Windows provider always reports unavailable without embedding generated assets.

- [x] **Step 5: Ignore generated payloads**

Add:

```gitignore
/internal/embeddedredis/assets/generated/*
!/internal/embeddedredis/assets/generated/.gitkeep
```

- [x] **Step 6: Run extraction and package tests**

Run: `go test ./internal/embeddedredis`

Expected: PASS.

- [x] **Step 7: Verify both platform providers compile**

Run:

```bash
go test ./internal/embeddedredis
GOOS=windows GOARCH=amd64 go test -c -o /tmp/embeddedredis-windows.test.exe ./internal/embeddedredis
```

Expected: both commands exit 0.

- [x] **Step 8: Commit extraction and embedded asset plumbing**

Commit the extraction, provider, `.gitkeep`, and ignore changes with test evidence.

### Task 3: Implement Redis Probe and Owned Process Supervision

**Files:**
- Create: `internal/embeddedredis/manager.go`
- Create: `internal/embeddedredis/manager_test.go`

- [x] **Step 1: Write failing tests for existing Redis reuse and conflicts**

Inject a probe function and assert:

- A durable Redis using configured credentials returns unchanged effective options and `Owned() == false`.
- A surviving managed Redis using the persisted secret is reused without spawning.
- A reachable Redis that fails AOF/no-eviction validation returns a durability error.
- An occupied non-Redis port returns a conflict error and does not spawn or kill anything.
- Remote/custom ACL configurations bypass the manager.

- [x] **Step 2: Run reuse tests and verify RED**

Run: `go test ./internal/embeddedredis -run 'TestPrepare.*Reuse|TestPrepare.*Conflict'`

Expected: FAIL because `Prepare` and `Manager` are missing.

- [x] **Step 3: Implement probes and effective options**

Use short bounded timeouts. Probe with configured credentials first, then the persisted managed secret. Validate `PING`, `appendonly=yes`, `appendfsync=always`, and `maxmemory-policy=noeviction` when `RequireAOF` is true. Treat successful TCP connection plus failed Redis protocol/authentication as a conflict unless the persisted managed secret succeeds.

- [x] **Step 4: Write failing tests for managed startup**

Inject asset, extraction, secret generation, process start, readiness probe, clock, and timeout seams. Assert a free local endpoint causes extraction, secure secret persistence, config creation, one child start, readiness wait, password injection into returned options, and `Owned() == true`. Cover early exit and readiness timeout.

- [x] **Step 5: Run startup tests and verify RED**

Run: `go test ./internal/embeddedredis -run 'TestPrepare.*Start|TestPrepare.*Timeout|TestPrepare.*Exit'`

Expected: FAIL because startup is incomplete.

- [x] **Step 6: Implement minimal startup supervision**

Start `<runtime>/redis-server.exe redis.conf` with `Cmd.Dir` set to the versioned runtime directory so adjacent MSYS2 DLLs resolve. Store stdout/stderr in `<data>/redis/redis.log`. Wait until authenticated readiness and durability succeed or the process exits/timeout occurs.

- [x] **Step 7: Write failing tests for shutdown ownership**

Require owned managers to send authenticated `SHUTDOWN`, wait for exit, and kill only their own child after timeout. Require reused managers to leave the Redis process untouched.

- [x] **Step 8: Implement bounded shutdown**

Accept Redis connection-close errors caused by successful `SHUTDOWN`, wait for the child, and call `Process.Kill` only on an owned child that exceeds the timeout.

- [x] **Step 9: Run manager tests and race detection**

Run:

```bash
go test ./internal/embeddedredis
go test -race ./internal/embeddedredis
```

Expected: PASS with no race reports.

- [x] **Step 10: Commit manager behavior**

Commit supervisor files with reuse/start/stop verification in the commit trailers.

### Task 4: Integrate the Manager into Cluster Runtime

**Files:**
- Modify: `internal/cluster/runtime.go`
- Create or modify: `internal/cluster/runtime_test.go`

- [x] **Step 1: Write a failing runtime lifecycle test**

Add an injectable embedded Redis preparation hook on `Runtime`. Assert worker startup uses returned address/username/password/DB without mutating `conf.Conf.Cluster.Redis`, and `stopLocked` closes the queue client before stopping the manager. Cover startup failure cleanup.

- [x] **Step 2: Run the lifecycle test and verify RED**

Run: `go test ./internal/cluster -run 'TestRuntime.*EmbeddedRedis'`

Expected: FAIL because the runtime has no manager hook or field.

- [x] **Step 3: Integrate effective Redis options**

Before `redis.NewClient`, call the embedded manager with the resolved `flags.DataDir`, role, configured address, credentials, DB, and `RequireAOF`. Store the manager on `Runtime`, construct the existing client from returned effective settings, and leave `conf.Conf` unchanged.

- [x] **Step 4: Centralize Redis cleanup order**

Add a locked helper used by normal stop and coordinator-fence cleanup. Close the normal Redis client first, then stop the embedded manager with a bounded context, log shutdown errors, and nil both fields. Never stop a reused Redis service.

- [x] **Step 5: Run focused cluster tests**

Run:

```bash
go test ./internal/cluster/... 
go test ./internal/conf/...
```

Expected: PASS.

- [x] **Step 6: Commit cluster integration**

Commit the runtime lifecycle changes and tests.

### Task 5: Prepare the Pinned Redis Build Payload

**Files:**
- Create: `scripts/prepare-embedded-redis.sh`
- Create: `scripts/prepare-embedded-redis-test.sh`
- Modify: `scripts/build-release-local.sh`

- [x] **Step 1: Write failing shell tests**

Source the helper behind a main guard. Create temporary ZIP fixtures and assert `verify_sha256` rejects a wrong digest, `assemble_payload` refuses missing required files, and a valid fixture produces `internal/embeddedredis/assets/generated/redis-windows.zip` containing exactly the server, five DLLs, and two license files.

- [x] **Step 2: Run shell tests and verify RED**

Run: `bash scripts/prepare-embedded-redis-test.sh`

Expected: FAIL because the helper does not exist.

- [x] **Step 3: Implement pinned download and payload assembly**

Pin:

```text
URL=https://github.com/redis-windows/redis-windows/releases/download/7.2.14/Redis-7.2.14-Windows-x64-msys2.zip
SHA256=b31d0f867608017f0b0962624d55a4c569a745587ad4b08f7fe9eea59d6916c1
```

Use `curl`, `unzip`, `zip`, and a portable SHA-256 helper (`sha256sum` or `shasum -a 256`). Download Redis BSD `COPYING` and the Windows port Apache `LICENSE`, then build a minimal `zip -X` payload. Provide `prepare` and `clean` commands.

- [x] **Step 4: Integrate the helper into local Windows builds**

For `windows-amd64`, run `prepare` before the backend build and register an EXIT trap that runs `clean`. Do not prepare Redis for other targets. After the build, inspect `build/compress/openlist-windows-amd64.zip` and fail unless it contains exactly one regular file named `openlist.exe`.

- [x] **Step 5: Run shell tests and syntax checks**

Run:

```bash
bash scripts/prepare-embedded-redis-test.sh
bash -n scripts/prepare-embedded-redis.sh
bash -n scripts/build-release-local.sh
```

Expected: PASS.

- [x] **Step 6: Run a real preparation and inspect the payload**

Run:

```bash
bash scripts/prepare-embedded-redis.sh prepare
unzip -l internal/embeddedredis/assets/generated/redis-windows.zip
bash scripts/prepare-embedded-redis.sh clean
```

Expected: checksum verification succeeds; the archive contains only the required eight files; cleanup removes the generated ZIP.

- [x] **Step 7: Commit build preparation**

Commit helper, tests, and local release integration with the verified URL and digest recorded in the body or trailers.

### Task 6: Documentation and End-to-End Verification

**Files:**
- Modify: `docs/cluster.md`
- Modify: `docs/superpowers/plans/2026-07-13-windows-embedded-redis.md` (mark completed checkboxes during execution)

- [x] **Step 1: Document Windows automatic Redis behavior**

Explain that the local Windows build embeds Redis, activation is limited to worker/hybrid loopback configuration, remote/custom Redis remains authoritative, runtime/data paths are separate, and the bundled community Windows port is not the recommended vendor-supported production deployment.

- [ ] **Step 2: Run formatting and static verification**

Run:

```bash
gofmt -w internal/embeddedredis/*.go internal/cluster/runtime.go
go vet ./internal/embeddedredis ./internal/cluster/...
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 3: Run focused and repository tests**

Run:

```bash
go test ./internal/embeddedredis ./internal/cluster/... ./internal/conf/...
go test ./cmd/... ./internal/...
```

Expected: PASS. If unrelated pre-existing failures occur, capture exact packages and errors without claiming they pass.

- [ ] **Step 4: Verify Windows cross-compilation with a real payload**

Run preparation, then build the Windows package using the available Zig or Docker+xgo path. At minimum run:

```bash
bash scripts/prepare-embedded-redis.sh prepare
GOOS=windows GOARCH=amd64 go test -c -o /tmp/embeddedredis-windows.test.exe ./internal/embeddedredis
bash scripts/prepare-embedded-redis.sh clean
```

For the full artifact, run `scripts/build-release-local.sh --target windows-amd64` with an available frontend build or `--skip-frontend-build` when its `dist/` is already present.

- [ ] **Step 5: Inspect the final archive and embedded payload evidence**

Run:

```bash
unzip -l build/compress/openlist-windows-amd64.zip
```

Expected: exactly one file, `openlist.exe`. Record the executable size increase and archive SHA-256.

- [ ] **Step 6: Run Windows runtime verification when available**

Start a worker/hybrid node from only `openlist.exe`, verify extraction paths, query `CONFIG GET appendonly appendfsync maxmemory-policy`, enqueue data, stop OpenList, restart it, and confirm the AOF-backed stream remains. If no Windows execution environment exists, report this explicitly as the remaining verification gap.

- [ ] **Step 7: Finalize verification state and commit it**

After fresh verification evidence is collected, mark only the completed verification steps, record any remaining gaps, and commit the final plan state.

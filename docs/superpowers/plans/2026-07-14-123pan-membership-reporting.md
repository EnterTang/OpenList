# 123Pan Membership Reporting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose 123Pan membership tier, active status, and expiration date through storage details and cluster provider-account inventory using the driver's existing signed user-info request.

**Architecture:** Add an optional reusable membership value to `model.StorageDetails`, then let the 123Pan driver populate that value from its existing user-info response and retain a mutex-protected runtime copy. Cluster inventory consumes the runtime copy through a narrow reporter interface while preserving explicit configured tier overrides and the existing tier-only fallback for other drivers.

**Tech Stack:** Go, Resty, standard `encoding/json`, standard `net/http` test transports, existing OpenList storage-details cache and cluster inventory protocol.

## Global Constraints

- Keep using `https://yun.123pan.com/b/api/user/info`; do not introduce the newer host.
- Reuse the current bearer token, dynamic request signature, `App-Version: 3`, and `platform: web` behavior.
- Do not add credentials, dependencies, database migrations, or configuration writes.
- Treat upstream `Vip` as authoritative for active/inactive state; do not compare the date with the local clock.
- Preserve existing storage-details disk-usage JSON and explicit membership-tier overrides.
- Do not create implementation commits until the repository co-author hook conflict is resolved.

---

### Task 1: Add Optional Membership Data to Shared Models

**Files:**
- Modify: `internal/model/storage.go`
- Create: `internal/model/storage_test.go`
- Modify: `internal/cluster/protocol/payloads.go`

**Interfaces:**
- Produces: `model.MembershipDetails{Tier, Status, ExpireDate string}`.
- Produces: `model.StorageDetails.Membership *model.MembershipDetails`.
- Produces: `protocol.ProviderAccountInventory.MembershipStatus string` and `MembershipExpireDate string`.

- [x] **Step 1: Write failing storage-details JSON tests**

Add tests that marshal `StorageDetails` with and without membership. Assert the no-membership JSON equals:

```json
{"free_space":60,"total_space":100,"used_space":40}
```

Assert the membership case additionally contains:

```json
"membership":{"tier":"vip","status":"active","expire_date":"2040-01-31"}
```

- [x] **Step 2: Run the model tests and verify RED**

Run: `go test ./internal/model -run 'TestStorageDetailsMarshalJSON' -count=1`

Expected: compilation fails because `MembershipDetails` and `StorageDetails.Membership` do not exist.

- [x] **Step 3: Implement the shared membership model and explicit JSON marshaling**

Add:

```go
type MembershipDetails struct {
	Tier       string `json:"tier,omitempty"`
	Status     string `json:"status,omitempty"`
	ExpireDate string `json:"expire_date,omitempty"`
}

type StorageDetails struct {
	DiskUsage
	Membership *MembershipDetails `json:"membership,omitempty"`
}
```

Define `StorageDetails.MarshalJSON` with an alias structure that emits `total_space`, `used_space`, `free_space`, and optional `membership`. This avoids the embedded `DiskUsage.MarshalJSON` method swallowing the new field.

Add to `ProviderAccountInventory`:

```go
MembershipStatus     string `json:"membership_status,omitempty"`
MembershipExpireDate string `json:"membership_expire_date,omitempty"`
```

- [x] **Step 4: Format and verify GREEN**

Run: `gofmt -w internal/model/storage.go internal/model/storage_test.go internal/cluster/protocol/payloads.go`

Run: `go test ./internal/model ./internal/cluster/protocol -count=1`

Expected: both packages pass and the legacy JSON assertion proves compatibility.

---

### Task 2: Parse and Retain 123Pan Runtime Membership

**Files:**
- Modify: `drivers/123/types.go`
- Modify: `drivers/123/driver.go`
- Create: `drivers/123/membership_test.go`

**Interfaces:**
- Consumes: `model.MembershipDetails` from Task 1.
- Produces: `membershipDetailsFromUserInfo(*UserInfoResp) model.MembershipDetails`.
- Produces: `(*Pan123).ClusterMembershipDetails() model.MembershipDetails`.
- Produces: `(*Pan123).ClusterMembershipTier() string`.

- [x] **Step 1: Write failing normalization and driver-detail tests**

Cover these mappings:

```text
Vip=false              -> ordinary, inactive
Vip=true, VipLevel=1   -> vip, active
Vip=true, VipLevel=2   -> svip, active
```

Use a test Resty transport that returns one successful `/b/api/user/info` JSON response containing capacity plus `Vip`, `VipLevel`, and `VipExpire`. Assert `GetDetails` performs one request, sends `Bearer test-token`, and returns both disk usage and membership.

Add an initialization test asserting the same response makes `ClusterMembershipTier()` return the runtime tier when configured as `unknown`. Add a configured-tier assertion proving `MembershipTier: "vip"` overrides runtime `svip`, while status and expiration remain runtime values.

- [x] **Step 2: Run the 123Pan tests and verify RED**

Run: `go test ./drivers/123 -run 'Test(123Membership|Pan123)' -count=1`

Expected: compilation fails because membership response fields and reporter methods are missing.

- [x] **Step 3: Implement response fields and synchronized runtime state**

Extend `UserInfoResp.Data` with:

```go
Vip       bool   `json:"Vip"`
VipLevel  int    `json:"VipLevel"`
VipExpire string `json:"VipExpire"`
```

Add a read/write mutex and `model.MembershipDetails` runtime value to `Pan123`. Implement the conservative mapping defined by the spec, a setter that stores a value copy, and reporter methods that return value copies. `ClusterMembershipDetails` applies only the explicit configured tier override; it preserves runtime status and expiration.

Update `Init` to decode its existing user-info request and seed runtime membership. Update `GetDetails` to derive membership from the same response, refresh runtime state, and return a copied membership pointer alongside disk usage.

- [x] **Step 4: Format and verify GREEN**

Run: `gofmt -w drivers/123/types.go drivers/123/driver.go drivers/123/membership_test.go`

Run: `go test ./drivers/123 -count=1`

Expected: all driver tests pass without a network request.

---

### Task 3: Add Membership Status and Expiration to Cluster Inventory

**Files:**
- Modify: `internal/cluster/worker/provider_inventory.go`
- Modify: `internal/cluster/worker/inventory_test.go`

**Interfaces:**
- Consumes: drivers implementing `ClusterMembershipDetails() model.MembershipDetails`.
- Preserves: drivers implementing only `ClusterMembershipTier() string`.
- Populates: `ProviderAccountInventory.MembershipTier`, `MembershipWeight`, `MembershipStatus`, and `MembershipExpireDate`.

- [x] **Step 1: Write failing inventory merge tests**

Add a table test around a pure `applyRuntimeMembership` helper:

```text
configured unknown + runtime svip -> tier svip, weight 300, active, 2040-01-31
configured vip + runtime svip      -> tier vip, weight 200, active, 2040-01-31
```

Also JSON-marshal the resulting account and assert the two new snake-case fields are present.

- [x] **Step 2: Run worker tests and verify RED**

Run: `go test ./internal/cluster/worker -run 'TestApplyRuntimeMembership' -count=1`

Expected: compilation fails because `applyRuntimeMembership` and the protocol fields do not exist in the current implementation.

- [x] **Step 3: Implement runtime membership merging**

Add the narrow reporter interface:

```go
type clusterMembershipDetailsReporter interface {
	ClusterMembershipDetails() model.MembershipDetails
}
```

Implement `applyRuntimeMembership` so it always copies runtime status and expiration, but replaces tier and recalculates weight only when the current tier is empty or `unknown`. In `defaultHydrateInventoryStorage`, prefer the details reporter and fall back to the existing tier-only reporter using an `else if` branch.

- [x] **Step 4: Format and verify GREEN**

Run: `gofmt -w internal/cluster/worker/provider_inventory.go internal/cluster/worker/inventory_test.go`

Run: `go test ./internal/cluster/worker ./internal/cluster/protocol -count=1`

Expected: worker and protocol tests pass, including configured-tier compatibility.

---

### Task 4: Repository Verification

**Files:**
- Verify only; do not modify unrelated files.

**Interfaces:**
- Verifies all interfaces introduced by Tasks 1-3 together.

- [x] **Step 1: Run focused race-enabled tests**

Run: `go test -race ./internal/model ./drivers/123 ./internal/cluster/protocol ./internal/cluster/worker -count=1`

Expected: all packages pass with no race reports.

- [x] **Step 2: Run repository tests**

Run: `go test ./... -count=1`

Expected: all packages pass. If an unrelated environment-dependent package fails, record the exact package and output and keep the focused proof.

- [x] **Step 3: Run static analysis and diff checks**

Run: `go vet ./internal/model ./drivers/123 ./internal/cluster/protocol ./internal/cluster/worker`

Run: `git diff --check`

Expected: both commands exit successfully.

- [x] **Step 4: Review the final scoped diff**

Run: `git diff -- internal/model/storage.go internal/model/storage_test.go drivers/123/types.go drivers/123/driver.go drivers/123/membership_test.go internal/cluster/protocol/payloads.go internal/cluster/worker/provider_inventory.go internal/cluster/worker/inventory_test.go`

Confirm every production change is covered by a failing-then-passing test and no credentials, generated files, or unrelated refactors are present.

## Verification Result

- Focused race-enabled tests passed for the model, 123Pan driver, cluster
  protocol, and cluster worker packages.
- Targeted subscription account-selection tests passed.
- Targeted `go vet` and `git diff --check` passed.
- `go test ./... -count=1` was attempted but remains blocked by pre-existing
  repository and environment failures outside this change: missing `fuse.h`,
  legacy non-constant formatting diagnostics, tests requiring a local Aria2
  service, an existing OSS proxy transport assertion, and an unrelated
  Telegram subscription-source count assertion.

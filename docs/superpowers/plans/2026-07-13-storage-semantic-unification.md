# Storage Semantic Unification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace subscription and cluster path-based storage semantics with provider-plus-folder semantics across backend and frontend, and add provider account capability-aware cluster scheduling.

**Architecture:** Introduce a backend storage-target model and shared resolver that converts `provider + folder` into a concrete local storage/account selection at runtime. Extend cluster inventory from mount-only reporting to provider-account capability pools, then update scheduler/admin APIs and the frontend subscription/cluster screens to consume the new model and stop exposing raw OpenList mount paths as user-facing configuration.

**Tech Stack:** Go 1.26.4, GORM, Gin, SolidJS, TypeScript, Hope UI, pnpm, Vite

## Global Constraints

- User-facing subscription and cluster configuration must not require OpenList absolute paths such as `/123` or `/139_60t`.
- All modes (`standalone`, `hybrid`, `worker`) must share the same provider-plus-folder semantics.
- Multi-account same-provider selection must use account capability pools rather than path disambiguation.
- `pan123` and `pan115` first version only use membership weight as a soft scheduling factor; no real-time throughput measurement.
- `yidong139` scheduling must enforce single-file upload limits as hard constraints: ordinary `5G`, silver `8G`, gold `20G`, diamond `500G`.
- Controller UI must display worker provider/account capability details for operational visibility.
- Frontend and backend changes ship together; the frontend must stop sending legacy path semantics.
- Legacy path values may be migrated once conservatively, but must not remain the primary UX.

---

## File Structure

### Backend files to modify

- `internal/model/subscription.go`
  - Replace or supplement `target_root`/`temp_transfer_root` semantics with explicit provider-target structs.
- `internal/subscription/config.go`
  - Normalize new config fields and conservative migration from legacy path values.
- `internal/subscription/naming.go`
  - Build logical media names from resolved targets rather than directly from user path input.
- `internal/subscription/service.go`
  - Feed new target model into transfer flow and item path persistence.
- `internal/subscription/share_runtime.go`
  - Stop treating temp roots as raw OpenList paths; delegate to resolver.
- `internal/subscription/share_save.go`
  - Save into resolved provider account folder rather than configured path strings.
- `internal/subscription/cluster_share.go`
  - Worker-side cluster save path resolution from provider-plus-folder request.
- `internal/subscription/cluster_dispatch.go`
  - Carry provider target requirements in cluster task payloads.
- `internal/cluster/protocol/payloads.go`
  - Add provider account inventory payloads and provider target requirements to task context.
- `internal/cluster/worker/inventory.go`
  - Report provider account capability pools instead of only mount-level data.
- `internal/cluster/runtime.go`
  - Update worker eligibility and routing to consume provider-account capabilities.
- `internal/cluster/subscription_dispatcher.go`
  - Route by required source/target providers, file size, and capability constraints.
- `internal/cluster/admin_config.go`
  - Remove or de-emphasize `etf_root_path` as a required user-facing semantic.

### Backend files to create

- `internal/subscription/storage_target.go`
  - Shared storage target types and validation.
- `internal/subscription/storage_target_test.go`
  - Unit tests for migration and validation.
- `internal/subscription/target_resolver.go`
  - Local resolver for provider account selection and folder ensuring.
- `internal/subscription/target_resolver_test.go`
  - Resolver selection tests and 139 upload-limit tests.
- `internal/cluster/provider_inventory.go`
  - Shared helpers for provider capability enrichment and sorting.
- `internal/cluster/provider_inventory_test.go`
  - Provider-account capability and weight mapping tests.

### Frontend files to modify

- `src/types/subscription.ts`
  - Replace `target_root` and path-root config fields with typed provider-target fields.
- `src/types/cluster.ts`
  - Add provider account inventory types and new runtime config fields.
- `src/utils/api.ts`
  - Update subscription/cluster API payload types.
- `src/pages/home/SubscriptionManagement.tsx`
  - Replace path inputs with provider selectors and folder inputs.
- `src/pages/manage/cluster/Nodes.tsx`
  - Render provider account capability pools per node.
- `src/pages/manage/cluster/Settings.tsx`
  - Remove legacy path-oriented cluster settings and align with new semantics.
- `src/lang/en/subscription.json`
- `src/lang/zh-CN/subscription.json`
- `src/lang/zh-TW/subscription.json`
- `src/lang/en/cluster.json`
- `src/lang-overrides/zh-CN/cluster.json`
- `src/lang-overrides/zh-TW/cluster.json`
  - Update copy to describe provider-plus-folder inputs and provider capability displays.

### Existing test files to extend

- `internal/subscription/config_test.go`
- `internal/subscription/naming_test.go`
- `internal/subscription/cluster_share_test.go`
- `internal/subscription/cluster_dispatch_test.go`
- `internal/subscription/share_runtime_test.go`
- `internal/cluster/worker/control_test.go`
- `internal/cluster/protocol/control_test.go`
- `internal/cluster/coordinator/materializer_test.go`

## Task 1: Backend Storage Target Model And Legacy Migration

**Files:**
- Create: `internal/subscription/storage_target.go`
- Test: `internal/subscription/storage_target_test.go`
- Modify: `internal/model/subscription.go`
- Modify: `internal/subscription/config.go`
- Test: `internal/subscription/config_test.go`

**Interfaces:**
- Consumes: `model.Subscription`, `model.SubscriptionConfig`, `model.SubscriptionTelegramPanConfig`
- Produces:
  - `type SubscriptionStorageTarget struct { Provider string; Folder string }`
  - `func NormalizeSubscriptionStorageTarget(target model.SubscriptionStorageTarget) model.SubscriptionStorageTarget`
  - `func MigrateLegacyPathTarget(raw string) (model.SubscriptionStorageTarget, bool)`
  - `func NormalizeSubscriptionConfigTargets(cfg model.SubscriptionConfig) model.SubscriptionConfig`

- [ ] **Step 1: Write the failing backend tests**

```go
func TestMigrateLegacyPathTarget(t *testing.T) {
	got, ok := MigrateLegacyPathTarget("/123/转存至移动")
	if !ok {
		t.Fatal("expected migration to succeed")
	}
	if got.Provider != "pan123" || got.Folder != "转存至移动" {
		t.Fatalf("target = %#v", got)
	}
}

func TestNormalizeSubscriptionStorageTargetRejectsPathLikeFolder(t *testing.T) {
	got := NormalizeSubscriptionStorageTarget(model.SubscriptionStorageTarget{
		Provider: "pan123",
		Folder:   "/123/转存至移动",
	})
	if got.Folder == "/123/转存至移动" {
		t.Fatal("expected path semantics to be stripped or rejected")
	}
}
```

- [ ] **Step 2: Run the targeted tests to verify they fail**

Run: `go test ./internal/subscription -run 'TestMigrateLegacyPathTarget|TestNormalizeSubscriptionStorageTargetRejectsPathLikeFolder' -v`

Expected: FAIL with undefined type/function errors for `SubscriptionStorageTarget`, `MigrateLegacyPathTarget`, or `NormalizeSubscriptionStorageTarget`.

- [ ] **Step 3: Add the minimal model and config implementation**

```go
type SubscriptionStorageTarget struct {
	Provider string `json:"provider"`
	Folder   string `json:"folder"`
}

type Subscription struct {
	// keep existing fields during migration window
	TargetRoot      string                    `json:"target_root"`
	TempTarget      SubscriptionStorageTarget `json:"temp_target" gorm:"-"`
	DeliveryTarget  SubscriptionStorageTarget `json:"delivery_target" gorm:"-"`
}

func MigrateLegacyPathTarget(raw string) (model.SubscriptionStorageTarget, bool) {
	cleaned := cleanConfigPath(raw)
	switch {
	case strings.HasPrefix(cleaned, "/123/"):
		return model.SubscriptionStorageTarget{Provider: "pan123", Folder: strings.TrimPrefix(cleaned, "/123/")}, true
	case strings.HasPrefix(cleaned, "/139_60t/"):
		return model.SubscriptionStorageTarget{Provider: "yidong139", Folder: strings.TrimPrefix(cleaned, "/139_60t/")}, true
	default:
		return model.SubscriptionStorageTarget{}, false
	}
}
```

- [ ] **Step 4: Re-run backend config tests**

Run: `go test ./internal/subscription -run 'TestMigrateLegacyPathTarget|TestNormalizeSubscriptionStorageTargetRejectsPathLikeFolder|TestNormalizeConfigPreservesDefaultTargetRootOnly' -v`

Expected: PASS for new migration tests and updated config tests.

- [ ] **Step 5: Commit**

```bash
git add internal/model/subscription.go internal/subscription/config.go internal/subscription/config_test.go internal/subscription/storage_target.go internal/subscription/storage_target_test.go
git commit -m "feat(subscription): add provider target model"
```

## Task 2: Resolver Layer For Provider Account Selection And Folder Ensuring

**Files:**
- Create: `internal/subscription/target_resolver.go`
- Test: `internal/subscription/target_resolver_test.go`
- Modify: `internal/subscription/share_runtime.go`
- Modify: `internal/subscription/share_save.go`
- Modify: `internal/subscription/service.go`
- Modify: `internal/subscription/naming.go`
- Test: `internal/subscription/share_runtime_test.go`
- Test: `internal/subscription/naming_test.go`

**Interfaces:**
- Consumes: `model.SubscriptionStorageTarget`, enabled storages from `db.GetEnabledStorages()`
- Produces:
  - `type ResolveProviderTargetRequest struct { Provider string; Folder string; NeedUpload bool; NeedShareSave bool; FileSize int64 }`
  - `type ResolvedProviderTarget struct { Provider string; StorageID uint; MountPath string; Folder string; FullPath string; MembershipTier string; MembershipWeight int; MaxSingleUploadBytes int64 }`
  - `func ResolveProviderTarget(ctx context.Context, req ResolveProviderTargetRequest) (ResolvedProviderTarget, error)`
  - `func EnsureResolvedProviderFolder(ctx context.Context, target ResolvedProviderTarget) (ResolvedProviderTarget, error)`

- [ ] **Step 1: Write failing resolver tests**

```go
func TestResolveProviderTargetPrefersHigherMembershipWeight(t *testing.T) {
	resolved, err := ResolveProviderTarget(context.Background(), ResolveProviderTargetRequest{
		Provider:      "pan123",
		Folder:        "转存至移动",
		NeedShareSave: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.StorageID != 2 {
		t.Fatalf("storage id = %d, want 2", resolved.StorageID)
	}
}

func TestResolveProviderTargetRejects139OversizeUpload(t *testing.T) {
	_, err := ResolveProviderTarget(context.Background(), ResolveProviderTargetRequest{
		Provider:   "yidong139",
		Folder:     "剧集",
		NeedUpload: true,
		FileSize:   9 << 30,
	})
	if err == nil {
		t.Fatal("expected upload limit rejection")
	}
}
```

- [ ] **Step 2: Run resolver tests to verify failure**

Run: `go test ./internal/subscription -run 'TestResolveProviderTargetPrefersHigherMembershipWeight|TestResolveProviderTargetRejects139OversizeUpload' -v`

Expected: FAIL with undefined resolver types/functions.

- [ ] **Step 3: Implement resolver and wire subscription runtime to it**

```go
type ResolveProviderTargetRequest struct {
	Provider      string
	Folder        string
	NeedUpload    bool
	NeedShareSave bool
	FileSize      int64
}

type ResolvedProviderTarget struct {
	Provider             string
	StorageID            uint
	MountPath            string
	Folder               string
	FullPath             string
	MembershipTier       string
	MembershipWeight     int
	MaxSingleUploadBytes int64
}

func ResolveProviderTarget(ctx context.Context, req ResolveProviderTargetRequest) (ResolvedProviderTarget, error) {
	candidates := listProviderAccountCandidates(req.Provider)
	eligible := filterProviderAccountCandidates(candidates, req)
	sortProviderAccountCandidates(eligible)
	if len(eligible) == 0 {
		return ResolvedProviderTarget{}, errors.New("no compatible provider account")
	}
	return eligible[0], nil
}
```

Update share/runtime callers so `TempTransferRoot` becomes a resolved provider target path at runtime instead of a persistent user-entered absolute path.

- [ ] **Step 4: Run focused subscription tests**

Run: `go test ./internal/subscription -run 'TestResolveProviderTarget|TestPlanTarget|TestSaveClusterShareSelection' -v`

Expected: PASS for resolver tests plus any updated naming/share tests.

- [ ] **Step 5: Commit**

```bash
git add internal/subscription/target_resolver.go internal/subscription/target_resolver_test.go internal/subscription/share_runtime.go internal/subscription/share_save.go internal/subscription/service.go internal/subscription/naming.go internal/subscription/share_runtime_test.go internal/subscription/naming_test.go
git commit -m "feat(subscription): resolve provider targets at runtime"
```

## Task 3: Cluster Protocol And Inventory Upgrade To Provider Account Pools

**Files:**
- Create: `internal/cluster/provider_inventory.go`
- Test: `internal/cluster/provider_inventory_test.go`
- Modify: `internal/cluster/protocol/payloads.go`
- Modify: `internal/cluster/worker/inventory.go`
- Modify: `internal/model/cluster.go`
- Modify: `internal/model/cluster_control.go`
- Test: `internal/cluster/protocol/control_test.go`
- Test: `internal/cluster/worker/control_test.go`

**Interfaces:**
- Consumes: storage records, `op.GetStorageDetails`, provider-specific membership metadata
- Produces:
  - `type ProviderAccountInventory struct { Provider string; StorageID uint; NodeMountID string; MountPath string; AccountAlias string; MembershipTier string; MembershipWeight int; MaxSingleUploadBytes int64; FreeBytes int64; TotalBytes int64; SupportsShareSave bool; SupportsDownload bool; SupportsUpload bool; SupportsETF bool; ActiveJobs int }`
  - `type InventoryReport struct { ... ProviderAccounts []ProviderAccountInventory }`
  - `func BuildProviderAccountInventory(ctx context.Context, storage model.Storage) (protocol.ProviderAccountInventory, error)`

- [ ] **Step 1: Write failing inventory tests**

```go
func TestBuildInventoryIncludesProviderAccounts(t *testing.T) {
	report, err := BuildInventory(context.Background(), "node-a", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.ProviderAccounts) == 0 {
		t.Fatal("expected provider accounts in inventory")
	}
}

func TestMobile139MembershipMapsUploadLimit(t *testing.T) {
	limit := mobile139MaxSingleUploadBytes("diamond")
	if limit != 500<<30 {
		t.Fatalf("limit = %d", limit)
	}
}
```

- [ ] **Step 2: Run cluster inventory tests to verify failure**

Run: `go test ./internal/cluster/... -run 'TestBuildInventoryIncludesProviderAccounts|TestMobile139MembershipMapsUploadLimit' -v`

Expected: FAIL because `ProviderAccounts` and mapping helpers do not exist yet.

- [ ] **Step 3: Implement protocol payload and worker inventory expansion**

```go
type ProviderAccountInventory struct {
	Provider             string `json:"provider"`
	StorageID            uint   `json:"storage_id"`
	NodeMountID          string `json:"node_mount_id"`
	MountPath            string `json:"mount_path"`
	AccountAlias         string `json:"account_alias,omitempty"`
	MembershipTier       string `json:"membership_tier,omitempty"`
	MembershipWeight     int    `json:"membership_weight,omitempty"`
	MaxSingleUploadBytes int64  `json:"max_single_upload_bytes,omitempty"`
	FreeBytes            int64  `json:"free_bytes,omitempty"`
	TotalBytes           int64  `json:"total_bytes,omitempty"`
	SupportsShareSave    bool   `json:"supports_share_save"`
	SupportsDownload     bool   `json:"supports_download"`
	SupportsUpload       bool   `json:"supports_upload"`
	SupportsETF          bool   `json:"supports_etf"`
	ActiveJobs           int    `json:"active_jobs,omitempty"`
}
```

Extend `BuildInventory` to populate both legacy `Mounts` and new `ProviderAccounts` during the transition.

- [ ] **Step 4: Run cluster protocol and worker tests**

Run: `go test ./internal/cluster/... -run 'TestBuildInventoryIncludesProviderAccounts|TestConfigApply|TestStorageApply' -v`

Expected: PASS, including updated JSON/protocol tests.

- [ ] **Step 5: Commit**

```bash
git add internal/cluster/provider_inventory.go internal/cluster/provider_inventory_test.go internal/cluster/protocol/payloads.go internal/cluster/worker/inventory.go internal/model/cluster.go internal/model/cluster_control.go internal/cluster/protocol/control_test.go internal/cluster/worker/control_test.go
git commit -m "feat(cluster): report provider account capability pools"
```

## Task 4: Cluster Dispatch And Admin Runtime Semantics

**Files:**
- Modify: `internal/cluster/runtime.go`
- Modify: `internal/cluster/subscription_dispatcher.go`
- Modify: `internal/subscription/cluster_dispatch.go`
- Modify: `internal/subscription/cluster_share.go`
- Modify: `internal/cluster/admin_config.go`
- Modify: `internal/cluster/coordinator/materializer.go`
- Test: `internal/subscription/cluster_dispatch_test.go`
- Test: `internal/subscription/cluster_share_test.go`
- Test: `internal/cluster/coordinator/materializer_test.go`
- Test: `internal/cluster/admin_config_test.go`

**Interfaces:**
- Consumes: `protocol.ProviderAccountInventory`, `ResolvedProviderTarget`
- Produces:
  - cluster task context with explicit source and delivery target requirements
  - eligibility checks based on provider account capabilities, file size, and required operations
  - admin config that no longer treats `etf_root_path` as mandatory user-facing storage semantics

- [ ] **Step 1: Write failing dispatch tests**

```go
func TestDispatchClusterItemsRequiresSourceProviderCapability(t *testing.T) {
	_, err := dispatchClusterItems(context.Background(), sub, items, ref, message)
	if err == nil {
		t.Fatal("expected dispatch to fail when no worker supports pan123")
	}
}

func TestNodeInventorySupportsRejectsMobile139Oversize(t *testing.T) {
	ok, err := nodeInventorySupports(context.Background(), "node-a", taskContext, []string{"mobile.upload"}, 9<<30)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected worker rejection for oversize upload")
	}
}
```

- [ ] **Step 2: Run targeted cluster dispatch tests**

Run: `go test ./internal/subscription ./internal/cluster -run 'TestDispatchClusterItemsRequiresSourceProviderCapability|TestNodeInventorySupportsRejectsMobile139Oversize' -v`

Expected: FAIL until dispatcher and eligibility code are updated.

- [ ] **Step 3: Implement provider-aware dispatch and admin semantics cleanup**

```go
type ClusterProviderTargetRequirement struct {
	Provider       string `json:"provider"`
	Folder         string `json:"folder"`
	NeedShareSave  bool   `json:"need_share_save,omitempty"`
	NeedUpload     bool   `json:"need_upload,omitempty"`
	RequiredBytes  int64  `json:"required_bytes,omitempty"`
}

func nodeInventorySupportsProviderTarget(accounts []protocol.ProviderAccountInventory, req ClusterProviderTargetRequirement) bool {
	for _, account := range accounts {
		if account.Provider != req.Provider {
			continue
		}
		if req.NeedUpload && account.MaxSingleUploadBytes > 0 && account.MaxSingleUploadBytes < req.RequiredBytes {
			continue
		}
		return true
	}
	return false
}
```

Also remove mandatory dependence on `etf_root_path` for subscription storage semantics; keep internal-only fallback only where truly unavoidable.

- [ ] **Step 4: Run backend dispatch and admin tests**

Run: `go test ./internal/subscription ./internal/cluster -run 'TestClusterDispatch|TestSaveClusterShareSelection|TestSaveAdminConfig' -v`

Expected: PASS with updated payload/context assertions.

- [ ] **Step 5: Commit**

```bash
git add internal/cluster/runtime.go internal/cluster/subscription_dispatcher.go internal/subscription/cluster_dispatch.go internal/subscription/cluster_share.go internal/cluster/admin_config.go internal/cluster/coordinator/materializer.go internal/subscription/cluster_dispatch_test.go internal/subscription/cluster_share_test.go internal/cluster/coordinator/materializer_test.go internal/cluster/admin_config_test.go
git commit -m "feat(cluster): dispatch by provider account capability"
```

## Task 5: Frontend Subscription And Cluster Settings Migration To Provider Targets

**Files:**
- Modify: `src/types/subscription.ts`
- Modify: `src/utils/api.ts`
- Modify: `src/pages/home/SubscriptionManagement.tsx`
- Modify: `src/pages/manage/cluster/Settings.tsx`
- Modify: `src/lang/en/subscription.json`
- Modify: `src/lang/zh-CN/subscription.json`
- Modify: `src/lang/zh-TW/subscription.json`
- Modify: `src/lang/en/cluster.json`

**Interfaces:**
- Consumes: backend subscription config/runtime config payloads
- Produces:
  - `SubscriptionStorageTarget` frontend type
  - subscription form that submits `temp_target` and `delivery_target`
  - cluster settings form that no longer encourages path semantics such as `etf_root_path`

- [ ] **Step 1: Introduce the new types and intentionally break consumers**

```ts
export interface SubscriptionStorageTarget {
  provider: "pan123" | "pan115" | "yidong139"
  folder: string
}

export interface Subscription {
  temp_target?: SubscriptionStorageTarget
  delivery_target?: SubscriptionStorageTarget
}
```

Then update the form code to reference `form().temp_target?.provider` and `form().delivery_target?.folder` before all helpers are wired.

- [ ] **Step 2: Run frontend type-check to verify failure**

Run: `pnpm lint`

Expected: FAIL in `SubscriptionManagement.tsx` and `Settings.tsx` because helper defaults and payload builders still reference legacy path fields.

- [ ] **Step 3: Implement the new form helpers and API payload shaping**

```ts
const emptyStorageTarget = (): SubscriptionStorageTarget => ({
  provider: "pan123",
  folder: "",
})

const normalizeStorageTarget = (target?: Partial<SubscriptionStorageTarget>) => ({
  provider: (target?.provider || "pan123") as SubscriptionStorageTarget["provider"],
  folder: (target?.folder || "").trim(),
})

const payload: Partial<Subscription> = {
  ...form(),
  temp_target: normalizeStorageTarget(form().temp_target),
  delivery_target: normalizeStorageTarget(form().delivery_target),
}
```

Update copy to say “盘内目录” and “不是 OpenList 挂载路径”.

- [ ] **Step 4: Run frontend verification**

Run: `pnpm lint && pnpm build`

Expected: PASS with the updated subscription and cluster settings forms.

- [ ] **Step 5: Commit**

```bash
git add src/types/subscription.ts src/utils/api.ts src/pages/home/SubscriptionManagement.tsx src/pages/manage/cluster/Settings.tsx src/lang/en/subscription.json src/lang/zh-CN/subscription.json src/lang/zh-TW/subscription.json src/lang/en/cluster.json
git commit -m "feat(frontend): switch subscription forms to provider targets"
```

## Task 6: Frontend Cluster Node Capability Pool Display

**Files:**
- Modify: `src/types/cluster.ts`
- Modify: `src/utils/api.ts`
- Modify: `src/pages/manage/cluster/Nodes.tsx`
- Modify: `src/pages/manage/cluster/components.tsx`
- Modify: `src/lang-overrides/zh-CN/cluster.json`
- Modify: `src/lang-overrides/zh-TW/cluster.json`

**Interfaces:**
- Consumes: `ProviderAccountInventory[]` returned in cluster node/inventory responses
- Produces:
  - per-node provider account capability UI
  - visible membership tier, free/total bytes, active jobs, and 139 single-file upload limits

- [ ] **Step 1: Reference new cluster types in the node page**

```ts
export interface ClusterProviderAccountInventory {
  provider: string
  storage_id: number
  account_alias?: string
  membership_tier?: string
  membership_weight?: number
  max_single_upload_bytes?: number
  free_bytes?: number
  total_bytes?: number
  supports_share_save: boolean
  supports_download: boolean
  supports_upload: boolean
  supports_etf: boolean
  active_jobs?: number
}
```

Update `Nodes.tsx` to render `node.provider_accounts` and let `pnpm lint` catch every missing adapter or display helper.

- [ ] **Step 2: Run frontend type-check to verify failure**

Run: `pnpm lint`

Expected: FAIL because cluster API types and rendering helpers do not yet expose provider account fields.

- [ ] **Step 3: Implement provider-account rendering**

```tsx
<For each={node.provider_accounts || []}>
  {(account) => (
    <Box borderWidth="1px" borderColor="$neutral5" rounded="$md" p="$3">
      <Text fontWeight="$semibold">{account.account_alias || account.provider}</Text>
      <Text size="sm">{account.membership_tier || "unknown"}</Text>
      <Text size="sm">{account.free_bytes} / {account.total_bytes}</Text>
      <Show when={account.provider === "yidong139"}>
        <Text size="sm">max upload: {account.max_single_upload_bytes}</Text>
      </Show>
    </Box>
  )}
</For>
```

Keep the current node overview table, but add a details section/card for provider capability pools.

- [ ] **Step 4: Run frontend verification**

Run: `pnpm lint && pnpm build`

Expected: PASS and node page renders provider account capability data without type errors.

- [ ] **Step 5: Commit**

```bash
git add src/types/cluster.ts src/utils/api.ts src/pages/manage/cluster/Nodes.tsx src/pages/manage/cluster/components.tsx src/lang-overrides/zh-CN/cluster.json src/lang-overrides/zh-TW/cluster.json
git commit -m "feat(cluster-ui): show provider account capability pools"
```

## Task 7: End-To-End Verification And Cleanup

**Files:**
- Modify: any files touched above as needed by review feedback
- Test: `internal/subscription/...`
- Test: `internal/cluster/...`
- Verify: `/Volumes/extend Disk/Github/OpenList-Frontend`

**Interfaces:**
- Consumes: all previous tasks
- Produces: verified backend + frontend feature set with legacy path entry points removed from primary UX

- [ ] **Step 1: Run the backend subscription and cluster suites**

Run: `go test ./internal/subscription ./internal/cluster/...`

Expected: PASS for new storage target, resolver, inventory, and dispatch behavior.

- [ ] **Step 2: Run frontend verification**

Run: `pnpm lint && pnpm build`

Expected: PASS for the updated subscription and cluster UI.

- [ ] **Step 3: Do a manual product walkthrough**

```text
1. Open subscription management.
2. Confirm the form offers provider selectors and folder inputs instead of OpenList paths.
3. Save a pan123 -> yidong139 subscription.
4. Open cluster nodes.
5. Confirm each node shows provider account capability pools.
6. Confirm yidong139 accounts show membership-derived upload limits.
```

Expected: No remaining primary UI paths instruct the user to enter `/123/...` or `/139_60t/...`.

- [ ] **Step 4: Remove any leftover legacy copy or dead adapters found during walkthrough**

```text
Clean up any remaining labels, placeholders, helper text, or API adapters that still describe target_root/temp_transfer_root as OpenList paths.
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(subscription): finalize provider target semantics"
```

## Self-Review

### Spec coverage check

- Provider-plus-folder semantics: covered by Tasks 1, 2, and 5.
- Shared behavior across `standalone`/`hybrid`/`worker`: covered by Tasks 2 and 4.
- Multi-account provider capability pools: covered by Tasks 3, 4, and 6.
- 139 upload hard limits: covered by Tasks 2, 3, and 4.
- Frontend + backend lockstep rollout: covered by Tasks 5, 6, and 7.
- Conservative one-time migration: covered by Task 1.

### Placeholder scan

- No `TODO`, `TBD`, or “similar to previous task” placeholders remain.
- Every task includes exact files, interfaces, verification commands, and commit commands.

### Type consistency check

- `SubscriptionStorageTarget` is the shared name for the new subscription target model in both backend and frontend tasks.
- `ProviderAccountInventory` / `ClusterProviderAccountInventory` carry consistent provider capability semantics.
- `ResolveProviderTargetRequest` / `ResolvedProviderTarget` are the runtime resolver interfaces used by later dispatch tasks.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-13-storage-semantic-unification.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**

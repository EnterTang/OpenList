# Cloudpan Share Subscription Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add first-class share-link directory parsing and share-link save/transfer support for Quark, Aliyun Drive, 123Pan, and 115Pan subscriptions.

**Architecture:** Add a subscription-local cloudpan provider layer instead of stretching the generic `driver.Driver` interface. Providers parse raw share URLs, list share trees, save matched share items into configured temporary roots, and return standard `TreeEntry` values so existing naming, detection, and `fs.Copy` transfer code stays reusable.

**Tech Stack:** Go, existing OpenList `internal/subscription` package, existing OpenList driver utilities where useful, `resty`/standard HTTP clients already present in the repo, GORM-backed subscription state.

## Global Constraints

- No new dependencies unless explicitly approved.
- Use TDD for every behavior change: write a failing test, verify failure, implement, verify pass.
- Keep old Telegram `channels` behavior compatible; grouped Quark/Aliyun/123/115 channels remain the provider routing source.
- Do not remove existing share drivers; this plan adds subscription share providers that can coexist with mount-style share drivers.
- Start with read-only parsing and dry-run preview; only then enable provider save operations.
- Save operations must be idempotent enough for repeated subscription checks: avoid duplicate target files where provider APIs expose existing destination state.
- Provider calls must respect context cancellation and avoid unbounded recursion.
- Any copied code from AGPL projects requires license review before direct inclusion. Prefer reimplementation from API behavior over copying code.

---

## Current State Summary

Current subscription behavior:

- `internal/subscription/telegram.go` extracts cloud links from Telegram rows and filters them by provider domain.
- `internal/subscription/service.go` records raw share links as skipped items with a message that provider transfer is required.
- `internal/subscription/tree.go` snapshots already-mounted OpenList paths, not raw share URLs.
- `runTelegramTempTransfers` scans configured temporary OpenList folders after some external process has already transferred the share.

Existing OpenList share drivers:

- `drivers/aliyundrive_share` can list Aliyun share contents and produce download links if configured with `refresh_token`, `share_id`, and optional `share_pwd`.
- `drivers/123_share` can list 123Pan share contents and produce download links, but write operations are unsupported.
- `drivers/115_share` can list 115 share contents and produce download links, but write operations are unsupported.
- There is no Quark share driver in this checkout; Quark account drivers are normal account mounts.

Reference projects:

- `Cp0204/quark-auto-save` has a Quark-only flow with URL extraction, stoken retrieval, share detail listing, save-file, mkdir, rename, and task polling.
- `OzoO0/cloud-auto-save-x` has a provider adapter shape for Quark, Aliyun, 123Pan, and 115Pan: `extract_url`, `get_stoken`, `get_detail`, `ls_dir`, `save_file`, `mkdir`, and `rename`.

## File Structure

Create:

- `internal/subscription/share_provider.go`
  - Provider interfaces, provider registry, common request/response structs.
- `internal/subscription/share_url.go`
  - Provider detection and URL parsing helpers shared by providers and tests.
- `internal/subscription/share_tree.go`
  - Generic recursive tree traversal over provider detail/list APIs, converting provider items to `TreeEntry`.
- `internal/subscription/share_save.go`
  - Provider save orchestration: ensure temp folder, save matching items, poll provider tasks where needed, return saved root path.
- `internal/subscription/share_quark.go`
  - Quark provider implementation.
- `internal/subscription/share_aliyun.go`
  - Aliyun Drive provider implementation.
- `internal/subscription/share_123.go`
  - 123Pan provider implementation.
- `internal/subscription/share_115.go`
  - 115Pan provider implementation.
- `internal/subscription/share_provider_test.go`
  - Interface-level registry and fake-provider tests.
- `internal/subscription/share_url_test.go`
  - URL parsing tests for the examples in the user request.
- `internal/subscription/share_tree_test.go`
  - Recursive listing and `TreeEntry` conversion tests using fake providers.
- `internal/subscription/share_save_test.go`
  - Save orchestration tests using fake providers.
- `internal/subscription/share_<provider>_test.go`
  - Provider-specific parsing/request-shape tests.

Modify:

- `internal/model/subscription.go`
  - Add provider account credentials/config fields under `SubscriptionTelegramPanConfig` or a new nested `SubscriptionCloudPanConfig`.
- `internal/subscription/config.go`
  - Normalize and merge new provider config fields.
- `internal/subscription/telegram.go`
  - Replace raw skipped link recording path with provider parse/list/save path when provider config is complete.
- `internal/subscription/service.go`
  - Add manual raw share-link processing through provider layer.
- `server/handles/subscription.go`
  - Optionally expose preview endpoint output fields for share-tree preview if the UI needs them.
- Frontend source repo, if available:
  - `src/types/subscription.ts`
  - `src/pages/home/SubscriptionManagement.tsx`
  - `src/utils/api.ts`

Do not modify in early tasks:

- Generic `driver.Driver` interface.
- Existing share drivers unless provider implementation deliberately reuses small internal helpers and tests justify the change.

---

### Task 1: Provider Interface And URL Parsing

**Files:**
- Create: `internal/subscription/share_provider.go`
- Create: `internal/subscription/share_url.go`
- Create: `internal/subscription/share_provider_test.go`
- Create: `internal/subscription/share_url_test.go`

**Interfaces:**
- Produces:
  - `type ShareProviderName string`
  - `const ShareProviderQuark ShareProviderName = "quark"` and equivalents for `aliyun_drive`, `pan123`, `pan115`
  - `type ShareRef struct { Provider ShareProviderName; RawURL string; ShareID string; Passcode string; ParentID string }`
  - `type ShareProvider interface { Name() ShareProviderName; ParseURL(raw string) (ShareRef, error) }`
  - `func DetectShareProvider(raw string) (ShareProviderName, bool)`
  - `func ParseShareURL(raw string) (ShareRef, error)`
- Consumes: none.

- [ ] **Step 1: Write failing URL parsing tests**

Add tests in `internal/subscription/share_url_test.go`:

```go
package subscription

import "testing"

func TestParseShareURLExamples(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		provider ShareProviderName
		shareID  string
		passcode string
	}{
		{name: "quark", raw: "https://pan.quark.cn/s/bc18e4ea5fb8", provider: ShareProviderQuark, shareID: "bc18e4ea5fb8"},
		{name: "aliyun", raw: "https://www.alipan.com/s/odeXVKsEKxr", provider: ShareProviderAliyunDrive, shareID: "odeXVKsEKxr"},
		{name: "123", raw: "https://www.123pan.com/s/7Tx1jv-pVu7v?pwd=xoxo#", provider: ShareProviderPan123, shareID: "7Tx1jv-pVu7v", passcode: "xoxo"},
		{name: "115", raw: "https://115cdn.com/s/swssal13zrk?password=t58d", provider: ShareProviderPan115, shareID: "swssal13zrk", passcode: "t58d"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref, err := ParseShareURL(tt.raw)
			if err != nil {
				t.Fatalf("parse share URL: %v", err)
			}
			if ref.Provider != tt.provider || ref.ShareID != tt.shareID || ref.Passcode != tt.passcode {
				t.Fatalf("ref = %#v, want provider=%s shareID=%s passcode=%s", ref, tt.provider, tt.shareID, tt.passcode)
			}
		})
	}
}

func TestParseShareURLRejectsUnknownHost(t *testing.T) {
	if _, err := ParseShareURL("https://example.com/s/not-pan"); err == nil {
		t.Fatal("expected unknown share URL error")
	}
}
```

- [ ] **Step 2: Run red test**

Run: `go test ./internal/subscription -run 'TestParseShareURL'`

Expected: build failure with undefined `ParseShareURL` and provider constants.

- [ ] **Step 3: Implement minimal provider constants and parser**

Implement `DetectShareProvider` using `net/url` host matching:

```go
func DetectShareProvider(raw string) (ShareProviderName, bool) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", false
	}
	host := strings.ToLower(parsed.Hostname())
	switch {
	case hostMatchesDomain(host, "pan.quark.cn"):
		return ShareProviderQuark, true
	case hostMatchesDomain(host, "alipan.com") || hostMatchesDomain(host, "aliyundrive.com"):
		return ShareProviderAliyunDrive, true
	case hostMatchesDomain(host, "123pan.com"):
		return ShareProviderPan123, true
	case hostMatchesDomain(host, "115cdn.com") || hostMatchesDomain(host, "115.com"):
		return ShareProviderPan115, true
	default:
		return "", false
	}
}
```

Implement `ParseShareURL` with provider-specific path extraction:

- Quark/Aliyun/123/115 all accept `/s/<shareID>`.
- 123 passcode is `pwd` query parameter.
- 115 passcode is `password`, fallback to `pwd`.
- Store fragment-derived parent directory later; Task 2 can extend `ParentID`.

- [ ] **Step 4: Run green test**

Run: `go test ./internal/subscription -run 'TestParseShareURL'`

Expected: PASS.

---

### Task 2: Generic Share Tree Traversal

**Files:**
- Create: `internal/subscription/share_tree.go`
- Create: `internal/subscription/share_tree_test.go`
- Modify: `internal/subscription/share_provider.go`

**Interfaces:**
- Consumes: `ShareRef`, `ShareProviderName`.
- Produces:
  - `type ShareItem struct { ID string; ParentID string; Name string; Size int64; Modified time.Time; IsDir bool; Raw any }`
  - `type ShareTreeLister interface { ShareProvider; ListShareChildren(ctx context.Context, ref ShareRef, parentID string) ([]ShareItem, error) }`
  - `func ListShareTree(ctx context.Context, provider ShareTreeLister, ref ShareRef) ([]TreeEntry, error)`

- [ ] **Step 1: Write failing traversal tests**

Test a fake provider returning:

- root children: folder `Season 1`, file `Movie.mkv`
- folder children: file `Show.S01E01.mkv`

Expected `TreeEntry` paths:

- `/Season 1`
- `/Season 1/Show.S01E01.mkv`
- `/Movie.mkv`

Also test context cancellation by returning `context.Canceled` from fake provider and verifying it propagates.

- [ ] **Step 2: Run red test**

Run: `go test ./internal/subscription -run 'TestListShareTree'`

Expected: undefined traversal types/functions.

- [ ] **Step 3: Implement traversal**

Implementation rules:

- Use depth-first recursion with `ctx.Err()` checked before each provider call.
- Root parent ID is `ref.ParentID`; if empty, use provider root convention `""`.
- Convert each `ShareItem` to `TreeEntry{RootPath: ref.RawURL, Path: clean relative path, ...}`.
- Limit recursion with `const maxShareTreeDepth = 64`; return error on overflow.

- [ ] **Step 4: Run green test**

Run: `go test ./internal/subscription -run 'TestListShareTree'`

Expected: PASS.

---

### Task 3: Save Orchestration Against A Fake Provider

**Files:**
- Create: `internal/subscription/share_save.go`
- Create: `internal/subscription/share_save_test.go`
- Modify: `internal/subscription/share_provider.go`

**Interfaces:**
- Consumes: `ShareRef`, `ShareItem`, `ListShareTree`.
- Produces:
  - `type ShareSaver interface { ShareTreeLister; EnsureDir(ctx context.Context, path string) (string, error); SaveShareItems(ctx context.Context, ref ShareRef, parentID string, items []ShareItem, dstDirID string) ([]string, error); WaitSaveComplete(ctx context.Context, taskIDs []string) error }`
  - `type SaveShareOptions struct { TempRoot string; Match func(TreeEntry) bool }`
  - `func SaveShareToTemp(ctx context.Context, provider ShareSaver, ref ShareRef, opts SaveShareOptions) ([]TreeEntry, error)`

- [ ] **Step 1: Write failing save orchestration tests**

Fake provider behavior:

- `EnsureDir("/tmp/quark")` returns `tmp-dir-id`.
- `ListShareChildren` returns two files and one folder.
- `Match` accepts only `.mkv` files.
- `SaveShareItems` records item IDs and returns task ID `task-1`.
- `WaitSaveComplete` records called task IDs.

Assert:

- only `.mkv` files are saved
- destination ID is `tmp-dir-id`
- returned entries are the matched entries
- empty `TempRoot` returns a clear error

- [ ] **Step 2: Run red test**

Run: `go test ./internal/subscription -run 'TestSaveShareToTemp'`

Expected: undefined save types/functions.

- [ ] **Step 3: Implement minimal orchestration**

Implementation rules:

- Validate `TempRoot` after `cleanConfigPath`.
- Call `ListShareTree`.
- Filter with `opts.Match`; if nil, save all non-directory entries.
- Group direct selected items by parent ID for provider save calls.
- Call `WaitSaveComplete` if provider returns task IDs.
- Return matched `TreeEntry` values.

- [ ] **Step 4: Run green test**

Run: `go test ./internal/subscription -run 'TestSaveShareToTemp'`

Expected: PASS.

---

### Task 4: Extend Subscription Config For Provider Credentials

**Files:**
- Modify: `internal/model/subscription.go`
- Modify: `internal/subscription/config.go`
- Modify: `internal/subscription/config_test.go`

**Interfaces:**
- Consumes: existing `SubscriptionTelegramPanConfig`.
- Produces:
  - `type SubscriptionCloudPanProviderConfig struct { Enabled bool; Cookie string; RefreshToken string; AccessToken string; TempTransferRoot string; DeleteSourceAfter bool }`
  - Or, if smaller, add credential fields directly to `SubscriptionTelegramPanConfig`.

Recommended shape:

```go
type SubscriptionTelegramPanConfig struct {
	Channels          []string `json:"channels"`
	TempTransferRoot  string   `json:"temp_transfer_root"`
	DeleteSourceAfter bool     `json:"delete_source_after"`
	Cookie            string   `json:"cookie,omitempty"`
	RefreshToken      string   `json:"refresh_token,omitempty"`
	AccessToken       string   `json:"access_token,omitempty"`
}
```

- [ ] **Step 1: Write failing config normalization test**

Add a test proving:

- `cookie`, `refresh_token`, and `access_token` are trimmed.
- default credentials merge into per-subscription source config.
- existing grouped channel merge behavior is unchanged.

- [ ] **Step 2: Run red test**

Run: `go test ./internal/subscription -run 'TestApplyConfigDefaultsMergesTelegramProviderCredentials'`

Expected: FAIL until fields and normalization exist.

- [ ] **Step 3: Implement config fields and merge logic**

Rules:

- Do not emit legacy `QuarkChannels` fields after normalization.
- Do not overwrite a non-empty per-subscription credential with default credential.
- Continue to clean `TempTransferRoot` with `cleanConfigPath`.

- [ ] **Step 4: Run green test**

Run: `go test ./internal/subscription -run 'TestApplyConfigDefaultsMergesTelegramProviderCredentials|TestApplyConfigDefaultsMergesTelegramChannelGroups'`

Expected: PASS.

---

### Task 5: Quark Provider Vertical Slice

**Files:**
- Create: `internal/subscription/share_quark.go`
- Create: `internal/subscription/share_quark_test.go`

**Interfaces:**
- Consumes: provider interfaces from Tasks 1-3.
- Produces:
  - `func NewQuarkShareProvider(cfg model.SubscriptionTelegramPanConfig) ShareSaver`

- [ ] **Step 1: Write failing Quark request-shape tests**

Use `httptest.Server` or an injectable HTTP client/base URL to assert request shape for:

- `get_stoken`: share ID and passcode are sent.
- `get_detail`: stoken, share ID, parent ID are sent.
- `save_file`: selected file IDs and destination folder ID are sent.

Expected: provider methods call the fake server endpoints and decode expected fixture JSON.

- [ ] **Step 2: Run red test**

Run: `go test ./internal/subscription -run 'TestQuarkShareProvider'`

Expected: undefined provider.

- [ ] **Step 3: Implement Quark provider**

Rules:

- Use configured Quark `Cookie`.
- Parse URL using Task 1 parser.
- Implement request methods analogous to Quark API behavior observed in `quark-auto-save`/CASX, but do not copy code verbatim.
- Keep API endpoint constants local to `share_quark.go`.
- Return `ShareItem.Raw` as the decoded file map/struct so `SaveShareItems` can access provider-specific `fid_token` if required.
- Poll save task with bounded retry and context cancellation.

- [ ] **Step 4: Run provider tests**

Run: `go test ./internal/subscription -run 'TestQuarkShareProvider|TestSaveShareToTemp'`

Expected: PASS.

---

### Task 6: Aliyun Drive Provider Vertical Slice

**Files:**
- Create: `internal/subscription/share_aliyun.go`
- Create: `internal/subscription/share_aliyun_test.go`

**Interfaces:**
- Consumes: provider interfaces from Tasks 1-3.
- Produces:
  - `func NewAliyunDriveShareProvider(cfg model.SubscriptionTelegramPanConfig) ShareSaver`

- [ ] **Step 1: Write failing Aliyun tests**

Use fixture responses for:

- refresh token to access token if `RefreshToken` is configured
- share token from `share_id/share_pwd`
- file list under `parent_file_id`
- share save/copy endpoint into destination directory

- [ ] **Step 2: Run red test**

Run: `go test ./internal/subscription -run 'TestAliyunDriveShareProvider'`

Expected: undefined provider.

- [ ] **Step 3: Implement Aliyun provider**

Rules:

- Reuse request patterns from `drivers/aliyundrive_share` conceptually.
- Do not require mounting an `AliyundriveShare` storage.
- `RefreshToken` is required unless a valid `AccessToken` is supplied.
- `get_share_token` uses passcode from parsed URL when present.
- `ListShareChildren` handles pagination markers.

- [ ] **Step 4: Run provider tests**

Run: `go test ./internal/subscription -run 'TestAliyunDriveShareProvider|TestListShareTree|TestSaveShareToTemp'`

Expected: PASS.

---

### Task 7: 123Pan Provider Vertical Slice

**Files:**
- Create: `internal/subscription/share_123.go`
- Create: `internal/subscription/share_123_test.go`

**Interfaces:**
- Consumes: provider interfaces from Tasks 1-3.
- Produces:
  - `func NewPan123ShareProvider(cfg model.SubscriptionTelegramPanConfig) ShareSaver`

- [ ] **Step 1: Write failing 123Pan tests**

Fixture coverage:

- parse `pwd` passcode.
- list share files via `shareKey`, `SharePwd`, `parentFileId`, pagination.
- create destination folder or resolve existing folder.
- save selected share files into destination.

- [ ] **Step 2: Run red test**

Run: `go test ./internal/subscription -run 'TestPan123ShareProvider'`

Expected: undefined provider.

- [ ] **Step 3: Implement 123Pan provider**

Rules:

- Reuse signing logic conceptually from `drivers/123_share/util.go`; if direct reuse requires exporting helpers from driver package, keep that change small and covered by tests.
- Use `AccessToken` for authenticated save/list destination operations.
- Preserve rate limiting similar to existing 123 share driver.

- [ ] **Step 4: Run provider tests**

Run: `go test ./internal/subscription -run 'TestPan123ShareProvider|TestListShareTree|TestSaveShareToTemp'`

Expected: PASS.

---

### Task 8: 115Pan Provider Vertical Slice

**Files:**
- Create: `internal/subscription/share_115.go`
- Create: `internal/subscription/share_115_test.go`

**Interfaces:**
- Consumes: provider interfaces from Tasks 1-3.
- Produces:
  - `func NewPan115ShareProvider(cfg model.SubscriptionTelegramPanConfig) ShareSaver`

- [ ] **Step 1: Write failing 115 tests**

Fixture coverage:

- parse `password` passcode.
- list share snapshot pages.
- save selected share files to destination folder.
- handle rate limit wait through context.

- [ ] **Step 2: Run red test**

Run: `go test ./internal/subscription -run 'TestPan115ShareProvider'`

Expected: undefined provider.

- [ ] **Step 3: Implement 115 provider**

Rules:

- Prefer existing `github.com/SheltonZhu/115driver` APIs already in repo if they expose share receive/save operations.
- If SDK lacks save API, implement minimal HTTP calls with fixtures first.
- Auth can use `Cookie` initially; QR login is out of scope for this task unless already available through existing driver helpers.

- [ ] **Step 4: Run provider tests**

Run: `go test ./internal/subscription -run 'TestPan115ShareProvider|TestListShareTree|TestSaveShareToTemp'`

Expected: PASS.

---

### Task 9: Subscription Runtime Integration

**Files:**
- Modify: `internal/subscription/telegram.go`
- Modify: `internal/subscription/service.go`
- Create: `internal/subscription/share_runtime_test.go`

**Interfaces:**
- Consumes: all provider constructors and `SaveShareToTemp`.
- Produces:
  - `func providerForShareRef(ref ShareRef, cfg model.SubscriptionTelegramSourceConfig) (ShareSaver, bool, error)`
  - `func runShareLink(ctx context.Context, sub *model.Subscription, cfg model.SubscriptionTelegramSourceConfig, row telegramCommandRow, link string, transfer bool, seenAt time.Time) ([]model.SubscriptionItem, int, int, int, error)`

- [ ] **Step 1: Write failing integration tests**

Use fake provider registry injection to assert:

- Telegram row with Quark link and Quark temp root calls `SaveShareToTemp`.
- Returned `TreeEntry` values become pending subscription items through existing `itemFromEntry`.
- If provider config is incomplete, link remains skipped with existing message.
- Manual source `links` also uses provider path when possible.

- [ ] **Step 2: Run red test**

Run: `go test ./internal/subscription -run 'TestRunTelegramShareProvider|TestRunManualShareProvider'`

Expected: FAIL until runtime integration exists.

- [ ] **Step 3: Implement runtime integration**

Rules:

- Keep raw-link skipped behavior as fallback.
- Only call provider save when:
  - parsed provider matches configured group
  - `TempTransferRoot` is non-empty
  - required credential for provider is present
- After provider save completes, call `SnapshotPaths` on temp root and use existing `MediaFiles` + `subscriptionEntryMatches` flow.
- Preserve `DeleteSourceAfter` cleanup behavior after final transfer to target root.

- [ ] **Step 4: Run green integration tests**

Run: `go test ./internal/subscription -run 'TestRunTelegramShareProvider|TestRunManualShareProvider|TestRowLinksForTelegramPanSources'`

Expected: PASS.

---

### Task 10: Preview API And UI Surface

**Files:**
- Modify: `server/handles/subscription.go`
- Modify frontend source if present:
  - `src/types/subscription.ts`
  - `src/utils/api.ts`
  - `src/pages/home/SubscriptionManagement.tsx`

**Interfaces:**
- Consumes: provider runtime integration.
- Produces:
  - Preview output includes provider name, share URL, and parsed tree/count where available.
  - Settings UI exposes credentials per provider only if backend config model supports it.

- [ ] **Step 1: Write backend handler test if existing handler tests can be extended**

If handler test infrastructure is available, add a test for preview response with provider-backed items.

- [ ] **Step 2: Add UI fields conservatively**

For each provider group:

- Channels textarea.
- Temp transfer root.
- Delete source after upload toggle.
- Credential fields:
  - Quark: cookie.
  - Aliyun: refresh token.
  - 123: access token.
  - 115: cookie.

- [ ] **Step 3: Run backend tests**

Run: `go test ./server/handles ./internal/subscription`

Expected: PASS.

- [ ] **Step 4: Build frontend if source repo is available**

Run in frontend repo: `pnpm test` if present, then `pnpm build`.

Expected: build succeeds and generated assets can be copied into backend `public/dist` only if that is the established project workflow.

---

### Task 11: End-To-End Verification Matrix

**Files:**
- Create or update: `docs/superpowers/specs/2026-07-07-cloudpan-share-subscription-verification.md`

**Interfaces:**
- Consumes: all implemented provider and runtime behavior.
- Produces: manual and automated verification checklist.

- [ ] **Step 1: Document provider test fixtures**

For each provider, record:

- example share URL shape
- required credential
- expected parse result
- expected list result
- expected save result

- [ ] **Step 2: Run package tests**

Run:

```bash
go test ./internal/subscription
go test ./server/handles
```

Expected: PASS.

- [ ] **Step 3: Run broader tests and record known unrelated failures**

Run:

```bash
go test ./...
```

Expected: may fail on existing repo/environment issues. Record exact unrelated failures, for example missing `fuse.h`, local aria2 service not running, or existing vet errors.

- [ ] **Step 4: Manual smoke with one provider at a time**

For each provider:

1. Configure global Telegram channels and provider credential.
2. Add a test subscription with a known share URL.
3. Run preview.
4. Confirm share tree is parsed.
5. Run check with transfer disabled.
6. Confirm temp root contains saved files if provider save is enabled.
7. Run check with transfer enabled.
8. Confirm target root contains named files and subscription items are transferred.

---

## Delivery Order

1. Tasks 1-3: Provider contracts, URL parsing, tree traversal, fake save orchestration.
2. Task 4: Config shape.
3. Tasks 5-6: Quark and Aliyun first vertical slices.
4. Task 9 partial: integrate Quark/Aliyun into subscriptions.
5. Tasks 7-8: 123 and 115.
6. Task 9 complete: all providers wired.
7. Task 10: UI/API polish.
8. Task 11: verification documentation and manual smoke.

## Risks And Mitigations

- Provider APIs are private or change frequently.
  - Mitigation: isolate each provider behind `ShareSaver`, keep fixture tests for request/response shapes, and make provider failure fall back to skipped link state instead of breaking the whole subscription run.
- Save APIs may require account-specific destination IDs, not paths.
  - Mitigation: `EnsureDir` returns provider destination folder ID and hides path traversal inside provider.
- 123/115 save APIs may not be exposed by existing repo SDKs.
  - Mitigation: implement read-only `ListShareTree` first; gate save behind credential availability and provider support.
- AGPL reference projects cannot be copied casually.
  - Mitigation: use them as behavioral references; write fresh implementation and tests.
- Repeated subscription checks may duplicate provider-side saved files.
  - Mitigation: provider `EnsureDir` plus destination listing should be used before save where API supports it; initial vertical slice should document provider-specific duplicate behavior.

## Self-Review

- Spec coverage: The plan covers domain filtering already implemented, raw share URL parsing, share tree traversal, provider save, subscription integration, UI config, and verification.
- Placeholder scan: No task contains "TBD" or "implement later"; provider API details are bounded by fixture-first tests.
- Type consistency: `ShareRef`, `ShareItem`, `ShareTreeLister`, `ShareSaver`, and `SaveShareToTemp` are defined before later tasks consume them.

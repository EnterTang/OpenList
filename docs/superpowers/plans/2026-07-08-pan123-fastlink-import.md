# Pan123 FastLink Import Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add manual subscription `imports_text` support for `123FSLinkV2`, `123FLCPV2`, and `123FastLink` JSON so `123pan` files can be temp-transferred through the existing subscription flow.

**Architecture:** Keep `ParseShareURL` focused on share-link inputs and add a dedicated import parser plus a parallel save helper for already-expanded `123pan` file metadata. `runManual` keeps existing `links` behavior, then parses `imports_text`, filters imported files with the same `subscriptionEntryMatches` logic, and saves them through the current `pan123` `SaveShareItems` implementation.

**Tech Stack:** Go, `internal/subscription`, existing `internal/model` subscription config types, current `pan123` share saver, Go standard library path/JSON helpers, existing Go test suite under `internal/subscription`.

## Global Constraints

- Do not redesign `ShareRef` into a generic import container.
- Do not change the external subscription CRUD or preview/check route paths.
- Do not add a new standalone database model for fastlink imports.
- Do not broaden this work to non-`123pan` providers.
- Do not attempt to support every historical or unofficial fastlink variant in one pass.
- Do not require frontend-only implementation to unlock backend support.
- Use TDD for every behavior change: write a failing test, verify failure, implement, verify pass.
- Keep `links` behavior backward compatible while adding `imports_text`.
- Keep `ParseShareURL` responsible for regular share URLs and single-file `123FSLinkV2` only.

---

## File Structure

Create:

- `internal/subscription/share_import.go`
  - Parse `imports_text`, normalize `etag` / `size` / `path`, support `123FLCPV2`, JSON, and raw `123FSLinkV2` imports.
- `internal/subscription/share_import_test.go`
  - Parser coverage for approved examples, Base62 conversion, path cleaning, and mixed payload parsing.
- `internal/subscription/share_import_save_test.go`
  - Save-helper coverage for imported file trees, directory grouping, filtering, and `pan123` raw payload shape.

Modify:

- `internal/model/subscription.go`
  - Add `ImportsText string `json:"imports_text"`` to `SubscriptionManualSourceConfig`.
- `internal/subscription/share_save.go`
  - Add `SaveImportedFilesToTemp` and any small internal helpers shared with `SaveShareToTemp`.
- `internal/subscription/service.go`
  - Preserve existing manual `links` flow and add `imports_text` execution.
- `internal/subscription/share_runtime_test.go`
  - Add manual runtime regression tests for `imports_text` success and config errors.
- `internal/subscription/share_123_test.go`
  - If needed, add assertions for imported `ShareItem.Raw` payload expectations.

Unchanged on purpose:

- `internal/subscription/share_url.go`
  - no new `FLCPV2` or JSON parsing here.
- `server/handles/subscription.go`
  - payload shape changes ride through existing `source_config` JSON handling.

---

### Task 1: Import Parser For `123FLCPV2`, JSON, And Raw Fastlink Text

**Files:**
- Create: `internal/subscription/share_import.go`
- Create: `internal/subscription/share_import_test.go`

**Interfaces:**
- Produces:
  - `type pan123ImportedFile struct { Etag string; Size int64; Path string; Name string }`
  - `type ImportParseIssue struct { Input string; Reason string }`
  - `func parseManualImportText(raw string) ([]pan123ImportedFile, []ImportParseIssue, error)`
  - `func parsePan123CommonPathFastLink(raw string) ([]pan123ImportedFile, []ImportParseIssue, error)`
  - `func parsePan123FastLinkJSON(raw string) ([]pan123ImportedFile, []ImportParseIssue, error)`
- Consumes:
  - existing Base62/hex helpers can stay local to this file; do not couple to `ShareRef`.

- [ ] **Step 1: Write the failing parser tests**

Add tests in `internal/subscription/share_import_test.go` covering the approved examples and width of JSON support:

```go
package subscription

import (
	"strings"
	"testing"
)

func TestParsePan123CommonPathFastLinkSupportsEmptyCommonPath(t *testing.T) {
	raw := "123FLCPV2$%69Y8N4KosSpjpcVCReGVzy#3531063629#达顿牧场 (2026) {tmdbid-299167}/Season 1/达顿牧场.S01E02.2026.1080p.Amazon Prime.WEB-DL.H.264.DDP 5.1-Ocat.mkv"
	files, issues, err := parsePan123CommonPathFastLink(raw)
	if err != nil {
		t.Fatalf("parse common-path fastlink: %v", err)
	}
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(files) != 1 {
		t.Fatalf("len(files) = %d, want 1", len(files))
	}
	if files[0].Path != "达顿牧场 (2026) {tmdbid-299167}/Season 1/达顿牧场.S01E02.2026.1080p.Amazon Prime.WEB-DL.H.264.DDP 5.1-Ocat.mkv" {
		t.Fatalf("path = %q", files[0].Path)
	}
	if len(files[0].Etag) != 32 {
		t.Fatalf("etag = %q, want 32-char hex", files[0].Etag)
	}
}

func TestParsePan123CommonPathFastLinkJoinsNestedCommonPath(t *testing.T) {
	raw := "123FLCPV2$4k普码电影/CCTV-6绝版蓝光高清电影大绝版18部/15-17、《大决战》三部曲顶级版/%4zGrzyjXOlXRd5rDXr9r3v#11973776568#1、《大决战》三部曲CCTV6顶级源码/3、《大决战》平津战役.CCTV6顶级.mkv"
	files, issues, err := parsePan123CommonPathFastLink(raw)
	if err != nil {
		t.Fatalf("parse nested common path: %v", err)
	}
	if len(issues) != 0 || len(files) != 1 {
		t.Fatalf("files/issues = %#v %#v", files, issues)
	}
	want := "4k普码电影/CCTV-6绝版蓝光高清电影大绝版18部/15-17、《大决战》三部曲顶级版/1、《大决战》三部曲CCTV6顶级源码/3、《大决战》平津战役.CCTV6顶级.mkv"
	if files[0].Path != want {
		t.Fatalf("path = %q, want %q", files[0].Path, want)
	}
}

func TestParsePan123FastLinkJSONSupportsOfficialAndLooseShapes(t *testing.T) {
	cases := []string{
		`{"commonPath":"Season 1/","usesBase62EtagsInExport":true,"files":[{"etag":"69Y8N4KosSpjpcVCReGVzy","size":3531063629,"path":"Episode 02.mkv"}]}`,
		`{"files":[{"etag":"bc18e4ea5fb89ec5778d1f38c9772f5f","size":"1024","path":"Movie.mkv"}]}`,
		`[{"etag":"bc18e4ea5fb89ec5778d1f38c9772f5f","size":1024,"path":"Movie.mkv"}]`,
	}
	for _, raw := range cases {
		files, issues, err := parsePan123FastLinkJSON(raw)
		if err != nil {
			t.Fatalf("parse JSON %s: %v", raw, err)
		}
		if len(issues) != 0 || len(files) != 1 {
			t.Fatalf("files/issues = %#v %#v", files, issues)
		}
		if len(files[0].Etag) != 32 || files[0].Name == "" {
			t.Fatalf("file = %#v", files[0])
		}
	}
}

func TestParseManualImportTextRejectsTraversalAndCollectsIssues(t *testing.T) {
	raw := strings.Join([]string{
		"123FSLinkV2$bc18e4ea5fb89ec5778d1f38c9772f5f#1024#Movie.mkv",
		"123FLCPV2$root/%bc18e4ea5fb89ec5778d1f38c9772f5f#2048#../escape.mkv",
	}, "\n")
	files, issues, err := parseManualImportText(raw)
	if err != nil {
		t.Fatalf("parse mixed imports: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("len(files) = %d, want 1", len(files))
	}
	if len(issues) != 1 || !strings.Contains(issues[0].Reason, "path") {
		t.Fatalf("issues = %#v, want path issue", issues)
	}
}
```

- [ ] **Step 2: Run parser tests to verify red**

Run: `go test ./internal/subscription -run 'TestParsePan123|TestParseManualImportText'`

Expected: build failure with undefined parser functions and types.

- [ ] **Step 3: Implement the minimal parser**

Implement `internal/subscription/share_import.go` with these behaviors:

```go
func parseManualImportText(raw string) ([]pan123ImportedFile, []ImportParseIssue, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil, nil
	}
	if files, issues, err := parsePan123FastLinkJSON(raw); err == nil {
		return dedupeImportedFiles(files), issues, nil
	}
	var all []pan123ImportedFile
	var issues []ImportParseIssue
	for _, match := range extractPan123CommonPathFastLinks(raw) {
		files, parseIssues, err := parsePan123CommonPathFastLink(match)
		if err != nil {
			issues = append(issues, ImportParseIssue{Input: match, Reason: err.Error()})
			continue
		}
		all = append(all, files...)
		issues = append(issues, parseIssues...)
	}
	for _, match := range extractPan123FastLinks(raw) {
		file, issue, err := parsePan123SingleImport(match)
		if err != nil {
			issues = append(issues, ImportParseIssue{Input: match, Reason: err.Error()})
			continue
		}
		if issue.Reason != "" {
			issues = append(issues, issue)
			continue
		}
		all = append(all, file)
	}
	all = dedupeImportedFiles(all)
	if len(all) == 0 {
		return nil, issues, fmt.Errorf("no supported pan123 fastlink imports found")
	}
	return all, issues, nil
}
```

Supporting rules:

- detect 32-char hex vs 22-char Base62 etags and normalize to lowercase 32-char hex;
- `cleanImportedPath(commonPath, relPath string) (string, error)` must reject empty paths and any `.` / `..` component;
- JSON parser must accept `size` as JSON number or numeric string;
- `123FLCPV2` parser splits on the first `%` and then per-file `$` records;
- `123FSLinkV2` import path is just the provided filename.

- [ ] **Step 4: Run parser tests to verify green**

Run: `go test ./internal/subscription -run 'TestParsePan123|TestParseManualImportText'`

Expected: PASS.

- [ ] **Step 5: Commit parser slice**

```bash
git add internal/subscription/share_import.go internal/subscription/share_import_test.go
git commit -m "feat(subscription): parse pan123 fastlink imports" -m "- add manual import parsing for 123FSLinkV2, 123FLCPV2, and JSON
- normalize Base62 etags and sanitize imported file paths"
```

---

### Task 2: Save Imported File Trees Through Existing `pan123` Save Primitives

**Files:**
- Modify: `internal/subscription/share_save.go`
- Create: `internal/subscription/share_import_save_test.go`

**Interfaces:**
- Consumes:
  - `type pan123ImportedFile struct { Etag string; Size int64; Path string; Name string }`
  - existing `SaveShareOptions`, `ShareSaver`, `ShareItem`, `TreeEntry`
- Produces:
  - `func SaveImportedFilesToTemp(ctx context.Context, provider ShareSaver, rootPath string, files []pan123ImportedFile, opts SaveShareOptions) ([]TreeEntry, error)`

- [ ] **Step 1: Write the failing save-helper tests**

Add tests in `internal/subscription/share_import_save_test.go` using a fake provider that records ensured directories and saved file groups:

```go
package subscription

import (
	"context"
	"reflect"
	"testing"
)

func TestSaveImportedFilesToTempCreatesDirectoriesAndGroupsByFolder(t *testing.T) {
	provider := &fakeShareSaver{dstDirID: "dst-root"}
	files := []pan123ImportedFile{
		{Etag: "bc18e4ea5fb89ec5778d1f38c9772f5f", Size: 1024, Path: "Season 1/Episode 01.mkv", Name: "Episode 01.mkv"},
		{Etag: "bc18e4ea5fb89ec5778d1f38c9772f5f", Size: 2048, Path: "Season 1/Episode 02.mkv", Name: "Episode 02.mkv"},
		{Etag: "11111111111111111111111111111111", Size: 512, Path: "Extras/Featurette.mkv", Name: "Featurette.mkv"},
	}
	entries, err := SaveImportedFilesToTemp(context.Background(), provider, "manual_import://pan123", files, SaveShareOptions{
		TempRoot: "/tmp/pan123",
		Match: func(entry TreeEntry) bool { return entry.Name != "Featurette.mkv" },
	})
	if err != nil {
		t.Fatalf("save imported files: %v", err)
	}
	if got, want := len(entries), 2; got != want {
		t.Fatalf("len(entries) = %d, want %d", got, want)
	}
	wantDirs := []string{"/tmp/pan123", "/tmp/pan123/Season 1"}
	if !reflect.DeepEqual(provider.ensureDirCalls[:2], wantDirs) {
		t.Fatalf("ensureDirCalls = %#v, want prefix %#v", provider.ensureDirCalls, wantDirs)
	}
	seasonItems := provider.saved["dir:/tmp/pan123/Season 1"]
	if len(seasonItems) != 2 {
		t.Fatalf("season items = %#v", seasonItems)
	}
	if seasonItems[0].Raw.(map[string]any)["file_name"] == "" {
		t.Fatalf("raw payload = %#v", seasonItems[0].Raw)
	}
}

func TestSaveImportedFilesToTempRequiresTempRootAndFiles(t *testing.T) {
	provider := &fakeShareSaver{}
	if _, err := SaveImportedFilesToTemp(context.Background(), provider, "manual_import://pan123", nil, SaveShareOptions{TempRoot: "/tmp/pan123"}); err == nil {
		t.Fatal("expected empty files error")
	}
	if _, err := SaveImportedFilesToTemp(context.Background(), provider, "manual_import://pan123", []pan123ImportedFile{{Path: "Movie.mkv", Name: "Movie.mkv", Etag: "bc18e4ea5fb89ec5778d1f38c9772f5f", Size: 1}}, SaveShareOptions{}); err == nil {
		t.Fatal("expected temp root error")
	}
}
```

If `fakeShareSaver` from `share_save_test.go` is reusable, extend it instead of duplicating it.

- [ ] **Step 2: Run save-helper tests to verify red**

Run: `go test ./internal/subscription -run 'TestSaveImportedFilesToTemp'`

Expected: build failure with undefined `SaveImportedFilesToTemp`.

- [ ] **Step 3: Implement the minimal save helper**

In `internal/subscription/share_save.go`, add the helper without changing `SaveShareToTemp` behavior:

```go
func SaveImportedFilesToTemp(ctx context.Context, provider ShareSaver, rootPath string, files []pan123ImportedFile, opts SaveShareOptions) ([]TreeEntry, error) {
	if provider == nil {
		return nil, fmt.Errorf("share provider is nil")
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("imported files are required")
	}
	tempRoot := cleanConfigPath(opts.TempRoot)
	if tempRoot == "" || tempRoot == "/" {
		return nil, fmt.Errorf("share temp root is required")
	}
	rootDirID, err := provider.EnsureDir(ctx, tempRoot)
	if err != nil {
		return nil, err
	}
	dirIDs := map[string]string{"": rootDirID}
	var selected []TreeEntry
	groups := map[string][]ShareItem{}
	for _, file := range files {
		dirPath := stdpath.Dir(file.Path)
		if dirPath == "." {
			dirPath = ""
		}
		entry := TreeEntry{RootPath: rootPath, Path: "/" + file.Path, Name: file.Name, Size: file.Size}
		if opts.Match != nil && !opts.Match(entry) {
			continue
		}
		dstDirID, err := ensureImportedDir(ctx, provider, tempRoot, dirPath, dirIDs)
		if err != nil {
			return selected, err
		}
		selected = append(selected, entry)
		groups[dstDirID] = append(groups[dstDirID], ShareItem{
			Name: file.Name,
			Size: file.Size,
			Raw: map[string]any{"etag": file.Etag, "size": file.Size, "file_name": file.Name, "type": 0},
		})
	}
	for dstDirID, items := range groups {
		taskIDs, err := provider.SaveShareItems(ctx, ShareRef{Provider: ShareProviderPan123, RawURL: rootPath}, "", items, dstDirID)
		if err != nil {
			return selected, err
		}
		if len(taskIDs) > 0 {
			if err := provider.WaitSaveComplete(ctx, taskIDs); err != nil {
				return selected, err
			}
		}
	}
	return selected, nil
}
```

Use `stdpath.Split`, `strings.Split`, or a small helper to ensure parent directories are created in order.

- [ ] **Step 4: Run save-helper tests to verify green**

Run: `go test ./internal/subscription -run 'TestSaveImportedFilesToTemp'`

Expected: PASS.

- [ ] **Step 5: Commit save-helper slice**

```bash
git add internal/subscription/share_save.go internal/subscription/share_import_save_test.go internal/subscription/share_save_test.go
git commit -m "feat(subscription): save imported pan123 file trees" -m "- add temp-transfer helper for parsed pan123 fastlink imports
- reuse existing SaveShareItems flow with directory grouping and filtering"
```

---

### Task 3: Extend Manual Subscription Config And Runtime For `imports_text`

**Files:**
- Modify: `internal/model/subscription.go`
- Modify: `internal/subscription/service.go`
- Modify: `internal/subscription/share_runtime_test.go`

**Interfaces:**
- Consumes:
  - `parseManualImportText`
  - `SaveImportedFilesToTemp`
  - existing `GetConfig`, `newShareSaver`, `subscriptionEntryMatches`, `cleanStringList`
- Produces:
  - `SubscriptionManualSourceConfig.ImportsText string`
  - manual runtime support for imported fastlink batches

- [ ] **Step 1: Write the failing manual runtime tests**

Add tests in `internal/subscription/share_runtime_test.go` for success and required-config errors:

```go
func TestRunManualImportsTextSavesMatchingPan123Files(t *testing.T) {
	oldGetConfig := getConfig
	oldNewShareSaver := newShareSaver
	oldSaveImported := saveImportedFilesToTemp
	defer func() {
		getConfig = oldGetConfig
		newShareSaver = oldNewShareSaver
		saveImportedFilesToTemp = oldSaveImported
	}()

	getConfig = func() (*model.SubscriptionConfig, error) {
		return &model.SubscriptionConfig{Telegram: model.SubscriptionTelegramSourceConfig{Pan123: model.SubscriptionTelegramPanConfig{AccessToken: "token-1", TempTransferRoot: "/tmp/pan123"}}}, nil
	}
	newShareSaver = func(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) (ShareSaver, error) {
		if provider != ShareProviderPan123 || cfg.AccessToken != "token-1" {
			t.Fatalf("provider/cfg = %s %#v", provider, cfg)
		}
		return &fakeShareSaver{}, nil
	}
	var importedRoot string
	var importedFiles []pan123ImportedFile
	saveImportedFilesToTemp = func(ctx context.Context, provider ShareSaver, rootPath string, files []pan123ImportedFile, opts SaveShareOptions) ([]TreeEntry, error) {
		importedRoot = rootPath
		importedFiles = append([]pan123ImportedFile(nil), files...)
		if !opts.Match(TreeEntry{Name: "达顿牧场.S01E02.2026.1080p.Amazon Prime.WEB-DL.H.264.DDP 5.1-Ocat.mkv"}) {
			t.Fatal("expected match function to accept matching entry")
		}
		return []TreeEntry{{Path: "/达顿牧场 (2026) {tmdbid-299167}/Season 1/达顿牧场.S01E02.2026.1080p.Amazon Prime.WEB-DL.H.264.DDP 5.1-Ocat.mkv", Name: "达顿牧场.S01E02.2026.1080p.Amazon Prime.WEB-DL.H.264.DDP 5.1-Ocat.mkv"}}, nil
	}

	sub := &model.Subscription{
		Name:       "达顿牧场",
		SourceType: model.SubscriptionSourceTypeManual,
		SourceConfig: `{"imports_text":"123FLCPV2$%69Y8N4KosSpjpcVCReGVzy#3531063629#达顿牧场 (2026) {tmdbid-299167}/Season 1/达顿牧场.S01E02.2026.1080p.Amazon Prime.WEB-DL.H.264.DDP 5.1-Ocat.mkv"}`,
	}
	_, _, _, _, _, err := runManual(context.Background(), sub, false)
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}
	if importedRoot == "" || len(importedFiles) != 1 {
		t.Fatalf("root/files = %q %#v", importedRoot, importedFiles)
	}
}

func TestRunManualImportsTextRequiresPan123Config(t *testing.T) {
	oldGetConfig := getConfig
	defer func() { getConfig = oldGetConfig }()
	getConfig = func() (*model.SubscriptionConfig, error) {
		return &model.SubscriptionConfig{}, nil
	}
	sub := &model.Subscription{SourceType: model.SubscriptionSourceTypeManual, SourceConfig: `{"imports_text":"123FSLinkV2$bc18e4ea5fb89ec5778d1f38c9772f5f#1024#Movie.mkv"}`}
	_, _, _, _, _, err := runManual(context.Background(), sub, false)
	if err == nil || !strings.Contains(err.Error(), "pan123") {
		t.Fatalf("err = %v, want pan123 config error", err)
	}
}
```

If `getConfig` is not already swappable, add a package-level variable alias in production code first in the smallest possible way.

- [ ] **Step 2: Run runtime tests to verify red**

Run: `go test ./internal/subscription -run 'TestRunManualImportsText'`

Expected: build failure due to missing `ImportsText` handling or undefined indirection variables.

- [ ] **Step 3: Implement the minimal runtime integration**

Production changes:

1. In `internal/model/subscription.go`:

```go
type SubscriptionManualSourceConfig struct {
	Paths       []string `json:"paths"`
	Links       []string `json:"links"`
	ImportsText string   `json:"imports_text"`
}
```

2. In `internal/subscription/service.go`, after existing `cfg.Links` processing:

```go
if strings.TrimSpace(cfg.ImportsText) != "" {
	files, _, err := parseManualImportText(cfg.ImportsText)
	if err != nil {
		return saved, sub.LastTreeHash, added, changed, transferred, err
	}
	globalCfg, err := getConfig()
	if err != nil {
		return saved, sub.LastTreeHash, added, changed, transferred, err
	}
	panCfg := normalizeTelegramPanConfig(globalCfg.Telegram.Pan123)
	if strings.TrimSpace(panCfg.TempTransferRoot) == "" {
		return saved, sub.LastTreeHash, added, changed, transferred, fmt.Errorf("pan123 temp_transfer_root is required for manual imports")
	}
	if strings.TrimSpace(panCfg.AccessToken) == "" {
		return saved, sub.LastTreeHash, added, changed, transferred, fmt.Errorf("pan123 access_token is required for manual imports")
	}
	provider, err := newShareSaver(ShareProviderPan123, panCfg)
	if err != nil {
		return saved, sub.LastTreeHash, added, changed, transferred, err
	}
	rootPath := "manual_import://pan123"
	_, err = saveImportedFilesToTemp(ctx, provider, rootPath, files, SaveShareOptions{
		TempRoot: panCfg.TempTransferRoot,
		Match: func(entry TreeEntry) bool { return !entry.IsDir && subscriptionEntryMatches(sub, entry) },
	})
	if err != nil {
		return saved, sub.LastTreeHash, added, changed, transferred, err
	}
	snapshotRoots = appendPathOnce(snapshotRoots, panCfg.TempTransferRoot)
}
```

3. Add package-level aliases if needed for testability:

```go
var getConfig = GetConfig
var saveImportedFilesToTemp = SaveImportedFilesToTemp
```

4. In `parseManualConfig`, trim `ImportsText`:

```go
cfg.ImportsText = strings.TrimSpace(cfg.ImportsText)
```

- [ ] **Step 4: Run runtime tests to verify green**

Run: `go test ./internal/subscription -run 'TestRunManualImportsText'`

Expected: PASS.

- [ ] **Step 5: Commit runtime slice**

```bash
git add internal/model/subscription.go internal/subscription/service.go internal/subscription/share_runtime_test.go
git commit -m "feat(subscription): run manual pan123 fastlink imports" -m "- add manual imports_text config for pan123 fastlink payloads
- execute parsed imports through existing subscription temp-transfer matching"
```

---

### Task 4: End-To-End Regression Sweep And Targeted Verification

**Files:**
- Modify: `internal/subscription/share_import_test.go`
- Modify: `internal/subscription/share_import_save_test.go`
- Modify: `internal/subscription/share_runtime_test.go`
- Optionally Modify: `docs/superpowers/specs/2026-07-08-pan123-fastlink-import-design.md` only if implementation reveals a real spec correction.

**Interfaces:**
- Consumes:
  - all code from Tasks 1-3
- Produces:
  - green targeted regression suite and any last small refactors needed to keep code coherent.

- [ ] **Step 1: Add any missing narrow regression tests before refactor**

If coverage gaps remain after Tasks 1-3, add focused tests for:

```go
func TestParseManualImportTextSupportsWholeJSONDocument(t *testing.T) {}
func TestSaveImportedFilesToTempSkipsAllUnmatchedFilesWithoutError(t *testing.T) {}
func TestRunManualImportsTextRejectsUnsupportedPayload(t *testing.T) {}
```

Keep each test to one behavior.

- [ ] **Step 2: Run targeted subscription tests**

Run:

```bash
go test ./internal/subscription -run 'Test(ParsePan123|ParseManualImportText|SaveImportedFilesToTemp|RunManualImportsText|ParseShareURL|ListShareChildrenPan123FastLink)'
```

Expected: PASS.

- [ ] **Step 3: Run broader subscription package verification**

Run:

```bash
go test ./internal/subscription
```

Expected: PASS.

- [ ] **Step 4: Commit the finished implementation**

```bash
git add internal/subscription internal/model/subscription.go
git commit -m "test(subscription): cover pan123 manual fastlink imports" -m "- add parser, save-helper, and runtime regressions for pan123 fastlink imports
- verify manual imports_text flows through pan123 temp-transfer behavior"
```

---

## Self-Review

### Spec Coverage

- Manual `imports_text` field: Task 3.
- `123FLCPV2` with empty and nested `commonPath`: Task 1 tests and implementation.
- JSON official and loose compatibility: Task 1 tests and implementation.
- Base62 `etag` normalization: Task 1 tests and implementation.
- Conservative path cleaning: Task 1 tests and implementation.
- Parallel save helper instead of stretching `ShareRef`: Task 2.
- Reuse existing `pan123` save primitives and match logic: Tasks 2 and 3.
- No new API routes: preserved by file scope and Task 3 integration.
- Runtime error handling for missing `pan123` config: Task 3.
- Targeted verification: Task 4.

### Placeholder Scan

- No `TODO`, `TBD`, or “implement later” placeholders remain.
- All tasks include exact file paths, concrete interfaces, explicit commands, and concrete code snippets.

### Type Consistency

- `pan123ImportedFile`, `ImportParseIssue`, `parseManualImportText`, and `SaveImportedFilesToTemp` are defined once and reused consistently.
- `ImportsText` is the only new manual config field name across tasks.
- Runtime indirection variables use `getConfig` and `saveImportedFilesToTemp` consistently.

## Summary

Implement this feature in four TDD slices:

1. parse approved fastlink import payloads;
2. save parsed imported file trees through existing `pan123` save primitives;
3. connect `imports_text` into manual subscription runtime;
4. run focused and package-level regression verification.

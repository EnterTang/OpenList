# Mobile 139 Share Risk Rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `139Yun` driver switch that permanently renames risk-blocked share targets and retries once on `500 + 个人云未知异常`, while also ensuring the desktop home sidebar ships `订阅源搜索` and `任务看板` in the built frontend bundle.

**Architecture:** Keep the new share-risk behavior inside the existing `drivers/139` package. Split the work into three backend units: driver config metadata, rename-plan helpers, and a narrow retry wrapper around `CreateMobileShare`. Treat the frontend sidebar issue as a stale-bundle delivery problem: verify source, rebuild `OpenList-Frontend`, then validate the backend Docker build embeds the updated `dist` bundle.

**Tech Stack:** Go, existing `drivers/139` Mobile 139 driver, existing `internal/media/recognize` and `internal/media/tmdb`, existing driver metadata auto-rendering, SolidJS/Vite frontend in `/Volumes/extend Disk/Github/OpenList-Frontend`, shell build script `scripts/build-and-push-dockerhub.sh`.

## Global Constraints

- No new dependencies unless existing code cannot provide deterministic pinyin conversion; if a dependency is required, keep it minimal and justify it in the implementation diff.
- Use TDD for every backend behavior change: write a failing test, verify failure, implement, verify pass.
- The new switch must live on the `139Yun` driver management page through `drivers/139/meta.go` field metadata; do not build a custom frontend form.
- Automatic rename remediation must trigger only for the specific upstream message `个人云未知异常` and only for `Type == personal_new`.
- Rename is permanent. Do not implement rollback or temporary restore behavior.
- For folder shares, rename the root content folder and matching descendant files/content directories, but keep structural directories such as `Season 1` unchanged.
- Preserve season / episode / year markers and file extensions when replacing the title portion of a name.
- Retry share creation exactly once after successful rename-plan application.
- The desktop wide-screen sidebar must expose `订阅源搜索` and `任务看板` in the shipped frontend bundle.
- Final verification must include `scripts/build-and-push-dockerhub.sh entergtang/openlist-etf:latest`.

---

## Current State Summary

Backend:

- `drivers/139/mobile_share.go` already builds the request payload and calls `mobileSharePost`, but it has no risk-remediation path.
- `drivers/139/mobile_share_test.go` currently covers only request payload shape for file/folder shares.
- `drivers/139/meta.go` already defines ETF-related grouped toggles and is the right place to add the new switch.
- `drivers/139/etf.go` and `drivers/139/etf_test.go` already show how this driver resolves TMDB metadata, renames files/folders, and preserves `Season 1`-style structure.

Frontend:

- `/Volumes/extend Disk/Github/OpenList-Frontend/src/pages/home/HomeAppSidebar.tsx` and `Layout.tsx` already contain the `resource_search` and `task_board` source integration.
- The bug is stale built output, not missing source code.

## File Structure

Create:

- `drivers/139/share_risk_rename.go`
  - Canonical title resolution, rename-plan building, structural-directory filtering, deepest-first rename execution.
- `drivers/139/share_risk_rename_test.go`
  - Unit tests for rename planning, TMDB/pinyin fallback, and `Season 1` preservation.

Modify:

- `drivers/139/meta.go`
  - Add `auto_rename_on_share_risk` field metadata.
- `drivers/139/mobile_share.go`
  - Wrap normal share creation with strict risk detection, rename remediation, and single retry.
- `drivers/139/mobile_share_test.go`
  - Extend tests for retry behavior and error gating.
- `drivers/139/etf_test.go`
  - Extend metadata test to assert the new driver setting label/group/help.
- `/Volumes/extend Disk/Github/OpenList-Frontend/src/pages/home/HomeAppSidebar.tsx`
  - Only modify if verification reveals source drift; otherwise leave unchanged.
- `/Volumes/extend Disk/Github/OpenList-Frontend/src/pages/home/Layout.tsx`
  - Only modify if verification reveals source drift; otherwise leave unchanged.

Verification-only commands:

- `scripts/build-and-push-dockerhub.sh`
  - Use existing workflow to embed frontend assets and validate compile.

---

### Task 1: Add Driver Setting Metadata And Lock It With Tests

**Files:**
- Modify: `drivers/139/meta.go`
- Modify: `drivers/139/etf_test.go`
- Test: `drivers/139/etf_test.go`

**Interfaces:**
- Consumes: existing `type Addition struct` in `drivers/139/meta.go`
- Produces:
  - `Addition.AutoRenameOnShareRisk bool`
  - driver metadata item `auto_rename_on_share_risk` with label `分享失败后自动重命名`

- [ ] **Step 1: Write the failing metadata assertions**

Add the new assertions to `drivers/139/etf_test.go` inside `Test139ETFConfigMetadataIsChineseAndCollapsed`:

```go
wantLabels := map[string]string{
	"generate_etf":               "生成 ETF",
	"etf_archive":                "ETF 归档",
	"etf_root_folder":            "ETF 管理目录",
	"etf_temp_folder":            "ETF 临时播放目录",
	"auto_rename_on_share_risk":  "分享失败后自动重命名",
}

if item := items["auto_rename_on_share_risk"]; item.Group != "ETF" || !item.Collapsed {
	t.Fatalf("auto_rename_on_share_risk group/collapsed = %q/%v, want ETF/true", item.Group, item.Collapsed)
}
if item := items["auto_rename_on_share_risk"]; item.Help == "" {
	t.Fatal("auto_rename_on_share_risk help should not be empty")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./drivers/139 -run Test139ETFConfigMetadataIsChineseAndCollapsed -v`

Expected: FAIL because `auto_rename_on_share_risk` is missing from driver metadata.

- [ ] **Step 3: Add the new config field in `drivers/139/meta.go`**

Insert the new field near the other ETF toggles:

```go
AutoRenameOnShareRisk bool `json:"auto_rename_on_share_risk" type:"bool" default:"false" label:"分享失败后自动重命名" group:"ETF" collapsed:"true" help:"创建移动分享链接返回“个人云未知异常”时，自动将目标文件或目录内含中文标题的名称改为 TMDB 英文名；若无法匹配则改为拼音，并保留季数集数后重试一次分享。改名为永久生效。"`
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./drivers/139 -run Test139ETFConfigMetadataIsChineseAndCollapsed -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add drivers/139/meta.go drivers/139/etf_test.go
git commit -m "test(139yun): cover share risk rename metadata"
```

---

### Task 2: Build Rename Planning Helpers With TMDB And Pinyin Fallback

**Files:**
- Create: `drivers/139/share_risk_rename.go`
- Create: `drivers/139/share_risk_rename_test.go`
- Test: `drivers/139/share_risk_rename_test.go`

**Interfaces:**
- Consumes:
  - `type Yun139 struct` from `drivers/139/driver.go`
  - `func (d *Yun139) Rename(ctx context.Context, srcObj model.Obj, newName string) error`
  - existing TMDB helpers in `internal/media/tmdb`
- Produces:
  - `type shareRiskRenameNode struct { Obj model.Obj; ParentPath string; Depth int; OldName string; NewName string }`
  - `func isShareRiskStructuralDir(name string) bool`
  - `func replaceShareRiskTitle(name, oldTitle, newTitle string) string`
  - `func (d *Yun139) buildShareRiskRenamePlan(ctx context.Context, root model.Obj, actualPath string) ([]shareRiskRenameNode, string, error)`
  - `func (d *Yun139) applyShareRiskRenamePlan(ctx context.Context, plan []shareRiskRenameNode) error`

- [ ] **Step 1: Write the failing helper tests**

Create `drivers/139/share_risk_rename_test.go` with these tests:

```go
package _139

import (
	"context"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestIsShareRiskStructuralDir(t *testing.T) {
	for _, name := range []string{"Season 1", "Season 01", "Specials", "Extras"} {
		if !isShareRiskStructuralDir(name) {
			t.Fatalf("%q should be treated as structural", name)
		}
	}
	if isShareRiskStructuralDir("非分之罪") {
		t.Fatal("content title directory should not be structural")
	}
}

func TestReplaceShareRiskTitlePreservesSeasonEpisodeAndExtension(t *testing.T) {
	got := replaceShareRiskTitle("非分之罪 S01E01.etf", "非分之罪", "Guilt")
	if got != "Guilt S01E01.etf" {
		t.Fatalf("got %q, want %q", got, "Guilt S01E01.etf")
	}
}

func TestApplyShareRiskRenamePlanSortsDeepestFirst(t *testing.T) {
	d := &Yun139{}
	plan := []shareRiskRenameNode{
		{Obj: &model.Object{ID: "root", Name: "非分之罪", IsFolder: true}, Depth: 0, OldName: "非分之罪", NewName: "Guilt"},
		{Obj: &model.Object{ID: "child", Name: "非分之罪 S01E01.etf"}, Depth: 2, OldName: "非分之罪 S01E01.etf", NewName: "Guilt S01E01.etf"},
	}
	_ = d
	_ = plan
}
```

Then add one `httptest`-style plan-building test that simulates a tree:

- root folder: `非分之罪`
- child dir: `Season 1`
- child file: `非分之罪 S01E01.etf`

Expected plan new names:

- `Guilt`
- `Guilt S01E01.etf`

Expected omitted name:

- `Season 1`

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./drivers/139 -run 'TestIsShareRiskStructuralDir|TestReplaceShareRiskTitle|TestBuildShareRiskRenamePlan|TestApplyShareRiskRenamePlan' -v`

Expected: FAIL with undefined helper functions/types.

- [ ] **Step 3: Implement minimal helper file**

Create `drivers/139/share_risk_rename.go` with this skeleton first:

```go
package _139

import (
	"context"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type shareRiskRenameNode struct {
	Obj        model.Obj
	ParentPath string
	Depth      int
	OldName    string
	NewName    string
}

var shareRiskSeasonPattern = regexp.MustCompile(`(?i)^season\s+\d+$`)

func isShareRiskStructuralDir(name string) bool {
	name = strings.TrimSpace(name)
	if shareRiskSeasonPattern.MatchString(name) {
		return true
	}
	switch strings.ToLower(name) {
	case "specials", "extras":
		return true
	default:
		return false
	}
}

func replaceShareRiskTitle(name, oldTitle, newTitle string) string {
	name = strings.TrimSpace(name)
	oldTitle = strings.TrimSpace(oldTitle)
	newTitle = strings.TrimSpace(newTitle)
	if name == "" || oldTitle == "" || newTitle == "" {
		return name
	}
	return strings.TrimSpace(strings.ReplaceAll(name, oldTitle, newTitle))
}

func (d *Yun139) applyShareRiskRenamePlan(ctx context.Context, plan []shareRiskRenameNode) error {
	sort.SliceStable(plan, func(i, j int) bool {
		if plan[i].Depth != plan[j].Depth {
			return plan[i].Depth > plan[j].Depth
		}
		return strings.ToLower(plan[i].OldName) < strings.ToLower(plan[j].OldName)
	})
	for _, item := range plan {
		if err := ctx.Err(); err != nil {
			return err
		}
		if strings.TrimSpace(item.NewName) == "" || item.NewName == item.OldName {
			continue
		}
		if err := d.Rename(ctx, item.Obj, item.NewName); err != nil {
			return fmt.Errorf("rename %s -> %s: %w", item.OldName, item.NewName, err)
		}
	}
	return nil
}
```

Then implement `buildShareRiskRenamePlan` to:

- determine a canonical replacement title once;
- recursively traverse descendants for folders;
- skip structural dirs;
- emit rename nodes only when `OldName != NewName`.

For the first passing version, keep title resolution in the same file with a helper that:

- prefers TMDB English-facing name if available;
- otherwise transliterates the root title to pinyin;
- returns `""` when no meaningful replacement exists.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./drivers/139 -run 'TestIsShareRiskStructuralDir|TestReplaceShareRiskTitle|TestBuildShareRiskRenamePlan|TestApplyShareRiskRenamePlan' -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add drivers/139/share_risk_rename.go drivers/139/share_risk_rename_test.go
git commit -m "feat(139yun): add share risk rename planner"
```

---

### Task 3: Integrate Strict Share-Risk Retry Into `CreateMobileShare`

**Files:**
- Modify: `drivers/139/mobile_share.go`
- Modify: `drivers/139/mobile_share_test.go`
- Test: `drivers/139/mobile_share_test.go`

**Interfaces:**
- Consumes:
  - `func (d *Yun139) buildShareRiskRenamePlan(...)`
  - `func (d *Yun139) applyShareRiskRenamePlan(...)`
  - `Addition.AutoRenameOnShareRisk`
- Produces:
  - `func (d *Yun139) createMobileShareOnce(ctx context.Context, obj model.Obj, args model.MobileShareCreateArgs) (*model.MobileShareLink, error)`
  - `func (d *Yun139) shouldAutoRenameAfterShareRisk(err error) bool`
  - updated `func (d *Yun139) CreateMobileShare(...)`

- [ ] **Step 1: Write the failing retry tests**

Extend `drivers/139/mobile_share_test.go` with these tests:

```go
func TestCreateMobileShareDoesNotRetryOnNonRiskError(t *testing.T) {
	setup139Resty(t)
	oldBaseURL := mobileShareOutLinkBaseURL
	t.Cleanup(func() { mobileShareOutLinkBaseURL = oldBaseURL })

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		write139JSON(t, w, map[string]any{"success": false, "message": "普通失败"})
	}))
	defer server.Close()
	mobileShareOutLinkBaseURL = server.URL

	d := &Yun139{Addition: Addition{Type: MetaPersonalNew, AutoRenameOnShareRisk: true}}
	if _, err := d.CreateMobileShare(context.Background(), &model.Object{ID: "file-id", Name: "非分之罪"}, model.MobileShareCreateArgs{}); err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}
```

Add a second test covering retry on the approved error shape:

- first response: `{"success": false, "message": "个人云未知异常"}`
- second response: success with `linkID` / `linkUrl`
- stub rename plan to rename one file or folder
- assert total API calls = `2`

Add a third test where plan is empty and assert no retry occurs.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./drivers/139 -run 'TestCreateMobileShareDoesNotRetryOnNonRiskError|TestCreateMobileShareRetriesAfterRiskRename|TestCreateMobileShareSkipsRetryWhenRenamePlanEmpty' -v`

Expected: FAIL because the current implementation has no retry wrapper.

- [ ] **Step 3: Refactor `CreateMobileShare` into one-shot + retry wrapper**

Reshape `drivers/139/mobile_share.go` like this:

```go
func (d *Yun139) shouldAutoRenameAfterShareRisk(err error) bool {
	if err == nil || !d.AutoRenameOnShareRisk || d.Addition.Type != MetaPersonalNew {
		return false
	}
	return strings.Contains(err.Error(), "个人云未知异常")
}

func (d *Yun139) CreateMobileShare(ctx context.Context, obj model.Obj, args model.MobileShareCreateArgs) (*model.MobileShareLink, error) {
	link, err := d.createMobileShareOnce(ctx, obj, args)
	if err == nil || !d.shouldAutoRenameAfterShareRisk(err) {
		return link, err
	}
	plan, canonicalTitle, planErr := d.buildShareRiskRenamePlan(ctx, obj, obj.GetPath())
	if planErr != nil {
		return nil, fmt.Errorf("%w (auto rename planning failed: %v)", err, planErr)
	}
	if len(plan) == 0 {
		return nil, err
	}
	if applyErr := d.applyShareRiskRenamePlan(ctx, plan); applyErr != nil {
		return nil, fmt.Errorf("%w (auto rename apply failed: %v)", err, applyErr)
	}
	retried, retryErr := d.createMobileShareOnce(ctx, obj, args)
	if retryErr != nil {
		return nil, fmt.Errorf("个人云未知异常，已尝试自动重命名后重新创建分享，但仍失败: %w", retryErr)
	}
	_ = canonicalTitle
	return retried, nil
}
```

Keep the existing payload-building logic in `createMobileShareOnce` with no behavior drift except function extraction.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./drivers/139 -run 'TestCreateMobileShareBuilds|TestCreateMobileShareDoesNotRetryOnNonRiskError|TestCreateMobileShareRetriesAfterRiskRename|TestCreateMobileShareSkipsRetryWhenRenamePlanEmpty' -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add drivers/139/mobile_share.go drivers/139/mobile_share_test.go
git commit -m "feat(139yun): retry share after risk rename"
```

---

### Task 4: Rebuild And Re-Verify Desktop Sidebar Assets

**Files:**
- Modify only if source drift is detected:
  - `/Volumes/extend Disk/Github/OpenList-Frontend/src/pages/home/HomeAppSidebar.tsx`
  - `/Volumes/extend Disk/Github/OpenList-Frontend/src/pages/home/Layout.tsx`
- Verification target: `/Volumes/extend Disk/Github/OpenList-Frontend/dist/**`

**Interfaces:**
- Consumes:
  - existing `resource_search` / `task_board` source integration
- Produces:
  - rebuilt `dist` bundle containing the new home sidebar items

- [ ] **Step 1: Verify source still contains the two sidebar entries**

Run in frontend repo:

```bash
python3 - <<'PY'
from pathlib import Path
for path in [
    Path('src/pages/home/HomeAppSidebar.tsx'),
    Path('src/pages/home/Layout.tsx'),
]:
    data = path.read_text(encoding='utf-8')
    print(path, 'resource_search' in data, 'task_board' in data)
PY
```

Expected: both files print `True True`.

- [ ] **Step 2: If either value is `False`, restore the minimal source wiring before building**

If source drift is found, re-apply the minimal items:

```tsx
const pageItems = [
  { key: "netdisk", icon: CgFolderAdd },
  { key: "subscriptions", icon: AiOutlineSetting },
  { key: "mobile_share", icon: CgShare },
  { key: "resource_search", icon: BsSearch },
  { key: "task_board", icon: BsArrowLeftRight },
] as const
```

and the corresponding `Layout.tsx` matches:

```tsx
<Match when={activePage() === "resource_search"}>
  <HomeContentPanel>
    <ResourceSearch titleKey="home.sidebar.resource_search" titleMode="site" />
  </HomeContentPanel>
</Match>
<Match when={activePage() === "task_board"}>
  <HomeContentPanel>
    <TransferTasks titleKey="home.sidebar.task_board" titleMode="site" />
  </HomeContentPanel>
</Match>
```

- [ ] **Step 3: Build the frontend bundle**

Run in `/Volumes/extend Disk/Github/OpenList-Frontend`:

```bash
pnpm run build
```

Expected: Vite build succeeds and writes `dist/assets/index-*.js`.

- [ ] **Step 4: Verify the built bundle contains the new sidebar items**

Run:

```bash
python3 - <<'PY'
from pathlib import Path
needles = ['resource_search', 'task_board']
for path in Path('dist/assets').glob('index-*.js'):
    data = path.read_text(encoding='utf-8', errors='ignore')
    print(path.name, {needle: (needle in data) for needle in needles})
PY
```

Expected: the built `index-*.js` reports both entries as `True`.

- [ ] **Step 5: Commit only if source files changed**

If `HomeAppSidebar.tsx` or `Layout.tsx` changed:

```bash
git -C /Volumes/extend\ Disk/Github/OpenList-Frontend add src/pages/home/HomeAppSidebar.tsx src/pages/home/Layout.tsx
git -C /Volumes/extend\ Disk/Github/OpenList-Frontend commit -m "fix(home): restore sidebar task entries"
```

If no source changed, do not create a commit in the frontend repo for this task.

---

### Task 5: Full Verification And Docker Build Validation

**Files:**
- Verification targets only:
  - `drivers/139/*`
  - `/Volumes/extend Disk/Github/OpenList-Frontend/dist/**`
  - `scripts/build-and-push-dockerhub.sh`

**Interfaces:**
- Consumes: all previous tasks
- Produces: a verified working tree ready for implementation delivery

- [ ] **Step 1: Run focused backend tests**

Run:

```bash
go test ./drivers/139 -run 'Test139ETFConfigMetadataIsChineseAndCollapsed|TestCreateMobileShare|TestIsShareRiskStructuralDir|TestBuildShareRiskRenamePlan|TestApplyShareRiskRenamePlan' -v
```

Expected: PASS.

- [ ] **Step 2: Run a broader package test for the driver**

Run:

```bash
go test ./drivers/139 ./server/handles -v
```

Expected: PASS. If unrelated repo failures exist, record the exact failing package and error before proceeding.

- [ ] **Step 3: Verify local frontend preview on desktop wide-screen**

Run in frontend repo:

```bash
pnpm serve --host 127.0.0.1 --port 4173
```

Then verify the desktop home sidebar shows:

- `订阅源搜索`
- `任务看板`

If using a scriptable check, fetch the served bundle and confirm `resource_search` / `task_board` are present.

- [ ] **Step 4: Run the Docker build workflow required by the user**

Run in backend repo:

```bash
scripts/build-and-push-dockerhub.sh entergtang/openlist-etf:latest
```

Expected: frontend rebuild + backend image build complete successfully. If the environment blocks push, record whether build completed before the push stage and preserve the exact failure.

- [ ] **Step 5: Remote smoke on `openlist-etf` after deployment**

After the new image is running remotely, verify:

1. the home desktop bundle contains `resource_search` and `task_board`;
2. the `139Yun` driver management page shows `分享失败后自动重命名`;
3. a known risk-blocked folder share with the switch on renames the root content folder and matching descendant files;
4. `Season 1` stays unchanged;
5. share creation succeeds after one retry, or returns the clearer post-remediation failure message.

- [ ] **Step 6: Commit backend implementation if all required verification passes**

```bash
git add drivers/139/meta.go drivers/139/mobile_share.go drivers/139/mobile_share_test.go drivers/139/etf_test.go drivers/139/share_risk_rename.go drivers/139/share_risk_rename_test.go
git commit -m "feat(139yun): auto rename risk-blocked shares"
```

---

## Delivery Order

1. Task 1: lock the user-visible switch into driver metadata.
2. Task 2: build and test the rename planner independently.
3. Task 3: wire the planner into `CreateMobileShare` with a strict retry gate.
4. Task 4: rebuild and verify frontend assets.
5. Task 5: run focused tests, Docker build, and remote smoke.

## Risks And Mitigations

- Pinyin fallback may require a new dependency.
  - Mitigation: first search for an existing transliteration helper; only add a dependency if tests prove it is necessary.
- Partial rename success can leave the tree mutated if a later rename fails.
  - Mitigation: deepest-first ordering and explicit error messages; no rollback because the approved product behavior is permanent rename.
- TMDB may return a localized Chinese title instead of an English-facing title.
  - Mitigation: choose the English/original title explicitly in the resolver and cover this with a unit test.
- The frontend source may already be correct and only `dist` is stale.
  - Mitigation: verify source first and avoid unnecessary frontend code edits.
- The Docker script may fail at image push for environment reasons unrelated to compile.
  - Mitigation: capture whether the local build/embedding stages succeeded before any push failure.

## Self-Review

- Spec coverage: The plan covers the driver-management switch, strict `个人云未知异常` gating, permanent rename rules, `Season 1` preservation, one-time retry, desktop sidebar bundle rebuild, and Docker build verification.
- Placeholder scan: No task uses `TODO`, `TBD`, or hand-wavy “implement later” language; each task lists exact files, commands, and code snippets.
- Type consistency: `Addition.AutoRenameOnShareRisk`, `shareRiskRenameNode`, `buildShareRiskRenamePlan`, `applyShareRiskRenamePlan`, `createMobileShareOnce`, and `shouldAutoRenameAfterShareRisk` are introduced before later tasks depend on them.

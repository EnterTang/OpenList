# Local File Plugins (AntiHash + ISO Rename) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Automatically modify Local-landed files (AntiHash trailer, then optional `name.ext.iso` rename) when plugins are enabled, and expose toggles plus an extension whitelist on a home-sidebar Plugins page.

**Architecture:** Add `internal/plugin` for AntiHash / ISO / whitelist / orchestration. After successful Local `op.Put` and same-storage `op.Copy` of a file, call a dedicated plugin hang point gated by `SkipPluginKey` (not `SkipHookKey`, because transfer tasks already set `SkipHookKey`). Settings are three PRIVATE `SettingItem`s. Frontend in `OpenList-Frontend` adds sidebar entry + Plugins settings page via existing admin setting APIs.

**Tech Stack:** Go 1.x (OpenList backend), SolidJS + Hope UI (OpenList-Frontend), existing `SettingItem` / `/admin/setting/*` APIs.

**Spec:** `docs/superpowers/specs/2026-07-28-local-file-plugins-antihash-iso-design.md`

## Global Constraints

- Only process files that have fully landed on storage whose `Config().Name == "Local"`.
- Two independent toggles; both on → AntiHash first, then ISO rename.
- Extension whitelist required; empty whitelist → process nothing.
- ISO naming: `movie.mkv` → `movie.mkv.iso`.
- AntiHash trailer: fixed 8-byte padding `OpenList` + 8-byte magic `ANTIHASH` (16 bytes total).
- Plugin failures log only; never fail the original put/copy/transfer.
- v1: no restore UI, no auto-restore, no stock-file scan, no non-Local processing.
- Frontend path: `/Volumes/extend Disk/Github/OpenList-Frontend`.
- Do not reuse directory-level `ObjsUpdateHook` for this feature.
- Use `SkipPluginKey` for plugin recursion avoidance; do **not** skip plugins merely because `SkipHookKey` is set.

## File Structure

### Backend (`OpenList`)

| File | Responsibility |
|------|----------------|
| `internal/conf/const.go` | Setting key constants + `SkipPluginKey` |
| `internal/bootstrap/data/setting.go` | Register three plugin settings |
| `internal/plugin/antihash.go` | Detect / modify / restore trailer on absolute paths |
| `internal/plugin/antihash_test.go` | AntiHash unit tests |
| `internal/plugin/whitelist.go` | Parse whitelist; match extension; temp-suffix skip |
| `internal/plugin/whitelist_test.go` | Whitelist / temp-name tests |
| `internal/plugin/iso.go` | Compute ISO target name; rename with conflict skip |
| `internal/plugin/iso_test.go` | ISO rename tests |
| `internal/plugin/process.go` | `ProcessAbsolutePath` + `MaybeAfterLocalWrite` |
| `internal/plugin/process_test.go` | Combined orchestration tests |
| `internal/op/fs.go` | Call `MaybeAfterLocalWrite` after Put/Copy success |

### Frontend (`OpenList-Frontend`)

| File | Responsibility |
|------|----------------|
| `src/pages/home/HomeAppSidebar.tsx` | Add `plugins` page key + icon |
| `src/pages/home/Layout.tsx` | Route `plugins` → `Plugins` page |
| `src/pages/home/Plugins.tsx` | Switches + whitelist editor + save |
| `src/lang/en/home.json` | English copy |
| `src/lang-overrides/zh-CN/home.json` | Simplified Chinese copy |
| `src/lang-overrides/zh-TW/home.json` | Traditional Chinese copy (sidebar label at minimum) |

---

### Task 1: AntiHash core library

**Files:**
- Create: `internal/plugin/antihash.go`
- Create: `internal/plugin/antihash_test.go`

**Interfaces:**
- Consumes: none
- Produces:
  - `var MagicTag = []byte("ANTIHASH")`
  - `var Padding = []byte("OpenList")` // len 8
  - `const TrailerSize = 16`
  - `func IsModified(path string) (bool, error)`
  - `func ModifyHash(path string) (changed bool, err error)`
  - `func RestoreHash(path string) (changed bool, err error)` // for tests / future; not wired to UI

- [ ] **Step 1: Write the failing tests**

```go
package plugin

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestModifyHashAppendsTrailerAndIsDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.mkv")
	if err := os.WriteFile(path, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	changed, err := ModifyHash(path)
	if err != nil || !changed {
		t.Fatalf("ModifyHash: changed=%v err=%v", changed, err)
	}
	ok, err := IsModified(path)
	if err != nil || !ok {
		t.Fatalf("IsModified: ok=%v err=%v", ok, err)
	}
	data, _ := os.ReadFile(path)
	if !bytes.HasSuffix(data, append(append([]byte{}, Padding...), MagicTag...)) {
		t.Fatalf("missing trailer: %q", data)
	}
	if len(data) != 5+TrailerSize {
		t.Fatalf("size=%d want=%d", len(data), 5+TrailerSize)
	}
}

func TestModifyHashSkipsWhenAlreadyModified(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.mkv")
	_ = os.WriteFile(path, []byte("hello"), 0o644)
	_, _ = ModifyHash(path)
	info1, _ := os.Stat(path)
	changed, err := ModifyHash(path)
	if err != nil || changed {
		t.Fatalf("second ModifyHash: changed=%v err=%v", changed, err)
	}
	info2, _ := os.Stat(path)
	if info1.Size() != info2.Size() {
		t.Fatalf("size grew on second modify")
	}
}

func TestRestoreHashTruncatesTrailer(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.mkv")
	_ = os.WriteFile(path, []byte("hello"), 0o644)
	_, _ = ModifyHash(path)
	changed, err := RestoreHash(path)
	if err != nil || !changed {
		t.Fatalf("RestoreHash: changed=%v err=%v", changed, err)
	}
	data, _ := os.ReadFile(path)
	if string(data) != "hello" {
		t.Fatalf("got %q", data)
	}
	ok, _ := IsModified(path)
	if ok {
		t.Fatal("still marked modified")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/plugin -run 'TestModifyHash|TestRestoreHash' -count=1`

Expected: FAIL (package or symbols not found)

- [ ] **Step 3: Implement AntiHash**

```go
package plugin

import (
	"bytes"
	"os"
	"sync"
)

var (
	MagicTag = []byte("ANTIHASH") // 8 bytes
	Padding  = []byte("OpenList") // 8 bytes
)

const TrailerSize = 16

var fileLocks sync.Map // path -> *sync.Mutex

func lockPath(path string) func() {
	v, _ := fileLocks.LoadOrStore(path, &sync.Mutex{})
	m := v.(*sync.Mutex)
	m.Lock()
	return m.Unlock
}

func IsModified(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	if info.Size() < int64(len(MagicTag)) {
		return false, nil
	}
	buf := make([]byte, len(MagicTag))
	if _, err := f.ReadAt(buf, info.Size()-int64(len(MagicTag))); err != nil {
		return false, err
	}
	return bytes.Equal(buf, MagicTag), nil
}

func ModifyHash(path string) (bool, error) {
	unlock := lockPath(path)
	defer unlock()
	ok, err := IsModified(path)
	if err != nil {
		return false, err
	}
	if ok {
		return false, nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return false, err
	}
	defer f.Close()
	payload := append(append([]byte{}, Padding...), MagicTag...)
	if _, err := f.Write(payload); err != nil {
		return false, err
	}
	return true, nil
}

func RestoreHash(path string) (bool, error) {
	unlock := lockPath(path)
	defer unlock()
	ok, err := IsModified(path)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	if info.Size() < TrailerSize {
		return false, nil
	}
	if err := os.Truncate(path, info.Size()-TrailerSize); err != nil {
		return false, err
	}
	return true, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/plugin -run 'TestModifyHash|TestRestoreHash' -count=1`

Expected: PASS

- [ ] **Step 5: Commit (OpenList)**

```bash
git add internal/plugin/antihash.go internal/plugin/antihash_test.go
git commit -m "$(cat <<'EOF'
feat(plugin): add AntiHash trailer modify and restore helpers

- Append fixed OpenList+ANTIHASH trailer to change file hash
- Detect existing magic and skip double-append
- Provide truncate-based restore for tests and future use

EOF
)"
```

---

### Task 2: Whitelist and ISO rename helpers

**Files:**
- Create: `internal/plugin/whitelist.go`
- Create: `internal/plugin/whitelist_test.go`
- Create: `internal/plugin/iso.go`
- Create: `internal/plugin/iso_test.go`

**Interfaces:**
- Consumes: none
- Produces:
  - `func ParseWhitelist(raw string) map[string]struct{}`
  - `func ExtensionAllowed(name string, whitelist map[string]struct{}) bool`
  - `func IsTempIncompleteName(name string) bool`
  - `func ISOTargetName(name string) (string, bool)` // ok=false if already .iso or empty
  - `func RenameToISO(absPath string) (newPath string, changed bool, err error)`

- [ ] **Step 1: Write the failing tests**

```go
package plugin

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseWhitelistAndMatch(t *testing.T) {
	wl := ParseWhitelist(" mkv, MP4 ,,avi ")
	if ExtensionAllowed("a.mkv", wl) != true {
		t.Fatal("mkv should match")
	}
	if ExtensionAllowed("a.MP4", wl) != true {
		t.Fatal("MP4 should match case-insensitively")
	}
	if ExtensionAllowed("a.txt", wl) != false {
		t.Fatal("txt should not match")
	}
	if ExtensionAllowed("a.mkv", ParseWhitelist("")) != false {
		t.Fatal("empty whitelist must deny all")
	}
}

func TestIsTempIncompleteName(t *testing.T) {
	for _, name := range []string{"a.mkv.tmp", "a.part", "a.aria2", "a.!qB", "a.crdownload"} {
		if !IsTempIncompleteName(name) {
			t.Fatalf("%s should be temp", name)
		}
	}
	if IsTempIncompleteName("a.mkv") {
		t.Fatal("a.mkv should not be temp")
	}
}

func TestISOTargetName(t *testing.T) {
	got, ok := ISOTargetName("movie.mkv")
	if !ok || got != "movie.mkv.iso" {
		t.Fatalf("got %q ok=%v", got, ok)
	}
	if _, ok := ISOTargetName("movie.mkv.iso"); ok {
		t.Fatal("already iso should skip")
	}
	if _, ok := ISOTargetName("movie.ISO"); ok {
		t.Fatal("already ISO should skip")
	}
}

func TestRenameToISO(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "movie.mkv")
	_ = os.WriteFile(src, []byte("x"), 0o644)
	newPath, changed, err := RenameToISO(src)
	if err != nil || !changed || filepath.Base(newPath) != "movie.mkv.iso" {
		t.Fatalf("new=%s changed=%v err=%v", newPath, changed, err)
	}
	if _, err := os.Stat(newPath); err != nil {
		t.Fatal(err)
	}
	// conflict
	src2 := filepath.Join(dir, "other.mkv")
	_ = os.WriteFile(src2, []byte("y"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "other.mkv.iso"), []byte("z"), 0o644)
	_, changed, err = RenameToISO(src2)
	if err != nil || changed {
		t.Fatalf("conflict should skip: changed=%v err=%v", changed, err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/plugin -run 'TestParseWhitelist|TestIsTemp|TestISO|TestRenameToISO' -count=1`

Expected: FAIL (undefined symbols)

- [ ] **Step 3: Implement helpers**

```go
// whitelist.go
package plugin

import (
	"path"
	"strings"
)

var tempSuffixes = []string{".tmp", ".part", ".aria2", ".!qb", ".crdownload"}

func ParseWhitelist(raw string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		ext := strings.ToLower(strings.TrimSpace(part))
		ext = strings.TrimPrefix(ext, ".")
		if ext == "" {
			continue
		}
		out[ext] = struct{}{}
	}
	return out
}

func ExtensionAllowed(name string, whitelist map[string]struct{}) bool {
	if len(whitelist) == 0 {
		return false
	}
	ext := strings.ToLower(strings.TrimPrefix(path.Ext(name), "."))
	if ext == "" {
		return false
	}
	_, ok := whitelist[ext]
	return ok
}

func IsTempIncompleteName(name string) bool {
	lower := strings.ToLower(name)
	for _, suf := range tempSuffixes {
		if strings.HasSuffix(lower, suf) {
			return true
		}
	}
	return false
}
```

```go
// iso.go
package plugin

import (
	"os"
	"path/filepath"
	"strings"
)

func ISOTargetName(name string) (string, bool) {
	if name == "" {
		return "", false
	}
	if strings.HasSuffix(strings.ToLower(name), ".iso") {
		return "", false
	}
	return name + ".iso", true
}

func RenameToISO(absPath string) (string, bool, error) {
	base := filepath.Base(absPath)
	targetName, ok := ISOTargetName(base)
	if !ok {
		return absPath, false, nil
	}
	dst := filepath.Join(filepath.Dir(absPath), targetName)
	if _, err := os.Stat(dst); err == nil {
		return absPath, false, nil // conflict: skip
	} else if !os.IsNotExist(err) {
		return absPath, false, err
	}
	if err := os.Rename(absPath, dst); err != nil {
		return absPath, false, err
	}
	return dst, true, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/plugin -run 'TestParseWhitelist|TestIsTemp|TestISO|TestRenameToISO' -count=1`

Expected: PASS

- [ ] **Step 5: Commit (OpenList)**

```bash
git add internal/plugin/whitelist.go internal/plugin/whitelist_test.go internal/plugin/iso.go internal/plugin/iso_test.go
git commit -m "$(cat <<'EOF'
feat(plugin): add extension whitelist and ISO rename helpers

- Parse comma-separated case-insensitive extension whitelist
- Skip temporary incomplete filenames
- Rename name.ext to name.ext.iso with conflict skip

EOF
)"
```

---

### Task 3: ProcessAbsolutePath orchestration

**Files:**
- Create: `internal/plugin/process.go`
- Create: `internal/plugin/process_test.go`

**Interfaces:**
- Consumes: `ModifyHash`, `RenameToISO`, `ParseWhitelist`, `ExtensionAllowed`, `IsTempIncompleteName`
- Produces:
  - `type ProcessOptions struct { AntiHash bool; ISORename bool; Whitelist string }`
  - `func ProcessAbsolutePath(absPath string, opts ProcessOptions) (resultPath string, err error)`
  - Order: whitelist/temp gates → AntiHash (if enabled) → ISO rename (if enabled)
  - Whitelist matching uses the **pre-rename** basename (e.g. `movie.mkv`, not `movie.mkv.iso`)

- [ ] **Step 1: Write the failing tests**

```go
package plugin

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProcessAbsolutePathHashThenISO(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "movie.mkv")
	_ = os.WriteFile(path, []byte("hello"), 0o644)
	out, err := ProcessAbsolutePath(path, ProcessOptions{
		AntiHash: true, ISORename: true, Whitelist: "mkv",
	})
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(out) != "movie.mkv.iso" {
		t.Fatalf("out=%s", out)
	}
	ok, err := IsModified(out)
	if err != nil || !ok {
		t.Fatalf("expected antihash on renamed file: ok=%v err=%v", ok, err)
	}
}

func TestProcessAbsolutePathRespectsWhitelistAndTemp(t *testing.T) {
	dir := t.TempDir()
	txt := filepath.Join(dir, "a.txt")
	_ = os.WriteFile(txt, []byte("x"), 0o644)
	out, err := ProcessAbsolutePath(txt, ProcessOptions{AntiHash: true, Whitelist: "mkv"})
	if err != nil || out != txt {
		t.Fatalf("txt should skip: out=%s err=%v", out, err)
	}
	tmp := filepath.Join(dir, "a.mkv.tmp")
	_ = os.WriteFile(tmp, []byte("x"), 0o644)
	out, err = ProcessAbsolutePath(tmp, ProcessOptions{AntiHash: true, Whitelist: "tmp,mkv"})
	// even if whitelist contains tmp as extension, temp suffix list wins
	if err != nil {
		t.Fatal(err)
	}
	ok, _ := IsModified(tmp)
	if ok {
		t.Fatal("temp file must not be modified")
	}
}

func TestProcessAbsolutePathEmptyWhitelistDoesNothing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.mkv")
	_ = os.WriteFile(path, []byte("x"), 0o644)
	out, err := ProcessAbsolutePath(path, ProcessOptions{AntiHash: true, ISORename: true, Whitelist: ""})
	if err != nil || out != path {
		t.Fatalf("out=%s err=%v", out, err)
	}
	ok, _ := IsModified(path)
	if ok {
		t.Fatal("should not modify")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/plugin -run TestProcessAbsolutePath -count=1`

Expected: FAIL

- [ ] **Step 3: Implement orchestration**

```go
package plugin

import (
	"fmt"
	"os"
	"path/filepath"
)

type ProcessOptions struct {
	AntiHash  bool
	ISORename bool
	Whitelist string
}

func ProcessAbsolutePath(absPath string, opts ProcessOptions) (string, error) {
	if !opts.AntiHash && !opts.ISORename {
		return absPath, nil
	}
	info, err := os.Stat(absPath)
	if err != nil {
		return absPath, err
	}
	if info.IsDir() {
		return absPath, nil
	}
	base := filepath.Base(absPath)
	if IsTempIncompleteName(base) {
		return absPath, nil
	}
	wl := ParseWhitelist(opts.Whitelist)
	if !ExtensionAllowed(base, wl) {
		return absPath, nil
	}
	result := absPath
	if opts.AntiHash {
		if _, err := ModifyHash(result); err != nil {
			return result, fmt.Errorf("antihash: %w", err)
		}
	}
	if opts.ISORename {
		newPath, _, err := RenameToISO(result)
		if err != nil {
			return result, fmt.Errorf("iso rename: %w", err)
		}
		result = newPath
	}
	return result, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/plugin -count=1`

Expected: PASS (all plugin tests)

- [ ] **Step 5: Commit (OpenList)**

```bash
git add internal/plugin/process.go internal/plugin/process_test.go
git commit -m "$(cat <<'EOF'
feat(plugin): orchestrate AntiHash then ISO rename for local files

- Gate on whitelist and temporary incomplete names
- Apply enabled steps in Hash-then-ISO order on absolute paths

EOF
)"
```

---

### Task 4: Settings keys and bootstrap registration

**Files:**
- Modify: `internal/conf/const.go` (add setting key constants + `SkipPluginKey`)
- Modify: `internal/bootstrap/data/setting.go` (register three items in `InitialSettings`)

**Interfaces:**
- Consumes: none
- Produces setting keys:
  - `conf.PluginAntiHashEnabled = "plugin_antihash_enabled"`
  - `conf.PluginISORenameEnabled = "plugin_iso_rename_enabled"`
  - `conf.PluginExtensionWhitelist = "plugin_extension_whitelist"`
  - `conf.SkipPluginKey` context key (next iota after `SkipHookKey`)

- [ ] **Step 1: Add constants to `internal/conf/const.go`**

In the settings key const block (near other feature keys), add:

```go
	PluginAntiHashEnabled    = "plugin_antihash_enabled"
	PluginISORenameEnabled   = "plugin_iso_rename_enabled"
	PluginExtensionWhitelist = "plugin_extension_whitelist"
```

In the `ContextKey` const block, append after `SkipHookKey`:

```go
	SkipPluginKey
```

- [ ] **Step 2: Register settings in `InitialSettings()`**

Add to the `initialSettingItems` slice (GLOBAL group is fine; keep them PRIVATE so only admin APIs expose them):

```go
		{Key: conf.PluginAntiHashEnabled, Value: "false", Type: conf.TypeBool, Group: model.GLOBAL, Flag: model.PRIVATE},
		{Key: conf.PluginISORenameEnabled, Value: "false", Type: conf.TypeBool, Group: model.GLOBAL, Flag: model.PRIVATE},
		{Key: conf.PluginExtensionWhitelist, Value: "", Type: conf.TypeText, Group: model.GLOBAL, Flag: model.PRIVATE, Help: "Comma-separated extensions without dots, e.g. mkv,mp4,avi. Empty means no files are processed."},
```

- [ ] **Step 3: Verify package builds**

Run: `go build ./internal/conf ./internal/bootstrap/data`

Expected: success

- [ ] **Step 4: Commit (OpenList)**

```bash
git add internal/conf/const.go internal/bootstrap/data/setting.go
git commit -m "$(cat <<'EOF'
feat(plugin): register AntiHash and ISO plugin settings

- Add plugin toggle and whitelist setting keys
- Add SkipPluginKey context key for recursion control

EOF
)"
```

---

### Task 5: Local write hang points in `op`

**Files:**
- Modify: `internal/plugin/process.go` (add `MaybeAfterLocalWrite`)
- Modify: `internal/op/fs.go` (call after Put/Copy success)
- Create: `internal/plugin/hook_test.go` (unit test for Local name gate via fake options path already covered; add table test for skip key behavior using Process options reading)

**Interfaces:**
- Consumes: `ProcessAbsolutePath`, `setting.GetBool` / `setting.GetStr`, `driver.Driver`
- Produces:
  - `func MaybeAfterLocalWrite(ctx context.Context, storage driver.Driver, actualFilePath string)`
  - Behavior:
    1. If `ctx.Value(conf.SkipPluginKey) != nil` → return
    2. If `storage.Config().Name != "Local"` → return
    3. If both plugin toggles false → return
    4. Resolve absolute disk path via `GetUnwrap` on storage+actualFilePath then `obj.GetPath()`
    5. `go` process with `context.WithoutCancel`; recover panics; log errors
    6. Does **not** check `SkipHookKey`

**Important:** Transfer / offline / subscription puts often set `SkipHookKey`. Plugins must still run. ISO rename must set `SkipPluginKey` only if it ever calls back into `op.Put`/`op.Copy` (current implementation uses `os.Rename` directly, so no re-entry).

- [ ] **Step 1: Add `MaybeAfterLocalWrite` to `internal/plugin/process.go`**

```go
import (
	"context"
	stdpath "path"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/setting"
	log "github.com/sirupsen/logrus"
)

// MaybeAfterLocalWrite runs enabled plugins after a file fully landed on Local storage.
// Safe to call synchronously; work is asynchronous. Never fails the caller.
func MaybeAfterLocalWrite(ctx context.Context, storage driver.Driver, actualFilePath string) {
	if ctx.Value(conf.SkipPluginKey) != nil {
		return
	}
	if storage == nil || storage.Config().Name != "Local" {
		return
	}
	anti := setting.GetBool(conf.PluginAntiHashEnabled)
	iso := setting.GetBool(conf.PluginISORenameEnabled)
	if !anti && !iso {
		return
	}
	whitelist := setting.GetStr(conf.PluginExtensionWhitelist)
	actualFilePath = stdpath.Clean(actualFilePath)
	bg := context.WithoutCancel(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("[plugin] panic processing %s: %v", actualFilePath, r)
			}
		}()
		obj, err := op.GetUnwrap(bg, storage, actualFilePath)
		if err != nil {
			log.Warnf("[plugin] get %s: %v", actualFilePath, err)
			return
		}
		abs := obj.GetPath()
		if abs == "" {
			log.Warnf("[plugin] empty absolute path for %s", actualFilePath)
			return
		}
		_, err = ProcessAbsolutePath(abs, ProcessOptions{
			AntiHash:  anti,
			ISORename: iso,
			Whitelist: whitelist,
		})
		if err != nil {
			log.Errorf("[plugin] process %s: %v", abs, err)
		}
	}()
}
```

**Cycle note:** `internal/setting` imports `op`, and `op` will import `plugin`, and `plugin` imports `op` + `setting`. That is an import cycle.

**Resolve cycle as follows (required):**

1. Do **not** import `setting` from `plugin`.
2. Pass booleans/whitelist into `MaybeAfterLocalWrite` from `op` (which already can call `GetSettingItemByKey`), OR put the hang-point function in `op` itself and keep `plugin` free of `op` imports.

Preferred (keep disk logic in `plugin`, hang point in `op`):

- Keep `ProcessAbsolutePath` in `plugin` (no `op` import).
- Add `maybeProcessLocalPlugin` private helper in `internal/op/fs.go` (or new `internal/op/plugin_hook.go`) that reads settings via existing `GetSettingItemByKey` / local helpers, resolves path with `GetUnwrap`, then calls `plugin.ProcessAbsolutePath`.

Delete the `MaybeAfterLocalWrite` version that imports `op`/`setting` if you added it; implement hang point only under `op`.

- [ ] **Step 2: Add `internal/op/plugin_hook.go`**

```go
package op

import (
	"context"
	stdpath "path"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/plugin"
	log "github.com/sirupsen/logrus"
)

func maybeProcessLocalPlugin(ctx context.Context, storage driver.Driver, actualFilePath string) {
	if ctx.Value(conf.SkipPluginKey) != nil || storage == nil {
		return
	}
	if storage.Config().Name != "Local" {
		return
	}
	anti := settingBool(conf.PluginAntiHashEnabled)
	iso := settingBool(conf.PluginISORenameEnabled)
	if !anti && !iso {
		return
	}
	whitelist := settingStr(conf.PluginExtensionWhitelist)
	actualFilePath = stdpath.Clean(actualFilePath)
	bg := context.WithoutCancel(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("[plugin] panic processing %s: %v", actualFilePath, r)
			}
		}()
		obj, err := GetUnwrap(bg, storage, actualFilePath)
		if err != nil {
			log.Warnf("[plugin] get %s: %v", actualFilePath, err)
			return
		}
		abs := obj.GetPath()
		if abs == "" {
			return
		}
		if _, err := plugin.ProcessAbsolutePath(abs, plugin.ProcessOptions{
			AntiHash: anti, ISORename: iso, Whitelist: whitelist,
		}); err != nil {
			log.Errorf("[plugin] process %s: %v", abs, err)
		}
	}()
}

func settingBool(key string) bool {
	item, _ := GetSettingItemByKey(key)
	if item == nil {
		return false
	}
	return item.Value == "true" || item.Value == "1"
}

func settingStr(key string) string {
	item, _ := GetSettingItemByKey(key)
	if item == nil {
		return ""
	}
	return item.Value
}
```

- [ ] **Step 3: Wire Put success in `internal/op/fs.go`**

In `Put`, inside `if err == nil { ... }` after cache update (and independent of `needHandleObjsUpdateHook`), add:

```go
		maybeProcessLocalPlugin(ctx, storage, dstPath)
```

`dstPath` is already `stdpath.Join(dstDirPath, file.GetName())` in that function.

- [ ] **Step 4: Wire Copy success in `internal/op/fs.go`**

In `Copy`, after successful copy and before/after objsUpdateHook, when `!srcObj.IsDir()`:

```go
	if !srcObj.IsDir() {
		maybeProcessLocalPlugin(ctx, storage, stdpath.Join(dstDirPath, srcObj.GetName()))
	}
```

Do **not** hook `Move` (same-storage move is rename of an already-landed file).

- [ ] **Step 5: Compile and run plugin + op tests**

Run:

```bash
go test ./internal/plugin ./internal/op -count=1
```

Expected: PASS (existing op tests still pass; plugin tests pass)

- [ ] **Step 6: Commit (OpenList)**

```bash
git add internal/op/plugin_hook.go internal/op/fs.go internal/plugin/process.go
git commit -m "$(cat <<'EOF'
feat(plugin): run Local plugins after successful Put and Copy

- Hang after Local file Put/Copy regardless of SkipHookKey
- Skip only when SkipPluginKey is set or plugins are disabled

EOF
)"
```

---

### Task 6: Frontend Plugins page and sidebar

**Files (in `/Volumes/extend Disk/Github/OpenList-Frontend`):**
- Modify: `src/pages/home/HomeAppSidebar.tsx`
- Modify: `src/pages/home/Layout.tsx`
- Create: `src/pages/home/Plugins.tsx`
- Modify: `src/lang/en/home.json`
- Modify: `src/lang-overrides/zh-CN/home.json`
- Modify: `src/lang-overrides/zh-TW/home.json`

**Interfaces:**
- Consumes: `GET /admin/setting/get?keys=plugin_antihash_enabled,plugin_iso_rename_enabled,plugin_extension_whitelist`
- Consumes: `POST /admin/setting/save` with `SettingItem[]`
- Produces: home page key `plugins`

- [ ] **Step 1: Add i18n strings**

`src/lang/en/home.json` — under `sidebar` add `"plugins": "Plugins"`, and add top-level:

```json
  "plugins": {
    "title": "Plugins",
    "antihash": "Modify file hash",
    "antihash_help": "After a file fully lands on Local storage, append a short trailer so its hash changes. Only whitelisted extensions are processed.",
    "iso_rename": "Rename extension to .iso",
    "iso_rename_help": "Rename name.ext to name.ext.iso after the file fully lands on Local storage.",
    "whitelist": "Extension whitelist",
    "whitelist_help": "Comma-separated extensions without dots (e.g. mkv,mp4,avi). Empty means no files are processed.",
    "whitelist_placeholder": "mkv,mp4,avi,ts,zip,rar",
    "order_note": "When both plugins are enabled, hash is modified first, then the file is renamed.",
    "save": "Save",
    "saved": "Plugins settings saved",
    "admin_only": "Admin permission required to change plugin settings"
  }
```

`src/lang-overrides/zh-CN/home.json` — under `sidebar` add `"plugins": "插件"`, and add:

```json
  "plugins": {
    "title": "插件",
    "antihash": "修改文件 Hash",
    "antihash_help": "文件完整落到 Local 存储后，追加短尾部数据以改变 Hash。仅处理白名单后缀。",
    "iso_rename": "修改后缀为 .iso",
    "iso_rename_help": "文件完整落到 Local 后，将 name.ext 重命名为 name.ext.iso。",
    "whitelist": "后缀白名单",
    "whitelist_help": "逗号分隔、不含点的后缀（如 mkv,mp4,avi）。为空则不处理任何文件。",
    "whitelist_placeholder": "mkv,mp4,avi,ts,zip,rar",
    "order_note": "两个插件都开启时，先修改 Hash，再改名为 .iso。",
    "save": "保存",
    "saved": "插件设置已保存",
    "admin_only": "需要管理员权限才能修改插件设置"
  }
```

`zh-TW`: at least add `"plugins": "外掛"` under `sidebar` (and mirror plugins block if convenient).

- [ ] **Step 2: Update `HomeAppSidebar.tsx`**

- Extend `HomePageKey` with `| "plugins"`.
- Import an icon (e.g. `AiOutlineAppstore` from `solid-icons/ai`).
- Append `{ key: "plugins", icon: AiOutlineAppstore }` to `pageItems`.

- [ ] **Step 3: Update `Layout.tsx`**

- Import `Plugins` from `./Plugins`.
- Allow `"plugins"` in `initialHomePage`.
- Add:

```tsx
            <Match when={activePage() === "plugins"}>
              <HomeContentPanel>
                <Plugins />
              </HomeContentPanel>
            </Match>
```

- [ ] **Step 4: Create `Plugins.tsx`**

Implement a page that:

1. On mount, `GET /admin/setting/get?keys=plugin_antihash_enabled,plugin_iso_rename_enabled,plugin_extension_whitelist`.
2. Renders two Hope `Switch`es and one text input/textarea bound to local store.
3. Save posts the three `SettingItem` objects back via `/admin/setting/save` (preserve `type`/`group`/`flag` from GET response; only change `value`).
4. If response is 401/403, show `home.plugins.admin_only`.
5. Use `UserMethods.is_admin(me())` to disable inputs for non-admin if `me()` is available (same pattern as other home admin pages).

Skeleton:

```tsx
import { Button, FormControl, FormHelperText, FormLabel, Heading, Input, Switch, VStack, Text } from "@hope-ui/solid"
import { createSignal, onMount, Show } from "solid-js"
import { createStore } from "solid-js/store"
import { useT } from "~/hooks"
import { me } from "~/store"
import { SettingItem, UserMethods } from "~/types"
import { handleResp, notify, r } from "~/utils"

const KEYS = [
  "plugin_antihash_enabled",
  "plugin_iso_rename_enabled",
  "plugin_extension_whitelist",
] as const

export const Plugins = () => {
  const t = useT()
  const [items, setItems] = createStore<SettingItem[]>([])
  const [loading, setLoading] = createSignal(true)
  const [saving, setSaving] = createSignal(false)
  const canEdit = () => UserMethods.is_admin(me())

  const valueOf = (key: string) => items.find((i) => i.key === key)?.value ?? ""
  const setValue = (key: string, value: string) => {
    const idx = items.findIndex((i) => i.key === key)
    if (idx >= 0) setItems(idx, "value", value)
  }

  const refresh = async () => {
    setLoading(true)
    const resp = await r.get(`/admin/setting/get?keys=${KEYS.join(",")}`)
    handleResp(resp, (data: SettingItem[]) => setItems(data))
    setLoading(false)
  }

  onMount(refresh)

  const save = async () => {
    setSaving(true)
    const resp = await r.post("/admin/setting/save", items)
    handleResp(resp, () => notify.success(t("home.plugins.saved")))
    setSaving(false)
  }

  return (
    <VStack alignItems="stretch" spacing="$4" w="$full">
      <Heading size="lg">{t("home.plugins.title")}</Heading>
      <Text fontSize="$sm" color="$neutral10">{t("home.plugins.order_note")}</Text>
      <Show when={!canEdit()}>
        <Text color="$warning10">{t("home.plugins.admin_only")}</Text>
      </Show>
      <FormControl>
        <FormLabel>{t("home.plugins.antihash")}</FormLabel>
        <Switch
          disabled={!canEdit() || loading()}
          checked={valueOf("plugin_antihash_enabled") === "true" || valueOf("plugin_antihash_enabled") === "1"}
          onChange={(e: any) => setValue("plugin_antihash_enabled", e.currentTarget.checked ? "true" : "false")}
        />
        <FormHelperText>{t("home.plugins.antihash_help")}</FormHelperText>
      </FormControl>
      <FormControl>
        <FormLabel>{t("home.plugins.iso_rename")}</FormLabel>
        <Switch
          disabled={!canEdit() || loading()}
          checked={valueOf("plugin_iso_rename_enabled") === "true" || valueOf("plugin_iso_rename_enabled") === "1"}
          onChange={(e: any) => setValue("plugin_iso_rename_enabled", e.currentTarget.checked ? "true" : "false")}
        />
        <FormHelperText>{t("home.plugins.iso_rename_help")}</FormHelperText>
      </FormControl>
      <FormControl>
        <FormLabel>{t("home.plugins.whitelist")}</FormLabel>
        <Input
          disabled={!canEdit() || loading()}
          value={valueOf("plugin_extension_whitelist")}
          placeholder={t("home.plugins.whitelist_placeholder")}
          onInput={(e) => setValue("plugin_extension_whitelist", e.currentTarget.value)}
        />
        <FormHelperText>{t("home.plugins.whitelist_help")}</FormHelperText>
      </FormControl>
      <Button disabled={!canEdit()} loading={saving()} onClick={save} alignSelf="start">
        {t("home.plugins.save")}
      </Button>
    </VStack>
  )
}

export default Plugins
```

Adjust Switch `onChange` typing to match Hope UI’s actual event API used elsewhere in the repo if different.

- [ ] **Step 5: Build frontend**

Run (in OpenList-Frontend):

```bash
pnpm exec tsc --noEmit
```

Expected: no errors from the new files (fix pre-existing unrelated errors only if already present).

- [ ] **Step 6: Commit (OpenList-Frontend)**

```bash
git add src/pages/home/HomeAppSidebar.tsx src/pages/home/Layout.tsx src/pages/home/Plugins.tsx \
  src/lang/en/home.json src/lang-overrides/zh-CN/home.json src/lang-overrides/zh-TW/home.json
git commit -m "$(cat <<'EOF'
feat(home): add Plugins sidebar page for AntiHash and ISO rename

- Add plugins entry to home app sidebar
- Wire settings toggles and extension whitelist editor

EOF
)"
```

---

### Task 7: Manual verification checklist

**Files:** none (verification only)

- [ ] **Step 1: Backend unit suite**

Run (OpenList):

```bash
go test ./internal/plugin ./internal/op -count=1
```

Expected: PASS

- [ ] **Step 2: Manual Local upload path**

1. Start OpenList with a Local storage mount.
2. Open home → Plugins; enable both toggles; set whitelist `mkv`.
3. Upload `sample.mkv` into Local.
4. Confirm file becomes `sample.mkv.iso` and last 8 bytes are `ANTIHASH`.
5. Upload `notes.txt`; confirm unchanged.
6. Disable plugins; upload another `sample2.mkv`; confirm unchanged name/hash trailer.

- [ ] **Step 3: Confirm transfer SkipHook still gets plugins**

Copy/move a whitelisted file from a cloud storage onto Local (path that uses transfer `Put` with `SkipHookKey`). Confirm plugins still apply.

- [ ] **Step 4: Final commits if verification prompted fixes**

Only commit real fixes discovered during verification; do not invent changelog entries.

---

## Spec Coverage Self-Review

| Spec requirement | Task |
|------------------|------|
| AntiHash append protocol (`OpenList`+`ANTIHASH`) | Task 1 |
| ISO `name.ext` → `name.ext.iso` | Task 2 |
| Whitelist + empty deny-all | Task 2–3 |
| Temp incomplete skip | Task 2–3 |
| Hash then ISO order | Task 3 |
| Settings keys | Task 4 |
| Local post-write hang (Put/Copy), not ObjsUpdateHook | Task 5 |
| Must run even when SkipHookKey set | Task 5 |
| SkipPluginKey recursion control | Task 4–5 |
| Home sidebar Plugins page | Task 6 |
| No restore / no stock scan / log-only errors | Tasks 3, 5, 7 |
| Frontend in OpenList-Frontend | Task 6 |

## Placeholder / Consistency Check

- No TBD/TODO left in tasks.
- Hang-point lives in `op` to avoid `plugin ? op` import cycle (explicitly called out in Task 5).
- Setting key names match the design spec exactly.
- ISO and AntiHash symbols used in later tasks match Task 1–2 definitions.

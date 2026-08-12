# Mobile 139 Share Risk Rename Design

## Goal

Deliver the remaining user-visible work for the OpenList + OpenList-Frontend pair with a narrow, deployable scope:

- Ensure the home desktop wide-screen sidebar exposes `订阅源搜索` and `任务看板` after rebuilding and shipping the updated frontend bundle.
- Add a `139Yun` driver-level switch named `分享失败后自动重命名` on the driver management page.
- When Mobile 139 share creation returns the specific risk-shaped failure `500 + 个人云未知异常`, optionally perform permanent rename remediation and retry share creation once.
- For rename remediation, replace Chinese title text with a TMDB English title when available, otherwise fall back to pinyin.
- Preserve season / episode information and keep structural directories such as `Season 1` unchanged.

The implementation should stay focused on the `139Yun` `personal_new` driver because Mobile Share is already implemented only there and the existing ETF/TMDB logic is already concentrated in that driver.

## Non-Goals

- Do not change share behavior for other storage drivers.
- Do not trigger rename remediation for generic share failures.
- Do not add temporary rename-and-restore behavior; rename is permanent by explicit product decision.
- Do not build a new settings page or custom frontend form for the new switch; use existing driver metadata rendering.
- Do not attempt multi-title inference across different series or movies inside one shared folder in this iteration.

## Product Decisions

The following decisions were explicitly approved during design:

- The new switch lives on the Mobile 139 driver management page.
- The switch is a persistent driver setting, not a per-share dialog toggle.
- Rename remediation triggers only for the specific failure pattern `500 + 个人云未知异常`.
- When the share target is a folder, rename scope includes:
  - the target folder itself;
  - descendant files whose names contain the content title;
  - descendant content directories when applicable.
- Structural directories such as `Season 1` must not be renamed.
- Rename is permanent and is not rolled back after share creation succeeds.

## Existing Context

### Frontend Sidebar

`OpenList-Frontend` source already contains:

- `resource_search` in `src/pages/home/HomeAppSidebar.tsx` and `src/pages/home/Layout.tsx`
- `task_board` in `src/pages/home/HomeAppSidebar.tsx` and `src/pages/home/Layout.tsx`

The observed problem is not missing source integration. The current local and remote bundles are stale, so the work required here is rebuild / embed / deploy verification.

### Mobile 139 Share Creation

Current share creation lives in:

- `server/handles/mobile_share.go`
- `drivers/139/mobile_share.go`

The current flow returns the upstream 139 message directly. When the upstream service replies with `个人云未知异常`, the user sees a plain 500 without actionable remediation.

### Reusable Building Blocks

The repository already has useful pieces to reuse:

- `drivers/139/Rename` for file/folder rename via `/file/update`
- `drivers/139` personal-new list APIs for recursive traversal
- `internal/media/recognize` for parsing title / season / episode signals from filenames
- `internal/media/tmdb` for TMDB lookup and metadata resolution
- ETF naming logic in `drivers/139/etf.go`, which already preserves season-oriented structure and knows how to apply TMDB-derived naming

## Settings

### New 139 Driver Setting

Add one new field to `drivers/139/meta.go` `Addition`:

- `auto_rename_on_share_risk`: bool, default `false`
- label: `分享失败后自动重命名`
- group: `ETF`
- collapsed: `true`
- help:
  `创建移动分享链接返回“个人云未知异常”时，自动将目标文件或目录内含中文标题的名称改为 TMDB 英文名；若无法匹配则改为拼音，并保留季数集数后重试一次分享。改名为永久生效。`

This keeps the switch inside the existing automatically rendered driver config UI and avoids dedicated frontend form work.

## High-Level Approach

### Recommended Approach

Use a precise retry wrapper around Mobile 139 share creation:

1. Try normal share creation once.
2. If it succeeds, return immediately.
3. If it fails, check whether the failure matches the approved trigger:
   - driver type is `personal_new`
   - setting `auto_rename_on_share_risk` is enabled
   - upstream message is `个人云未知异常`
4. Build a rename plan for the target file or folder tree.
5. If the rename plan is empty, return the original error without retry.
6. Execute permanent rename operations from deepest node to shallowest node.
7. Retry share creation exactly once.
8. Return the retried result or a clearer wrapped error.

### Rejected Alternatives

- Rename on every share failure: too risky and likely to mutate names for unrelated server/network errors.
- Pinyin-only rename: simpler, but does not satisfy the approved preference for TMDB English names when possible.
- Per-file TMDB lookup across the whole tree: higher latency, more TMDB traffic, and unnecessary complexity for the first version.

## Components

### `drivers/139/meta.go`

Responsibilities:

- Declare the new driver setting metadata.
- Place the new switch with existing ETF-related controls.

### `drivers/139/mobile_share.go`

Responsibilities:

- Keep the current normal share creation path.
- Detect the approved risk-shaped failure.
- Trigger rename remediation when enabled.
- Retry share creation once after rename.
- Return richer errors and logs when remediation was attempted.

### New helper logic inside `drivers/139`

Add narrowly scoped helper functions to keep `CreateMobileShare` readable:

- `shouldAutoRenameAfterShareRisk(err error) bool`
- `createMobileShareOnce(...)`
- `buildShareRiskRenamePlan(...)`
- `applyShareRiskRenamePlan(...)`
- `resolveShareRiskCanonicalTitle(...)`
- `renameShareRiskNode(...)`

The exact function names may vary, but responsibilities should remain split this way.

### Optional small helper package for transliteration

If no existing pinyin utility already exists in the repository, introduce the smallest practical helper for converting Chinese text to pinyin with spacing normalization. Keep it narrow and deterministic.

## Rename Model

### Canonical Title Strategy

One shared target gets one canonical replacement title.

Priority:

1. TMDB English title
2. Pinyin derived from the recognized Chinese title
3. If neither yields a meaningful replacement, do not rename and return the original share error

The design intentionally uses one canonical title for the whole shared target rather than inferring different titles for each descendant item.

### Title Resolution

For the root target:

- Use `internal/media/recognize` on the target name and parent path.
- Use `internal/media/tmdb` to resolve metadata.
- Prefer TMDB original / English-facing title over localized Chinese title.
- If TMDB does not return a usable English title, derive pinyin from the recognized or original title segment.

### Scope Rules

#### Shared file

Rename only the file itself.

#### Shared folder

Traverse the directory tree recursively and evaluate rename candidates.

Rename candidates include:

- the root folder itself;
- descendant files whose names contain the root content title;
- descendant content directories whose names also represent the content title.

Do not rename:

- structural directories like `Season 1`, `Season 01`, `Specials`, `Extras`;
- nodes whose names are already English / pinyin and contain no Chinese title text requiring replacement;
- nodes where no safe replacement can be constructed.

### Preservation Rules

Rename only the title portion of a node name.

Preserve:

- season markers like `Season 1`, `S01`, `第1季`
- episode markers like `E01`, `S01E01`, `第1集`
- years such as `(2024)` or `.2024.` where already present
- file extensions such as `.etf`, `.mkv`, `.mp4`

Examples:

- `非分之罪` -> `Guilt`
- `非分之罪 S01E01.etf` -> `Guilt S01E01.etf`
- `非分之罪.2024.S01E01.mkv` -> `Guilt.2024.S01E01.mkv`
- `非分之罪/Season 1/非分之罪 S01E01.etf` -> `Guilt/Season 1/Guilt S01E01.etf`

### Execution Order

Apply rename operations from deepest path to shallowest path:

1. descendant files
2. descendant directories
3. root folder

This prevents parent-path mutation from invalidating later child operations.

## Flow

### Desktop Sidebar Delivery

1. Rebuild `OpenList-Frontend/dist` from the already updated source.
2. Confirm the built bundle contains the new home sidebar items.
3. Use the existing backend build workflow to embed `dist` into `public/dist` during Docker build.
4. Verify desktop wide-screen sidebar now shows:
   - `订阅源搜索`
   - `任务看板`

### Share Creation With Optional Risk Remediation

1. `server/handles/mobile_share.go` resolves the target object and calls the driver share creator.
2. The driver tries one normal share creation request.
3. If success, return the created link.
4. If failure, test the remediation gate:
   - feature enabled
   - `personal_new`
   - upstream message equals `个人云未知异常`
5. Build the canonical title and rename plan.
6. If no plan items are produced, return the original error.
7. Apply the rename plan deepest-first.
8. Retry share creation once.
9. If retry succeeds, return the link.
10. If retry fails, return a wrapped error indicating that automatic rename was attempted.

## Error Handling

- Keep the trigger strict: only `个人云未知异常` should start rename remediation.
- If traversal, TMDB lookup, pinyin generation, or rename plan construction fails, return the original share failure plus remediation context in logs.
- If rename partially succeeds and a later rename fails, return a clear error that the target may already be partially renamed. Do not attempt rollback because the approved behavior is permanent rename and rollback adds more risk.
- If share retry fails after successful rename, return a clearer message such as:
  `个人云未知异常，已尝试自动重命名后重新创建分享，但仍失败`
- Logs should include:
  - storage ID / mount path
  - target object ID and original name
  - whether target is a directory
  - whether auto-rename was enabled
  - canonical replacement title source (`tmdb` or `pinyin`)
  - rename plan count
  - retry result

## Testing

Add focused tests for:

### Driver metadata

- new `auto_rename_on_share_risk` item exists in 139 driver metadata
- label/help/group/collapsed metadata are correct and localized

### Share retry behavior

- successful first share does not trigger rename
- non-matching errors do not trigger rename
- `个人云未知异常` triggers rename only when switch is enabled
- empty rename plan does not retry
- retry happens exactly once after successful rename plan application

### Rename scope and naming

- file target renames only the file
- folder target renames root folder and matching descendant files
- `Season 1` remains unchanged
- title replacement preserves season / episode / year / extension markers
- TMDB English title is preferred over pinyin
- pinyin fallback is used when TMDB English title is unavailable
- deepest-first ordering is respected

### Build / regression verification

- rebuilt frontend bundle contains the sidebar entries
- desktop wide-screen home sidebar renders the two entries after deployment
- `scripts/build-and-push-dockerhub.sh entergtang/openlist-etf:latest` completes successfully

## Risks

- TMDB can misidentify ambiguous titles. The design limits the blast radius by using a strict trigger and one canonical title per share target.
- Permanent rename means partially completed rename plans cannot be transparently undone if a later step fails.
- Some folder trees may contain mixed unrelated content; the single-title approach intentionally avoids broad media-library inference, but that also means some descendants may remain unrenamed.
- Introducing a pinyin dependency should be done carefully to avoid excessive dependency growth if no suitable existing utility is already present.

## Acceptance Criteria

The work is complete when all of the following are true:

- `139Yun` driver management page shows the new `分享失败后自动重命名` switch.
- A Mobile 139 share request that fails with `个人云未知异常` and has the switch enabled attempts permanent rename remediation and retries once.
- Folder remediation renames the root content folder and matching descendant files while leaving `Season 1`-style structure directories unchanged.
- Desktop wide-screen home sidebar shows `订阅源搜索` and `任务看板` after rebuilt assets are deployed.
- The Docker build script still compiles successfully for `entergtang/openlist-etf:latest`.

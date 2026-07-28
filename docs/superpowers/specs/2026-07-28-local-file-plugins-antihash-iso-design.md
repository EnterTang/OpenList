# Local File Plugins: AntiHash and ISO Rename Design

## Goal

Add a home-sidebar **Plugins** page with two independently toggleable automatic plugins that run only after a file has fully landed on a **Local** storage:

1. **AntiHash** ¡ª append a magic-tagged trailer so the file hash changes without rewriting existing bytes.
2. **ISO rename** ¡ª rename `name.ext` to `name.ext.iso`.

When both are enabled, process **AntiHash first**, then ISO rename.

## Confirmed Decisions

- Interaction model: automatic post-write processing (not manual per-file toolbar actions).
- Trigger scope: any successful write path that leaves a complete file on Local (upload, copy/move onto Local, offline download / subscription transfer that ultimately puts into Local, etc.).
- Completeness: run only after the write API returns success; canceled / partial Local puts that remove the file must not trigger processing.
- Plugin combination: two independent toggles; both on ¡ú Hash then ISO.
- File filter: user-configured extension whitelist; empty whitelist means process nothing.
- ISO naming: `movie.mkv` ¡ú `movie.mkv.iso` (preserve original extension in the filename).
- v1 restore: out of scope (no UI restore, no auto-restore when disabling plugins, no bulk reverse of existing files).
- Frontend lives in `/Volumes/extend Disk/Github/OpenList-Frontend`.

## Approach

Use a **dedicated Local post-write hook** at the `op` layer for the single completed file. Do **not** reuse directory-level `ObjsUpdateHook` (it lists whole directories and is gated by `handle_hook_after_writing`).

## Architecture

### Repositories

| Area | Repo |
|------|------|
| Processing, settings, write hooks | `OpenList` |
| Home sidebar Plugins page | `OpenList-Frontend` |

### Backend package: `internal/plugin`

New package responsibilities:

- AntiHash modify / detect (restore helpers may exist for tests / future use, but are unused by v1 UI).
- ISO rename helper.
- Unified entry: `ProcessLocalFile(ctx, storage, virtualPath)`:
  1. Confirm storage is Local and resolve absolute disk path.
  2. Gate checks (file exists, not dir, whitelist, not temp name).
  3. If AntiHash enabled ¡ú append trailer if not already tagged.
  4. If ISO rename enabled ¡ú rename to `*.iso` if not already ending in `.iso`.
  5. Log failures; never fail the original put/copy task; do not roll back a successful prior step.

### Write success hang points

After a successful single-file write whose destination storage is Local, and when the write context is not marked `SkipHook`, asynchronously call `ProcessLocalFile` for **that file only**.

Must cover:

- `op.Put` success
- Copy onto Local that results in a complete destination file
- Transfer / offline / subscription paths that complete via the above put/copy paths

Must avoid recursion:

- ISO rename and cache refresh use `SkipHook` (or an equivalent plugin-internal guard) so rename does not re-enter processing.

### Configuration (`SettingItem`)

Register in bootstrap settings (admin-writable, `Flag: PRIVATE`):

| Key | Type | Default | Meaning |
|-----|------|---------|---------|
| `plugin_antihash_enabled` | bool | `false` | Enable AntiHash |
| `plugin_iso_rename_enabled` | bool | `false` | Enable ISO rename |
| `plugin_extension_whitelist` | text | `""` | Comma-separated extensions without dots, case-insensitive (e.g. `mkv,mp4,avi`) |

No new REST resource; the Plugins page uses existing settings APIs.

## AntiHash Protocol

Trailer layout:

```text
[original bytes][padding bytes][8-byte magic "ANTIHASH"]
```

- Magic tag: fixed 8 bytes `ANTIHASH`.
- Padding: fixed 8 bytes `OpenList` (not random), so the total trailer is always 16 bytes and a future restore can `ftruncate(size - 16)`.
- Modify: open append-only; write `OpenList` + `ANTIHASH`; if tail already equals magic, skip.
- Detect: read last 8 bytes; compare to magic.
- Concurrency: short per-file lock (mutex and/or flock) around modify to avoid corrupted trailers.

## ISO Rename Rules

- Input: `movie.mkv` ¡ú output: `movie.mkv.iso`.
- Skip if the name already ends with `.iso` (case-insensitive).
- Skip if the destination name already exists; log and leave the file as-is.
- After rename, refresh Local caches as existing rename paths do.

## Gate Checks (silent skip)

Skip processing when any of the following is true:

- Plugin(s) relevant to the step are disabled
- Storage is not Local
- Path is a directory
- File does not exist (incomplete / removed)
- Extension not in whitelist, or whitelist is empty
- Temporary / incomplete name suffixes, including at least: `.tmp`, `.part`, `.aria2`, `.!qB`, `.crdownload`
- AntiHash step: already tagged
- ISO step: already ends with `.iso`, or rename target conflicts

## Frontend Design (`OpenList-Frontend`)

### Sidebar

- Extend `HomePageKey` with `"plugins"`.
- Add item in `HomeAppSidebar` page list with a plugin-like icon.
- i18n key: `home.sidebar.plugins` (at least zh / en).
- Persist active page via existing `home_app_page` localStorage handling.

### Page: `src/pages/home/Plugins.tsx`

Layout follows existing home subpages (`ClusterControl` / manage form patterns):

1. Switch ¡ª AntiHash (`plugin_antihash_enabled`) + short help text.
2. Switch ¡ª ISO rename (`plugin_iso_rename_enabled`) + note that naming is `name.ext.iso`.
3. Text field ¡ª extension whitelist (`plugin_extension_whitelist`) with placeholder `mkv,mp4,avi,ts,zip,rar` and help: empty means no files are processed; only complete Local landings matching the whitelist are processed.
4. Note: when both enabled, order is Hash then rename; v1 has no restore UI.

Admin-only write; non-admin behavior matches other private settings (read-only or permission denied).

Wire through `Layout.tsx` `Switch`/`Match` like other home sidebar pages.

## Error Handling

- Plugin errors are logged only; they must not change the success/failure of the original write/copy/transfer task.
- If AntiHash succeeds and ISO rename fails, leave the hash-modified file under the original name.
- Do not attempt compensating restore in v1.

## Out of Scope (v1)

- Hash restore UI / batch restore
- Stripping `.iso` back to original name via UI
- Auto-restore when a plugin is turned off
- Full-disk scan of existing Local files
- Processing non-Local storages
- Manual per-file toolbar / context-menu actions

## Testing

### Backend unit tests

- AntiHash: unmodified ¡ú modify ¡ú detect; no double-append; truncate restore helper correctness (even if unused by UI).
- Whitelist: empty / match / mismatch / case-insensitivity.
- ISO: `a.mkv` ¡ú `a.mkv.iso`; already `.iso` skip; destination conflict skip.
- Combined: both on ¡ú trailer then rename; each alone behaves correctly.
- Temp-suffix files skipped.

### Integration / manual

- Complete Local upload triggers processing when plugins enabled.
- Canceled Local upload does not leave a processable partial and does not trigger plugins.
- Copy onto Local triggers; copy onto cloud storage does not.
- Plugins page saves toggles and whitelist; subsequent new files respect settings.
- Home sidebar shows Plugins entry and navigates correctly.

## Success Criteria

- Admin can enable either or both plugins from the home Plugins page and set an extension whitelist.
- After a file fully lands on Local with a whitelisted extension, enabled plugins run automatically in the agreed order.
- Original transfer/upload success is unaffected by plugin failures.
- No restore or stock-file scanning behavior in v1.

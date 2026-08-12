# Pan123 FastLink Import Design

## Goal

Extend OpenList subscription manual imports so users can paste `123FastLink` content directly and reuse the existing temp-transfer workflow for `123pan`.

This iteration should support three input families:

- single-file `123FSLinkV2` text;
- folder / common-path `123FLCPV2` text;
- `123FastLink` JSON exports, including the official object shape and approved looser variants.

The result should let the existing manual subscription flow act as the explicit import entry for raw fastlink content without adding a separate import API.

## Non-Goals

- Do not redesign `ShareRef` into a generic import container.
- Do not change the external subscription CRUD or preview/check route paths.
- Do not add a new standalone database model for fastlink imports.
- Do not broaden this work to non-`123pan` providers.
- Do not attempt to support every historical or unofficial fastlink variant in one pass.
- Do not require frontend-only implementation to unlock backend support.

## Existing Context

Current relevant behavior already exists in several pieces:

- `internal/subscription/share_url.go`
  - parses regular cloud share URLs;
  - now supports single-file `123FSLinkV2` as a `ShareProviderPan123` share reference.
- `internal/subscription/share_123.go`
  - treats `123FSLinkV2` as a single-file share tree;
  - reuses existing `upload_request` save logic using `etag`, `size`, and `file_name`.
- `internal/subscription/share_save.go`
  - saves files by collecting a share tree, filtering entries, grouping files by parent, and calling `SaveShareItems`.
- `internal/subscription/service.go`
  - `runManual` currently reads `paths` and `links` from manual source config;
  - `links` already flow into `trySaveShareLinkToTemp`.
- `server/handles/subscription.go`
  - create, update, preview, and check endpoints already exist and can carry richer `source_config` payloads without new route design.

The approved design must stay aligned with the current split:

- share URLs are handled through `ParseShareURL` and `trySaveShareLinkToTemp`;
- batch import payloads are handled as manual import content, not as share URLs.

## Approved Product Decisions

The following decisions are already confirmed:

- Reuse the existing manual subscription model instead of creating a dedicated import API.
- Add a new raw text field to manual source config rather than forcing callers to split everything into `links`.
- Keep `links` for regular share URLs and legacy usage.
- Treat `123FLCPV2` and JSON as import payloads, not `ShareRef` inputs.
- Continue to use the existing title-matching / temp-transfer filtering behavior so imported files only save when they match the subscription.
- JSON import should be loose enough to accept the official export structure and close variants.

## Recommended Approach

Introduce a small import parser and a parallel save path for pre-expanded `123pan` file metadata.

The design should keep these responsibilities separate:

- `ParseShareURL`
  - regular share URLs;
  - single-file `123FSLinkV2`.
- `share_import.go`
  - parse raw manual import text;
  - support `123FLCPV2` and JSON;
  - normalize imported file metadata into one internal representation.
- `runManual`
  - continue to process `links` as before;
  - additionally parse and execute `imports_text` when present.
- `SaveImportedFilesToTemp`
  - save already-parsed imported files into the configured temp root using the existing `ShareSaver` save API.

This keeps the current share-link architecture stable while adding a clean path for folder imports.

## Manual Source Config Changes

Extend `model.SubscriptionManualSourceConfig` with one new field:

- `ImportsText string \`json:"imports_text"\``

Semantics:

- `paths`
  - existing snapshot roots;
  - unchanged.
- `links`
  - existing regular share-link array;
  - unchanged.
- `imports_text`
  - raw pasted import text;
  - supports:
    - `123FSLinkV2`;
    - `123FLCPV2`;
    - `123FastLink` JSON;
    - mixed multi-line text containing supported import payloads.

`parseManualConfig` should trim and preserve this field alongside existing cleaned `paths` and `links`.

## Internal Import Model

Introduce one internal normalized representation, for example:

- `pan123ImportedFile`
  - `Etag string`
  - `Size int64`
  - `Path string`
  - `Name string`

Normalization requirements:

- `Etag`
  - accept both 32-char hex and 22-char Base62 inputs;
  - convert and store internally as lowercase 32-char hex.
- `Size`
  - must parse as a non-negative integer.
- `Path`
  - must be the final relative file path inside the imported tree.
- `Name`
  - derived from `path.Base(Path)`.

This representation is intentionally file-focused and does not attempt to preserve every export metadata field.

## Supported Formats

### `123FSLinkV2`

Supported as a single-file import unit.

Format:

- `123FSLinkV2$<etag>#<size>#<filename>`

Behavior:

- still supported by `ParseShareURL` for existing temp-transfer share handling;
- also accepted inside manual `imports_text` and normalized into one `pan123ImportedFile`.

### `123FLCPV2`

Supported as the primary folder/common-path text format.

Format:

- `123FLCPV2$<commonPath>%<etag>#<size>#<relativePath>$<etag>#<size>#<relativePath>...`

Observed approved cases include:

- empty common path:
  - `123FLCPV2$%...`
- multi-level common path ending with `/`.

Parsing rules:

- `commonPath` may be empty;
- `commonPath` may be multi-level;
- each trailing file record contains relative path data, not an absolute path;
- final file path is `cleanJoin(commonPath, relativePath)`;
- `etag` supports Base62 and hex forms.

### JSON

Support the official export shape and approved looser variants.

Accepted shapes:

- official object:
  - `{ commonPath, usesBase62EtagsInExport, files: [...] }`
- loose object:
  - `{ files: [...] }`
- loose array:
  - `[ { etag, size, path }, ... ]`

Rules:

- each file must contain `etag`, `size`, and `path`;
- when `usesBase62EtagsInExport` is true, convert file `etag` values from Base62;
- when the flag is absent, detect Base62 vs hex per file;
- when `commonPath` is present, prepend it via the same cleaned path join used for `123FLCPV2`.

## Path Cleaning Rules

Imported file paths must be normalized conservatively.

Allowed:

- Chinese characters;
- spaces;
- punctuation commonly seen in file and folder names.

Required normalization:

- trim surrounding whitespace;
- normalize separators to `/`;
- collapse repeated separators;
- clean joins using safe path utilities.

Rejected:

- empty final paths;
- `.` or `..` path components;
- traversal attempts such as `../x`.

If a file path is invalid after normalization, that file should be reported as invalid and excluded from saving.

## Parsing Entry Points

Add a dedicated parser file, for example `internal/subscription/share_import.go`.

Primary responsibilities:

- `parseManualImportText(raw string) ([]pan123ImportedFile, []ImportParseIssue, error)`
- `parsePan123CommonPathFastLink(raw string) ([]pan123ImportedFile, []ImportParseIssue, error)`
- `parsePan123FastLinkJSON(raw string) ([]pan123ImportedFile, []ImportParseIssue, error)`
- helper functions for etag normalization and path cleaning.

`ImportParseIssue` here means a small structured parse warning / error detail for invalid individual file records. The exact name can vary, but the implementation should keep per-file issues separate from the top-level fatal parse error.

Recommended parse strategy for `imports_text`:

1. Trim the full payload.
2. If the full payload parses as supported JSON, treat it as one JSON import.
3. Otherwise scan text for all supported fastlink tokens:
   - `123FLCPV2...`
   - `123FSLinkV2...`
4. Normalize, deduplicate, and merge the resulting files.
5. If no supported content is found, return a clear import-parse error.

Ordinary share URLs should remain the responsibility of `links`, not `imports_text`.

## Save Path Design

Add a new save helper alongside `SaveShareToTemp`, for example:

- `SaveImportedFilesToTemp(ctx, provider, rootPath, files, opts)`

This function should:

1. ensure the destination temp root exists using `provider.EnsureDir`;
2. build a virtual directory tree from imported file paths;
3. create deterministic virtual parent groups based on directory path;
4. build `TreeEntry` values for filtering and reporting;
5. apply the existing `opts.Match` logic to files;
6. ensure destination subdirectories exist under the temp root;
7. call `provider.SaveShareItems` per target directory group using the same raw metadata format already used by `pan123` single-file fastlinks;
8. wait for tasks using `provider.WaitSaveComplete`.

This helper intentionally skips `ListShareChildren` because imported files are already a fully materialized file list.

## Why a Separate Save Helper

`SaveShareToTemp` currently assumes the provider can enumerate a source share tree with `ListShareChildren`.

That assumption is true for:

- regular share URLs;
- `123FSLinkV2`, which is already adapted into a single-file tree.

It is not naturally true for:

- `123FLCPV2`;
- JSON imports.

A parallel save helper avoids distorting `ShareRef` or `ShareSaver` into a generic import container while still reusing the existing provider save primitives.

## `pan123` Save Reuse

No new `123pan` upload API behavior is required.

Imported files should be converted into `ShareItem` values whose raw payload matches the current `pan123` save expectations:

- `etag`
- `size`
- `file_name`
- optional `type: 0`

This allows `pan123ShareProvider.SaveShareItems` to keep using the current `upload_request` path.

## `runManual` Integration

`runManual` should be extended as follows.

### Existing `links`

Keep current behavior unchanged:

- parse each item as a share URL or supported single-file fastlink;
- use `trySaveShareLinkToTemp`;
- append temp roots and item statuses as today.

### New `imports_text`

After `links`, process `imports_text` when present:

1. parse raw import text into normalized imported files;
2. load global subscription config to obtain `Telegram.Pan123` credentials;
3. normalize and validate the `pan123` temp-transfer config;
4. require:
   - `TempTransferRoot`;
   - `AccessToken`.
5. create a `pan123` share saver;
6. call `SaveImportedFilesToTemp` with the existing match function:
   - `!entry.IsDir && subscriptionEntryMatches(sub, entry)`.
7. append the temp root to `snapshotRoots` so imported files participate in preview and follow-up handling like other temp-transferred files.

This keeps imported-file matching behavior aligned with Telegram and PanSou temp transfer behavior.

## API / Entry Surface

No new route is required in this iteration.

The explicit import entry is the existing manual subscription payload passed through:

- `POST /api/admin/subscription/create`
- `POST /api/admin/subscription/update`
- `POST /api/admin/subscription/preview`
- `POST /api/admin/subscription/check`

The new contract is simply that manual `source_config` may now include:

```json
{
  "paths": [],
  "links": [],
  "imports_text": "123FLCPV2$... or JSON..."
}
```

If the frontend form is available later, it should expose `imports_text` as a multiline paste area. Backend support must not depend on that UI work existing in this repository.

## Error Handling

### Parse-Level Errors

If `imports_text` contains no supported import payload, manual execution should fail with a clear parse error.

### Partial Invalid Files

Use a permissive strategy:

- invalid file records are collected and reported;
- valid file records continue through save;
- the run should only fail completely when there are no valid importable files or when saving valid files fails.

This is better aligned with real-world batch imports where a few malformed entries should not block all valid files.

### Credential Errors

When `imports_text` is present but `pan123` temp-transfer config is unusable, return explicit errors for missing:

- `temp_transfer_root`;
- `access_token`.

## Testing Strategy

Add or extend tests in the following areas.

### Import Parsing

Cover:

- `123FLCPV2` with empty common path;
- `123FLCPV2` with nested common path;
- Base62 etag conversion to hex;
- official JSON export object;
- loose JSON object with only `files`;
- loose JSON array input;
- path cleaning and traversal rejection;
- mixed text containing multiple fastlink payloads.

### Manual Runtime

Cover:

- `runManual` reads and executes `imports_text`;
- missing `pan123` access token causes a clear error;
- missing `temp_transfer_root` causes a clear error;
- imported entries are filtered by `subscriptionEntryMatches`.

### Save Path

Cover:

- `SaveImportedFilesToTemp` creates nested directories correctly;
- files are grouped by target directory before save;
- only matched files are saved;
- `pan123` save payloads use normalized `etag`, `size`, and `file_name`.

## Risks and Constraints

- `123FastLink` formats may evolve; parser boundaries should stay modular.
- Loose JSON support must remain conservative enough to avoid accidentally treating unrelated JSON as import data.
- Imported file trees can be large, so grouping and directory creation logic should avoid quadratic behavior.
- This design assumes `123pan` fast transfer continues to accept save requests based on current `etag` reuse behavior.

## Implementation Plan Shape

The implementation should likely proceed in this order:

1. add parser tests for `123FLCPV2`, Base62 etags, JSON variants, and path cleaning;
2. implement import parsing in a dedicated file;
3. add save helper tests for imported file trees;
4. implement `SaveImportedFilesToTemp`;
5. extend manual config parsing and `runManual` integration;
6. add runtime regression tests for manual import behavior;
7. run targeted subscription tests.

## Summary

This design adds explicit raw fastlink import support to the existing manual subscription workflow by:

- extending manual config with `imports_text`;
- parsing `123FLCPV2`, `123FSLinkV2`, and JSON into normalized `123pan` file metadata;
- saving imported files through a dedicated import-save helper that reuses existing `pan123` provider save logic;
- reusing current preview/check endpoints instead of introducing a new import API.

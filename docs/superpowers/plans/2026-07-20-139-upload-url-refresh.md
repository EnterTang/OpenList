# 139 Upload URL Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover `personal_new` multipart uploads when Mobile 139 presigned URLs expire, without restarting the existing upload session or buffering the entire file.

**Architecture:** Keep the existing `/file/create`, `/file/getUploadUrl`, and `/file/complete` session flow. Refactor the part uploader to read each part through `FileStreamer.RangeRead`, return a typed expiration error containing the failed part number, and let `Put` refresh URLs for the failed and remaining parts before retrying. Keep uploads sequential and cap refresh attempts.

**Tech Stack:** Go, `net/http`, `encoding/xml`, OpenList `model.FileStreamer`, `pkg/http_range`, existing `httptest` driver tests.

## Global Constraints

- Apply only to the `MetaPersonalNew` multipart upload path.
- Preserve the existing 139 `fileId`/`uploadId` session and call `/file/complete` only after all parts succeed.
- Refresh only explicit 139 `AccessDenied` / `Request has expired` responses.
- Read retry data with `FileStreamer.RangeRead`; do not buffer the entire file.
- Do not introduce parallel uploads, authentication changes, or cluster scheduling changes.
- Never log presigned URLs, credentials, or upload tokens.

---

## File Map

- Modify: `drivers/139/driver.go` — split upload parts into bounded batches and coordinate URL refresh/resume.
- Modify: `drivers/139/util.go` — add typed expiration response parsing and range-based part upload.
- Modify: `drivers/139/driver_test.go` — add HTTP fixture tests for expired URL recovery, offsets, progress, and terminal errors.
- Create: `docs/superpowers/specs/2026-07-20-139-upload-url-refresh-design.md` — approved design.

### Task 1: Add typed expiration classification and range-based upload primitive

**Files:**
- Modify: `drivers/139/util.go:737-787`
- Test: `drivers/139/driver_test.go:82-125`

**Interfaces:**
- Produce `type personalUploadPartError struct { PartNumber int; Expired bool; Err error }` with `Error() string` and `Unwrap() error`.
- Produce `func isPersonalUploadURLExpired(err error) bool` using `errors.As`.
- Change the uploader boundary to accept `model.FileStreamer` and upload a supplied `[]PersonalPartInfo` using `PartInfo.PartOffset` and `PartInfo.PartSize`.

- [ ] **Step 1: Write failing tests for expiration classification.**

Add tests that build a 403 XML response with `Code=AccessDenied` and `Message=Request has expired`, then assert `errors.As` succeeds and `Expired` is true. Add a second response with another message and assert it is not refreshable.

- [ ] **Step 2: Run the focused test and verify it fails.**

Run:

```bash
go test ./drivers/139 -run 'TestPersonalUpload.*Expired|TestPersonalUpload.*Range' -count=1
```

Expected: FAIL because the typed error and range uploader do not exist yet.

- [ ] **Step 3: Implement the response parser and typed error.**

Use a private XML response struct with `Code` and `Message` fields. Read the body only for non-200 responses. Wrap the original formatted HTTP error with `personalUploadPartError{PartNumber: ..., Expired: true}` only when both values match exactly after trimming.

- [ ] **Step 4: Change one-part reading to `FileStreamer.RangeRead`.**

For each `PartInfo`, call:

```go
reader, err := stream.RangeRead(http_range.Range{
    Start:  partInfo.ParallelHashCtx.PartOffset,
    Length: partInfo.PartSize,
})
```

Close the returned reader when the request finishes. Wrap the reader with `driver.NewLimitedUploadStream(ctx, reader)`, then `io.LimitReader` only if needed to enforce the exact part length. Update progress only after the HTTP response is `200 OK`.

- [ ] **Step 5: Run the focused tests and confirm the primitive passes.**

Run:

```bash
go test ./drivers/139 -run 'TestPersonalUpload.*Expired|TestPersonalUpload.*Range' -count=1
```

Expected: PASS.

### Task 2: Add refresh-and-resume coordination to `Put`

**Files:**
- Modify: `drivers/139/driver.go:696-780`
- Modify: `drivers/139/util.go` — expose the typed part failure to the coordinator.
- Test: `drivers/139/driver_test.go`

**Interfaces:**
- Consume the range-based uploader and `personalUploadPartError` from Task 1.
- Produce a private coordinator that receives `fileId`, `uploadId`, all `PartInfo` values, the initial URL response, the source stream, progress, and the existing context.

- [ ] **Step 1: Add a failing end-to-end fixture test.**

Use one `httptest.Server` with handlers for:

- `/file/getUploadUrl`: return a fresh URL for the failed part;
- the first upload URL: return the expired 403 XML;
- the refreshed upload URL: verify the request body is the exact bytes for the failed offset and return 200;
- later parts: return 200;
- `/file/complete`: record that it is called once.

Use a `FileStream` over deterministic bytes with at least two parts. Assert that the refreshed request receives the failed part bytes, progress reaches 100 once, and completion is called exactly once.

- [ ] **Step 2: Run the fixture test and verify it fails.**

Run:

```bash
go test ./drivers/139 -run TestPersonalUploadRefreshesExpiredPart -count=1
```

Expected: FAIL because `Put` currently returns the first upload error.

- [ ] **Step 3: Implement bounded refresh coordination.**

Process URL responses in batches no larger than `personalUploadPartInfoLimit`. Track the next part index and the current URL map. On an expired typed error, request URLs for the failed part through the end of the current batch using the existing `/file/getUploadUrl` payload, replace only those URLs, and retry from the failed index. Permit a fixed maximum of three refreshes for one part. Preserve the same `fileId` and `uploadId` for every refresh.

- [ ] **Step 4: Keep non-expiration failures terminal.**

If the typed error is not marked expired, return it immediately. If `/file/getUploadUrl` fails or returns no URL for a requested part, return an error containing the part number and the refresh cause. Do not call `/file/complete` in either case.

- [ ] **Step 5: Run the end-to-end fixture tests.**

Run:

```bash
go test ./drivers/139 -run 'TestPersonalUploadRefreshesExpiredPart|TestPersonalUploadDoesNotRefreshOther403|TestPersonalUploadRefreshLimit' -count=1
```

Expected: PASS.

### Task 3: Preserve existing behavior and add cancellation coverage

**Files:**
- Modify: `drivers/139/driver_test.go`
- Modify: `drivers/139/driver.go` only if the focused tests expose integration regressions.
- Modify: `drivers/139/util.go` only if the focused tests expose integration regressions.

**Interfaces:**
- Consume the completed refresh coordinator.
- Preserve existing CDN URL fallback, invalid part validation, cancellation, ETF finalization, and normal `/file/complete` behavior.

- [ ] **Step 1: Add regression tests.**

Cover:

```go
func TestUploadPersonalPartsFallsBackToCdnUploadURL(t *testing.T) { /* existing test remains */ }
func TestPersonalUploadCancellationDoesNotRefresh(t *testing.T) { /* canceled context returns context.Canceled */ }
func TestPersonalUploadRefreshUsesCorrectPartOffset(t *testing.T) { /* body equals exact failed range */ }
```

The tests must use deterministic byte slices and inspect request bodies rather than relying only on status codes.

- [ ] **Step 2: Run all 139 tests.**

Run:

```bash
go test ./drivers/139 -count=1
```

Expected: PASS.

- [ ] **Step 3: Run the related transfer tests.**

Run:

```bash
go test ./internal/fs ./internal/cluster/worker ./internal/task_group -count=1
```

Expected: PASS.

- [ ] **Step 4: Inspect the diff and verify no credentials or URLs are logged.**

Run:

```bash
git diff --check
git diff -- drivers/139/driver.go drivers/139/util.go drivers/139/driver_test.go
```

Expected: no whitespace errors; only the 139 upload recovery change and its tests are present.

### Task 4: Final verification

- [ ] **Step 1: Run the complete Go test suite.**

Run:

```bash
go test ./... -count=1
```

Expected: PASS. If unrelated pre-existing failures occur, record their exact package and output rather than claiming a clean suite.

- [ ] **Step 2: Review behavior against the design.**

Verify that expiration recovery uses the same upload session, retries from the failed part, does not double-count progress, does not retry ordinary 403 responses, and does not call completion after a terminal failure.

- [ ] **Step 3: Report the changed files and test output.**

Include the exact commands run and their observed results. Do not claim remote 139 validation unless an actual upload was performed.

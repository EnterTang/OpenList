# 139 Multipart Upload URL Refresh Design

## Goal

Make `personal_new` uploads resilient when Mobile 139 presigned multipart upload URLs expire during a long upload.

The implementation will keep the existing 139 upload session (`fileId` and `uploadId`), refresh URLs for the failed and remaining parts after an explicit `Request has expired` response, and resume without restarting the whole file upload.

## Scope

- Apply only to the `MetaPersonalNew` multipart upload path.
- Preserve the existing upload session and completion API.
- Detect the 139 XML `AccessDenied` response whose message is `Request has expired`.
- Re-read the failed part from the source without buffering the entire file.
- Refresh only the current upload batch and continue with later parts.
- Limit refresh attempts and return the original failure if recovery is exhausted.
- Keep non-expiration 403 responses and unrelated transport errors on the existing failure path.

## Non-Goals

- Do not change 139 authentication or token refresh behavior.
- Do not introduce parallel multipart uploads in this change.
- Do not redesign cluster scheduling or native transfer task management.
- Do not retry arbitrary HTTP failures automatically.
- Do not buffer an entire media file in memory or a temporary file.

## Design

### Upload source

The upload loop will use the existing `PartInfo.PartOffset` and `PartInfo.PartSize` metadata to obtain a fresh reader for the current part. The implementation must support the source abstraction used by `FileStreamer`; if the source cannot be reopened or seeked, the operation will return a clear unsupported-resume error rather than silently uploading the wrong bytes.

The progress callback must only advance after a part succeeds. A failed part may be read more than once but must not double-count progress.

### URL refresh

The first URL response is used for the initial batch. A batch upload returns structured information when a part fails, including the failed part number and whether the response is an expired presigned URL.

On expiration:

1. Build a request containing the current and remaining `PartInfo` values.
2. Call `/file/getUploadUrl` with the existing `fileId`, `uploadId`, and account information.
3. Retry the failed part using the newly returned URL.
4. Continue through the refreshed batch.
5. Request the next batch only after the current batch succeeds.

The default batch size remains bounded by the 139 API limit of 100 parts. The implementation may use a smaller internal batch size if required by the retry design, but it must not increase the number of parts requested from 139 beyond the supported limit.

### Error classification

The response body is inspected only for non-200 responses. A response is refreshable when it contains the 139 object-storage error shape and `Code=AccessDenied` with `Message=Request has expired`.

These errors remain terminal without URL refresh:

- Other 403 responses;
- authentication or account errors;
- malformed upload responses;
- context cancellation;
- source read failures;
- URL refresh API failures after the retry limit.

### Retry limits

Use a small fixed maximum number of URL refresh attempts per part/batch. Each refresh must be observable in debug logs with the file ID, upload ID, and part number, while presigned URLs and credentials remain redacted.

## Data flow

1. `Put` computes the full SHA256 and creates the 139 upload session.
2. The driver obtains the first batch of part URLs.
3. The uploader obtains the source bytes for one part and sends the PUT request.
4. On success, progress advances and the next part begins.
5. On URL expiration, the uploader requests fresh URLs and retries from the failed part.
6. After all parts succeed, `Put` calls `/file/complete` exactly once.
7. Existing ETF finalization and cleanup continue unchanged.

## Tests

Add focused tests for:

- Successful multipart upload with a seekable/reopenable source;
- URL fallback from `UploadUrl` to `CdnUploadUrl`;
- expired URL response causes `/file/getUploadUrl` and retries the failed part;
- refreshed retry reads the correct part offset and does not duplicate progress;
- non-expired 403 does not refresh;
- refresh failure respects the retry limit;
- cancellation interrupts the upload without refreshing;
- `/file/complete` is called only after all parts succeed.

Existing 139 driver, ETF, and cluster transfer tests must remain green.

## Risks and Mitigations

- **Source is not seekable**: detect this before attempting resume and return an explicit error; do not risk sending incorrect bytes.
- **Repeated expiration**: cap refresh attempts and preserve the original error context.
- **Progress duplication**: report progress only after successful part completion.
- **Partial cloud state**: reuse the existing upload session and complete it only after every part succeeds.
- **Credential leakage**: never include presigned URLs in logs or error messages.

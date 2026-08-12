# Subscription remediation and 115 direct-share delivery implementation plan

**Goal:** make subscription execution convergent and compensating, while enabling a guarded 115 share-link direct-download attempt with a single fallback to share-save.

**Constraints:** direct URLs are short-lived and must stay in worker memory; provider credentials and response bodies must not enter coordinator payloads or durable errors; upstream 115 HTML/405 and invalid credentials must be classified rather than blindly retried; existing user worktree changes and provider behavior must remain backward compatible.

## 1. Lock the failure contracts with tests

- Add a migration regression test that inserts a nullable `state_version`, runs normalization, and proves the durable value is repaired before reconciliation.
- Add a reconciliation regression test with a terminal child job and an active subscription run; prove the run projection is finalized and no longer reports dispatching/running after all children are terminal.
- Add 123 share metadata coverage for `S3KeyFlag` persistence and direct-link payload construction.
- Add inventory refresh loop coverage so a worker republishes provider health after the initial storage-load report.
- Add 115 direct-link HTTP coverage for the modern app endpoint, legacy web fallback, 405/no-JSON classification, and URL non-persistence.

## 2. Repair durable subscription state

- Make `state_version` non-null/default-zero at the model/schema boundary and backfill existing NULL values during startup migration.
- Make optimistic updates NULL-safe with `COALESCE`, including compensation/reset paths.
- Reconcile the latest running subscription run from durable item/job state, updating counts, stage statuses, completion state, error, and finish time only when the job set has converged.

## 3. Repair provider and worker execution

- Preserve 123 `S3KeyFlag` from share listing through `SubscriptionItem.ProviderData` and the sealed worker source object.
- Advertise `share.download` for healthy 115 accounts and route direct-download tasks only to workers with that capability and a fresh provider-health lease.
- Periodically refresh worker inventory after storage initialization; retain heartbeat/inventory separation so online workers with expired credentials cannot receive new work.
- Keep lease expiry and existing stage retry behavior compensating; classify source EOF/transient transfer failures as retryable without discarding a reusable staged source.

## 4. Add guarded 115 direct-share delivery

- Implement the verified public 115 share-download contract using the official RSA-encrypted app POST endpoint and legacy web GET fallback on 405.
- Reuse the existing account limiter, bounded retry policy, HTTP metadata classification, credential redaction, and referer/UA handling.
- Add the 115 provider to the optional `ShareDirectDownloader` interface.
- Keep the default feature gate unchanged. When enabled, the worker tries direct download once, verifies target size, and falls back at most once to the existing share-save path; no direct URL is serialized or persisted.

## 5. Verify before handoff

Run focused Go tests for `internal/db`, `internal/subscription`, `internal/cluster`, then run the repository’s standard formatting, test, and static checks. Review the diff for secret/URL leakage and report any external-provider behavior that cannot be verified without live credentials.

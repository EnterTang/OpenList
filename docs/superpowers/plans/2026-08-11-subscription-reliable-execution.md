# Subscription Reliable Execution Implementation Plan
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make subscription execution lossless and compensating: dispatch reflects actual work, transient failures retry safely, permanent failures are classified, stale state is reconciled, and multi-episode media is not silently misidentified.

**Architecture:** Preserve the current subscription/cluster job model and repair its boundaries rather than introducing a second queue. Add typed 115 failure classification at the provider boundary, make each dispatched chunk an independent durable parent batch, add database reconciliation/recovery for orphaned and expired work, and keep subscription run status pending/running until transfer outcomes are durable.

**Tech Stack:** Go, GORM, existing cluster runtime/worker protocol, existing subscription scheduler, Go test.

---

## 1. Lock failure semantics with tests

- [ ] Add provider tests proving 405/HTML is not blindly retried and exposes status/content type; 429/5xx/network remain retryable; credential/signature/share-invalid errors are non-retryable with stable categories.
- [ ] Add dispatcher tests proving every <=100-item chunk has its own parent batch ID and exact expected child count, including partial chunk failures.
- [ ] Add reconciliation tests for pending items with no job, stale transferring/notifying items, terminal jobs with mismatched item state, and retryable versus blocked outcomes.
- [ ] Add release/parser tests for `S03E23E24` and target naming without Episode 0.
- [ ] Run the focused tests and observe the expected failures before production edits.

## 2. Harden provider requests and worker-visible diagnostics

- [ ] Introduce stable 115 error categories and preserve HTTP status, Content-Type, response kind, and bounded response summary.
- [ ] Retry only rate limits, transport failures, and 5xx with bounded exponential backoff; never retry credential invalidation, share invalidation, signature errors, or 405 method/gateway responses.
- [ ] Ensure worker task results carry the category so the coordinator can decide whether to requeue, wait for account repair, or finish terminally.

## 3. Make batch accounting durable and exact

- [ ] Derive a deterministic unique parent batch ID per chunk and retain the original subscription/task relationship for retries.
- [ ] Record actual child count per parent and map errors to the correct item/chunk.
- [ ] Preserve idempotency so retrying a chunk does not duplicate successful children.

## 4. Add reconciliation, leases, and compensation

- [ ] Reconcile item state against jobs in one transaction: repair success/failed/transferring/notifying mismatches, release stale claims, and requeue safe orphaned work.
- [ ] Recover expired running/queued jobs with bounded attempts; mark unavailable-worker work as waiting/blocked instead of failing it as a provider error.
- [ ] Make explicit failed-subscription retry include failed items with no surviving job by rebuilding dispatch from persisted source metadata, while retaining successful items.
- [ ] Run reconciliation from the scheduler/cluster processor at a bounded cadence and make it idempotent.

## 5. Correct run status and content identity

- [ ] Keep scan/dispatch completion separate from transfer completion; a run/subscription cannot be `success` while work is pending, blocked, or failed.
- [ ] Track multi-episode ranges through recognition and target naming so `S03E23E24` is not converted to Episode 0 or silently truncated.
- [ ] Keep skipped, duplicate, not-found, parse-invalid, blocked, retryable, and terminal failure counts distinguishable.

## 6. Verification and handoff

- [ ] Run focused package tests, then relevant repository tests and static checks available in the worktree.
- [ ] Review the diff for scope, user changes, idempotency, and backward compatibility.
- [ ] Report changed files, verification evidence, and remaining operational risks; do not claim remote data repair unless a separately authorized remote connection is available.

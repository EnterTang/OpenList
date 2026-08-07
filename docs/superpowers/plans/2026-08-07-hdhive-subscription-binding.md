# HDHive Subscription Binding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add HDHive as an automatic subscription entry while ensuring scheduled runs reuse a subscription-bound share and only attempt paid HDHive unlocks after Telegram and PanSou produce no usable candidates.

**Architecture:** Keep existing `telegram` and `pansou` subscription execution unchanged for backward compatibility. A subscription whose `source_type` is `hdhive` becomes a federated entry: it first processes a persisted bound share, then re-triggers configured Telegram and PanSou searches, then resolves free HDHive resources; paid HDHive resources are eligible only when both regular sources have no usable candidate. A successful explicit or automatic HDHive unlock is persisted as the bound share so later runs do not charge the same resource again.

**Tech Stack:** Go, Gin, GORM serializers, SQLite/MySQL AutoMigrate, existing subscription share-inspection/transfer pipeline, Symedia HDHive client.

---

### Task 1: Lock the binding and source-policy contracts with model tests

**Files:**
- Modify: `internal/model/subscription.go`
- Test: `internal/model/subscription_test.go` (create if absent)
- Test: `internal/subscription/config_test.go`

- [ ] **Step 1: Write the failing model test**

Add a JSON round-trip test proving a subscription can carry one bound share without losing provider, access code, HDHive resource identity, or paid/free metadata. Add a source-config normalization test proving `source_type=hdhive` accepts an empty config and defaults its cloud type and limit.

- [ ] **Step 2: Run the focused tests to verify they fail**

Run: `go test ./internal/model ./internal/subscription -run 'TestSubscription(BoundShare|HDHiveSource)' -count=1`

Expected: FAIL because the bound-share and HDHive source configuration types do not exist.

- [ ] **Step 3: Add the smallest model surface**

Add:

```go
type SubscriptionBoundShare struct {
    SourceType       string    `json:"source_type,omitempty"`
    Provider         string    `json:"provider,omitempty"`
    ShareURL         string    `json:"share_url,omitempty"`
    AccessCode       string    `json:"access_code,omitempty"`
    ResourceURL      string    `json:"resource_url,omitempty"`
    ResourceSlug     string    `json:"resource_slug,omitempty"`
    RequiresUnlock   bool      `json:"requires_unlock,omitempty"`
    UnlockPoints     *int      `json:"unlock_points,omitempty"`
    BoundAt          time.Time `json:"bound_at,omitempty"`
}

type SubscriptionHDHiveSourceConfig struct {
    CloudType string `json:"cloud_type,omitempty"`
    Limit     int    `json:"limit,omitempty"`
}
```

Add `BoundShare *SubscriptionBoundShare` to `Subscription` with `gorm:"serializer:json"`, and add the HDHive source config type to the public model surface.

- [ ] **Step 4: Run the focused tests to verify they pass**

Run: `go test ./internal/model ./internal/subscription -run 'TestSubscription(BoundShare|HDHiveSource)' -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/model/subscription.go internal/model/subscription_test.go internal/subscription/config_test.go
git commit -m "feat(subscription): model hdhive bound shares"
```

### Task 2: Add durable binding persistence and API operations

**Files:**
- Modify: `internal/db/db.go`
- Modify: `internal/db/subscription.go`
- Modify: `server/router.go`
- Modify: `server/handles/subscription.go`
- Modify: `internal/model/subscription.go`
- Test: `internal/db/subscription_test.go`
- Test: `server/handles/subscription_test.go`

- [ ] **Step 1: Write the failing persistence and handler tests**

Test that updating a subscription with a bound share persists it through the database, that `POST /admin/subscription/resource/bind` stores a normalized share URL plus access code, and that `POST /admin/subscription/resource/unbind` removes it. The bind handler must reject a missing subscription ID, an unsupported cloud share URL, or an HDHive resource URL without a resolved cloud share URL; it must not call the unlock endpoint implicitly.

- [ ] **Step 2: Run the focused tests to verify they fail**

Run: `go test ./internal/db ./server/handles -run 'Test(SubscriptionBoundShare|BindSubscriptionResource|UnbindSubscriptionResource)' -count=1`

Expected: FAIL because the binding request/handler routes do not exist.

- [ ] **Step 3: Implement explicit, non-spending bind/unbind operations**

Add request/response types, routes, and handlers. The bind operation should:

1. Load the subscription.
2. Normalize `share_url` with `access_code` using the existing link normalizer.
3. Validate it with `ParseShareURL`.
4. Store the cloud share and optional HDHive resource metadata in `Subscription.BoundShare`.
5. Save the subscription and return it.

The handler must never invoke `UnlockHDHiveResource`; users explicitly unlock first, then bind the returned cloud share. Preserve the existing subscription update fields when saving.

- [ ] **Step 4: Run the focused tests to verify they pass**

Run: `go test ./internal/db ./server/handles -run 'Test(SubscriptionBoundShare|BindSubscriptionResource|UnbindSubscriptionResource)' -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/db/db.go internal/db/subscription.go internal/model/subscription.go server/router.go server/handles/subscription.go internal/db/subscription_test.go server/handles/subscription_test.go
git commit -m "feat(subscription): bind selected share links"
```

### Task 3: Normalize and expose the HDHive automatic source

**Files:**
- Modify: `internal/subscription/config.go`
- Modify: `internal/subscription/external.go`
- Modify: `server/handles/subscription.go`
- Test: `internal/subscription/config_test.go`
- Test: `internal/subscription/external_test.go`

- [ ] **Step 1: Write the failing source-normalization tests**

Cover these behaviors:

```text
source_type=hdhive is accepted;
empty HDHive source config normalizes to cloud_type=all and the configured HDHive/PanSou limits;
legacy manual/telegram/pansou source types keep their existing normalization;
external subscription creation accepts source_type=hdhive and rejects unknown source types.
```

- [ ] **Step 2: Run the focused tests to verify they fail**

Run: `go test ./internal/subscription -run 'Test(ApplyConfigDefaultsHDHive|External.*HDHive)' -count=1`

Expected: FAIL because HDHive is not accepted by the external source-type validator and has no source-config merger.

- [ ] **Step 3: Implement source normalization**

Add `mergeHDHiveSourceConfig` and `parseHDHiveConfig` with `cloud_type` restricted to `all`, `channel_115`, `channel_123`, `channel_189`, `channel_quark`, and `channel_alipan`. Add HDHive to `ApplyConfigDefaults` and the external source-type switch. Do not copy proxy secrets into the subscription source config; credentials remain in the global Telegram/HDHive config.

- [ ] **Step 4: Run the focused tests to verify they pass**

Run: `go test ./internal/subscription -run 'Test(ApplyConfigDefaultsHDHive|External.*HDHive)' -count=1`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/subscription/config.go internal/subscription/external.go server/handles/subscription.go internal/subscription/config_test.go internal/subscription/external_test.go
git commit -m "feat(subscription): expose hdhive as a source"
```

### Task 4: Implement bound-first federated scheduled execution

**Files:**
- Create: `internal/subscription/hdhive_run.go`
- Modify: `internal/subscription/service.go`
- Modify: `internal/subscription/telegram_hdhive.go`
- Modify: `internal/subscription/resource_search.go`
- Test: `internal/subscription/hdhive_run_test.go`

- [ ] **Step 1: Write failing policy tests**

Use fakes for Telegram/PanSou search, HDHive `Search`/`Share`/`Unlock`, share inspection, and transfer. Test separately that:

1. A usable bound share is processed before any new HDHive unlock and prevents paid HDHive resolution when it supplies a candidate.
2. Telegram and PanSou are invoked on every HDHive-source run when configured.
3. A free HDHive resource is resolved even when regular sources have candidates.
4. A paid HDHive resource is not resolved when either regular source has a candidate.
5. A paid HDHive resource is resolved only after both regular sources have no candidate.
6. A successful HDHive resolution persists the returned cloud share as the subscription bound share.
7. An unknown unlock-point value is treated as paid/unsafe and is never unlocked automatically.

- [ ] **Step 2: Run the policy tests to verify they fail**

Run: `go test ./internal/subscription -run 'TestHDHiveSubscription' -count=1`

Expected: FAIL because `source_type=hdhive` is unsupported and no federated runner exists.

- [ ] **Step 3: Implement the standalone federated runner**

Implement the following ordered flow:

```text
process bound share (if present)
run configured Telegram search with HDHive enrichment disabled
run configured PanSou search
search HDHive by subscription media_type + tmdb_id
for each HDHive resource:
    GET share metadata
    if an existing share URL is returned, process it
    else if unlock_points == 0 or is_free_for_user, resolve it
    else if unlock_points > 0 and Telegram/PanSou have no candidate, resolve it
    else skip without calling unlock
after a successful HDHive resolution, save its cloud URL as BoundShare
```

Reuse `inspectShareLinkCandidatesFn`, `selectShareTransferCandidates`, and `transferSelectedShareCandidates` so provider credential resolution, media matching, deduplication, and mobile delivery remain identical to the existing sources. Aggregate result hashes and counts without changing old source-type paths. Treat missing unlock-point metadata as unsafe; it is not evidence of a free resource.

- [ ] **Step 4: Add the HDHive source branch and preserve cursor state**

Route `source_type=hdhive` through the federated runner. When delegating to Telegram, use a copied subscription with the global Telegram config and copy its updated cursor back to the parent subscription. Do not expose or persist HDHive proxy credentials in the HDHive source config.

- [ ] **Step 5: Run the focused tests to verify they pass**

Run: `go test ./internal/subscription -run 'TestHDHiveSubscription' -count=1`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/subscription/hdhive_run.go internal/subscription/service.go internal/subscription/telegram_hdhive.go internal/subscription/resource_search.go internal/subscription/hdhive_run_test.go
git commit -m "feat(subscription): protect hdhive unlocks during runs"
```

### Task 5: Cover cluster execution and regression verification

**Files:**
- Modify: `internal/subscription/cluster_run.go`
- Test: `internal/subscription/cluster_run_test.go`
- Modify: `docs/subscription-resource-matching.md` (or the nearest existing subscription design document)

- [ ] **Step 1: Write the failing cluster policy test**

Verify an HDHive-source cluster run dispatches a bound share and free HDHive share through the existing inspect-observation queue, and does not dispatch a paid unlock while Telegram or PanSou has emitted a candidate.

- [ ] **Step 2: Run the focused test to verify it fails**

Run: `go test ./internal/subscription -run 'TestHDHiveCluster' -count=1`

Expected: FAIL because cluster source dispatch does not recognize HDHive.

- [ ] **Step 3: Implement cluster dispatch using the same policy**

Add the HDHive branch to `runClusterBySource`, reuse `dispatchClusterInspectObservation`, and use dispatchable regular-source observations as the no-candidate signal. Never perform an automatic paid unlock when a regular source run failed or its candidate state cannot be determined.

- [ ] **Step 4: Run all targeted regression checks**

Run:

```bash
go test ./internal/model ./internal/db ./internal/subscription ./server/handles
go vet ./internal/model ./internal/db ./internal/subscription ./server/handles
go test -race ./internal/subscription ./server/handles
git diff --check
```

Expected: all targeted tests pass, vet is clean for touched packages, race tests pass, and `git diff --check` emits no output. Record known unrelated repository-wide failures separately if `go test ./...` remains blocked by existing environment/toolchain issues.

- [ ] **Step 5: Update the design documentation**

Document the public source value `hdhive`, the bind/unbind API contract, the no-implicit-unlock rule, the free/paid/unknown policy, and the bound-share-first execution order. Explicitly state that a paid resource is unlocked at most once by automatic execution because the successful share is persisted.

- [ ] **Step 6: Commit**

```bash
git add internal/subscription/cluster_run.go internal/subscription/cluster_run_test.go docs/subscription-resource-matching.md
git commit -m "test(subscription): verify hdhive source fallback policy"
```

### Self-review checklist

- HDHive is selectable as an automatic subscription source.
- Explicit resource selection can bind a resolved cloud share without spending points in the bind endpoint.
- Bound share is attempted before new searches.
- Telegram and PanSou are retried on each HDHive-source run when configured.
- Free HDHive resources may be retried every run.
- Paid HDHive resources are considered only after both regular sources have no candidate.
- Unknown point metadata is never treated as free.
- Successful automatic unlock persists the returned cloud share.
- Existing Telegram/PanSou/manual source behavior remains unchanged.
- Standalone and cluster execution follow the same safety policy.

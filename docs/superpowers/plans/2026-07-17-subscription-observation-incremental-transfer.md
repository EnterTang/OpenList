# Subscription Observation Incremental Transfer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dispatch media transfers as soon as an episode/movie slot is closed by priority or size floor, while showing one observation parent job and enforcing typed worker concurrency pools.

**Architecture:** Keep child `share.inspect` jobs under a new parent type `share.inspect.observation`. Each sealed (or failed-empty) manifest triggers incremental slot closing in the subscription package. Workers accept jobs only within per-type slot limits. ListJobs hides child inspects by default.

**Tech Stack:** Go, GORM/SQLite, existing cluster protocol, Gin admin APIs

**Spec:** `docs/superpowers/specs/2026-07-17-subscription-observation-incremental-transfer-design.md`

**Prerequisite:** Land or keep the already-local P0–P2 changes (30m inspect lease, failed empty manifest, cleanup skip for inspect, redispatch priority). Do not revert them.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `internal/model/subscription.go` | Early-close byte fields on `SubscriptionConfig` |
| `internal/subscription/config.go` | Normalize defaults (1 GiB / 20 GiB) |
| `internal/subscription/slot_close.go` | Pure slot-close decision (priority + size floor) |
| `internal/subscription/slot_close_test.go` | Unit tests for close rules |
| `internal/subscription/cluster_dispatch.go` | Incremental apply + close + dispatch |
| `internal/cluster/subscription_dispatcher.go` | Parent observation create; progress counting via failed+success manifests |
| `internal/model/cluster_job.go` | `ClusterJobTypeShareInspectObservation` |
| `internal/cluster/runtime.go` | `DispatchShareInspectObservation` (parent + children) |
| `internal/conf/config.go` | Typed worker slot defaults |
| `internal/cluster/worker/service.go` | Accept-time typed slot gates |
| `internal/cluster/coordinator/service.go` | `ListJobs` hide children unless `include_children=true` |
| `server/handles/cluster.go` | Pass `include_children` query |

---

### Task 1: Early-close config fields

**Files:**
- Modify: `internal/model/subscription.go` (`SubscriptionConfig`)
- Modify: `internal/subscription/config.go`
- Test: `internal/subscription/config_test.go`

- [ ] **Step 1: Write failing test for defaults**

```go
func TestSubscriptionConfigEarlyCloseDefaults(t *testing.T) {
	cfg := model.SubscriptionConfig{}
	normalized := normalizeSubscriptionConfig(cfg) // wire helper if missing
	if normalized.EpisodeEarlyCloseMinBytes == nil || *normalized.EpisodeEarlyCloseMinBytes != 1<<30 {
		t.Fatalf("episode default = %v, want 1GiB", normalized.EpisodeEarlyCloseMinBytes)
	}
	if normalized.MovieEarlyCloseMinBytes == nil || *normalized.MovieEarlyCloseMinBytes != 20<<30 {
		t.Fatalf("movie default = %v, want 20GiB", normalized.MovieEarlyCloseMinBytes)
	}
	zero := int64(0)
	cfg.EpisodeEarlyCloseMinBytes = &zero
	cfg.MovieEarlyCloseMinBytes = &zero
	normalized = normalizeSubscriptionConfig(cfg)
	if *normalized.EpisodeEarlyCloseMinBytes != 0 || *normalized.MovieEarlyCloseMinBytes != 0 {
		t.Fatalf("zero must remain disabled: %#v", normalized)
	}
}
```

- [ ] **Step 2: Run test — expect FAIL**

Run: `go test ./internal/subscription -run TestSubscriptionConfigEarlyCloseDefaults -count=1`

- [ ] **Step 3: Add fields and normalize**

In `SubscriptionConfig`:

```go
EpisodeEarlyCloseMinBytes *int64 `json:"episode_early_close_min_bytes,omitempty"`
MovieEarlyCloseMinBytes   *int64 `json:"movie_early_close_min_bytes,omitempty"`
```

Helper:

```go
func earlyCloseMinBytes(value *int64, defaultBytes int64) int64 {
	if value == nil {
		return defaultBytes
	}
	if *value < 0 {
		return 0
	}
	return *value
}
```

On normalize of empty config, set pointers to defaults. Explicit `0` stays `0`.

- [ ] **Step 4: Run test — expect PASS**

- [ ] **Step 5: Commit**

```bash
git add internal/model/subscription.go internal/subscription/config.go internal/subscription/config_test.go
git commit -m "feat(subscription): add configurable early-close size floors"
```

---

### Task 2: Pure slot-close decision

**Files:**
- Create: `internal/subscription/slot_close.go`
- Create: `internal/subscription/slot_close_test.go`

- [ ] **Step 1: Write failing tests**

```go
func TestSlotClosePriorityClosedWhenRemainingWeaker(t *testing.T) {
	priority := []string{"pan123", "quark", "aliyun_drive"}
	decision := decideSlotClose(slotCloseInput{
		MediaType:        "tv",
		Winner:           &model.SubscriptionItem{SourceProvider: "pan123", FileSize: 100, Episode: 1, Season: 1},
		PendingProviders: []string{"quark"},
		EpisodeMinBytes:  1 << 30,
		MovieMinBytes:    20 << 30,
		Priority:         priority,
	})
	if !decision.Closed || decision.Reason != "priority_closed" {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestSlotCloseSizeFloorForSameProvider(t *testing.T) {
	decision := decideSlotClose(slotCloseInput{
		MediaType:        "tv",
		Winner:           &model.SubscriptionItem{SourceProvider: "pan123", FileSize: 1<<30 + 1, Episode: 1, Season: 1},
		PendingProviders: []string{"pan123"},
		EpisodeMinBytes:  1 << 30,
		MovieMinBytes:    20 << 30,
		Priority:         []string{"pan123", "quark"},
	})
	if !decision.Closed || decision.Reason != "size_floor" {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestSlotCloseWaitsSameProviderBelowFloor(t *testing.T) {
	decision := decideSlotClose(slotCloseInput{
		MediaType:        "tv",
		Winner:           &model.SubscriptionItem{SourceProvider: "pan123", FileSize: 500 << 20, Episode: 1, Season: 1},
		PendingProviders: []string{"pan123"},
		EpisodeMinBytes:  1 << 30,
		MovieMinBytes:    20 << 30,
		Priority:         []string{"pan123", "quark"},
	})
	if decision.Closed {
		t.Fatalf("should wait for same-tier inspect: %#v", decision)
	}
}

func TestSlotCloseSizeFloorDisabledWhenZero(t *testing.T) {
	decision := decideSlotClose(slotCloseInput{
		MediaType:        "tv",
		Winner:           &model.SubscriptionItem{SourceProvider: "pan123", FileSize: 2 << 30, Episode: 1, Season: 1},
		PendingProviders: []string{"pan123"},
		EpisodeMinBytes:  0,
		MovieMinBytes:    0,
		Priority:         []string{"pan123"},
	})
	if decision.Closed {
		t.Fatalf("size floor disabled: %#v", decision)
	}
}
```

- [ ] **Step 2: Run tests — expect FAIL**

Run: `go test ./internal/subscription -run TestSlotClose -count=1`

- [ ] **Step 3: Implement**

```go
type slotCloseInput struct {
	MediaType        string
	Winner           *model.SubscriptionItem
	PendingProviders []string
	EpisodeMinBytes  int64
	MovieMinBytes    int64
	Priority         []string
}

type slotCloseDecision struct {
	Closed bool
	Reason string // "size_floor" | "priority_closed" | ""
}

func decideSlotClose(in slotCloseInput) slotCloseDecision {
	if in.Winner == nil {
		return slotCloseDecision{}
	}
	minBytes := in.EpisodeMinBytes
	if normalizeMediaType(in.MediaType) == "movie" {
		minBytes = in.MovieMinBytes
	}
	if minBytes > 0 && in.Winner.FileSize >= minBytes {
		return slotCloseDecision{Closed: true, Reason: "size_floor"}
	}
	priorityIndex := map[string]int{}
	for i, p := range normalizeTransferPriority(in.Priority) {
		priorityIndex[p] = i
	}
	winnerRank := providerPriorityRank(normalizeSubscriptionProvider(in.Winner.SourceProvider), priorityIndex)
	for _, provider := range in.PendingProviders {
		rank := providerPriorityRank(normalizeSubscriptionProvider(provider), priorityIndex)
		if rank <= winnerRank {
			return slotCloseDecision{}
		}
	}
	return slotCloseDecision{Closed: true, Reason: "priority_closed"}
}
```

- [ ] **Step 4: Run tests — expect PASS**

- [ ] **Step 5: Commit**

```bash
git add internal/subscription/slot_close.go internal/subscription/slot_close_test.go
git commit -m "feat(subscription): add priority and size-floor slot close decisions"
```

---

### Task 3: Incremental observation apply

**Files:**
- Modify: `internal/subscription/cluster_dispatch.go`
- Modify: `internal/cluster/subscription_dispatcher.go`
- Test: `internal/cluster/subscription_dispatcher_test.go`
- Test: `internal/subscription/cluster_dispatch_test.go`

- [ ] **Step 1: Write failing tests**

```go
func TestConsumeSubscriptionShareInspectDispatchesWhenPriorityClosed(t *testing.T) {
	// observation expected=2
	// first manifest: pan123 episode file (any size)
	// second child still non-terminal with provider quark
	// after first consume: MUST dispatch media for pan123 (quark weaker)
}

func TestConsumeSubscriptionShareInspectWaitsSameProviderBelowFloor(t *testing.T) {
	// expected=2, both pan123; first 500MB → no dispatch while sibling pending
}
```

Follow existing DB+dispatcher patterns in `subscription_dispatcher_test.go`.

- [ ] **Step 2: Run — expect FAIL** (current code waits for `Terminal >= Expected`)

- [ ] **Step 3: Change consume gate**

1. Upsert arrived manifests into candidate set (including empty failed ones).
2. Compute `pendingProviders` from non-terminal child jobs for this observation.
3. For each slot: if accepted → skip; else pick winner among arrived; `decideSlotClose`; if closed and pending → dispatch that winner only.
4. When all children terminal: force-close remaining open slots (or skip if no candidate); mark manifests consumed; reconcile parent job.

Helper:

```go
func pendingShareInspectProviders(ctx context.Context, subscriptionID uint, observationKey string) ([]string, error)
```

- [ ] **Step 4: Run targeted tests — expect PASS**

Run: `go test ./internal/cluster ./internal/subscription -run 'ShareInspect|SlotClose|ClusterInspect' -count=1`

- [ ] **Step 5: Commit**

```bash
git add internal/subscription/cluster_dispatch.go internal/cluster/subscription_dispatcher.go \
  internal/cluster/subscription_dispatcher_test.go internal/subscription/cluster_dispatch_test.go
git commit -m "feat(subscription): incrementally close and dispatch observation slots"
```

---

### Task 4: Parent observation job + deduped child dispatch

**Files:**
- Modify: `internal/model/cluster_job.go`
- Modify: `internal/cluster/runtime.go`
- Modify: `internal/cluster/subscription_dispatcher.go`
- Modify: `internal/subscription/cluster_run.go`
- Test: cluster package tests

- [ ] **Step 1: Add constant**

```go
ClusterJobTypeShareInspectObservation = "share.inspect.observation"
```

- [ ] **Step 2: Write failing test**

```go
func TestDispatchSubscriptionInspectCreatesParentAndDedupedChildren(t *testing.T) {
	// same ObservationKey + same share fingerprint → 1 parent, 1 child
	// second distinct share → 1 parent, 2 children
	// Parent.Type == share.inspect.observation, ExpectedItems == 2
	// Children.ParentJobID == parent.ID
}
```

- [ ] **Step 3: Implement batch dispatch**

```go
type DispatchShareInspectObservationRequest struct {
	ObservationKey      string
	ObservationExpected int
	SubscriptionID      uint
	Children            []DispatchShareInspectRequest
}

func (r *Runtime) DispatchShareInspectObservation(ctx context.Context, req DispatchShareInspectObservationRequest) (parentID string, childIDs []string, err error)
```

1. Idempotent parent: `idempotency_key = "inspect-observation:"+observationKey`.
2. Parent `Type=share.inspect.observation`, `ExpectedItems=len(children)`, `Status=running`.
3. Each child via `DispatchShareInspect` with `LeaseDuration=inspectJobLeaseDuration` and `ParentJobID` set.
4. Change `cluster_run` to build all `ClusterInspectTask`s then call `DispatchSubscriptionInspectBatch` once per observation.

- [ ] **Step 4: Reconcile parent on child terminal**

Extend `reconcileParentJobTx` for `share.inspect.observation`: all children terminal → `succeeded` or `partial_failed`.

- [ ] **Step 5: Tests PASS + commit**

```bash
git commit -m "feat(cluster): add share.inspect.observation parent jobs"
```

---

### Task 5: Typed worker slot pools

**Files:**
- Modify: `internal/conf/config.go`
- Modify: `internal/cluster/worker/service.go`
- Test: `internal/cluster/worker/service_test.go`

- [ ] **Step 1: Add config**

```go
InspectSlots int `json:"inspect_slots" env:"INSPECT_SLOTS"`
MediaSlots   int `json:"media_slots" env:"MEDIA_SLOTS"`
BatchSlots   int `json:"batch_slots" env:"BATCH_SLOTS"`
```

Defaults when <=0: inspect=4, media=3, batch=2.

- [ ] **Step 2: Failing test**

```go
func TestAcceptJobTypedSlots(t *testing.T) {
	// media slots=1 with one active media → reject new media
	// inspect still acceptable
}
```

```go
func (s *Service) typedSlotAvailable(jobType string) bool {
	limit := s.slotLimit(jobType)
	active := 0
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, task := range s.active {
		if task.offer.JobType == jobType {
			active++
		}
	}
	return active < limit
}
```

In `acceptJob` after cleanup check:

```go
if !s.typedSlotAvailable(offer.JobType) {
	return fmt.Errorf("worker %s slot limit reached", offer.JobType)
}
```

Align `downloadGate` limit with `media_slots`. Keep inspect exempt from cleanup backlog (existing).

- [ ] **Step 3: PASS + commit**

```bash
git commit -m "feat(cluster): enforce typed worker accept slot limits"
```

---

### Task 6: ListJobs hides child inspects by default

**Files:**
- Modify: `internal/cluster/coordinator/service.go`
- Modify: `server/handles/cluster.go`
- Test: `internal/cluster/coordinator/service_test.go`

- [ ] **Step 1: Failing test**

```go
func TestListJobsHidesInspectChildrenByDefault(t *testing.T) {
	// parent observation + child inspect with ParentJobID
	// default ListJobs → parent only
	// includeChildren=true → both
}
```

- [ ] **Step 2: Implement**

```go
func (s *Service) ListJobs(ctx context.Context, status string, includeArchived, includeChildren bool, limit int) ([]model.ClusterJob, error) {
	// ...
	if !includeChildren {
		query = query.Where("type <> ? OR parent_job_id = '' OR parent_job_id IS NULL", model.ClusterJobTypeShareInspect)
	}
}
```

Wire `include_children=true` in `server/handles/cluster.go`.

- [ ] **Step 3: PASS + commit**

```bash
git commit -m "feat(cluster): hide nested share.inspect jobs in default job list"
```

---

### Task 7: Full package verification

- [ ] **Step 1: Run**

```bash
go test ./internal/subscription ./internal/cluster ./internal/cluster/coordinator ./internal/cluster/worker -count=1
gofmt -w internal/subscription/*.go internal/cluster/*.go internal/cluster/worker/service.go internal/cluster/coordinator/service.go
```

Expected: all PASS.

- [ ] **Step 2: Manual deploy checklist (do not invent results)**

- New subscription → one `share.inspect.observation` parent
- High-priority provider closes before weaker inspects finish
- ≥1GiB episode closes with same-provider pending
- Failed child does not block closed slots
- Default job list is not flooded with child inspects

---

## Spec coverage checklist

| Spec requirement | Task |
|------------------|------|
| Priority-closed dispatch | Task 2–3 |
| Size-floor early close (configurable) | Task 1–2 |
| Failed empty manifest counts | Prerequisite + Task 3 |
| Parent observation job | Task 4 |
| Child dedupe | Task 4 |
| Typed slot pools | Task 5 |
| UI hide children | Task 6 |
| No reassign after transferring | Task 3 |
| Unknown TV episodes passthrough | unchanged (no slot close) |

## Placeholder scan

No TBD. Parent type is `share.inspect.observation`. Config uses `*int64` so explicit `0` disables floors.

---

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-17-subscription-observation-incremental-transfer.md`.

**Two execution options:**

1. **Subagent-Driven (recommended)** — fresh subagent per task, review between tasks
2. **Inline Execution** — execute tasks in this session with checkpoints

Which approach?

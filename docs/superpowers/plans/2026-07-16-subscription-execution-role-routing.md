# Subscription Execution Role Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make manual subscription reruns use the same role-aware local-or-cluster execution path as automatic scheduled runs.

**Architecture:** Add a shared `subscription.RunForRole` entry point that preserves local `transfer` semantics for standalone deployments and delegates every non-standalone role to `RunCluster`. Route both the HTTP handler and scheduler through it while retaining explicit `Run` and `RunCluster` APIs.

**Tech Stack:** Go, Gin, GORM, in-memory SQLite, existing cluster dispatcher interface

---

## File Structure

- Modify `internal/subscription/service.go`: own role-aware execution.
- Modify `internal/subscription/scheduler.go`: use the shared entry point.
- Modify `internal/subscription/scheduler_test.go`: cover role classification.
- Modify `internal/subscription/standalone_target_integration_test.go`: prove the standalone `transfer` flag is preserved.
- Modify `server/handles/subscription.go`: route manual checks by role.
- Modify `server/handles/subscription_test.go`: prove Hybrid manual checks dispatch to the cluster.

### Task 1: Lock Manual Hybrid Execution To Cluster Dispatch

**Files:**
- Test: `server/handles/subscription_test.go`

- [ ] **Step 1: Add a recording dispatcher and failing handler test**

Add `context` and `strings` to the standard-library imports and import `internal/subscription`. Define:

```go
type recordingSubscriptionDispatcher struct {
	inspectTasks []subscription.ClusterInspectTask
}

func (d *recordingSubscriptionDispatcher) DispatchSubscriptionInspect(_ context.Context, task subscription.ClusterInspectTask) (string, error) {
	d.inspectTasks = append(d.inspectTasks, task)
	return "handler-inspect-job", nil
}

func (d *recordingSubscriptionDispatcher) DispatchSubscriptionMedia(context.Context, []subscription.ClusterMediaTask) ([]subscription.ClusterDispatchResult, error) {
	return nil, nil
}
```

Add:

```go
func TestCheckSubscriptionUsesClusterDispatchForHybridRole(t *testing.T) {
	setupSubscriptionHandleDB(t)
	conf.Conf.Cluster.Role = model.ClusterRoleHybrid
	dispatcher := &recordingSubscriptionDispatcher{}
	subscription.RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { subscription.RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{
		Name:            "Hybrid manual check",
		SourceType:      model.SubscriptionSourceManual,
		SourceConfig:    `{"links":["https://www.123pan.com/s/example"]}`,
		TransferEnabled: true,
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/admin/subscription/check",
		strings.NewReader(`{"id":`+strconv.Itoa(int(sub.ID))+`,"transfer":true}`))
	c.Request.Header.Set("Content-Type", "application/json")

	CheckSubscription(c)

	resp := decodeHandleResp[model.SubscriptionRunResult](t, recorder)
	if resp.Code != 200 {
		t.Fatalf("code = %d, want 200: %s", resp.Code, recorder.Body.String())
	}
	if len(dispatcher.inspectTasks) != 1 {
		t.Fatalf("inspect tasks = %#v, want one cluster inspection", dispatcher.inspectTasks)
	}
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
go test ./server/handles -run '^TestCheckSubscriptionUsesClusterDispatchForHybridRole$' -count=1 -v
```

Expected: FAIL because the handler calls local `subscription.Run`; the request errors before the dispatcher records an inspection.

### Task 2: Share Role-Aware Execution Between Handler And Scheduler

**Files:**
- Modify: `internal/subscription/service.go`
- Modify: `internal/subscription/scheduler.go`
- Modify: `internal/subscription/scheduler_test.go`
- Modify: `internal/subscription/standalone_target_integration_test.go`
- Modify: `server/handles/subscription.go`

- [ ] **Step 1: Add the shared entry point**

Add next to `Run` and `RunCluster`:

```go
// RunForRole selects the same subscription execution path for manual and
// scheduled runs. Only standalone deployments may enqueue local transfers.
func RunForRole(ctx context.Context, subscriptionID uint, transfer bool, role string) (*model.SubscriptionRunResult, error) {
	if subscriptionTransfersLocally(role) {
		return Run(ctx, subscriptionID, transfer)
	}
	return RunCluster(ctx, subscriptionID)
}

func subscriptionTransfersLocally(role string) bool {
	role = strings.ToLower(strings.TrimSpace(role))
	return role == "" || role == model.ClusterRoleStandalone
}
```

- [ ] **Step 2: Route the manual handler through the shared entry point**

Replace its direct call with:

```go
result, err := subscription.RunForRole(c.Request.Context(), req.ID, req.Transfer, conf.Conf.Cluster.Role)
```

- [ ] **Step 3: Route the scheduler through the shared entry point**

Replace its role branch with:

```go
_, err := RunForRole(context.Background(), id, true, conf.Conf.Cluster.Role)
if err != nil {
	log.Errorf("subscription %d run failed: %+v", id, err)
}
```

Remove `schedulerTransfersLocally` and unused `strings` and `model` imports.

- [ ] **Step 4: Cover the shared role predicate**

Replace the old scheduler predicate test with:

```go
func TestSubscriptionExecutionTransfersLocallyOnlyForStandaloneRole(t *testing.T) {
	for _, role := range []string{"", model.ClusterRoleStandalone, " STANDALONE "} {
		if !subscriptionTransfersLocally(role) {
			t.Fatalf("role %q should keep local transfer behavior", role)
		}
	}
	for _, role := range []string{model.ClusterRoleCoordinator, model.ClusterRoleWorker, model.ClusterRoleHybrid} {
		if subscriptionTransfersLocally(role) {
			t.Fatalf("%s must not bypass cluster dispatch with a local transfer", role)
		}
	}
}
```

Add the `internal/model` import to `scheduler_test.go`.

- [ ] **Step 5: Prove standalone preserves `transfer=false`**

Add an integration test that invokes `RunForRole` with `model.ClusterRoleStandalone` and `transfer=false`, then asserts the subscription is discovered without folder-ensure or transfer side effects. Mutation-check the test by temporarily forcing the local branch to pass `true`; the test must fail before restoring the correct implementation.

- [ ] **Step 6: Format and verify GREEN**

Run:

```bash
gofmt -w internal/subscription/service.go internal/subscription/scheduler.go internal/subscription/scheduler_test.go internal/subscription/standalone_target_integration_test.go server/handles/subscription.go server/handles/subscription_test.go
go test ./internal/subscription ./server/handles -run 'TestRunForRolePreservesStandaloneTransferFlag|TestSubscriptionExecutionTransfersLocallyOnlyForStandaloneRole|TestCheckSubscriptionUsesClusterDispatchForHybridRole' -count=1 -v
```

Expected: both tests PASS and the handler test records one cluster inspection.

### Task 3: Regression Verification And Commit

- [ ] **Step 1: Run package tests**

```bash
go test ./internal/subscription ./server/handles -count=1
```

Expected: PASS with zero failures.

- [ ] **Step 2: Run repository verification**

```bash
go test ./... -count=1
go vet ./...
```

Expected: both commands exit successfully. Run any repository-defined additional static-analysis command if available and report unavailable checks honestly.

- [ ] **Step 3: Inspect the final diff**

```bash
git diff --check
git diff -- internal/subscription/service.go internal/subscription/scheduler.go internal/subscription/scheduler_test.go server/handles/subscription.go server/handles/subscription_test.go
```

Expected: no whitespace errors and only the approved role-routing changes.

- [ ] **Step 4: Commit the implementation**

Stage only the five Go files and this plan. Use a Conventional Commit title, a concise Markdown-list body, and Lore trailers containing actual verification evidence and remaining gaps.

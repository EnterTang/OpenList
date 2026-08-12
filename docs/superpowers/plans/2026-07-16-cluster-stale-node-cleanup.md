# Cluster Stale Node Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate ghost-online cluster nodes by reconciling stale sessions on startup, timing out dead heartbeats, hiding stale offline nodes by default, and providing an admin cleanup path for permanently offline nodes.

**Architecture:** Extend the coordinator service so node liveness is derived from the persisted heartbeat timestamp plus startup reconciliation, not only from `OnDisconnect`. Keep scheduling behavior unchanged by touching only coordinator persistence, list rendering, and admin-management APIs. Treat stale-node deletion as an explicit admin transaction that removes node-owned metadata while preserving historical job records.

**Tech Stack:** Go, GORM, Gin, SQLite-backed Go tests.

## Global Constraints

- Do not support dispatching work to offline nodes.
- Do not add wake-up, boot, or remote start behavior for offline nodes.
- Do not change cluster job offer / outbox online-send semantics.
- Default list behavior must hide stale offline nodes.
- Manual cleanup may remove only stale offline nodes.
- Historical cluster job / attempt / outbox / upload manifest records must be preserved.

---

## File Structure

### Backend: /Volumes/extend Disk/Github/OpenList

- Modify: `internal/cluster/coordinator/service.go` - startup reconciliation, heartbeat timeout helpers, stale filtering, stale-node deletion transaction, list API parameter support.
- Modify: `internal/cluster/runtime.go` - invoke startup reconciliation and run heartbeat-timeout sweeper during coordinator/hybrid runtime startup.
- Modify: `server/handles/cluster.go` - pass `include_stale` to node listing and add an admin delete handler for stale nodes.
- Modify: `server/router.go` - register the stale-node cleanup route.
- Test: `internal/cluster/coordinator/service_test.go` - reconciliation, timeout sweep, list filtering, deletion rules.
- Test: `internal/cluster/runtime_security_test.go` or `internal/cluster/embedded_redis_runtime_test.go` - coordinator startup invokes reconciliation and survives the new hook shape.
- Optional helper test: `server/handles/cluster_test.go` if existing handle coverage is insufficient for new request parameters and delete behavior.

### Remote Operations

- One-time manual SQL cleanup on `192.168.1.182` after database backup.
- Verification queries against copied SQLite database.

## Task 1: Back up the remote database and clean the confirmed stale node

**Files:**
- Remote runtime data only; no repository file changes.

**Interfaces:**
- Consumes: SSH access to `entertang@192.168.1.182`, Docker container `openlist-etf`, SQLite database at `/opt/openlist/data/data.db` inside the container.
- Produces: Cleaned node record for `oplist-etf-139cloudPC`, backup copy of the database, post-cleanup verification output.

- [ ] **Step 1: Copy the live SQLite files to timestamped backup files on the remote host.**

```bash
sshpass -p 'Twain1996' ssh -o StrictHostKeyChecking=no entertang@192.168.1.182 'bash -s' <<'REMOTE'
set -euo pipefail
stamp=$(date +%Y%m%d-%H%M%S)
mkdir -p /tmp/openlist-db-backup-$stamp
docker cp openlist-etf:/opt/openlist/data/data.db /tmp/openlist-db-backup-$stamp/data.db
(docker cp openlist-etf:/opt/openlist/data/data.db-wal /tmp/openlist-db-backup-$stamp/data.db-wal >/dev/null 2>&1 || true)
(docker cp openlist-etf:/opt/openlist/data/data.db-shm /tmp/openlist-db-backup-$stamp/data.db-shm >/dev/null 2>&1 || true)
ls -lah /tmp/openlist-db-backup-$stamp
REMOTE
```

- [ ] **Step 2: Verify the stale node is still present before mutating anything.**

```bash
sshpass -p 'Twain1996' ssh -o StrictHostKeyChecking=no entertang@192.168.1.182 'bash -s' <<'REMOTE'
set -euo pipefail
rm -f /tmp/openlist-etf-data.db /tmp/openlist-etf-data.db-wal /tmp/openlist-etf-data.db-shm
docker cp openlist-etf:/opt/openlist/data/data.db /tmp/openlist-etf-data.db
(docker cp openlist-etf:/opt/openlist/data/data.db-wal /tmp/openlist-etf-data.db-wal >/dev/null 2>&1 || true)
(docker cp openlist-etf:/opt/openlist/data/data.db-shm /tmp/openlist-etf-data.db-shm >/dev/null 2>&1 || true)
sqlite3 /tmp/openlist-etf-data.db ".headers on" ".mode column" "select id,status,last_session_id,last_heartbeat_at from x_cluster_nodes where id='oplist-etf-139cloudPC';"
sqlite3 /tmp/openlist-etf-data.db ".headers on" ".mode column" "select id,status,connected_at,disconnected_at from x_cluster_node_sessions where node_id='oplist-etf-139cloudPC' order by connected_at desc limit 10;"
REMOTE
```

Expected: node exists with `status = online`; at least one session still shows `status = connected` and `disconnected_at` empty.

- [ ] **Step 3: Run the minimal SQL cleanup inside a transaction against a copied database, then replace the live files only if the SQL succeeds.**

```bash
sshpass -p 'Twain1996' ssh -o StrictHostKeyChecking=no entertang@192.168.1.182 'bash -s' <<'REMOTE'
set -euo pipefail
rm -f /tmp/openlist-etf-fix.db /tmp/openlist-etf-fix.db-wal /tmp/openlist-etf-fix.db-shm
docker cp openlist-etf:/opt/openlist/data/data.db /tmp/openlist-etf-fix.db
(docker cp openlist-etf:/opt/openlist/data/data.db-wal /tmp/openlist-etf-fix.db-wal >/dev/null 2>&1 || true)
(docker cp openlist-etf:/opt/openlist/data/data.db-shm /tmp/openlist-etf-fix.db-shm >/dev/null 2>&1 || true)
sqlite3 /tmp/openlist-etf-fix.db <<'SQL'
BEGIN IMMEDIATE;
UPDATE x_cluster_node_sessions
SET status = 'disconnected',
    disconnected_at = COALESCE(disconnected_at, CURRENT_TIMESTAMP),
    disconnect_error = CASE
        WHEN disconnect_error IS NULL OR disconnect_error = '' THEN 'manual stale session cleanup'
        ELSE disconnect_error
    END,
    updated_at = CURRENT_TIMESTAMP
WHERE node_id = 'oplist-etf-139cloudPC'
  AND status = 'connected';

UPDATE x_cluster_nodes
SET status = 'offline',
    updated_at = CURRENT_TIMESTAMP
WHERE id = 'oplist-etf-139cloudPC'
  AND status = 'online';
COMMIT;
SQL
sqlite3 /tmp/openlist-etf-fix.db ".headers on" ".mode column" "select id,status,last_session_id,last_heartbeat_at from x_cluster_nodes where id='oplist-etf-139cloudPC';"
sqlite3 /tmp/openlist-etf-fix.db ".headers on" ".mode column" "select id,status,connected_at,disconnected_at,disconnect_error from x_cluster_node_sessions where node_id='oplist-etf-139cloudPC' order by connected_at desc limit 10;"
REMOTE
```

Expected: copied database now shows node `offline`; connected sessions become `disconnected`.

- [ ] **Step 4: Replace the live database files with the cleaned copy during a short container stop/start window.**

```bash
sshpass -p 'Twain1996' ssh -o StrictHostKeyChecking=no entertang@192.168.1.182 'bash -s' <<'REMOTE'
set -euo pipefail
docker stop openlist-etf
cp /tmp/openlist-etf-fix.db /tmp/openlist-live-data.db
install -m 600 /tmp/openlist-live-data.db /tmp/openlist-live-data.db
container_id=$(docker create entergtang/openlist-etf:latest)
trap 'docker rm -f "$container_id" >/dev/null 2>&1 || true' EXIT
mkdir -p /tmp/openlist-live-data
cp /tmp/openlist-etf-fix.db /tmp/openlist-live-data/data.db
(docker cp /tmp/openlist-live-data/data.db "$container_id":/opt/openlist/data/data.db >/dev/null 2>&1 || true)
docker rm -f "$container_id" >/dev/null 2>&1 || true
trap - EXIT
docker start openlist-etf
sleep 5
docker logs --tail 40 openlist-etf
REMOTE
```

Expected: container restarts successfully and resumes serving.

- [ ] **Step 5: Re-run the verification queries after restart.**

```bash
sshpass -p 'Twain1996' ssh -o StrictHostKeyChecking=no entertang@192.168.1.182 'bash -s' <<'REMOTE'
set -euo pipefail
rm -f /tmp/openlist-etf-post.db /tmp/openlist-etf-post.db-wal /tmp/openlist-etf-post.db-shm
docker cp openlist-etf:/opt/openlist/data/data.db /tmp/openlist-etf-post.db
(docker cp openlist-etf:/opt/openlist/data/data.db-wal /tmp/openlist-etf-post.db-wal >/dev/null 2>&1 || true)
(docker cp openlist-etf:/opt/openlist/data/data.db-shm /tmp/openlist-etf-post.db-shm >/dev/null 2>&1 || true)
sqlite3 /tmp/openlist-etf-post.db ".headers on" ".mode column" "select id,status,last_heartbeat_at from x_cluster_nodes where id='oplist-etf-139cloudPC';"
sqlite3 /tmp/openlist-etf-post.db ".headers on" ".mode column" "select id,status,disconnected_at,disconnect_error from x_cluster_node_sessions where node_id='oplist-etf-139cloudPC' order by connected_at desc limit 10;"
REMOTE
```

Expected: node stays `offline`; no stale connected sessions remain.

## Task 2: Add failing coordinator-service tests for reconciliation, timeout, list filtering, and stale deletion

**Files:**
- Modify: `internal/cluster/coordinator/service_test.go`

**Interfaces:**
- Consumes: `coordinator.New(database, token)`, `openCoordinatorTestDB(t)`.
- Produces: test coverage for `ReconcileNodeSessions`, `SweepExpiredHeartbeats`, `ListNodes(ctx, includeStale bool)` or equivalent API, and `DeleteStaleNode(ctx, nodeID string, now time.Time, staleAfter time.Duration)` or equivalent API.

- [ ] **Step 1: Write the failing reconciliation test.**

```go
func TestReconcileNodeSessionsMarksConnectedSessionsAndOnlineNodesOffline(t *testing.T) {
	database := openCoordinatorTestDB(t)
	now := time.Unix(1721110000, 0).UTC()
	heartbeat := now.Add(-2 * time.Minute)
	require.NoError(t, database.Create(&model.ClusterNode{
		ID: "ghost-1", Name: "ghost-1", Role: model.ClusterRoleWorker,
		Status: model.ClusterNodeStatusOnline, LastSessionID: "session-1", LastHeartbeatAt: &heartbeat,
	}).Error)
	require.NoError(t, database.Create(&model.ClusterNodeSession{
		ID: "session-1", NodeID: "ghost-1", Status: model.ClusterSessionStatusConnected,
		ConnectedAt: now.Add(-10 * time.Minute),
	}).Error)
	require.NoError(t, database.Create(&model.ClusterNode{
		ID: "disabled-1", Status: model.ClusterNodeStatusDisabled, Disabled: true,
	}).Error)

	service := New(database, "secret")
	affected, err := service.ReconcileNodeSessions(context.Background(), now)
	require.NoError(t, err)
	require.EqualValues(t, 2, affected)

	var node model.ClusterNode
	require.NoError(t, database.First(&node, "id = ?", "ghost-1").Error)
	require.Equal(t, model.ClusterNodeStatusOffline, node.Status)

	var session model.ClusterNodeSession
	require.NoError(t, database.First(&session, "id = ?", "session-1").Error)
	require.Equal(t, model.ClusterSessionStatusDisconnected, session.Status)
	require.NotNil(t, session.DisconnectedAt)
	require.Contains(t, session.DisconnectError, "startup reconciliation")
}
```

- [ ] **Step 2: Write the failing heartbeat-timeout sweep test.**

```go
func TestSweepExpiredHeartbeatsMarksTimedOutNodesOffline(t *testing.T) {
	database := openCoordinatorTestDB(t)
	service := New(database, "secret")
	now := time.Unix(1721111000, 0).UTC()
	stale := now.Add(-2 * time.Minute)
	fresh := now.Add(-20 * time.Second)
	require.NoError(t, database.Create(&model.ClusterNode{
		ID: "timed-out", Status: model.ClusterNodeStatusOnline,
		LastSessionID: "session-timeout", LastHeartbeatAt: &stale,
	}).Error)
	require.NoError(t, database.Create(&model.ClusterNodeSession{
		ID: "session-timeout", NodeID: "timed-out", Status: model.ClusterSessionStatusConnected,
		ConnectedAt: now.Add(-5 * time.Minute),
	}).Error)
	require.NoError(t, database.Create(&model.ClusterNode{
		ID: "fresh", Status: model.ClusterNodeStatusOnline,
		LastSessionID: "session-fresh", LastHeartbeatAt: &fresh,
	}).Error)

	affected, err := service.SweepExpiredHeartbeats(context.Background(), now, time.Minute)
	require.NoError(t, err)
	require.EqualValues(t, 1, affected)

	var timedOut model.ClusterNode
	require.NoError(t, database.First(&timedOut, "id = ?", "timed-out").Error)
	require.Equal(t, model.ClusterNodeStatusOffline, timedOut.Status)

	var freshNode model.ClusterNode
	require.NoError(t, database.First(&freshNode, "id = ?", "fresh").Error)
	require.Equal(t, model.ClusterNodeStatusOnline, freshNode.Status)
}
```

- [ ] **Step 3: Write the failing list-filtering and effective-status tests.**

```go
func TestListNodesHidesStaleOfflineByDefaultAndShowsTimedOutNodeOffline(t *testing.T) {
	database := openCoordinatorTestDB(t)
	service := New(database, "secret")
	now := time.Unix(1721112000, 0).UTC()
	staleHeartbeat := now.Add(-8 * 24 * time.Hour)
	timedOutHeartbeat := now.Add(-2 * time.Minute)
	require.NoError(t, database.Create(&model.ClusterNode{
		ID: "stale-offline", Status: model.ClusterNodeStatusOffline, LastHeartbeatAt: &staleHeartbeat,
	}).Error)
	require.NoError(t, database.Create(&model.ClusterNode{
		ID: "timed-out-online", Status: model.ClusterNodeStatusOnline, LastHeartbeatAt: &timedOutHeartbeat,
	}).Error)

	defaultList, err := service.ListNodes(context.Background(), false, now)
	require.NoError(t, err)
	require.Len(t, defaultList, 1)
	require.Equal(t, "timed-out-online", defaultList[0].ID)
	require.Equal(t, model.ClusterNodeStatusOffline, defaultList[0].Status)

	fullList, err := service.ListNodes(context.Background(), true, now)
	require.NoError(t, err)
	require.Len(t, fullList, 2)
}
```

- [ ] **Step 4: Write the failing stale-node deletion tests.**

```go
func TestDeleteStaleNodeRemovesNodeOwnedMetadataButPreservesJobs(t *testing.T) {
	database := openCoordinatorTestDB(t)
	service := New(database, "secret")
	now := time.Unix(1721113000, 0).UTC()
	staleHeartbeat := now.Add(-8 * 24 * time.Hour)
	require.NoError(t, database.Create(&model.ClusterNode{
		ID: "stale-delete", Status: model.ClusterNodeStatusOffline, LastHeartbeatAt: &staleHeartbeat,
	}).Error)
	require.NoError(t, database.Create(&model.ClusterNodeSession{ID: "session-delete", NodeID: "stale-delete", Status: model.ClusterSessionStatusDisconnected}).Error)
	require.NoError(t, database.Create(&model.ClusterNodeInventory{ID: "inventory-delete", NodeID: "stale-delete", Revision: 1, CollectedAt: staleHeartbeat}).Error)
	require.NoError(t, database.Create(&model.ClusterNodeDesiredConfig{NodeID: "stale-delete", Status: model.ClusterDesiredStatusApplied}).Error)
	require.NoError(t, database.Create(&model.ClusterJob{ID: "job-delete", AssignedNodeID: "stale-delete", Status: model.ClusterJobStatusQueued}).Error)

	require.NoError(t, service.DeleteStaleNode(context.Background(), "stale-delete", now, 7*24*time.Hour))

	var count int64
	require.NoError(t, database.Model(&model.ClusterNode{}).Where("id = ?", "stale-delete").Count(&count).Error)
	require.Zero(t, count)
	require.NoError(t, database.Model(&model.ClusterJob{}).Where("id = ?", "job-delete").Count(&count).Error)
	require.EqualValues(t, 1, count)
}

func TestDeleteStaleNodeRejectsOnlineAndFreshOfflineNodes(t *testing.T) {
	database := openCoordinatorTestDB(t)
	service := New(database, "secret")
	now := time.Unix(1721114000, 0).UTC()
	freshHeartbeat := now.Add(-24 * time.Hour)
	onlineHeartbeat := now.Add(-10 * time.Second)
	require.NoError(t, database.Create(&model.ClusterNode{ID: "fresh-offline", Status: model.ClusterNodeStatusOffline, LastHeartbeatAt: &freshHeartbeat}).Error)
	require.NoError(t, database.Create(&model.ClusterNode{ID: "online-node", Status: model.ClusterNodeStatusOnline, LastHeartbeatAt: &onlineHeartbeat}).Error)

	err := service.DeleteStaleNode(context.Background(), "fresh-offline", now, 7*24*time.Hour)
	require.ErrorContains(t, err, "not stale offline")
	err = service.DeleteStaleNode(context.Background(), "online-node", now, 7*24*time.Hour)
	require.ErrorContains(t, err, "cannot be removed")
}
```

- [ ] **Step 5: Run the focused coordinator tests to confirm the expected failures.**

Run: `go test ./internal/cluster/coordinator -run 'Test(ReconcileNodeSessions|SweepExpiredHeartbeats|ListNodesHidesStaleOfflineByDefaultAndShowsTimedOutNodeOffline|DeleteStaleNode)' -count=1`

Expected: compile failure because the new service methods and the expanded `ListNodes` signature do not exist yet.

## Task 3: Implement coordinator-service reconciliation, timeout, visibility, and cleanup

**Files:**
- Modify: `internal/cluster/coordinator/service.go`

**Interfaces:**
- Consumes: persisted `model.ClusterNode`, `model.ClusterNodeSession`, `model.ClusterNodeInventory`, `model.ClusterNodeDesiredConfig`.
- Produces:
  - `func (s *Service) ReconcileNodeSessions(ctx context.Context, now time.Time) (int64, error)`
  - `func (s *Service) SweepExpiredHeartbeats(ctx context.Context, now time.Time, timeout time.Duration) (int64, error)`
  - `func (s *Service) ListNodes(ctx context.Context, includeStale bool, now time.Time) ([]NodeSummary, error)`
  - `func (s *Service) DeleteStaleNode(ctx context.Context, nodeID string, now time.Time, staleAfter time.Duration) error`
  - helper predicates for effective status and stale cutoff.

- [ ] **Step 1: Add the liveness helper functions near `NodeSummary`.**

```go
const defaultStaleOfflineThreshold = 7 * 24 * time.Hour

func effectiveHeartbeatTimeout(interval time.Duration) time.Duration {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	timeout := interval * 3
	if timeout < time.Minute {
		return time.Minute
	}
	return timeout
}

func nodeReferenceTime(node model.ClusterNode) time.Time {
	if node.LastHeartbeatAt != nil && !node.LastHeartbeatAt.IsZero() {
		return node.LastHeartbeatAt.UTC()
	}
	if !node.UpdatedAt.IsZero() {
		return node.UpdatedAt.UTC()
	}
	return node.CreatedAt.UTC()
}

func effectiveNodeStatus(node model.ClusterNode, now time.Time, heartbeatTimeout time.Duration) string {
	switch node.Status {
	case model.ClusterNodeStatusDisabled, model.ClusterNodeStatusRevoked, model.ClusterNodeStatusDraining:
		return node.Status
	case model.ClusterNodeStatusOnline:
		if now.IsZero() || node.LastHeartbeatAt == nil || node.LastHeartbeatAt.IsZero() {
			return node.Status
		}
		if node.LastHeartbeatAt.UTC().Before(now.Add(-heartbeatTimeout)) {
			return model.ClusterNodeStatusOffline
		}
	}
	return node.Status
}

func isStaleOfflineNode(node model.ClusterNode, now time.Time, staleAfter time.Duration, heartbeatTimeout time.Duration) bool {
	if staleAfter <= 0 {
		staleAfter = defaultStaleOfflineThreshold
	}
	if effectiveNodeStatus(node, now, heartbeatTimeout) != model.ClusterNodeStatusOffline {
		return false
	}
	return nodeReferenceTime(node).Before(now.Add(-staleAfter))
}
```

- [ ] **Step 2: Implement startup reconciliation and heartbeat timeout sweep.**

```go
func (s *Service) ReconcileNodeSessions(ctx context.Context, now time.Time) (int64, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var affected int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		sessionResult := tx.Model(&model.ClusterNodeSession{}).
			Where("status = ?", model.ClusterSessionStatusConnected).
			Updates(map[string]any{
				"status":           model.ClusterSessionStatusDisconnected,
				"disconnected_at":  now,
				"disconnect_error": "startup reconciliation",
			})
		if sessionResult.Error != nil {
			return sessionResult.Error
		}
		affected += sessionResult.RowsAffected

		nodeResult := tx.Model(&model.ClusterNode{}).
			Where("status = ?", model.ClusterNodeStatusOnline).
			Updates(map[string]any{"status": model.ClusterNodeStatusOffline, "updated_at": now})
		if nodeResult.Error != nil {
			return nodeResult.Error
		}
		affected += nodeResult.RowsAffected
		return nil
	})
	return affected, err
}

func (s *Service) SweepExpiredHeartbeats(ctx context.Context, now time.Time, timeout time.Duration) (int64, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if timeout <= 0 {
		timeout = time.Minute
	}
	cutoff := now.Add(-timeout)
	var nodes []model.ClusterNode
	if err := s.db.WithContext(ctx).
		Where("status = ? AND last_heartbeat_at IS NOT NULL AND last_heartbeat_at < ?", model.ClusterNodeStatusOnline, cutoff).
		Find(&nodes).Error; err != nil {
		return 0, err
	}
	var affected int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, node := range nodes {
			if node.Disabled || node.Status == model.ClusterNodeStatusRevoked {
				continue
			}
			result := tx.Model(&model.ClusterNode{}).
				Where("id = ? AND status = ?", node.ID, model.ClusterNodeStatusOnline).
				Updates(map[string]any{"status": model.ClusterNodeStatusOffline, "updated_at": now})
			if result.Error != nil {
				return result.Error
			}
			affected += result.RowsAffected
			if strings.TrimSpace(node.LastSessionID) != "" {
				if err := tx.Model(&model.ClusterNodeSession{}).
					Where("id = ? AND status = ?", node.LastSessionID, model.ClusterSessionStatusConnected).
					Updates(map[string]any{
						"status":           model.ClusterSessionStatusDisconnected,
						"disconnected_at":  now,
						"disconnect_error": "heartbeat timeout",
					}).Error; err != nil {
					return err
				}
			}
		}
		return nil
	})
	return affected, err
}
```

- [ ] **Step 3: Expand `ListNodes` to apply effective status and stale filtering.**

```go
func (s *Service) ListNodes(ctx context.Context, includeStale bool, now time.Time) ([]NodeSummary, error) {
	var nodes []model.ClusterNode
	if err := s.db.WithContext(ctx).Order("name ASC, id ASC").Find(&nodes).Error; err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return []NodeSummary{}, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	heartbeatTimeout := effectiveHeartbeatTimeout(15 * time.Second)
	filtered := make([]model.ClusterNode, 0, len(nodes))
	for _, node := range nodes {
		node.Status = effectiveNodeStatus(node, now, heartbeatTimeout)
		if !includeStale && isStaleOfflineNode(node, now, defaultStaleOfflineThreshold, heartbeatTimeout) {
			continue
		}
		filtered = append(filtered, node)
	}
	if len(filtered) == 0 {
		return []NodeSummary{}, nil
	}
	nodeIDs := make([]string, 0, len(filtered))
	for _, node := range filtered {
		nodeIDs = append(nodeIDs, node.ID)
	}
	// keep existing inventory-loading code, but iterate over filtered instead of nodes.
}
```

When finishing the method, preserve the existing inventory unmarshalling and summary assembly logic; only switch the source slice from `nodes` to `filtered`.

- [ ] **Step 4: Implement the stale-node deletion transaction at the end of `service.go`.**

```go
func (s *Service) DeleteStaleNode(ctx context.Context, nodeID string, now time.Time, staleAfter time.Duration) error {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return errors.New("cluster node id is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var node model.ClusterNode
		if err := tx.First(&node, "id = ?", nodeID).Error; err != nil {
			return err
		}
		status := effectiveNodeStatus(node, now, effectiveHeartbeatTimeout(15*time.Second))
		if status != model.ClusterNodeStatusOffline || !isStaleOfflineNode(node, now, staleAfter, effectiveHeartbeatTimeout(15*time.Second)) {
			return errors.New("cluster node is not stale offline")
		}
		if err := tx.Where("node_id = ?", nodeID).Delete(&model.ClusterNodeSession{}).Error; err != nil {
			return err
		}
		if err := tx.Where("node_id = ?", nodeID).Delete(&model.ClusterNodeInventory{}).Error; err != nil {
			return err
		}
		if err := tx.Where("node_id = ?", nodeID).Delete(&model.ClusterNodeDesiredConfig{}).Error; err != nil {
			return err
		}
		if err := tx.Where("id = ?", nodeID).Delete(&model.ClusterNode{}).Error; err != nil {
			return err
		}
		return nil
	})
}
```

Before finalizing the code, extend the deletion block to every actual node-owned config table discovered in `internal/model` or `internal/cluster` helper code, but do not delete job, attempt, outbox, inbox, or manifest tables.

- [ ] **Step 5: Run the focused coordinator suite.**

Run: `go test ./internal/cluster/coordinator -run 'Test(ReconcileNodeSessions|SweepExpiredHeartbeats|ListNodesHidesStaleOfflineByDefaultAndShowsTimedOutNodeOffline|DeleteStaleNode)' -count=1`

Expected: PASS.

## Task 4: Wire reconciliation and heartbeat sweeping into runtime startup

**Files:**
- Modify: `internal/cluster/runtime.go`
- Test: `internal/cluster/runtime_security_test.go` or `internal/cluster/embedded_redis_runtime_test.go`

**Interfaces:**
- Consumes: `coordinator.New(...)`, `heartbeatInterval()`.
- Produces:
  - startup call to `coordinatorService.ReconcileNodeSessions(...)`
  - background goroutine `runHeartbeatTimeoutSweep(ctx, coordinatorService)`

- [ ] **Step 1: Write the failing runtime startup test.**

```go
func TestRuntimeStartReconcilesCoordinatorSessionsBeforeServing(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	conf.Conf.Cluster.Role = string(RoleCoordinator)
	conf.Conf.Cluster.NodeID = "coordinator-a"
	conf.Conf.Cluster.EnrollmentToken = "enrollment-secret"
	t.Cleanup(func() { conf.Conf = original })

	database, err := gorm.Open(sqlite.Open("file:runtime_reconcile?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, database.AutoMigrate(&model.ClusterNode{}, &model.ClusterNodeSession{}, &model.ClusterCoordinatorLease{}))
	db.Init(database)
	staleHeartbeat := time.Now().Add(-5 * time.Minute).UTC()
	require.NoError(t, database.Create(&model.ClusterNode{ID: "ghost-1", Status: model.ClusterNodeStatusOnline, LastSessionID: "session-1", LastHeartbeatAt: &staleHeartbeat}).Error)
	require.NoError(t, database.Create(&model.ClusterNodeSession{ID: "session-1", NodeID: "ghost-1", Status: model.ClusterSessionStatusConnected, ConnectedAt: time.Now().Add(-10 * time.Minute).UTC()}).Error)

	runtime := &Runtime{}
	require.NoError(t, runtime.Start())
	t.Cleanup(runtime.Stop)

	var node model.ClusterNode
	require.NoError(t, database.First(&node, "id = ?", "ghost-1").Error)
	require.Equal(t, model.ClusterNodeStatusOffline, node.Status)
}
```

- [ ] **Step 2: Run the runtime test to verify it fails first.**

Run: `go test ./internal/cluster -run TestRuntimeStartReconcilesCoordinatorSessionsBeforeServing -count=1`

Expected: FAIL because runtime startup does not yet reconcile stale coordinator state.

- [ ] **Step 3: Add the runtime hook and sweeper goroutine.**

```go
func (r *Runtime) runHeartbeatTimeoutSweep(ctx context.Context, service *coordinator.Service) {
	interval := heartbeatInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if _, err := service.SweepExpiredHeartbeats(ctx, now.UTC(), effectiveHeartbeatTimeout(interval)); err != nil {
				log.Warnf("sweep expired cluster heartbeats: %v", err)
			}
		}
	}
}
```

In `Runtime.Start`, immediately after `coordinatorService := coordinator.New(...)` and before assigning `r.hub`, insert:

```go
if _, err := coordinatorService.ReconcileNodeSessions(runtimeCtx, time.Now().UTC()); err != nil {
	r.stopLocked()
	return fmt.Errorf("reconcile stale cluster sessions: %w", err)
}
```

Then after `go r.runManifestProcessor(runtimeCtx, coordinatorService)`, add:

```go
go r.runHeartbeatTimeoutSweep(runtimeCtx, coordinatorService)
```

- [ ] **Step 4: Re-run the focused runtime test and relevant cluster package tests.**

Run: `go test ./internal/cluster -run 'TestRuntimeStartReconcilesCoordinatorSessionsBeforeServing|TestFenceLostCoordinator' -count=1`

Expected: PASS.

## Task 5: Expose stale filtering and manual cleanup through admin HTTP handlers

**Files:**
- Modify: `server/handles/cluster.go`
- Modify: `server/router.go`
- Test: `server/handles/cluster_test.go` if file exists; otherwise create focused tests under `server/handles/cluster_test.go`.

**Interfaces:**
- Consumes: `service.ListNodes(ctx, includeStale, now)` and `service.DeleteStaleNode(ctx, nodeID, now, 7*24*time.Hour)`.
- Produces:
  - `GET /api/admin/cluster/nodes?include_stale=true|false`
  - `POST /api/admin/cluster/nodes/:id/delete` or repository-consistent delete endpoint for stale nodes only.

- [ ] **Step 1: Write the failing HTTP handler tests.**

```go
func TestListClusterNodesHidesStaleOfflineByDefault(t *testing.T) {
	gin.SetMode(gin.TestMode)
	database := openCoordinatorTestDB(t)
	service := coordinator.New(database, "secret")
	cluster.SetDefaultRuntimeForTest(service) // replace with the actual test hook already used by this package
	staleHeartbeat := time.Now().Add(-8 * 24 * time.Hour).UTC()
	require.NoError(t, database.Create(&model.ClusterNode{ID: "stale-offline", Status: model.ClusterNodeStatusOffline, LastHeartbeatAt: &staleHeartbeat}).Error)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/admin/cluster/nodes", nil)
	handles.ListClusterNodes(c)
	require.Equal(t, http.StatusOK, w.Code)
	require.NotContains(t, w.Body.String(), "stale-offline")
}

func TestDeleteClusterNodeRejectsFreshOfflineNode(t *testing.T) {
	gin.SetMode(gin.TestMode)
	database := openCoordinatorTestDB(t)
	service := coordinator.New(database, "secret")
	cluster.SetDefaultRuntimeForTest(service)
	freshHeartbeat := time.Now().Add(-24 * time.Hour).UTC()
	require.NoError(t, database.Create(&model.ClusterNode{ID: "fresh-offline", Status: model.ClusterNodeStatusOffline, LastHeartbeatAt: &freshHeartbeat}).Error)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Params = gin.Params{{Key: "id", Value: "fresh-offline"}}
	c.Request = httptest.NewRequest(http.MethodPost, "/api/admin/cluster/nodes/fresh-offline/delete", nil)
	handles.DeleteClusterNode(c)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "not stale offline")
}
```

Use the actual cluster-runtime test hook available in this package; if none exists, instantiate the handler dependencies directly rather than inventing a new global hook.

- [ ] **Step 2: Run the handler tests to confirm they fail.**

Run: `go test ./server/handles -run 'Test(ListClusterNodesHidesStaleOfflineByDefault|DeleteClusterNodeRejectsFreshOfflineNode)' -count=1`

Expected: compile failure because the handler and route contract do not yet support the new behavior.

- [ ] **Step 3: Implement the list parameter and cleanup handler.**

```go
func ListClusterNodes(c *gin.Context) {
	service := cluster.CoordinatorService()
	if service == nil {
		common.ErrorStrResp(c, "cluster coordinator is disabled", 400)
		return
	}
	includeStale := c.Query("include_stale") == "true"
	nodes, err := service.ListNodes(c.Request.Context(), includeStale, time.Now().UTC())
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, nodes)
}

func DeleteClusterNode(c *gin.Context) {
	service := cluster.CoordinatorService()
	if service == nil {
		common.ErrorStrResp(c, "cluster coordinator is disabled", 400)
		return
	}
	if err := service.DeleteStaleNode(c.Request.Context(), c.Param("id"), time.Now().UTC(), 7*24*time.Hour); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	common.SuccessResp(c, gin.H{"deleted": true})
}
```

In `server/router.go`, register the new cleanup route next to the existing cluster node routes using the repository’s established admin route style.

- [ ] **Step 4: Re-run the handler tests.**

Run: `go test ./server/handles -run 'Test(ListClusterNodesHidesStaleOfflineByDefault|DeleteClusterNodeRejectsFreshOfflineNode)' -count=1`

Expected: PASS.

## Task 6: Run the end-to-end verification set and inspect the diff

**Files:**
- Modify: any files touched by Tasks 2-5
- Plan/status docs only if you track execution progress there

**Interfaces:**
- Consumes: completed implementation from previous tasks.
- Produces: verified code changes ready for review and optional commit.

- [ ] **Step 1: Run the focused backend test suites.**

Run: `go test ./internal/cluster/coordinator ./internal/cluster ./server/handles -count=1`

Expected: PASS.

- [ ] **Step 2: Run the broader cluster-related regression suite.**

Run: `go test ./internal/cluster/... ./internal/subscription -count=1`

Expected: PASS.

- [ ] **Step 3: Inspect the working tree before any commit.**

Run: `git status --short`

Expected: only the intended cluster stale-node files and any pre-existing unrelated changes.

- [ ] **Step 4: Review the diff for liveness semantics and route shape.**

Run: `git diff -- internal/cluster/coordinator/service.go internal/cluster/runtime.go server/handles/cluster.go server/router.go internal/cluster/coordinator/service_test.go`

Expected: diff shows reconciliation, timeout sweep, stale filtering, cleanup handler, and tests only.

- [ ] **Step 5: Commit the implementation when the diff is clean.**

```bash
git add internal/cluster/coordinator/service.go internal/cluster/runtime.go server/handles/cluster.go server/router.go internal/cluster/coordinator/service_test.go server/handles/cluster_test.go docs/superpowers/specs/2026-07-16-cluster-stale-node-cleanup-design.md docs/superpowers/plans/2026-07-16-cluster-stale-node-cleanup.md
git commit -m "fix(cluster): reconcile stale nodes and hide stale offline workers"
```

## Self-Review

- Spec coverage check: Task 1 covers remote SQL cleanup. Tasks 2-4 cover startup reconciliation and heartbeat timeout. Tasks 3 and 5 cover default stale-node hiding and manual cleanup API. Task 6 covers verification.
- Placeholder scan: all tasks specify exact files, commands, and expected outcomes; no `TODO`/`TBD` placeholders remain.
- Type consistency check: the plan consistently uses `ReconcileNodeSessions`, `SweepExpiredHeartbeats`, `ListNodes(ctx, includeStale, now)`, and `DeleteStaleNode` as the coordinator-service API surface.

package coordinator

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/transport"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Service struct {
	db                     *gorm.DB
	enrollmentToken        string
	heartbeatInterval      time.Duration
	inspectMu              sync.RWMutex
	inspectProcessMu       sync.Mutex
	inspectConsumer        ShareInspectConsumer
	torrentDispatcher      TorrentJobDispatcher
	moviePilotControllerMu sync.RWMutex
	moviePilotController   MoviePilotTorrentController
}

const (
	defaultHeartbeatInterval    = 15 * time.Second
	defaultHeartbeatTimeout     = time.Minute
	defaultStaleOfflineDuration = 7 * 24 * time.Hour
)

type NodeInventorySummary struct {
	CollectedAt      *time.Time                          `json:"collected_at,omitempty"`
	InventoryHash    string                              `json:"inventory_hash,omitempty"`
	Capabilities     *protocol.NodeCapabilities          `json:"capabilities,omitempty"`
	Mounts           []protocol.MountInventory           `json:"mounts,omitempty"`
	ProviderAccounts []protocol.ProviderAccountInventory `json:"provider_accounts,omitempty"`
}

type NodeSummary struct {
	model.ClusterNode
	LatestInventory *NodeInventorySummary `json:"latest_inventory,omitempty"`
}

// RetrySubscriptionResult describes the durable recovery performed for a
// subscription's failed media children. Unmatched counts are intentionally
// reported instead of being reset: a failed item without a durable cluster
// job cannot be safely replayed by the coordinator.
type RetrySubscriptionResult struct {
	Requeued  int
	Unmatched int
}

// ShareInspectConsumer is the subscription-facing durable handoff. The
// manifest remains pending until the consumer returns nil, so a Coordinator
// restart cannot lose an inspected share tree.
type ShareInspectConsumer func(context.Context, model.ClusterShareInspectManifest, protocol.ShareInspectManifest) error

var ErrShareInspectObservationIncomplete = errors.New("cluster share inspect observation is incomplete")

func New(database *gorm.DB, enrollmentToken string) *Service {
	return &Service{db: database, enrollmentToken: strings.TrimSpace(enrollmentToken), heartbeatInterval: defaultHeartbeatInterval}
}

func (s *Service) SetHeartbeatInterval(interval time.Duration) {
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}
	s.heartbeatInterval = interval
}

func (s *Service) HeartbeatInterval() time.Duration {
	interval := s.heartbeatInterval
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}
	return interval
}

func (s *Service) heartbeatTimeout() time.Duration {
	interval := s.heartbeatInterval
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}
	timeout := interval * 3
	if timeout < defaultHeartbeatTimeout {
		return defaultHeartbeatTimeout
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
		if heartbeatTimeout <= 0 {
			heartbeatTimeout = defaultHeartbeatTimeout
		}
		if node.LastHeartbeatAt.UTC().Before(now.Add(-heartbeatTimeout)) {
			return model.ClusterNodeStatusOffline
		}
	}
	return node.Status
}

func isStaleOfflineNode(node model.ClusterNode, now time.Time, staleAfter, heartbeatTimeout time.Duration) bool {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if staleAfter <= 0 {
		staleAfter = defaultStaleOfflineDuration
	}
	if effectiveNodeStatus(node, now, heartbeatTimeout) != model.ClusterNodeStatusOffline {
		return false
	}
	return nodeReferenceTime(node).Before(now.Add(-staleAfter))
}

func (s *Service) SetShareInspectConsumer(consumer ShareInspectConsumer) {
	s.inspectMu.Lock()
	s.inspectConsumer = consumer
	s.inspectMu.Unlock()
}

func (s *Service) ShareInspectManifest(ctx context.Context, jobID string) (*model.ClusterShareInspectManifest, error) {
	var item model.ClusterShareInspectManifest
	if err := s.db.WithContext(ctx).First(&item, "job_id = ?", strings.TrimSpace(jobID)).Error; err != nil {
		return nil, err
	}
	return &item, nil
}

// ProcessPendingShareInspects delivers sealed manifests to the registered
// subscription consumer. Delivery is intentionally pull/retry based: result
// persistence succeeds even when no consumer is currently registered.
func (s *Service) ProcessPendingShareInspects(ctx context.Context, limit int) (int, error) {
	s.inspectProcessMu.Lock()
	defer s.inspectProcessMu.Unlock()
	s.inspectMu.RLock()
	consumer := s.inspectConsumer
	s.inspectMu.RUnlock()
	if consumer == nil {
		return 0, nil
	}
	if limit <= 0 {
		limit = 20
	}
	var items []model.ClusterShareInspectManifest
	if err := s.db.WithContext(ctx).Where("status = ?", model.ClusterShareInspectStatusPending).Order("julianday(updated_at) ASC, julianday(created_at) ASC").Limit(limit).Find(&items).Error; err != nil {
		return 0, err
	}
	consumed := 0
	for i := range items {
		var current model.ClusterShareInspectManifest
		if err := s.db.WithContext(ctx).Select("status").First(&current, "id = ?", items[i].ID).Error; err != nil {
			return consumed, err
		}
		if current.Status != model.ClusterShareInspectStatusPending {
			continue
		}
		var manifest protocol.ShareInspectManifest
		if err := json.Unmarshal([]byte(items[i].PayloadJSON), &manifest); err != nil {
			_ = s.db.WithContext(ctx).Model(&items[i]).Updates(map[string]any{
				"last_error": err.Error(), "updated_at": time.Now().UTC(),
			}).Error
			continue
		}
		if err := consumer(ctx, items[i], manifest); err != nil {
			if !errors.Is(err, ErrShareInspectObservationIncomplete) {
				// Move failed records to the back of the pending queue. A stale
				// observation must not starve newer providers, especially when a
				// rate-limited HDHive observation keeps failing validation.
				_ = s.db.WithContext(ctx).Model(&items[i]).Updates(map[string]any{
					"last_error": err.Error(), "updated_at": time.Now().UTC(),
				}).Error
			}
			continue
		}
		now := time.Now().UTC()
		result := s.db.WithContext(ctx).Model(&model.ClusterShareInspectManifest{}).
			Where("id = ? AND status = ?", items[i].ID, model.ClusterShareInspectStatusPending).
			Updates(map[string]any{"status": model.ClusterShareInspectStatusConsumed, "consumed_at": now, "last_error": ""})
		if result.Error != nil {
			return consumed, result.Error
		}
		consumed += int(result.RowsAffected)
	}
	return consumed, nil
}

func (s *Service) Authenticate(_ context.Context, _ *http.Request, hello protocol.Hello) error {
	if strings.TrimSpace(hello.NodeID) == "" {
		return errors.New("node id is required")
	}
	if s.enrollmentToken == "" {
		return errors.New("cluster enrollment token is not configured")
	}
	provided := []byte(hello.EnrollmentToken)
	expected := []byte(s.enrollmentToken)
	if len(provided) != len(expected) || subtle.ConstantTimeCompare(provided, expected) != 1 {
		return errors.New("invalid enrollment token")
	}
	if s.db != nil {
		var node model.ClusterNode
		err := s.db.First(&node, "id = ?", hello.NodeID).Error
		if err == nil && (node.Disabled || node.Status == model.ClusterNodeStatusRevoked) {
			return errors.New("cluster node is disabled or revoked")
		}
		if err == nil && node.KeyID != "" && hello.KeyAgreement == nil {
			return errors.New("cluster node must present its pinned key agreement identity")
		}
		if err == nil && hello.KeyAgreement != nil {
			if node.KeyID != "" && (node.KeyID != hello.KeyAgreement.KeyID || node.KeyPublic != hello.KeyAgreement.PublicKey) {
				return errors.New("cluster node key does not match the pinned identity")
			}
			if node.KeyID == "" {
				if err := s.db.Model(&node).Updates(map[string]any{
					"key_algorithm": hello.KeyAgreement.Algorithm,
					"key_id":        hello.KeyAgreement.KeyID,
					"key_public":    hello.KeyAgreement.PublicKey,
				}).Error; err != nil {
					return err
				}
			}
		}
		if errors.Is(err, gorm.ErrRecordNotFound) && hello.KeyAgreement != nil {
			node = model.ClusterNode{
				ID: hello.NodeID, Name: hello.NodeName, Role: model.ClusterRoleWorker,
				Status: model.ClusterNodeStatusPending, KeyAlgorithm: hello.KeyAgreement.Algorithm,
				KeyID: hello.KeyAgreement.KeyID, KeyPublic: hello.KeyAgreement.PublicKey,
			}
			if err := s.db.Create(&node).Error; err != nil {
				return err
			}
			err = nil
		}
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
	}
	return nil
}

func (s *Service) SetNodeState(ctx context.Context, nodeID, state string) error {
	nodeID = strings.TrimSpace(nodeID)
	state = strings.TrimSpace(strings.ToLower(state))
	if nodeID == "" {
		return errors.New("cluster node id is required")
	}
	updates := map[string]any{"status": state, "drain": false, "disabled": false}
	switch state {
	case model.ClusterNodeStatusDraining:
		updates["drain"] = true
	case model.ClusterNodeStatusDisabled, model.ClusterNodeStatusRevoked:
		updates["disabled"] = true
	case model.ClusterNodeStatusOnline:
	default:
		return fmt.Errorf("unsupported cluster node state %q", state)
	}
	result := s.db.WithContext(ctx).Model(&model.ClusterNode{}).Where("id = ?", nodeID).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func (s *Service) OnConnect(peer transport.Peer) {
	if s.db == nil {
		return
	}
	now := time.Now().UTC()
	node := model.ClusterNode{
		ID:              peer.NodeID(),
		Role:            model.ClusterRoleWorker,
		Status:          model.ClusterNodeStatusOnline,
		ProtocolVersion: protocol.Version1,
		LastSessionID:   peer.SessionID(),
		LastHeartbeatAt: &now,
	}
	_ = s.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "id"}},
		DoUpdates: clause.Assignments(map[string]any{
			"status":            node.Status,
			"protocol_version":  node.ProtocolVersion,
			"last_session_id":   node.LastSessionID,
			"last_heartbeat_at": now,
			"updated_at":        now,
		}),
	}).Create(&node).Error
	session := model.ClusterNodeSession{
		ID:              peer.SessionID(),
		NodeID:          peer.NodeID(),
		Status:          model.ClusterSessionStatusConnected,
		ProtocolVersion: protocol.Version1,
		ConnectedAt:     now,
		ConnectionEpoch: peer.ConnectionEpoch(),
	}
	_ = s.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&session).Error
}

func (s *Service) OnDisconnect(peer transport.Peer, cause error) {
	if s.db == nil {
		return
	}
	now := time.Now().UTC()
	updates := map[string]any{"status": model.ClusterSessionStatusDisconnected, "disconnected_at": now}
	if cause != nil {
		updates["disconnect_error"] = cause.Error()
	}
	if sequenced, ok := peer.(transport.SequencePeer); ok {
		updates["last_received_seq"] = sequenced.LastReceivedSeq()
		updates["last_sent_seq"] = sequenced.LastSentSeq()
	}
	_ = s.db.Model(&model.ClusterNodeSession{}).Where("id = ?", peer.SessionID()).Updates(updates).Error
	_ = s.db.Model(&model.ClusterNode{}).Where("id = ? AND last_session_id = ?", peer.NodeID(), peer.SessionID()).Updates(map[string]any{"status": model.ClusterNodeStatusOffline}).Error
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := s.ReconcileWorkerOfflineTorrentControl(ctx, 100); err != nil {
			log.Warnf("coordinator: pause MoviePilot torrents after Worker disconnect: %v", err)
		}
	}()
}

func (s *Service) ReconcileNodeSessions(ctx context.Context, now time.Time) (int64, error) {
	if s.db == nil {
		return 0, errors.New("cluster database is unavailable")
	}
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

// RequeueNodeAttempts releases attempts owned by a worker process that has
// restarted. The worker's active-task map is in memory, so those attempts
// cannot be resumed after a process restart. Requeueing them before the new
// session connects avoids waiting for the normal media lease timeout.
func (s *Service) RequeueNodeAttempts(ctx context.Context, nodeID string, now time.Time) (int64, error) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return 0, errors.New("cluster node id is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var requeued int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var attempts []model.ClusterJobAttempt
		if err := tx.Where("node_id = ? AND status IN ?", nodeID, []string{
			model.ClusterAttemptStatusOffered,
			model.ClusterAttemptStatusAccepted,
			model.ClusterAttemptStatusRunning,
		}).Find(&attempts).Error; err != nil {
			return err
		}
		for i := range attempts {
			attempt := &attempts[i]
			var job model.ClusterJob
			if err := tx.First(&job, "id = ?", attempt.JobID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					continue
				}
				return err
			}
			if job.CurrentAttemptID != attempt.ID || job.CurrentGeneration != attempt.Generation {
				continue
			}
			if job.Status != model.ClusterJobStatusLeased && job.Status != model.ClusterJobStatusRunning {
				continue
			}
			if err := failNonTerminalAttemptStagesTx(tx, &job, "worker_restarted", "worker process restarted before completion", now); err != nil {
				return err
			}
			if err := tx.Model(attempt).Updates(map[string]any{
				"status":      model.ClusterAttemptStatusLost,
				"finished_at": now,
				"error_code":  "worker_restarted",
				"error":       "worker process restarted before completion",
			}).Error; err != nil {
				return err
			}
			result := tx.Model(&model.ClusterJob{}).
				Where("id = ? AND current_attempt_id = ? AND current_generation = ?", job.ID, attempt.ID, attempt.Generation).
				Updates(map[string]any{
					"status":             model.ClusterJobStatusQueued,
					"assigned_node_id":   "",
					"current_attempt_id": "",
					"finished_at":        nil,
					"last_error_code":    "worker_restarted",
					"last_error":         "worker process restarted before completion",
					"available_at":       now,
				})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected == 0 {
				continue
			}
			if err := tx.Model(&model.ClusterOutbox{}).
				Where("correlation_id = ? AND message_type = ? AND status IN ?", attempt.JobID, protocol.MessageJobOffer, []string{model.ClusterMessageStatusPending, model.ClusterMessageStatusSending}).
				Updates(map[string]any{"status": model.ClusterMessageStatusFailed, "last_error": "worker restarted; superseded by retry"}).Error; err != nil {
				return err
			}
			requeued++
		}
		return nil
	})
	return requeued, err
}

func (s *Service) SweepExpiredHeartbeats(ctx context.Context, now time.Time, timeout time.Duration) (int64, error) {
	if s.db == nil {
		return 0, errors.New("cluster database is unavailable")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if timeout <= 0 {
		timeout = s.heartbeatTimeout()
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
			if strings.TrimSpace(node.LastSessionID) == "" {
				continue
			}
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
		return nil
	})
	return affected, err
}

func (s *Service) HandleMessage(ctx context.Context, peer transport.Peer, message protocol.Envelope) error {
	if s.db == nil {
		return errors.New("cluster database is unavailable")
	}
	switch message.Type {
	case protocol.MessageHeartbeat:
		payload, err := protocol.DecodePayload[protocol.Heartbeat](message)
		if err != nil {
			return err
		}
		return s.handleHeartbeat(ctx, peer, payload)
	case protocol.MessageInventoryReport:
		payload, err := protocol.DecodePayload[protocol.InventoryReport](message)
		if err != nil {
			return err
		}
		return s.handleInventory(ctx, peer, payload)
	case protocol.MessageUploadETFManifest:
		payload, err := protocol.DecodePayload[protocol.UploadETFManifest](message)
		if err != nil {
			return err
		}
		return s.handleUploadManifest(ctx, peer, message, payload)
	case protocol.MessageJobAccept:
		payload, err := protocol.DecodePayload[protocol.JobAccept](message)
		if err != nil {
			return err
		}
		return s.handleJobAccept(ctx, peer, message, payload)
	case protocol.MessageJobReject:
		payload, err := protocol.DecodePayload[protocol.JobReject](message)
		if err != nil {
			return err
		}
		return s.handleJobReject(ctx, peer, message, payload)
	case protocol.MessageJobProgress:
		payload, err := protocol.DecodePayload[protocol.JobProgress](message)
		if err != nil {
			return err
		}
		return s.handleJobProgress(ctx, peer, message, payload)
	case protocol.MessageJobResult:
		payload, err := protocol.DecodePayload[protocol.JobResult](message)
		if err != nil {
			return err
		}
		return s.handleJobResult(ctx, peer, message, payload)
	case protocol.MessageLeaseRenew:
		payload, err := protocol.DecodePayload[protocol.LeaseRenew](message)
		if err != nil {
			return err
		}
		return s.handleLeaseRenew(ctx, peer, message, payload)
	case protocol.MessageStagePermitRequest:
		payload, err := protocol.DecodePayload[protocol.StagePermitRequest](message)
		if err != nil {
			return err
		}
		return s.handleStagePermitRequest(ctx, peer, message, payload)
	case protocol.MessageStageStatus:
		payload, err := protocol.DecodePayload[protocol.StageStatusUpdate](message)
		if err != nil {
			return err
		}
		return s.handleStageStatus(ctx, peer, message, payload)
	case protocol.MessageConfigObserved:
		payload, err := protocol.DecodePayload[protocol.ConfigObserved](message)
		if err != nil {
			return err
		}
		return s.handleConfigObserved(ctx, peer, message, payload)
	case protocol.MessageStorageApplyResult:
		payload, err := protocol.DecodePayload[protocol.StorageApplyResult](message)
		if err != nil {
			return err
		}
		return s.handleStorageApplyResult(ctx, peer, message, payload)
	case protocol.MessageAck:
		payload, err := protocol.DecodePayload[protocol.Ack](message)
		if err != nil {
			return err
		}
		return s.handleAck(ctx, peer, message, payload)
	default:
		return s.recordInbox(ctx, peer, message, model.ClusterMessageStatusProcessed, "")
	}
}

func (s *Service) handleJobProgress(ctx context.Context, peer transport.Peer, message protocol.Envelope, progress protocol.JobProgress) error {
	if progress.EventSeq == 0 {
		return errors.New("cluster job progress event_seq is required")
	}
	if strings.TrimSpace(progress.Stage) == "" {
		return errors.New("cluster job progress stage is required")
	}
	if progress.CompletedBytes < 0 || progress.TotalBytes < 0 || (progress.TotalBytes > 0 && progress.CompletedBytes > progress.TotalBytes) {
		return errors.New("cluster job progress byte counts are invalid")
	}
	observedAt := progress.ObservedAt.UTC()
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	progress.ObservedAt = observedAt
	progressJSON, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		duplicate, err := s.claimInboxTx(tx, peer, message)
		if err != nil || duplicate {
			return err
		}
		var job model.ClusterJob
		if err := tx.First(&job, "id = ?", progress.JobID).Error; err != nil {
			return err
		}
		if job.CurrentAttemptID != progress.AttemptID || job.CurrentGeneration != progress.Generation || job.AssignedNodeID != peer.NodeID() {
			return errors.New("cluster job progress is stale")
		}
		attempt, err := loadAndValidateAttempt(tx, peer, progress.AttemptRef)
		if err != nil {
			return err
		}
		if !containsString([]string{model.ClusterAttemptStatusAccepted, model.ClusterAttemptStatusRunning}, attempt.Status) {
			return fmt.Errorf("cluster job attempt cannot report progress from status %q", attempt.Status)
		}
		if progress.EventSeq <= attempt.LastEventSeq {
			return s.finishInboxTx(tx, peer, message, model.ClusterMessageStatusProcessed, "")
		}

		now := time.Now().UTC()
		attemptUpdates := map[string]any{"last_event_seq": progress.EventSeq, "status": model.ClusterAttemptStatusRunning}
		if attempt.StartedAt == nil {
			attemptUpdates["started_at"] = now
		}
		if err := tx.Model(&model.ClusterJobAttempt{}).Where("id = ? AND last_event_seq < ?", attempt.ID, progress.EventSeq).Updates(attemptUpdates).Error; err != nil {
			return err
		}
		var existingStage model.ClusterJobStage
		err = tx.Select("status").Where("attempt_id = ? AND name = ?", attempt.ID, progress.Stage).First(&existingStage).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil && containsString([]string{model.ClusterStageStatusSucceeded, model.ClusterStageStatusFailed}, existingStage.Status) {
			return s.finishInboxTx(tx, peer, message, model.ClusterMessageStatusProcessed, "")
		}
		stage := model.ClusterJobStage{
			ID: uuid.NewString(), JobID: job.ID, AttemptID: attempt.ID, Name: progress.Stage,
			Status: model.ClusterStageStatusRunning, ProgressJSON: string(progressJSON), StartedAt: &now,
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "attempt_id"}, {Name: "name"}},
			DoUpdates: clause.Assignments(map[string]any{
				"progress_json": string(progressJSON),
				"status":        model.ClusterStageStatusRunning,
				"started_at":    gorm.Expr("COALESCE(started_at, ?)", now),
			}),
		}).Create(&stage).Error; err != nil {
			return err
		}

		fraction := float64(0)
		if progress.TotalBytes > 0 {
			fraction = float64(progress.CompletedBytes) / float64(progress.TotalBytes)
		}
		var task protocol.TaskContext
		if json.Unmarshal([]byte(job.TaskContextJSON), &task) == nil && task.Torrent != nil {
			switch {
			case job.Type == model.ClusterJobTypeTorrentObserve:
				if err := tx.Model(&model.MoviePilotTorrentBinding{}).Where("id = ?", task.Torrent.BindingID).Updates(map[string]any{
					"last_qb_progress": fraction,
					"status":           model.MoviePilotTorrentStatusDownloading,
				}).Error; err != nil && !isOptionalMoviePilotTableError(err) {
					return err
				}
			case job.Type == model.ClusterJobTypeMediaTransfer && progress.Stage == model.ClusterStageUploadingMobile:
				if err := tx.Model(&model.MoviePilotDeliveryFile{}).Where("cluster_job_id = ?", job.ID).Updates(map[string]any{
					"upload_progress": fraction,
					"status":          model.MoviePilotDeliveryStatusUploading,
				}).Error; err != nil && !isOptionalMoviePilotTableError(err) {
					return err
				}
			}
		}
		return s.finishInboxTx(tx, peer, message, model.ClusterMessageStatusProcessed, "")
	})
}

func (s *Service) handleStageStatus(ctx context.Context, peer transport.Peer, message protocol.Envelope, update protocol.StageStatusUpdate) error {
	internalStage := isInternalWorkerStage(update.Stage)
	if update.Stage != model.ClusterStageSavingShare && update.Stage != model.ClusterStageUploadingMobile && update.Stage != model.ClusterStageWorkerMediaCleanup && !internalStage {
		return fmt.Errorf("cluster stage %q cannot receive status updates", update.Stage)
	}
	if update.Status != model.ClusterStageStatusRunning && update.Status != model.ClusterStageStatusSucceeded && update.Status != model.ClusterStageStatusFailed {
		return fmt.Errorf("cluster stage status %q is invalid", update.Status)
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		duplicate, err := s.claimInboxTx(tx, peer, message)
		if err != nil || duplicate {
			return err
		}
		var job model.ClusterJob
		if err := tx.First(&job, "id = ?", update.JobID).Error; err != nil {
			return err
		}
		if job.CurrentAttemptID != update.AttemptID || job.CurrentGeneration != update.Generation || job.AssignedNodeID != peer.NodeID() {
			return errors.New("cluster stage status is stale")
		}
		attempt, err := loadAndValidateAttempt(tx, peer, update.AttemptRef)
		if err != nil {
			return err
		}
		var stage model.ClusterJobStage
		if err := tx.Where("attempt_id = ? AND name = ?", attempt.ID, update.Stage).First(&stage).Error; err != nil {
			if !errors.Is(err, gorm.ErrRecordNotFound) || (!internalStage && update.Stage != model.ClusterStageWorkerMediaCleanup) {
				return err
			}
			stage = model.ClusterJobStage{
				ID: uuid.NewString(), JobID: job.ID, AttemptID: attempt.ID,
				Name: update.Stage, Status: model.ClusterStageStatusPending,
			}
			if err := tx.Create(&stage).Error; err != nil {
				return err
			}
		}
		if update.Stage == model.ClusterStageWorkerMediaCleanup || internalStage {
			switch stage.Status {
			case model.ClusterStageStatusPending:
				if update.Status != model.ClusterStageStatusRunning && update.Status != model.ClusterStageStatusSucceeded && update.Status != model.ClusterStageStatusFailed {
					return fmt.Errorf("cluster cleanup transition %s -> %s is invalid", stage.Status, update.Status)
				}
			case model.ClusterStageStatusRunning:
				if update.Status != model.ClusterStageStatusRunning && update.Status != model.ClusterStageStatusSucceeded && update.Status != model.ClusterStageStatusFailed {
					return fmt.Errorf("cluster cleanup transition %s -> %s is invalid", stage.Status, update.Status)
				}
			case model.ClusterStageStatusFailed:
				if update.Status != model.ClusterStageStatusRunning && update.Status != model.ClusterStageStatusSucceeded && update.Status != model.ClusterStageStatusFailed {
					return fmt.Errorf("cluster cleanup transition %s -> %s is invalid", stage.Status, update.Status)
				}
			case model.ClusterStageStatusSucceeded:
				if update.Status != model.ClusterStageStatusSucceeded {
					return fmt.Errorf("cluster terminal cleanup cannot transition to %s", update.Status)
				}
				return s.finishInboxTx(tx, peer, message, model.ClusterMessageStatusProcessed, "")
			default:
				return fmt.Errorf("cluster cleanup current status %q is invalid", stage.Status)
			}
		} else {
			switch stage.Status {
			case model.ClusterStageStatusPermitted:
				if update.Status != model.ClusterStageStatusRunning {
					return fmt.Errorf("cluster stage transition %s -> %s is invalid", stage.Status, update.Status)
				}
			case model.ClusterStageStatusRunning:
				if update.Status != model.ClusterStageStatusSucceeded && update.Status != model.ClusterStageStatusFailed {
					return fmt.Errorf("cluster stage transition %s -> %s is invalid", stage.Status, update.Status)
				}
			case model.ClusterStageStatusSucceeded, model.ClusterStageStatusFailed:
				if update.Status != stage.Status {
					return fmt.Errorf("cluster terminal stage %s cannot transition to %s", stage.Status, update.Status)
				}
				return s.finishInboxTx(tx, peer, message, model.ClusterMessageStatusProcessed, "")
			default:
				return fmt.Errorf("cluster stage current status %q is invalid", stage.Status)
			}
		}
		now := time.Now().UTC()
		updates := map[string]any{"status": update.Status}
		if update.Status == model.ClusterStageStatusRunning && stage.StartedAt == nil {
			updates["started_at"] = now
		}
		if update.Status == model.ClusterStageStatusSucceeded || update.Status == model.ClusterStageStatusFailed {
			updates["finished_at"] = now
			if update.Status == model.ClusterStageStatusFailed {
				updates["error"] = update.Error
			} else {
				updates["error"] = ""
			}
		}
		if err := tx.Model(&model.ClusterJobStage{}).Where("id = ? AND attempt_id = ?", stage.ID, attempt.ID).Updates(updates).Error; err != nil {
			return err
		}
		if update.Status == model.ClusterStageStatusRunning {
			attemptUpdates := map[string]any{"status": model.ClusterAttemptStatusRunning}
			if attempt.StartedAt == nil {
				attemptUpdates["started_at"] = now
			}
			if err := tx.Model(&model.ClusterJobAttempt{}).Where("id = ?", attempt.ID).Updates(attemptUpdates).Error; err != nil {
				return err
			}
		}
		if update.Stage == model.ClusterStageWorkerMediaCleanup {
			cleanupStatus := model.ClusterCleanupStatusRunning
			if update.Status == model.ClusterStageStatusSucceeded {
				cleanupStatus = model.ClusterCleanupStatusSucceeded
			} else if update.Status == model.ClusterStageStatusFailed {
				cleanupStatus = model.ClusterCleanupStatusFailed
			}
			if err := tx.Model(&model.ClusterJob{}).Where("id = ?", job.ID).Update("worker_cleanup_status", cleanupStatus).Error; err != nil {
				return err
			}
		}
		return s.finishInboxTx(tx, peer, message, model.ClusterMessageStatusProcessed, "")
	})
}

func isInternalWorkerStage(stage string) bool {
	switch stage {
	case model.ClusterStageQBObserving, model.ClusterStageQBCopying, model.ClusterStageRetentionCheck, model.ClusterStageQBDeleting:
		return true
	default:
		return false
	}
}

func (s *Service) handleStagePermitRequest(ctx context.Context, peer transport.Peer, message protocol.Envelope, request protocol.StagePermitRequest) error {
	if request.Stage != model.ClusterStageSavingShare && request.Stage != model.ClusterStageUploadingMobile {
		return fmt.Errorf("cluster stage %q cannot receive an external side-effect permit", request.Stage)
	}
	var permit protocol.StagePermit
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		duplicate, err := s.claimInboxTx(tx, peer, message)
		if err != nil || duplicate {
			return err
		}
		var job model.ClusterJob
		if err := tx.First(&job, "id = ?", request.JobID).Error; err != nil {
			return err
		}
		if job.CurrentAttemptID != request.AttemptID || job.CurrentGeneration != request.Generation || job.AssignedNodeID != peer.NodeID() {
			return errors.New("cluster stage permit request is stale")
		}
		attempt, err := loadAndValidateAttempt(tx, peer, request.AttemptRef)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		if !attempt.LeaseUntil.After(now.Add(5 * time.Second)) {
			return errors.New("cluster lease is too close to expiry for a stage permit")
		}
		if !containsString([]string{model.ClusterAttemptStatusAccepted, model.ClusterAttemptStatusRunning}, attempt.Status) {
			return fmt.Errorf("cluster attempt status %q cannot receive a stage permit", attempt.Status)
		}
		operationKey := job.IdempotencyKey + ":" + request.Stage
		if request.OperationKey != operationKey {
			return errors.New("cluster stage operation key does not match the job")
		}
		expiresAt := now.Add(30 * time.Second)
		if expiresAt.After(attempt.LeaseUntil) {
			expiresAt = attempt.LeaseUntil
		}
		token := uuid.NewString()
		stage := model.ClusterJobStage{
			ID: uuid.NewString(), JobID: job.ID, AttemptID: attempt.ID, Name: request.Stage,
			Status: model.ClusterStageStatusPermitted, OperationKey: operationKey,
			PermitTokenHash: fmt.Sprintf("%x", sha256.Sum256([]byte(token))), PermitExpiresAt: &expiresAt,
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "attempt_id"}, {Name: "name"}},
			DoUpdates: clause.Assignments(map[string]any{
				"status": model.ClusterStageStatusPermitted, "operation_key": operationKey,
				"permit_token_hash": stage.PermitTokenHash, "permit_expires_at": expiresAt,
			}),
		}).Create(&stage).Error; err != nil {
			return err
		}
		permit = protocol.StagePermit{
			AttemptRef: request.AttemptRef, Stage: request.Stage, OperationKey: operationKey,
			PermitToken: token, PermitExpiresAt: expiresAt,
		}
		return s.finishInboxTx(tx, peer, message, model.ClusterMessageStatusProcessed, "")
	})
	if err != nil {
		return err
	}
	if permit.PermitToken == "" {
		return nil
	}
	response, err := protocol.NewEnvelope(protocol.MessageStagePermit, permit)
	if err != nil {
		return err
	}
	response.CorrelationID = message.MessageID
	response.NodeID = peer.NodeID()
	return peer.Send(ctx, *response)
}

func (s *Service) handleLeaseRenew(ctx context.Context, peer transport.Peer, message protocol.Envelope, renewal protocol.LeaseRenew) error {
	now := time.Now().UTC()
	requestedUntil := renewal.RequestedUntil.UTC()
	if !requestedUntil.After(now) {
		return errors.New("cluster lease renewal must extend into the future")
	}
	maxUntil := now.Add(30 * time.Minute)
	if requestedUntil.After(maxUntil) {
		requestedUntil = maxUntil
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		duplicate, err := s.claimInboxTx(tx, peer, message)
		if err != nil || duplicate {
			return err
		}
		var job model.ClusterJob
		if err := tx.First(&job, "id = ?", renewal.JobID).Error; err != nil {
			return err
		}
		if job.CurrentAttemptID != renewal.AttemptID || job.CurrentGeneration != renewal.Generation || job.AssignedNodeID != peer.NodeID() {
			return errors.New("cluster lease renewal is stale")
		}
		attempt, err := loadAndValidateAttempt(tx, peer, renewal.AttemptRef)
		if err != nil {
			return err
		}
		if !containsString([]string{model.ClusterAttemptStatusAccepted, model.ClusterAttemptStatusRunning}, attempt.Status) {
			return fmt.Errorf("cluster attempt status %q cannot renew a lease", attempt.Status)
		}
		if attempt.LeaseUntil.Before(now.Add(-30 * time.Second)) {
			return errors.New("cluster lease is already expired")
		}
		if err := tx.Model(attempt).Updates(map[string]any{
			"lease_until":    requestedUntil,
			"last_event_seq": gorm.Expr("CASE WHEN last_event_seq > ? THEN last_event_seq ELSE ? END", renewal.LastEventSeq, renewal.LastEventSeq),
			"status":         model.ClusterAttemptStatusRunning,
		}).Error; err != nil {
			return err
		}
		return s.finishInboxTx(tx, peer, message, model.ClusterMessageStatusProcessed, "")
	})
}

func (s *Service) handleAck(ctx context.Context, peer transport.Peer, message protocol.Envelope, ack protocol.Ack) error {
	messageID := strings.TrimSpace(ack.MessageID)
	if messageID == "" {
		messageID = strings.TrimSpace(message.CorrelationID)
	}
	if messageID == "" {
		return nil
	}
	now := time.Now().UTC()
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		duplicate, err := s.claimInboxTx(tx, peer, message)
		if err != nil || duplicate {
			return err
		}
		if err := tx.Model(&model.ClusterOutbox{}).Where("message_id = ? AND peer_node_id = ?", messageID, peer.NodeID()).Updates(map[string]any{
			"status":   model.ClusterMessageStatusAcked,
			"acked_at": now,
		}).Error; err != nil {
			return err
		}
		return s.finishInboxTx(tx, peer, message, model.ClusterMessageStatusProcessed, "")
	})
}

func (s *Service) handleJobAccept(ctx context.Context, peer transport.Peer, message protocol.Envelope, accepted protocol.JobAccept) error {
	now := accepted.AcceptedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		duplicate, err := s.claimInboxTx(tx, peer, message)
		if err != nil || duplicate {
			return err
		}
		var job model.ClusterJob
		if err := tx.First(&job, "id = ?", accepted.JobID).Error; err != nil {
			return err
		}
		if job.CurrentAttemptID != accepted.AttemptID || job.CurrentGeneration != accepted.Generation || job.AssignedNodeID != peer.NodeID() {
			return errors.New("cluster job accept is stale")
		}
		attempt, err := loadAndValidateAttempt(tx, peer, accepted.AttemptRef)
		if err != nil {
			return err
		}
		if attempt.LeaseUntil.Before(time.Now().UTC()) {
			return errors.New("cluster job accept lease has expired")
		}
		if attempt.Status != model.ClusterAttemptStatusOffered && attempt.Status != model.ClusterAttemptStatusAccepted {
			return fmt.Errorf("cluster job attempt cannot be accepted from status %q", attempt.Status)
		}
		result := tx.Model(&model.ClusterJobAttempt{}).
			Where("id = ? AND status IN ?", attempt.ID, []string{model.ClusterAttemptStatusOffered, model.ClusterAttemptStatusAccepted}).
			Updates(map[string]any{"status": model.ClusterAttemptStatusAccepted, "accepted_at": now})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return errors.New("cluster job attempt is stale or belongs to another node")
		}
		if err := tx.Model(&model.ClusterJob{}).Where("id = ?", accepted.JobID).Update("status", model.ClusterJobStatusRunning).Error; err != nil {
			return err
		}
		return s.finishInboxTx(tx, peer, message, model.ClusterMessageStatusProcessed, "")
	})
}

const workerAdmissionRetryDelay = 10 * time.Second

func (s *Service) handleJobReject(ctx context.Context, peer transport.Peer, message protocol.Envelope, rejected protocol.JobReject) error {
	code := strings.TrimSpace(rejected.Code)
	if code == "" {
		return errors.New("cluster job rejection code is required")
	}
	reason := strings.TrimSpace(rejected.Reason)
	if reason == "" {
		reason = code
	}
	now := time.Now().UTC()
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		duplicate, err := s.claimInboxTx(tx, peer, message)
		if err != nil || duplicate {
			return err
		}
		var job model.ClusterJob
		if err := tx.First(&job, "id = ?", rejected.JobID).Error; err != nil {
			return err
		}
		if job.CurrentAttemptID != rejected.AttemptID || job.CurrentGeneration != rejected.Generation || job.AssignedNodeID != peer.NodeID() {
			return errors.New("cluster job rejection is stale")
		}
		attempt, err := loadAndValidateAttempt(tx, peer, rejected.AttemptRef)
		if err != nil {
			return err
		}
		if !containsString([]string{model.ClusterAttemptStatusOffered, model.ClusterAttemptStatusAccepted, model.ClusterAttemptStatusRunning}, attempt.Status) {
			return fmt.Errorf("cluster job attempt cannot be rejected from status %q", attempt.Status)
		}
		if err := tx.Model(attempt).Updates(map[string]any{
			"status": model.ClusterAttemptStatusRejected, "finished_at": now,
			"error_code": code, "error": reason,
		}).Error; err != nil {
			return err
		}
		jobUpdates := map[string]any{
			"assigned_node_id": "", "current_attempt_id": "",
			"last_error_code": code, "last_error": reason,
		}
		exhausted := rejected.Retryable && job.Type == model.ClusterJobTypeMediaTransfer && attempt.Generation >= automaticMediaTransferAttemptLimit
		if rejected.Retryable && !exhausted {
			jobUpdates["status"] = model.ClusterJobStatusQueued
			jobUpdates["finished_at"] = nil
			jobUpdates["archived_at"] = nil
			jobUpdates["available_at"] = now.Add(workerAdmissionRetryDelay)
		} else if exhausted {
			jobUpdates["status"] = model.ClusterJobStatusDeadLetter
			jobUpdates["finished_at"] = now
			jobUpdates["last_error_code"] = "retry_limit_exceeded"
			jobUpdates["last_error"] = "worker rejected the media job and the automatic retry limit was reached"
		} else {
			jobUpdates["status"] = model.ClusterJobStatusFailed
			jobUpdates["finished_at"] = now
		}
		updated := tx.Model(&model.ClusterJob{}).
			Where("id = ? AND current_attempt_id = ? AND current_generation = ?", job.ID, attempt.ID, attempt.Generation).
			Updates(jobUpdates)
		if updated.Error != nil {
			return updated.Error
		}
		if updated.RowsAffected == 0 {
			return errors.New("cluster job rejection is stale or belongs to another attempt")
		}
		if err := tx.Model(&model.ClusterOutbox{}).
			Where("correlation_id = ? AND message_type = ? AND status IN ?", job.ID, protocol.MessageJobOffer, []string{model.ClusterMessageStatusPending, model.ClusterMessageStatusSending}).
			Updates(map[string]any{"status": model.ClusterMessageStatusFailed, "last_error": "worker rejected offer: " + code}).Error; err != nil {
			return err
		}
		return s.finishInboxTx(tx, peer, message, model.ClusterMessageStatusProcessed, "")
	})
}

const automaticMediaTransferAttemptLimit uint64 = 3

func shouldRetryMediaTransfer(errorCode string, generation uint64) bool {
	if generation >= automaticMediaTransferAttemptLimit {
		return false
	}
	switch strings.TrimSpace(errorCode) {
	case "source_unexpected_eof", "source_range_failed", "source_link_expired", "network_timeout", "timeout",
		"share_save_retryable", "share_save_rate_limited", "share_save_gateway_response", "share_save_transient":
		return true
	default:
		return false
	}
}

func mediaTransferRetryDelay(generation uint64) time.Duration {
	switch generation {
	case 0, 1:
		return 15 * time.Second
	case 2:
		return time.Minute
	default:
		return 3 * time.Minute
	}
}

func (s *Service) handleJobResult(ctx context.Context, peer transport.Peer, message protocol.Envelope, result protocol.JobResult) error {
	log.Infof("coordinator: handleJobResult jobID=%s, attempt=%s, gen=%d, status=%s", result.JobID, result.AttemptID, result.Generation, result.Status)
	now := result.FinishedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	storedInspect := false
	storedTorrentObservation := false
	storedTorrentRetention := false
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		duplicate, err := s.claimInboxTx(tx, peer, message)
		if err != nil || duplicate {
			return err
		}
		var job model.ClusterJob
		if err := tx.First(&job, "id = ?", result.JobID).Error; err != nil {
			return err
		}
		storedTorrentRetention = job.Type == model.ClusterJobTypeTorrentRetention
		if job.CurrentAttemptID != result.AttemptID || job.CurrentGeneration != result.Generation || job.AssignedNodeID != peer.NodeID() {
			return errors.New("cluster job result is stale")
		}
		attempt, err := loadAndValidateAttempt(tx, peer, result.AttemptRef)
		if err != nil {
			return err
		}
		allowedStatuses := []string{model.ClusterAttemptStatusAccepted, model.ClusterAttemptStatusRunning}
		if result.Status == "succeeded" && attempt.Status == model.ClusterAttemptStatusSucceeded && attempt.ResultHash == result.ResultHash {
			allowedStatuses = append(allowedStatuses, model.ClusterAttemptStatusSucceeded)
		}
		if result.Status != "succeeded" && attempt.Status == model.ClusterAttemptStatusFailed && attempt.ResultHash == result.ResultHash {
			allowedStatuses = append(allowedStatuses, model.ClusterAttemptStatusFailed)
		}
		if !containsString(allowedStatuses, attempt.Status) {
			return fmt.Errorf("cluster job attempt cannot report a result from status %q", attempt.Status)
		}
		attemptStatus := model.ClusterAttemptStatusFailed
		jobUpdates := map[string]any{"last_error_code": result.ErrorCode, "last_error": result.Error}
		if result.Status == "succeeded" {
			directDelivery := false
			if result.Result != nil {
				if mode, ok := result.Result["delivery_mode"].(string); ok && strings.EqualFold(strings.TrimSpace(mode), model.SubscriptionDeliveryModeDirectDownload) {
					directDelivery = true
				}
			}
			if job.Type == model.ClusterJobTypeShareInspect {
				payloadHash, err := persistShareInspectResultTx(tx, peer.NodeID(), &job, attempt, result)
				if err != nil {
					return err
				}
				if result.ResultHash == "" {
					result.ResultHash = payloadHash
				}
				storedInspect = true
			} else if job.Type == model.ClusterJobTypeTorrentObserve {
				storedTorrentObservation = true
			}
			attemptStatus = model.ClusterAttemptStatusSucceeded
			if job.Type == model.ClusterJobTypeShareInspect || job.Type == model.ClusterJobTypeTorrentObserve || job.Type == model.ClusterJobTypeTorrentRetention {
				jobUpdates["status"] = model.ClusterJobStatusSucceeded
				jobUpdates["finished_at"] = now
			} else if directDelivery {
				jobUpdates["status"] = model.ClusterJobStatusSucceeded
				jobUpdates["finished_at"] = now
			} else if job.Status == model.ClusterJobStatusSucceeded {
				jobUpdates["status"] = model.ClusterJobStatusSucceeded
			} else {
				jobUpdates["status"] = model.ClusterJobStatusRunning
			}
			jobUpdates["result_delivery_status"] = model.ClusterResultDeliveryStatusQueued
		} else {
			retryScheduled := false
			if job.Type == model.ClusterJobTypeMediaTransfer {
				jobUpdates["notification_status"] = model.ClusterNotificationStatusNotStarted
				if err := failNonTerminalAttemptStagesTx(tx, &job, result.ErrorCode, result.Error, now); err != nil {
					return err
				}
				if shouldRetryMediaTransfer(result.ErrorCode, job.CurrentGeneration) {
					retryScheduled = true
					jobUpdates["status"] = model.ClusterJobStatusQueued
					jobUpdates["finished_at"] = nil
					jobUpdates["archived_at"] = nil
					jobUpdates["assigned_node_id"] = ""
					jobUpdates["current_attempt_id"] = ""
					jobUpdates["available_at"] = now.Add(mediaTransferRetryDelay(job.CurrentGeneration))
				}
			}
			if !retryScheduled {
				jobUpdates["status"] = model.ClusterJobStatusFailed
				jobUpdates["finished_at"] = now
			}
			if job.Type == model.ClusterJobTypeShareInspect {
				if _, err := persistFailedShareInspectResultTx(tx, peer.NodeID(), &job, attempt, result); err != nil {
					return err
				}
				storedInspect = true
			}
			if job.Type == model.ClusterJobTypeTorrentObserve && jobUpdates["status"] == model.ClusterJobStatusFailed && job.TaskContextJSON != "" {
				var task protocol.TaskContext
				if json.Unmarshal([]byte(job.TaskContextJSON), &task) == nil && task.Torrent != nil {
					_ = tx.Model(&model.MoviePilotTorrentBinding{}).Where("id = ?", task.Torrent.BindingID).Updates(map[string]any{
						"status": model.MoviePilotTorrentStatusFailed, "last_error_code": result.ErrorCode, "last_error": result.Error,
					}).Error
				}
			}
		}
		updated := tx.Model(&model.ClusterJobAttempt{}).
			Where("id = ? AND status IN ?", attempt.ID, allowedStatuses).
			Updates(map[string]any{
				"status":      attemptStatus,
				"finished_at": now,
				"result_hash": result.ResultHash,
				"error_code":  result.ErrorCode,
				"error":       result.Error,
			})
		if updated.Error != nil {
			return updated.Error
		}
		if updated.RowsAffected == 0 {
			return errors.New("cluster job result is stale or belongs to another node")
		}
		if err := tx.Model(&model.ClusterJob{}).Where("id = ?", result.JobID).Updates(jobUpdates).Error; err != nil {
			return err
		}
		if result.Status == "succeeded" && job.Type == model.ClusterJobTypeMediaTransfer {
			if mode, ok := result.Result["delivery_mode"].(string); ok && strings.EqualFold(strings.TrimSpace(mode), model.SubscriptionDeliveryModeDirectDownload) && job.SubscriptionItemID != 0 {
				updates := map[string]any{
					"status":          model.SubscriptionItemStatusTransferred,
					"last_error":      "",
					"last_error_code": "",
					"retry_at":        nil,
					"blocked_reason":  "",
					"delivery_mode":   model.SubscriptionDeliveryModeDirectDownload,
					"state_version":   gorm.Expr("COALESCE(state_version, 0) + 1"),
				}
				if fallbackReason, ok := result.Result["fallback_reason"].(string); ok {
					updates["fallback_reason"] = strings.TrimSpace(fallbackReason)
				}
				if err := tx.Model(&model.SubscriptionItem{}).
					Where("id = ? AND cluster_job_id = ?", job.SubscriptionItemID, job.ID).
					Updates(updates).Error; err != nil {
					return err
				}
			}
		}
		if result.Status != "succeeded" && job.Type == model.ClusterJobTypeMediaTransfer && job.SubscriptionItemID != 0 && jobUpdates["status"] == model.ClusterJobStatusFailed {
			if err := tx.Model(&model.SubscriptionItem{}).
				Where("id = ? AND cluster_job_id = ?", job.SubscriptionItemID, job.ID).
				Updates(map[string]any{"status": model.SubscriptionItemStatusFailed, "last_error": result.Error, "last_error_code": result.ErrorCode, "state_version": gorm.Expr("COALESCE(state_version, 0) + 1")}).Error; err != nil {
				return err
			}
		}
		if result.Status != "succeeded" && job.Type == model.ClusterJobTypeMediaTransfer {
			deliveryStatus := model.MoviePilotDeliveryStatusFailed
			if jobUpdates["status"] == model.ClusterJobStatusQueued {
				deliveryStatus = model.MoviePilotDeliveryStatusPending
			}
			if err := tx.Model(&model.MoviePilotDeliveryFile{}).Where("cluster_job_id = ?", job.ID).Updates(map[string]any{
				"status": deliveryStatus, "last_error_code": result.ErrorCode, "last_error": result.Error,
			}).Error; err != nil && !isOptionalMoviePilotTableError(err) {
				return err
			}
		}
		if job.ParentJobID != "" {
			if err := reconcileParentJobTx(tx, job.ParentJobID, now); err != nil {
				return err
			}
		}
		return s.finishInboxTx(tx, peer, message, model.ClusterMessageStatusProcessed, "")
	})
	if err != nil {
		log.Warnf("coordinator: handleJobResult failed for job %s: %v", result.JobID, err)
		return err
	}
	if storedInspect {
		_, _ = s.ProcessPendingShareInspects(ctx, 1)
	}
	if storedTorrentObservation {
		if err := s.ObserveTorrent(ctx, result.JobID, result.Result); err != nil {
			log.Warnf("coordinator: persist MoviePilot torrent observation %s: %v", result.JobID, err)
		}
	}
	if storedTorrentRetention {
		if err := s.completeTorrentRetention(ctx, result.JobID, result); err != nil {
			log.Warnf("coordinator: finalize MoviePilot torrent retention %s: %v", result.JobID, err)
		}
	}
	return nil
}

func failNonTerminalAttemptStagesTx(tx *gorm.DB, job *model.ClusterJob, errorCode, stageError string, now time.Time) error {
	if tx == nil || job == nil || strings.TrimSpace(job.CurrentAttemptID) == "" {
		return nil
	}
	if strings.TrimSpace(stageError) == "" {
		stageError = "cluster media transfer failed before the worker reported a terminal stage status"
	}
	return tx.Model(&model.ClusterJobStage{}).
		Where("job_id = ? AND attempt_id = ? AND status IN ?", job.ID, job.CurrentAttemptID, []string{
			model.ClusterStageStatusPending,
			model.ClusterStageStatusPermitted,
			model.ClusterStageStatusRunning,
		}).
		Updates(map[string]any{
			"status":      model.ClusterStageStatusFailed,
			"finished_at": now,
			"error_code":  errorCode,
			"error":       stageError,
		}).Error
}

func persistShareInspectResultTx(tx *gorm.DB, nodeID string, job *model.ClusterJob, attempt *model.ClusterJobAttempt, result protocol.JobResult) (string, error) {
	if job == nil || attempt == nil {
		return "", errors.New("share inspection job context is unavailable")
	}
	raw, err := json.Marshal(result.Result)
	if err != nil {
		return "", fmt.Errorf("encode share inspection result: %w", err)
	}
	var manifest protocol.ShareInspectManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return "", fmt.Errorf("decode share inspection result: %w", err)
	}
	var taskContext protocol.TaskContext
	if err := json.Unmarshal([]byte(job.TaskContextJSON), &taskContext); err != nil {
		return "", fmt.Errorf("decode share inspection task context: %w", err)
	}
	if strings.TrimSpace(manifest.Version) == "" || manifest.Version != taskContext.SealedManifestVersion {
		return "", errors.New("share inspection manifest version does not match the sealed task version")
	}
	if manifest.Share.Provider != taskContext.Share.Provider || manifest.Share.URL != taskContext.Share.URL || manifest.Share.Passcode != taskContext.Share.Passcode {
		return "", errors.New("share inspection manifest does not match the offered share")
	}
	objectsJSON, err := json.Marshal(manifest.Objects)
	if err != nil {
		return "", fmt.Errorf("encode share inspection objects: %w", err)
	}
	objectHash := fmt.Sprintf("%x", sha256.Sum256(objectsJSON))
	if !strings.EqualFold(strings.TrimSpace(manifest.ObjectHash), objectHash) {
		return "", errors.New("share inspection manifest object hash is invalid")
	}
	payloadHash := fmt.Sprintf("%x", sha256.Sum256(raw))
	if result.ResultHash != "" && !strings.EqualFold(result.ResultHash, payloadHash) {
		return "", errors.New("share inspection result hash is invalid")
	}
	item := model.ClusterShareInspectManifest{
		ID: uuid.NewString(), JobID: job.ID, AttemptID: attempt.ID, NodeID: nodeID,
		Generation: attempt.Generation, SubscriptionID: job.SubscriptionID,
		ObservationKey:      taskContext.Subscription.ObservationKey,
		ObservationExpected: taskContext.Subscription.ObservationExpected,
		Version:             manifest.Version, CanonicalRef: manifest.CanonicalRef,
		ObjectHash: objectHash, PayloadJSON: string(raw), PayloadHash: payloadHash,
		Status: model.ClusterShareInspectStatusPending, InspectedAt: manifest.InspectedAt.UTC(),
	}
	if strings.TrimSpace(item.ObservationKey) == "" {
		item.ObservationKey = job.ID
	}
	if item.ObservationExpected <= 0 {
		item.ObservationExpected = 1
	}
	if item.InspectedAt.IsZero() {
		item.InspectedAt = time.Now().UTC()
	}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&item).Error; err != nil {
		return "", err
	}
	var stored model.ClusterShareInspectManifest
	if err := tx.First(&stored, "job_id = ?", job.ID).Error; err != nil {
		return "", err
	}
	if stored.PayloadHash != payloadHash {
		return "", errors.New("share inspection job already has a conflicting sealed manifest")
	}
	return payloadHash, nil
}

func persistFailedShareInspectResultTx(tx *gorm.DB, nodeID string, job *model.ClusterJob, attempt *model.ClusterJobAttempt, result protocol.JobResult) (string, error) {
	if job == nil || attempt == nil {
		return "", errors.New("share inspection job context is unavailable")
	}
	var taskContext protocol.TaskContext
	if err := json.Unmarshal([]byte(job.TaskContextJSON), &taskContext); err != nil {
		return "", fmt.Errorf("decode share inspection task context: %w", err)
	}
	objectsJSON := []byte("[]")
	objectHash := fmt.Sprintf("%x", sha256.Sum256(objectsJSON))
	manifest := protocol.ShareInspectManifest{
		Version:      taskContext.SealedManifestVersion,
		Share:        taskContext.Share,
		CanonicalRef: strings.TrimSpace(taskContext.Share.URL),
		Objects:      nil,
		ObjectHash:   objectHash,
		InspectedAt:  nowOr(result.FinishedAt),
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("encode failed share inspection result: %w", err)
	}
	payloadHash := fmt.Sprintf("%x", sha256.Sum256(raw))
	item := model.ClusterShareInspectManifest{
		ID: uuid.NewString(), JobID: job.ID, AttemptID: attempt.ID, NodeID: nodeID,
		Generation: attempt.Generation, SubscriptionID: job.SubscriptionID,
		ObservationKey:      taskContext.Subscription.ObservationKey,
		ObservationExpected: taskContext.Subscription.ObservationExpected,
		Version:             manifest.Version, CanonicalRef: manifest.CanonicalRef,
		ObjectHash: objectHash, PayloadJSON: string(raw), PayloadHash: payloadHash,
		Status: model.ClusterShareInspectStatusPending, InspectedAt: manifest.InspectedAt.UTC(),
		LastError: strings.TrimSpace(result.Error),
	}
	if strings.TrimSpace(item.ObservationKey) == "" {
		item.ObservationKey = job.ID
	}
	if item.ObservationExpected <= 0 {
		item.ObservationExpected = 1
	}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&item).Error; err != nil {
		return "", err
	}
	var stored model.ClusterShareInspectManifest
	if err := tx.First(&stored, "job_id = ?", job.ID).Error; err != nil {
		return "", err
	}
	if stored.PayloadHash != payloadHash {
		return "", errors.New("share inspection job already has a conflicting sealed manifest")
	}
	return payloadHash, nil
}

func nowOr(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now().UTC()
	}
	return value.UTC()
}

func reconcileParentJobTx(tx *gorm.DB, parentJobID string, now time.Time) error {
	parentJobID = strings.TrimSpace(parentJobID)
	if parentJobID == "" {
		return nil
	}
	var parent model.ClusterJob
	if err := tx.Select("expected_items").First(&parent, "id = ?", parentJobID).Error; err != nil {
		return err
	}
	var children []model.ClusterJob
	if err := tx.Select("status").Where("parent_job_id = ?", parentJobID).Find(&children).Error; err != nil {
		return err
	}
	if len(children) == 0 {
		return nil
	}
	succeeded := 0
	failed := 0
	for i := range children {
		switch children[i].Status {
		case model.ClusterJobStatusSucceeded:
			succeeded++
		case model.ClusterJobStatusFailed, model.ClusterJobStatusDeadLetter, model.ClusterJobStatusCancelled:
			failed++
		}
	}
	updates := map[string]any{"status": model.ClusterJobStatusRunning, "finished_at": nil}
	expected := parent.ExpectedItems
	if expected <= 0 {
		expected = len(children)
	}
	if succeeded == expected && len(children) == expected {
		updates["status"] = model.ClusterJobStatusSucceeded
		updates["finished_at"] = now
	} else if failed > 0 || len(children) < expected {
		updates["status"] = model.ClusterJobStatusPartialFailed
		if len(children) == expected && succeeded+failed == expected {
			updates["finished_at"] = now
		}
	}
	return tx.Model(&model.ClusterJob{}).Where("id = ?", parentJobID).Updates(updates).Error
}

func (s *Service) ReplayOutbox(ctx context.Context, peer transport.Peer) error {
	var items []model.ClusterOutbox
	if err := s.db.WithContext(ctx).Where("peer_node_id = ? AND status IN ? AND available_at <= ?", peer.NodeID(), []string{model.ClusterMessageStatusPending, model.ClusterMessageStatusSending}, time.Now().UTC()).Order("created_at ASC").Limit(100).Find(&items).Error; err != nil {
		return err
	}
	for i := range items {
		item := &items[i]
		messageType := protocol.MessageType(item.MessageType)
		message := protocol.Envelope{
			ProtocolVersion: protocol.Version1,
			Type:            messageType,
			MessageID:       item.MessageID,
			CorrelationID:   item.CorrelationID,
			NodeID:          peer.NodeID(),
			SentAt:          time.Now().UTC(),
			Payload:         json.RawMessage(item.PayloadJSON),
		}
		if err := peer.Send(ctx, message); err != nil {
			_ = s.db.WithContext(ctx).Model(&model.ClusterOutbox{}).Where("id = ?", item.ID).Updates(map[string]any{
				"status":     model.ClusterMessageStatusPending,
				"last_error": err.Error(),
			}).Error
			return err
		}
		now := time.Now().UTC()
		if err := s.db.WithContext(ctx).Model(&model.ClusterOutbox{}).Where("id = ?", item.ID).Updates(map[string]any{
			"status":        model.ClusterMessageStatusSending,
			"session_id":    peer.SessionID(),
			"last_sent_at":  now,
			"attempt_count": gorm.Expr("attempt_count + 1"),
			"last_error":    "",
		}).Error; err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) SweepExpiredLeases(ctx context.Context, now time.Time) (int64, error) {
	var affected int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var attempts []model.ClusterJobAttempt
		if err := tx.Where("lease_until < ? AND status IN ?", now, []string{model.ClusterAttemptStatusOffered, model.ClusterAttemptStatusAccepted, model.ClusterAttemptStatusRunning}).Find(&attempts).Error; err != nil {
			return err
		}
		for i := range attempts {
			attempt := &attempts[i]
			if err := tx.Model(attempt).Updates(map[string]any{"status": model.ClusterAttemptStatusLost, "finished_at": now, "error_code": "lease_expired"}).Error; err != nil {
				return err
			}
			var job model.ClusterJob
			if err := tx.Select("id", "type", "subscription_item_id").First(&job, "id = ?", attempt.JobID).Error; err != nil {
				// The attempt can outlive its job after manual cleanup or a
				// partial restore. It is still safe to retire the attempt; the
				// subscription reconciler will recover any item that referenced it.
				if errors.Is(err, gorm.ErrRecordNotFound) {
					continue
				}
				return err
			}
			jobUpdates := map[string]any{
				"assigned_node_id":   "",
				"current_attempt_id": "",
				"last_error_code":    "lease_expired",
				"last_error":         "worker lease expired before completion",
				"available_at":       now,
			}
			if job.Type == model.ClusterJobTypeMediaTransfer && attempt.Generation >= automaticMediaTransferAttemptLimit {
				jobUpdates["status"] = model.ClusterJobStatusDeadLetter
				jobUpdates["finished_at"] = now
				jobUpdates["last_error_code"] = "lease_expired_attempt_limit"
				jobUpdates["last_error"] = "worker lease expired and automatic retry limit was reached"
			} else {
				jobUpdates["status"] = model.ClusterJobStatusQueued
			}
			result := tx.Model(&model.ClusterJob{}).Where("id = ? AND current_attempt_id = ? AND current_generation = ?", attempt.JobID, attempt.ID, attempt.Generation).Updates(jobUpdates)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected > 0 {
				if err := tx.Model(&model.ClusterOutbox{}).
					Where("correlation_id = ? AND message_type = ? AND status IN ?", attempt.JobID, protocol.MessageJobOffer, []string{model.ClusterMessageStatusPending, model.ClusterMessageStatusSending}).
					Updates(map[string]any{"status": model.ClusterMessageStatusFailed, "last_error": "lease expired; superseded by retry"}).Error; err != nil {
					return err
				}
				if job.Type == model.ClusterJobTypeMediaTransfer && job.SubscriptionItemID != 0 && jobUpdates["status"] == model.ClusterJobStatusDeadLetter {
					if err := tx.Model(&model.SubscriptionItem{}).
						Where("id = ? AND cluster_job_id = ?", job.SubscriptionItemID, attempt.JobID).
						Updates(map[string]any{
							"status":          model.SubscriptionItemStatusFailed,
							"last_error":      "worker lease expired and automatic retry limit was reached",
							"last_error_code": "lease_expired_attempt_limit",
							"state_version":   gorm.Expr("COALESCE(state_version, 0) + 1"),
						}).Error; err != nil {
						return err
					}
				}
			}
			affected += result.RowsAffected
		}
		return nil
	})
	return affected, err
}

// SweepStalledAttempts recovers media attempts that were accepted by a
// worker but never emitted a stage event. A worker must not be allowed to
// renew such an attempt forever: without this guard the subscription item
// remains transferring while no download/upload task is actually running.
func (s *Service) SweepStalledAttempts(ctx context.Context, now time.Time, grace time.Duration) (int64, error) {
	if grace <= 0 {
		grace = 10 * time.Minute
	}
	cutoff := now.Add(-grace)
	var affected int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var attempts []model.ClusterJobAttempt
		if err := tx.Where("status IN ? AND (accepted_at <= ? OR (accepted_at IS NULL AND offered_at <= ?))", []string{model.ClusterAttemptStatusAccepted, model.ClusterAttemptStatusRunning}, cutoff, cutoff).Find(&attempts).Error; err != nil {
			return err
		}
		for i := range attempts {
			attempt := &attempts[i]
			var job model.ClusterJob
			if err := tx.First(&job, "id = ?", attempt.JobID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					continue
				}
				return err
			}
			if (job.Type != model.ClusterJobTypeMediaTransfer && job.Type != model.ClusterJobTypeShareInspect) ||
				job.CurrentAttemptID != attempt.ID || job.CurrentGeneration != attempt.Generation ||
				(job.Status != model.ClusterJobStatusRunning && job.Status != model.ClusterJobStatusLeased) {
				continue
			}
			if job.Type == model.ClusterJobTypeMediaTransfer {
				var stageCount int64
				if err := tx.Model(&model.ClusterJobStage{}).Where("job_id = ? AND attempt_id = ?", job.ID, attempt.ID).Count(&stageCount).Error; err != nil {
					return err
				}
				if stageCount > 0 {
					continue
				}
			} else {
				// Share inspection jobs do not emit resumable stages. Their durable
				// completion marker is the sealed inspect manifest instead.
				var manifestCount int64
				if err := tx.Model(&model.ClusterShareInspectManifest{}).
					Where("job_id = ? AND attempt_id = ?", job.ID, attempt.ID).
					Count(&manifestCount).Error; err != nil {
					return err
				}
				if manifestCount > 0 {
					continue
				}
			}
			errorCode := "worker_start_timeout"
			errorMessage := "worker accepted the job but did not start a stage"
			if job.Type == model.ClusterJobTypeShareInspect {
				errorCode = "share_inspect_timeout"
				errorMessage = "worker accepted the share inspection but did not report a manifest"
			}
			if err := tx.Model(attempt).Updates(map[string]any{
				"status": model.ClusterAttemptStatusLost, "finished_at": now,
				"error_code": errorCode, "error": errorMessage,
			}).Error; err != nil {
				return err
			}
			jobUpdates := map[string]any{
				"assigned_node_id": "", "current_attempt_id": "",
				"last_error_code": errorCode,
				"last_error":      errorMessage,
				"available_at":    now,
			}
			if attempt.Generation >= automaticMediaTransferAttemptLimit {
				if job.Type != model.ClusterJobTypeMediaTransfer {
					jobUpdates["status"] = model.ClusterJobStatusQueued
					jobUpdates["finished_at"] = nil
				} else {
					jobUpdates["status"] = model.ClusterJobStatusDeadLetter
					jobUpdates["finished_at"] = now
					jobUpdates["last_error_code"] = "worker_start_timeout_attempt_limit"
					jobUpdates["last_error"] = "worker accepted the job but did not start a stage and automatic retry limit was reached"
				}
			} else {
				jobUpdates["status"] = model.ClusterJobStatusQueued
				jobUpdates["finished_at"] = nil
			}
			updated := tx.Model(&model.ClusterJob{}).
				Where("id = ? AND current_attempt_id = ? AND current_generation = ?", job.ID, attempt.ID, attempt.Generation).
				Updates(jobUpdates)
			if updated.Error != nil {
				return updated.Error
			}
			if updated.RowsAffected == 0 {
				continue
			}
			if err := tx.Model(&model.ClusterOutbox{}).
				Where("correlation_id = ? AND message_type = ? AND status IN ?", job.ID, protocol.MessageJobOffer, []string{model.ClusterMessageStatusPending, model.ClusterMessageStatusSending}).
				Updates(map[string]any{"status": model.ClusterMessageStatusFailed, "last_error": "worker start timeout; superseded by retry"}).Error; err != nil {
				return err
			}
			affected++
		}
		return nil
	})
	return affected, err
}

// SweepStalledParentJobs periodically re-aggregates share.batch parents. A
// parent normally closes when its last child reports a result, but that
// callback may be lost during a coordinator or worker restart. Reconciliation
// is safe because the child statuses remain the source of truth.
func (s *Service) SweepStalledParentJobs(ctx context.Context, now time.Time, grace time.Duration) (int64, error) {
	if grace <= 0 {
		grace = 10 * time.Minute
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cutoff := now.Add(-grace)
	var affected int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var parents []model.ClusterJob
		if err := tx.Where("type = ? AND status IN ? AND created_at <= ?", model.ClusterJobTypeShareBatch, []string{
			model.ClusterJobStatusPlanning,
			model.ClusterJobStatusLeased,
			model.ClusterJobStatusRunning,
		}, cutoff).Order("created_at ASC").Find(&parents).Error; err != nil {
			return err
		}
		for i := range parents {
			parent := &parents[i]
			beforeStatus := parent.Status
			beforeFinished := parent.FinishedAt
			if err := reconcileParentJobTx(tx, parent.ID, now); err != nil {
				return err
			}

			var childCount int64
			if err := tx.Model(&model.ClusterJob{}).Where("parent_job_id = ?", parent.ID).Count(&childCount).Error; err != nil {
				return err
			}
			if childCount == 0 && parent.ExpectedItems > 0 {
				updated := tx.Model(&model.ClusterJob{}).
					Where("id = ? AND status IN ?", parent.ID, []string{model.ClusterJobStatusPlanning, model.ClusterJobStatusLeased, model.ClusterJobStatusRunning}).
					Updates(map[string]any{
						"status":          model.ClusterJobStatusPartialFailed,
						"finished_at":     now,
						"last_error_code": "share_batch_without_children",
						"last_error":      "share batch remained active without any child jobs",
						"updated_at":      now,
					})
				if updated.Error != nil {
					return updated.Error
				}
				if updated.RowsAffected > 0 {
					affected += updated.RowsAffected
					continue
				}
			}

			var refreshed model.ClusterJob
			if err := tx.Select("status", "finished_at").First(&refreshed, "id = ?", parent.ID).Error; err != nil {
				return err
			}
			if refreshed.Status != beforeStatus || !sameTimePointer(refreshed.FinishedAt, beforeFinished) {
				affected++
			}
		}
		return nil
	})
	return affected, err
}

func sameTimePointer(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

func (s *Service) handleHeartbeat(ctx context.Context, peer transport.Peer, heartbeat protocol.Heartbeat) error {
	now := heartbeat.ObservedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return s.db.WithContext(ctx).Model(&model.ClusterNode{}).Where("id = ?", peer.NodeID()).Updates(map[string]any{
		"status":            model.ClusterNodeStatusOnline,
		"last_heartbeat_at": now,
		"last_session_id":   peer.SessionID(),
	}).Error
}

func (s *Service) handleInventory(ctx context.Context, peer transport.Peer, inventory protocol.InventoryReport) error {
	capabilities, err := json.Marshal(inventory.Capabilities)
	if err != nil {
		return err
	}
	mounts, err := json.Marshal(inventory.Mounts)
	if err != nil {
		return err
	}
	providerAccounts, err := json.Marshal(inventory.ProviderAccounts)
	if err != nil {
		return err
	}
	item := model.ClusterNodeInventory{
		ID:                   uuid.NewString(),
		NodeID:               peer.NodeID(),
		Revision:             inventory.Revision,
		CollectedAt:          inventory.CollectedAt.UTC(),
		InventoryHash:        inventory.InventoryHash,
		CapabilitiesJSON:     string(capabilities),
		MountsJSON:           string(mounts),
		ProviderAccountsJSON: string(providerAccounts),
	}
	if item.CollectedAt.IsZero() {
		item.CollectedAt = time.Now().UTC()
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&item).Error; err != nil {
			return err
		}
		return tx.Model(&model.ClusterNode{}).Where("id = ?", peer.NodeID()).Updates(map[string]any{
			"last_inventory_hash": inventory.InventoryHash,
			"last_error":          inventory.RecentError,
		}).Error
	})
}

func (s *Service) handleUploadManifest(ctx context.Context, peer transport.Peer, envelope protocol.Envelope, manifest protocol.UploadETFManifest) error {
	if err := manifest.Validate(); err != nil {
		return err
	}
	payloadHash, err := protocol.HashUploadETFManifest(manifest)
	if err != nil {
		return err
	}
	payloadJSON, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	ack := protocol.UploadETFManifestAck{
		JobID:       manifest.JobID,
		AttemptID:   manifest.AttemptID,
		MediaItemID: manifest.MediaItemID,
		PayloadHash: payloadHash,
		ConsumedAt:  time.Now().UTC(),
	}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job model.ClusterJob
		if err := tx.First(&job, "id = ?", manifest.JobID).Error; err != nil {
			ack.Outcome = protocol.ManifestAckContextMismatch
			ack.ErrorCode = "job_not_found"
			ack.Error = "cluster job was not found"
			return nil
		}
		if job.TaskContextHash != manifest.TaskContextHash || job.MediaItemID != manifest.MediaItemID || job.SubscriptionID != manifest.Subscription.SubscriptionID || job.SubscriptionItemID != manifest.Subscription.SubscriptionItemID {
			ack.Outcome = protocol.ManifestAckContextMismatch
			ack.ErrorCode = "task_context_mismatch"
			ack.Error = "worker task context does not match the coordinator snapshot"
			return nil
		}
		attempt, attemptErr := loadAndValidateAttempt(tx, peer, manifest.AttemptRef)
		if attemptErr != nil {
			ack.Outcome = protocol.ManifestAckContextMismatch
			ack.ErrorCode = "attempt_fencing_failed"
			ack.Error = attemptErr.Error()
			return nil
		}
		if !manifestAttemptStatusAllowed(attempt.Status) {
			ack.Outcome = protocol.ManifestAckContextMismatch
			ack.ErrorCode = "attempt_status_invalid"
			ack.Error = fmt.Sprintf("cluster job attempt cannot report a manifest from status %q", attempt.Status)
			return nil
		}
		var uploadStage model.ClusterJobStage
		if err := tx.First(&uploadStage, "attempt_id = ? AND name = ?", manifest.AttemptID, model.ClusterStageUploadingMobile).Error; err != nil {
			ack.Outcome = protocol.ManifestAckContextMismatch
			ack.ErrorCode = "stage_permit_missing"
			ack.Error = "upload stage permit was not issued for this attempt"
			return nil
		}
		permitHash := fmt.Sprintf("%x", sha256.Sum256([]byte(manifest.StagePermitToken)))
		if uploadStage.OperationKey != job.IdempotencyKey+":"+model.ClusterStageUploadingMobile ||
			len(permitHash) != len(uploadStage.PermitTokenHash) || subtle.ConstantTimeCompare([]byte(permitHash), []byte(uploadStage.PermitTokenHash)) != 1 {
			ack.Outcome = protocol.ManifestAckContextMismatch
			ack.ErrorCode = "stage_permit_invalid"
			ack.Error = "upload result does not match the issued stage permit"
			return nil
		}

		var existing model.ClusterUploadManifest
		findErr := tx.Where("job_id = ? AND media_item_id = ?", manifest.JobID, manifest.MediaItemID).First(&existing).Error
		if findErr == nil {
			ack.ManifestID = existing.ID
			if existing.TaskContextHash != manifest.TaskContextHash {
				ack.Outcome = protocol.ManifestAckContextMismatch
				ack.ErrorCode = "task_context_mismatch"
				ack.Error = "stored result belongs to a different task context"
				return nil
			}
			if existing.PayloadHash == payloadHash || strings.EqualFold(existing.SHA256, manifest.SHA256) {
				ack.Outcome = protocol.ManifestAckDuplicate
				return nil
			}
			ack.Outcome = protocol.ManifestAckConflict
			ack.ErrorCode = "result_conflict"
			ack.Error = "media item already has a different upload result"
			return nil
		}
		if !errors.Is(findErr, gorm.ErrRecordNotFound) {
			return findErr
		}
		outcome := protocol.ManifestAckAccepted
		if job.CurrentAttemptID != manifest.AttemptID || job.CurrentGeneration != manifest.Generation || job.AssignedNodeID != peer.NodeID() || attempt.Status == model.ClusterAttemptStatusLost {
			outcome = protocol.ManifestAckAdopted
		}
		item := model.ClusterUploadManifest{
			ID:                   uuid.NewString(),
			JobID:                manifest.JobID,
			ParentBatchID:        manifest.ParentBatchID,
			MediaItemID:          manifest.MediaItemID,
			AttemptID:            manifest.AttemptID,
			NodeID:               peer.NodeID(),
			Generation:           manifest.Generation,
			OperationKey:         manifest.OperationKey,
			TaskContextHash:      manifest.TaskContextHash,
			WorkflowVersion:      manifest.WorkflowVersion,
			SubscriptionID:       manifest.Subscription.SubscriptionID,
			SubscriptionItemID:   manifest.Subscription.SubscriptionItemID,
			MediaType:            manifest.Media.MediaType,
			TMDBID:               manifest.Media.TMDBID,
			TMDBName:             manifest.Media.TMDBName,
			Season:               manifest.Media.Season,
			Episode:              manifest.Media.Episode,
			LogicalTargetPath:    manifest.Media.LogicalTargetPath,
			MobileAccountBinding: manifest.MobileAccountBinding,
			RemoteFileID:         manifest.RemoteFileID,
			RemoteParentID:       manifest.RemoteParentID,
			RemotePath:           manifest.RemotePath,
			Name:                 manifest.Name,
			Size:                 manifest.Size,
			SHA256:               strings.ToUpper(manifest.SHA256),
			HashSource:           manifest.HashSource,
			UploadReceipt:        manifest.UploadReceipt,
			SourceObjectsJSON:    mustJSON(manifest.SourceObjects),
			PayloadJSON:          string(payloadJSON),
			PayloadHash:          payloadHash,
			Status:               model.ClusterUploadManifestStatusAccepted,
			AckOutcome:           outcome,
			ReceivedAt:           time.Now().UTC(),
		}
		if err := tx.Create(&item).Error; err != nil {
			return err
		}
		stage := model.ClusterJobStage{
			ID:        uuid.NewString(),
			JobID:     job.ID,
			AttemptID: manifest.AttemptID,
			Name:      model.ClusterStageETFMaterializing,
			Status:    model.ClusterStageStatusPending,
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&stage).Error; err != nil {
			return err
		}
		if err := tx.Model(&model.ClusterJob{}).Where("id = ?", job.ID).Updates(map[string]any{
			"result_delivery_status": model.ClusterResultDeliveryStatusConsumed,
			"worker_cleanup_status":  model.ClusterCleanupStatusPending,
			"status":                 model.ClusterJobStatusRunning,
		}).Error; err != nil {
			return err
		}
		ack.Outcome = outcome
		ack.ManifestID = item.ID
		return s.recordInboxTx(tx, peer, envelope, model.ClusterMessageStatusProcessed, "")
	})
	if err != nil {
		return err
	}
	response, err := protocol.NewEnvelope(protocol.MessageUploadETFManifestAck, ack)
	if err != nil {
		return err
	}
	response.CorrelationID = envelope.MessageID
	response.NodeID = peer.NodeID()
	return peer.Send(ctx, *response)
}

func loadAndValidateAttempt(tx *gorm.DB, peer transport.Peer, ref protocol.AttemptRef) (*model.ClusterJobAttempt, error) {
	if strings.TrimSpace(ref.JobID) == "" || strings.TrimSpace(ref.AttemptID) == "" || ref.Generation == 0 || ref.LeaseToken == "" {
		return nil, errors.New("job, attempt, generation, and lease token are required")
	}
	var attempt model.ClusterJobAttempt
	if err := tx.First(&attempt, "id = ?", ref.AttemptID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("cluster job attempt was not found")
		}
		return nil, err
	}
	if attempt.JobID != ref.JobID || attempt.NodeID != peer.NodeID() || attempt.Generation != ref.Generation {
		return nil, errors.New("cluster job attempt does not match job, node, or generation")
	}
	provided := sha256.Sum256([]byte(ref.LeaseToken))
	providedHash := fmt.Sprintf("%x", provided)
	if len(providedHash) != len(attempt.LeaseTokenHash) || subtle.ConstantTimeCompare([]byte(providedHash), []byte(attempt.LeaseTokenHash)) != 1 {
		return nil, errors.New("cluster job attempt lease token is invalid")
	}
	return &attempt, nil
}

func manifestAttemptStatusAllowed(status string) bool {
	return containsString([]string{
		model.ClusterAttemptStatusAccepted,
		model.ClusterAttemptStatusRunning,
		model.ClusterAttemptStatusSucceeded,
		model.ClusterAttemptStatusLost,
	}, status)
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func (s *Service) recordInbox(ctx context.Context, peer transport.Peer, message protocol.Envelope, status, messageError string) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		duplicate, err := s.claimInboxTx(tx, peer, message)
		if err != nil || duplicate {
			return err
		}
		return s.finishInboxTx(tx, peer, message, status, messageError)
	})
}

func (s *Service) recordInboxTx(tx *gorm.DB, peer transport.Peer, message protocol.Envelope, status, messageError string) error {
	duplicate, err := s.claimInboxTx(tx, peer, message)
	if err != nil || duplicate {
		return err
	}
	return s.finishInboxTx(tx, peer, message, status, messageError)
}

func (s *Service) claimInboxTx(tx *gorm.DB, peer transport.Peer, message protocol.Envelope) (bool, error) {
	item := model.ClusterInbox{
		ID:            uuid.NewString(),
		MessageID:     message.MessageID,
		PeerNodeID:    peer.NodeID(),
		SessionID:     peer.SessionID(),
		Seq:           message.Seq,
		CorrelationID: message.CorrelationID,
		MessageType:   string(message.Type),
		PayloadHash:   hashBytes(message.Payload),
		Status:        model.ClusterMessageStatusPending,
		ReceivedAt:    time.Now().UTC(),
	}
	result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&item)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected == 0, nil
}

func (s *Service) finishInboxTx(tx *gorm.DB, peer transport.Peer, message protocol.Envelope, status, messageError string) error {
	if err := tx.Model(&model.ClusterInbox{}).Where("message_id = ?", message.MessageID).Updates(map[string]any{
		"status": status, "processed_at": time.Now().UTC(), "error": messageError,
	}).Error; err != nil {
		return err
	}
	updates := map[string]any{"last_received_seq": message.Seq}
	if message.Type == protocol.MessageAck {
		if ack, err := protocol.DecodePayload[protocol.Ack](message); err == nil {
			updates["last_acked_seq"] = ack.AckSeq
		}
	}
	return tx.Model(&model.ClusterNodeSession{}).Where("id = ?", peer.SessionID()).Updates(updates).Error
}

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
	heartbeatTimeout := s.heartbeatTimeout()
	filtered := make([]model.ClusterNode, 0, len(nodes))
	for _, node := range nodes {
		node.Status = effectiveNodeStatus(node, now, heartbeatTimeout)
		if !includeStale && isStaleOfflineNode(node, now, defaultStaleOfflineDuration, heartbeatTimeout) {
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
	var inventories []model.ClusterNodeInventory
	if err := s.db.WithContext(ctx).Where("node_id IN ?", nodeIDs).Order("node_id ASC, revision DESC").Find(&inventories).Error; err != nil {
		return nil, err
	}
	latestByNode := make(map[string]model.ClusterNodeInventory, len(inventories))
	for _, inventory := range inventories {
		if _, exists := latestByNode[inventory.NodeID]; !exists {
			latestByNode[inventory.NodeID] = inventory
		}
	}
	result := make([]NodeSummary, 0, len(filtered))
	for _, node := range filtered {
		summary := NodeSummary{ClusterNode: node}
		if inventory, ok := latestByNode[node.ID]; ok {
			view := NodeInventorySummary{CollectedAt: &inventory.CollectedAt, InventoryHash: inventory.InventoryHash}
			if strings.TrimSpace(inventory.CapabilitiesJSON) != "" {
				var capabilities protocol.NodeCapabilities
				if err := json.Unmarshal([]byte(inventory.CapabilitiesJSON), &capabilities); err != nil {
					return nil, err
				}
				view.Capabilities = &capabilities
			}
			if strings.TrimSpace(inventory.MountsJSON) != "" {
				if err := json.Unmarshal([]byte(inventory.MountsJSON), &view.Mounts); err != nil {
					return nil, err
				}
			}
			if strings.TrimSpace(inventory.ProviderAccountsJSON) != "" {
				if err := json.Unmarshal([]byte(inventory.ProviderAccountsJSON), &view.ProviderAccounts); err != nil {
					return nil, err
				}
			}
			summary.LatestInventory = &view
		}
		result = append(result, summary)
	}
	return result, nil
}

func (s *Service) DeleteNode(ctx context.Context, nodeID string, now time.Time) error {
	if s.db == nil {
		return errors.New("cluster database is unavailable")
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return errors.New("cluster node id is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	heartbeatTimeout := s.heartbeatTimeout()
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var node model.ClusterNode
		if err := tx.First(&node, "id = ?", nodeID).Error; err != nil {
			return err
		}
		status := effectiveNodeStatus(node, now, heartbeatTimeout)
		switch status {
		case model.ClusterNodeStatusOnline:
			return errors.New("connected cluster node cannot be removed")
		case model.ClusterNodeStatusDraining:
			return errors.New("draining cluster node cannot be removed")
		}
		// Keep immutable execution and result history for auditability, but remove
		// all coordinator-owned identity, configuration, inventory, and transport
		// state so an inactive Worker no longer remains registered.
		if err := tx.Where("peer_node_id = ?", nodeID).Delete(&model.ClusterOutbox{}).Error; err != nil {
			return err
		}
		if err := tx.Where("peer_node_id = ?", nodeID).Delete(&model.ClusterInbox{}).Error; err != nil {
			return err
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
		if err := tx.Where("node_id = ?", nodeID).Delete(&model.ClusterStorageProfile{}).Error; err != nil {
			return err
		}
		return tx.Delete(&model.ClusterNode{}, "id = ?", nodeID).Error
	})
}

func (s *Service) ListUploadManifests(ctx context.Context, limit int) ([]model.ClusterUploadManifest, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var items []model.ClusterUploadManifest
	err := s.db.WithContext(ctx).Order("received_at DESC").Limit(limit).Find(&items).Error
	return items, err
}

func (s *Service) ListJobs(ctx context.Context, status string, includeArchived bool, limit int) ([]model.ClusterJob, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := s.db.WithContext(ctx).Model(&model.ClusterJob{})
	if !includeArchived {
		query = query.Where("archived_at IS NULL")
	}
	if status = strings.TrimSpace(status); status != "" {
		if strings.EqualFold(status, "active") || strings.EqualFold(status, "undone") {
			query = query.Where("status IN ?", []string{
				model.ClusterJobStatusQueued,
				model.ClusterJobStatusPlanning,
				model.ClusterJobStatusLeased,
				model.ClusterJobStatusRunning,
				model.ClusterJobStatusRetryWait,
				model.ClusterJobStatusCancelRequested,
			})
		} else {
			query = query.Where("status = ?", status)
		}
	}
	var jobs []model.ClusterJob
	if err := query.Order("created_at DESC").Limit(limit).Find(&jobs).Error; err != nil || len(jobs) == 0 {
		return jobs, err
	}
	jobIDs := make([]string, 0, len(jobs))
	currentAttemptByJobID := make(map[string]string, len(jobs))
	for i := range jobs {
		jobIDs = append(jobIDs, jobs[i].ID)
		currentAttemptByJobID[jobs[i].ID] = jobs[i].CurrentAttemptID
	}
	var stages []model.ClusterJobStage
	if err := s.db.WithContext(ctx).
		Where("job_id IN ?", jobIDs).
		Order("job_id ASC, created_at ASC, id ASC").
		Find(&stages).Error; err != nil {
		return nil, err
	}
	stagesByJobID := make(map[string][]model.ClusterJobStage, len(jobs))
	for i := range stages {
		stage := stages[i]
		currentAttemptID := currentAttemptByJobID[stage.JobID]
		if currentAttemptID == "" || stage.AttemptID != currentAttemptID {
			continue
		}
		stagesByJobID[stage.JobID] = append(stagesByJobID[stage.JobID], stage)
	}
	for i := range jobs {
		jobs[i].Stages = stagesByJobID[jobs[i].ID]
	}
	return jobs, nil
}

func (s *Service) RetryJob(ctx context.Context, jobID string) error {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return errors.New("cluster job id is required")
	}
	now := time.Now().UTC()
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return retryJobTx(tx, jobID, now)
	})
}

// FailQueuedJob moves a queued job to a terminal failure and reconciles its
// parent batch in the same transaction. It is used for coordinator-side
// failures where no worker attempt was created, so the normal result handler
// cannot close the parent job for us.
func (s *Service) FailQueuedJob(ctx context.Context, jobID, errorCode, reason string) error {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return errors.New("cluster job id is required")
	}
	errorCode = strings.TrimSpace(errorCode)
	if errorCode == "" {
		return errors.New("cluster job error code is required")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = errorCode
	}
	now := time.Now().UTC()
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job model.ClusterJob
		if err := tx.Select("id", "parent_job_id").First(&job, "id = ?", jobID).Error; err != nil {
			return err
		}
		updated := tx.Model(&model.ClusterJob{}).Where("id = ? AND status = ?", jobID, model.ClusterJobStatusQueued).Updates(map[string]any{
			"status":             model.ClusterJobStatusFailed,
			"finished_at":        now,
			"available_at":       now,
			"assigned_node_id":   "",
			"current_attempt_id": "",
			"last_error_code":    errorCode,
			"last_error":         reason,
		})
		if updated.Error != nil {
			return updated.Error
		}
		if updated.RowsAffected == 0 {
			return nil
		}
		return reconcileParentJobTx(tx, job.ParentJobID, now)
	})
}

var retryableSubscriptionJobStatuses = []string{
	model.ClusterJobStatusFailed,
	model.ClusterJobStatusPartialFailed,
	model.ClusterJobStatusDeadLetter,
	model.ClusterJobStatusCancelled,
}

func isRetryableSubscriptionJobStatus(status string) bool {
	for _, retryable := range retryableSubscriptionJobStatuses {
		if status == retryable {
			return true
		}
	}
	return false
}

// RetryFailedSubscriptionItems reopens the existing media-transfer children
// for a subscription. It deliberately searches by SubscriptionItemID as a
// fallback because older retry handlers cleared subscription_item.cluster_job_id
// after a job had already been persisted.
func (s *Service) RetryFailedSubscriptionItems(ctx context.Context, subscriptionID uint) (RetrySubscriptionResult, error) {
	if subscriptionID == 0 {
		return RetrySubscriptionResult{}, errors.New("subscription id is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var recovered RetrySubscriptionResult
	now := time.Now().UTC()
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var items []model.SubscriptionItem
		if err := tx.Where("subscription_id = ? AND status IN ?", subscriptionID, []string{
			model.SubscriptionItemStatusFailed,
			model.SubscriptionItemStatusPending,
			model.SubscriptionItemStatusRetryWait,
			model.SubscriptionItemStatusUnknown,
			model.SubscriptionItemStatusBlocked,
		}).Order("id ASC").Find(&items).Error; err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}

		var jobs []model.ClusterJob
		if err := tx.Where("subscription_id = ? AND type = ?", subscriptionID, model.ClusterJobTypeMediaTransfer).
			Order("created_at DESC, id DESC").Find(&jobs).Error; err != nil {
			return err
		}
		jobsByID := make(map[string]*model.ClusterJob, len(jobs))
		latestByItemID := make(map[uint]*model.ClusterJob)
		for i := range jobs {
			job := &jobs[i]
			jobsByID[job.ID] = job
			if job.SubscriptionItemID == 0 {
				continue
			}
			if _, exists := latestByItemID[job.SubscriptionItemID]; !exists {
				latestByItemID[job.SubscriptionItemID] = job
			}
		}

		retriedJobs := make(map[string]struct{}, len(items))
		for i := range items {
			item := &items[i]
			job := jobsByID[strings.TrimSpace(item.ClusterJobID)]
			if job == nil {
				job = latestByItemID[item.ID]
			}
			if job == nil {
				if item.Status != model.SubscriptionItemStatusTransferred && item.Status != model.SubscriptionItemStatusSkipped {
					recovered.Unmatched++
				}
				continue
			}
			if !isRetryableSubscriptionJobStatus(job.Status) {
				// A live job is already in flight. A succeeded or otherwise
				// terminal job cannot be replayed safely, so report a failed item
				// that has no retryable durable task instead of hiding it.
				if item.Status != model.SubscriptionItemStatusTransferred && item.Status != model.SubscriptionItemStatusSkipped && !isActiveSubscriptionJobStatus(job.Status) {
					recovered.Unmatched++
				}
				continue
			}
			if _, alreadyRetried := retriedJobs[job.ID]; alreadyRetried {
				continue
			}
			if err := retryJobTx(tx, job.ID, now); err != nil {
				return err
			}
			retriedJobs[job.ID] = struct{}{}
			if err := tx.Model(&model.SubscriptionItem{}).Where("id = ?", item.ID).Updates(map[string]any{
				"status":          model.SubscriptionItemStatusPending,
				"cluster_job_id":  job.ID,
				"last_error":      "",
				"last_error_code": "",
				"retry_at":        nil,
				"blocked_reason":  "",
				"state_version":   gorm.Expr("COALESCE(state_version, 0) + 1"),
			}).Error; err != nil {
				return err
			}
			if err := tx.Model(&model.SubscriptionEpisodeSource{}).
				Where("subscription_id = ? AND source_item_id = ?", subscriptionID, item.ID).
				Updates(map[string]any{
					"status":         model.SubscriptionItemStatusPending,
					"cluster_job_id": job.ID,
				}).Error; err != nil {
				return err
			}
			recovered.Requeued++
		}
		return nil
	})
	return recovered, err
}

func isActiveSubscriptionJobStatus(status string) bool {
	switch status {
	case model.ClusterJobStatusQueued,
		model.ClusterJobStatusPlanning,
		model.ClusterJobStatusLeased,
		model.ClusterJobStatusRunning,
		model.ClusterJobStatusRetryWait,
		model.ClusterJobStatusCancelRequested:
		return true
	default:
		return false
	}
}

func retryJobTx(tx *gorm.DB, jobID string, now time.Time) error {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return errors.New("cluster job id is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var job model.ClusterJob
	if err := tx.Select("id", "parent_job_id").First(&job, "id = ?", jobID).Error; err != nil {
		return err
	}
	result := tx.Model(&model.ClusterJob{}).
		Where("id = ? AND status IN ?", jobID, retryableSubscriptionJobStatuses).
		Updates(map[string]any{
			"status": model.ClusterJobStatusQueued, "available_at": now, "archived_at": nil,
			"assigned_node_id": "", "current_attempt_id": "", "last_error_code": "", "last_error": "", "finished_at": nil,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return errors.New("cluster job is not in a retryable terminal state")
	}
	return reconcileParentJobTx(tx, job.ParentJobID, now)
}

func (s *Service) ArchiveFailedJobs(ctx context.Context) (int64, error) {
	now := time.Now().UTC()
	result := s.db.WithContext(ctx).Model(&model.ClusterJob{}).
		Where("archived_at IS NULL AND status IN ?", []string{model.ClusterJobStatusFailed, model.ClusterJobStatusPartialFailed, model.ClusterJobStatusDeadLetter, model.ClusterJobStatusCancelled}).
		Update("archived_at", now)
	return result.RowsAffected, result.Error
}

func hashBytes(value []byte) string {
	return fmt.Sprintf("%x", sha256Sum(value))
}

func sha256Sum(value []byte) [32]byte {
	return sha256.Sum256(value)
}

func mustJSON(value any) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}

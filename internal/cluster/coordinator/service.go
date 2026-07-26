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
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Service struct {
	db                *gorm.DB
	enrollmentToken   string
	heartbeatInterval time.Duration
	inspectMu         sync.RWMutex
	inspectProcessMu  sync.Mutex
	inspectConsumer   ShareInspectConsumer
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
	if err := s.db.WithContext(ctx).Where("status = ?", model.ClusterShareInspectStatusPending).Order("created_at ASC").Limit(limit).Find(&items).Error; err != nil {
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
			_ = s.db.WithContext(ctx).Model(&items[i]).Update("last_error", err.Error()).Error
			continue
		}
		if err := consumer(ctx, items[i], manifest); err != nil {
			if !errors.Is(err, ErrShareInspectObservationIncomplete) {
				_ = s.db.WithContext(ctx).Model(&items[i]).Update("last_error", err.Error()).Error
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

func (s *Service) handleStageStatus(ctx context.Context, peer transport.Peer, message protocol.Envelope, update protocol.StageStatusUpdate) error {
	if update.Stage != model.ClusterStageSavingShare && update.Stage != model.ClusterStageUploadingMobile && update.Stage != model.ClusterStageWorkerMediaCleanup {
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
			if !errors.Is(err, gorm.ErrRecordNotFound) || update.Stage != model.ClusterStageWorkerMediaCleanup {
				return err
			}
			stage = model.ClusterJobStage{
				ID: uuid.NewString(), JobID: job.ID, AttemptID: attempt.ID,
				Name: model.ClusterStageWorkerMediaCleanup, Status: model.ClusterStageStatusPending,
			}
			if err := tx.Create(&stage).Error; err != nil {
				return err
			}
		}
		if update.Stage == model.ClusterStageWorkerMediaCleanup {
			switch stage.Status {
			case model.ClusterStageStatusPending:
				if update.Status != model.ClusterStageStatusRunning && update.Status != model.ClusterStageStatusSucceeded && update.Status != model.ClusterStageStatusFailed {
					return fmt.Errorf("cluster cleanup transition %s -> %s is invalid", stage.Status, update.Status)
				}
			case model.ClusterStageStatusRunning:
				if update.Status != model.ClusterStageStatusSucceeded && update.Status != model.ClusterStageStatusFailed {
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
			"last_event_seq": renewal.LastEventSeq,
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

const automaticMediaTransferAttemptLimit uint64 = 3

func shouldRetryMediaTransfer(errorCode string, generation uint64) bool {
	if generation >= automaticMediaTransferAttemptLimit {
		return false
	}
	switch strings.TrimSpace(errorCode) {
	case "source_unexpected_eof", "source_range_failed", "source_link_expired":
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
	now := result.FinishedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	storedInspect := false
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		duplicate, err := s.claimInboxTx(tx, peer, message)
		if err != nil || duplicate {
			return err
		}
		var job model.ClusterJob
		if err := tx.First(&job, "id = ?", result.JobID).Error; err != nil {
			return err
		}
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
			if job.Type == model.ClusterJobTypeShareInspect {
				payloadHash, err := persistShareInspectResultTx(tx, peer.NodeID(), &job, attempt, result)
				if err != nil {
					return err
				}
				if result.ResultHash == "" {
					result.ResultHash = payloadHash
				}
				storedInspect = true
			}
			attemptStatus = model.ClusterAttemptStatusSucceeded
			if job.Type == model.ClusterJobTypeShareInspect {
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
		if result.Status != "succeeded" && job.Type == model.ClusterJobTypeMediaTransfer && job.SubscriptionItemID != 0 && jobUpdates["status"] == model.ClusterJobStatusFailed {
			if err := tx.Model(&model.SubscriptionItem{}).
				Where("id = ? AND cluster_job_id = ?", job.SubscriptionItemID, job.ID).
				Updates(map[string]any{"status": model.SubscriptionItemStatusFailed, "last_error": result.Error}).Error; err != nil {
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
		return err
	}
	if storedInspect {
		_, _ = s.ProcessPendingShareInspects(ctx, 1)
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
			result := tx.Model(&model.ClusterJob{}).Where("id = ? AND current_attempt_id = ? AND current_generation = ?", attempt.JobID, attempt.ID, attempt.Generation).Updates(map[string]any{
				"status":             model.ClusterJobStatusQueued,
				"assigned_node_id":   "",
				"current_attempt_id": "",
				"last_error_code":    "lease_expired",
				"last_error":         "worker lease expired before completion",
				"available_at":       now,
			})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected > 0 {
				if err := tx.Model(&model.ClusterOutbox{}).
					Where("correlation_id = ? AND message_type = ? AND status IN ?", attempt.JobID, protocol.MessageJobOffer, []string{model.ClusterMessageStatusPending, model.ClusterMessageStatusSending}).
					Updates(map[string]any{"status": model.ClusterMessageStatusFailed, "last_error": "lease expired; superseded by retry"}).Error; err != nil {
					return err
				}
			}
			affected += result.RowsAffected
		}
		return nil
	})
	return affected, err
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
		query = query.Where("status = ?", status)
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
		var job model.ClusterJob
		if err := tx.Select("id", "parent_job_id").First(&job, "id = ?", jobID).Error; err != nil {
			return err
		}
		result := tx.Model(&model.ClusterJob{}).
			Where("id = ? AND status IN ?", jobID, []string{model.ClusterJobStatusFailed, model.ClusterJobStatusPartialFailed, model.ClusterJobStatusDeadLetter, model.ClusterJobStatusCancelled}).
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
	})
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

package coordinator

import (
	"context"
	"errors"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/transport"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func (s *Service) handleConfigObserved(ctx context.Context, peer transport.Peer, message protocol.Envelope, observed protocol.ConfigObserved) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		duplicate, err := s.claimInboxTx(tx, peer, message)
		if err != nil || duplicate {
			return err
		}
		status := model.ClusterDesiredStatusApplied
		if !strings.EqualFold(observed.Status, "applied") {
			status = model.ClusterDesiredStatusFailed
		}
		result := tx.Model(&model.ClusterNodeDesiredConfig{}).
			Where("node_id = ? AND revision = ? AND desired_hash = ?", peer.NodeID(), observed.Revision, observed.DesiredHash).
			Updates(map[string]any{
				"status": status, "observed_revision": observed.Revision,
				"observed_hash": observed.ObservedHash, "observed_at": observed.ObservedAt,
				"last_error": observed.Error,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return errors.New("config observation does not match desired state")
		}
		if err := ackControlOutboxTx(tx, peer.NodeID(), message.CorrelationID); err != nil {
			return err
		}
		_ = tx.Create(&model.ClusterControlAudit{
			ID: uuid.NewString(), NodeID: peer.NodeID(), Action: "config.observed",
			ResourceType: "node_config", ResourceID: peer.NodeID(), Revision: observed.Revision,
			Outcome: status, Detail: observed.ErrorCode,
		}).Error
		return s.finishInboxTx(tx, peer, message, model.ClusterMessageStatusProcessed, "")
	})
}

func (s *Service) handleStorageApplyResult(ctx context.Context, peer transport.Peer, message protocol.Envelope, observed protocol.StorageApplyResult) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		duplicate, err := s.claimInboxTx(tx, peer, message)
		if err != nil || duplicate {
			return err
		}
		status := model.ClusterDesiredStatusApplied
		if !strings.EqualFold(observed.Status, "applied") {
			status = model.ClusterDesiredStatusFailed
		}
		query := tx.Model(&model.ClusterStorageProfile{}).
			Where("node_id = ? AND revision = ? AND desired_hash = ?", peer.NodeID(), observed.Revision, observed.DesiredHash)
		result := query.Updates(map[string]any{
			"status": status, "observed_revision": observed.Revision,
			"observed_hash": observed.DesiredHash, "observed_at": observed.AppliedAt,
			"observed_storage_id": observed.StorageID, "node_mount_id": observed.NodeMountID,
			"last_error": observed.Error,
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return errors.New("storage observation does not match desired state")
		}
		if err := ackControlOutboxTx(tx, peer.NodeID(), message.CorrelationID); err != nil {
			return err
		}
		_ = tx.Create(&model.ClusterControlAudit{
			ID: uuid.NewString(), NodeID: peer.NodeID(), Action: "storage.observed",
			ResourceType: "storage_profile", ResourceID: observed.NodeMountID,
			Revision: observed.Revision, Outcome: status, Detail: observed.ErrorCode,
		}).Error
		return s.finishInboxTx(tx, peer, message, model.ClusterMessageStatusProcessed, "")
	})
}

func ackControlOutboxTx(tx *gorm.DB, nodeID, messageID string) error {
	if strings.TrimSpace(messageID) == "" {
		return errors.New("control result correlation id is required")
	}
	result := tx.Model(&model.ClusterOutbox{}).
		Where("peer_node_id = ? AND message_id = ?", nodeID, messageID).
		Updates(map[string]any{"status": model.ClusterMessageStatusAcked, "last_error": ""})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return errors.New("control result does not match a durable outbox message")
	}
	return nil
}

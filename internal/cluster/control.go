package cluster

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/secure"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ControlActor struct {
	Name      string
	RemoteIP  string
	RequestID string
}

type SecretWriteRequest struct {
	ID    string         `json:"id,omitempty"`
	Alias string         `json:"alias"`
	Kind  string         `json:"kind"`
	Value map[string]any `json:"value"`
}

type StorageProfileWriteRequest struct {
	ID            string         `json:"id,omitempty"`
	NodeID        string         `json:"node_id"`
	NodeMountID   string         `json:"node_mount_id"`
	Driver        string         `json:"driver"`
	SchemaVersion string         `json:"schema_version"`
	MountPath     string         `json:"mount_path"`
	Parameters    map[string]any `json:"parameters,omitempty"`
	CredentialRef string         `json:"credential_ref"`
	Operation     string         `json:"operation,omitempty"`
	Remark        string         `json:"remark,omitempty"`
	Disabled      bool           `json:"disabled,omitempty"`
}

func ApplyNodeConfig(ctx context.Context, nodeID string, desired protocol.WorkerDesiredConfig, actor ControlActor) (*model.ClusterNodeDesiredConfig, error) {
	return DefaultRuntime.ApplyNodeConfig(ctx, nodeID, desired, actor)
}

func ApplyStorageProfile(ctx context.Context, req StorageProfileWriteRequest, actor ControlActor) (*model.ClusterStorageProfile, error) {
	return DefaultRuntime.ApplyStorageProfile(ctx, req, actor)
}

func ListSecrets(ctx context.Context) ([]model.ClusterSecret, error) {
	var secrets []model.ClusterSecret
	err := db.GetDb().WithContext(ctx).Order("alias ASC").Find(&secrets).Error
	return secrets, err
}

func WriteSecret(ctx context.Context, req SecretWriteRequest, actor ControlActor) (*model.ClusterSecret, error) {
	if strings.TrimSpace(req.Alias) == "" || strings.TrimSpace(req.Kind) == "" || len(req.Value) == 0 {
		return nil, errors.New("secret alias, kind, and non-empty value are required")
	}
	plaintext, err := json.Marshal(req.Value)
	if err != nil {
		return nil, errors.New("secret value is invalid")
	}
	ciphertext, nonce, fingerprint, err := encryptCoordinatorSecret(plaintext)
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(req.ID)
	if id == "" {
		id = uuid.NewString()
	}
	now := time.Now().UTC()
	secret := &model.ClusterSecret{}
	err = db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existing model.ClusterSecret
		lookup := tx.First(&existing, "id = ? OR alias = ?", id, strings.TrimSpace(req.Alias)).Error
		version := uint64(1)
		createdAt := now
		if lookup == nil {
			id, version, createdAt = existing.ID, existing.Version+1, existing.CreatedAt
		} else if !errors.Is(lookup, gorm.ErrRecordNotFound) {
			return lookup
		}
		*secret = model.ClusterSecret{
			ID: id, CreatedAt: createdAt, UpdatedAt: now, Alias: strings.TrimSpace(req.Alias),
			Kind: strings.TrimSpace(req.Kind), Ciphertext: ciphertext, Nonce: nonce,
			Fingerprint: fingerprint, Version: version, RotatedAt: now,
		}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, DoUpdates: clause.AssignmentColumns([]string{
			"updated_at", "alias", "kind", "ciphertext", "nonce", "fingerprint", "version", "rotated_at", "revoked_at",
		})}).Create(secret).Error; err != nil {
			return err
		}
		return createControlAudit(tx, actor, "secret.write", "secret", secret.ID, secret.Version, "succeeded", "")
	})
	if err != nil {
		return nil, err
	}
	return secret, nil
}

func RevokeSecret(ctx context.Context, id string, actor ControlActor) error {
	now := time.Now().UTC()
	return db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var secret model.ClusterSecret
		if err := tx.First(&secret, "id = ?", strings.TrimSpace(id)).Error; err != nil {
			return err
		}
		if err := tx.Model(&secret).Update("revoked_at", now).Error; err != nil {
			return err
		}
		return createControlAudit(tx, actor, "secret.revoke", "secret", secret.ID, secret.Version, "succeeded", "")
	})
}

func (r *Runtime) ApplyNodeConfig(ctx context.Context, nodeID string, desired protocol.WorkerDesiredConfig, actor ControlActor) (*model.ClusterNodeDesiredConfig, error) {
	r.controlMu.Lock()
	defer r.controlMu.Unlock()
	if err := desired.Validate(); err != nil {
		return nil, err
	}
	hash, err := protocol.HashWorkerDesiredConfig(desired)
	if err != nil {
		return nil, err
	}
	raw, _ := json.Marshal(desired)
	var current model.ClusterNodeDesiredConfig
	revision := uint64(1)
	if err := db.GetDb().WithContext(ctx).First(&current, "node_id = ?", nodeID).Error; err == nil {
		revision = current.Revision + 1
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	payload := protocol.ConfigApply{Revision: revision, DesiredHash: hash, ConfigJSON: string(raw), DesiredConfig: &desired}
	state := &model.ClusterNodeDesiredConfig{
		NodeID: nodeID, Revision: revision, DesiredHash: hash, ConfigJSON: string(raw), Status: model.ClusterDesiredStatusPending,
	}
	if err := r.sendDurableControl(ctx, nodeID, protocol.MessageConfigApply, nodeID, payload, func(tx *gorm.DB) error {
		if err := ensureControlNode(tx, nodeID); err != nil {
			return err
		}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "node_id"}}, DoUpdates: clause.AssignmentColumns([]string{
			"updated_at", "revision", "desired_hash", "config_json", "status", "last_error",
		})}).Create(state).Error; err != nil {
			return err
		}
		return createControlAudit(tx, actor, "config.apply", "node_config", nodeID, revision, "queued", "")
	}); err != nil {
		return state, err
	}
	return state, nil
}

func (r *Runtime) ApplyStorageProfile(ctx context.Context, req StorageProfileWriteRequest, actor ControlActor) (*model.ClusterStorageProfile, error) {
	r.controlMu.Lock()
	defer r.controlMu.Unlock()
	if strings.TrimSpace(req.NodeID) == "" || strings.TrimSpace(req.CredentialRef) == "" {
		return nil, errors.New("node_id and credential_ref are required")
	}
	var node model.ClusterNode
	if err := db.GetDb().WithContext(ctx).First(&node, "id = ?", req.NodeID).Error; err != nil {
		return nil, err
	}
	if node.Disabled || node.Status == model.ClusterNodeStatusRevoked || node.KeyPublic == "" {
		return nil, errors.New("cluster node is disabled, revoked, or has no pinned public key")
	}
	var secret model.ClusterSecret
	if err := db.GetDb().WithContext(ctx).First(&secret, "id = ? AND revoked_at IS NULL", req.CredentialRef).Error; err != nil {
		return nil, err
	}
	secretRaw, err := decryptCoordinatorSecret(secret)
	if err != nil {
		return nil, err
	}
	var secretParameters map[string]any
	if err := json.Unmarshal(secretRaw, &secretParameters); err != nil {
		return nil, errors.New("stored secret payload is invalid")
	}
	profileID := strings.TrimSpace(req.ID)
	if profileID == "" {
		profileID = uuid.NewString()
	}
	var existing model.ClusterStorageProfile
	revision := uint64(1)
	if err := db.GetDb().WithContext(ctx).First(&existing, "id = ?", profileID).Error; err == nil {
		revision = existing.Revision + 1
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	parametersRaw, err := json.Marshal(req.Parameters)
	if err != nil {
		return nil, errors.New("storage parameters are invalid")
	}
	hashRaw, _ := json.Marshal(struct {
		NodeID, NodeMountID, Driver, SchemaVersion, MountPath, Parameters, CredentialFingerprint string
		Disabled                                                                                 bool
	}{req.NodeID, req.NodeMountID, req.Driver, req.SchemaVersion, req.MountPath, string(parametersRaw), secret.Fingerprint, req.Disabled})
	desiredHash := fmt.Sprintf("%x", sha256.Sum256(hashRaw))
	apply := protocol.StorageApply{
		Revision: revision, DesiredHash: desiredHash, NodeMountID: req.NodeMountID,
		Driver: req.Driver, SchemaVersion: req.SchemaVersion, MountPath: req.MountPath,
		Parameters: req.Parameters, CredentialRef: secret.ID, Operation: req.Operation,
		Remark: req.Remark, Disabled: req.Disabled,
	}
	apply.SecretEnvelope, err = secure.SealJSON(node.KeyPublic, secretParameters, protocol.StorageApplyAAD(req.NodeID, apply))
	if err != nil {
		return nil, err
	}
	profile := &model.ClusterStorageProfile{
		ID: profileID, NodeID: req.NodeID, NodeMountID: req.NodeMountID, Revision: revision,
		DesiredHash: desiredHash, Driver: req.Driver, SchemaVersion: req.SchemaVersion,
		MountPath: req.MountPath, ParametersJSON: string(parametersRaw), CredentialRef: secret.ID,
		Status: model.ClusterDesiredStatusPending,
	}
	if err := r.sendDurableControl(ctx, req.NodeID, protocol.MessageStorageApply, profile.ID, apply, func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, DoUpdates: clause.AssignmentColumns([]string{
			"updated_at", "node_id", "node_mount_id", "revision", "desired_hash", "driver", "schema_version", "mount_path",
			"parameters_json", "credential_ref", "status", "last_error",
		})}).Create(profile).Error; err != nil {
			return err
		}
		return createControlAudit(tx, actor, "storage.apply", "storage_profile", profile.ID, revision, "queued", "")
	}); err != nil {
		return profile, err
	}
	return profile, nil
}

func ListStorageProfiles(ctx context.Context) ([]model.ClusterStorageProfile, error) {
	var profiles []model.ClusterStorageProfile
	err := db.GetDb().WithContext(ctx).Order("node_id ASC, mount_path ASC").Find(&profiles).Error
	return profiles, err
}

func ListControlAudit(ctx context.Context, limit int) ([]model.ClusterControlAudit, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var rows []model.ClusterControlAudit
	err := db.GetDb().WithContext(ctx).Order("created_at DESC").Limit(limit).Find(&rows).Error
	return rows, err
}

func (r *Runtime) sendDurableControl(ctx context.Context, nodeID string, messageType protocol.MessageType, correlationID string, payload any, persist func(*gorm.DB) error) error {
	r.mu.RLock()
	hub := r.hub
	r.mu.RUnlock()
	if hub == nil {
		return errors.New("cluster coordinator is disabled")
	}
	message, err := protocol.NewEnvelope(messageType, payload)
	if err != nil {
		return err
	}
	message.CorrelationID = correlationID
	now := time.Now().UTC()
	outbox := &model.ClusterOutbox{
		ID: uuid.NewString(), MessageID: message.MessageID, PeerNodeID: nodeID, CorrelationID: correlationID,
		MessageType: string(messageType), PayloadJSON: string(message.Payload),
		PayloadHash: fmt.Sprintf("%x", sha256.Sum256(message.Payload)), Status: model.ClusterMessageStatusPending, AvailableAt: now,
	}
	r.outboxMu.Lock()
	persistErr := db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := persist(tx); err != nil {
			return err
		}
		var lastSeq uint64
		if err := tx.Model(&model.ClusterOutbox{}).Where("peer_node_id = ?", nodeID).Select("COALESCE(MAX(seq), 0)").Scan(&lastSeq).Error; err != nil {
			return err
		}
		outbox.Seq = lastSeq + 1
		return tx.Create(outbox).Error
	})
	r.outboxMu.Unlock()
	if persistErr != nil {
		return persistErr
	}
	if err := hub.Send(ctx, nodeID, *message); err != nil {
		_ = db.GetDb().Model(outbox).Updates(map[string]any{"status": model.ClusterMessageStatusPending, "last_error": err.Error()}).Error
		return err
	}
	return db.GetDb().Model(outbox).Updates(map[string]any{
		"status": model.ClusterMessageStatusSending, "last_sent_at": time.Now().UTC(), "attempt_count": 1,
	}).Error
}

func ensureControlNode(tx *gorm.DB, nodeID string) error {
	var node model.ClusterNode
	if err := tx.First(&node, "id = ?", nodeID).Error; err != nil {
		return err
	}
	if node.Disabled || node.Status == model.ClusterNodeStatusRevoked {
		return errors.New("cluster node is disabled or revoked")
	}
	return nil
}

func createControlAudit(tx *gorm.DB, actor ControlActor, action, resourceType, resourceID string, revision uint64, outcome, detail string) error {
	return tx.Create(&model.ClusterControlAudit{
		ID: uuid.NewString(), Actor: actor.Name, RemoteIP: actor.RemoteIP, RequestID: actor.RequestID,
		Action: action, ResourceType: resourceType, ResourceID: resourceID,
		Revision: revision, Outcome: outcome, Detail: detail,
	}).Error
}

func encryptCoordinatorSecret(plaintext []byte) (string, string, string, error) {
	key, err := coordinatorMasterKey()
	if err != nil {
		return "", "", "", err
	}
	aead, err := coordinatorAEAD(key)
	if err != nil {
		return "", "", "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", "", "", err
	}
	ciphertext := aead.Seal(nil, nonce, plaintext, []byte("openlist-cluster-secret-v1"))
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(plaintext))
	return base64.RawStdEncoding.EncodeToString(ciphertext), base64.RawStdEncoding.EncodeToString(nonce), fingerprint, nil
}

func decryptCoordinatorSecret(secret model.ClusterSecret) ([]byte, error) {
	key, err := coordinatorMasterKey()
	if err != nil {
		return nil, err
	}
	aead, err := coordinatorAEAD(key)
	if err != nil {
		return nil, err
	}
	nonce, err := base64.RawStdEncoding.DecodeString(secret.Nonce)
	if err != nil || len(nonce) != aead.NonceSize() {
		return nil, errors.New("stored secret nonce is invalid")
	}
	ciphertext, err := base64.RawStdEncoding.DecodeString(secret.Ciphertext)
	if err != nil {
		return nil, errors.New("stored secret ciphertext is invalid")
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, []byte("openlist-cluster-secret-v1"))
	if err != nil {
		return nil, errors.New("stored secret authentication failed")
	}
	return plaintext, nil
}

func coordinatorMasterKey() ([]byte, error) {
	value := strings.TrimSpace(conf.Conf.Cluster.SecretMasterKey)
	if value == "" {
		return nil, errors.New("cluster secret master key is not configured")
	}
	if raw, err := hex.DecodeString(value); err == nil && len(raw) == 32 {
		return raw, nil
	}
	if raw, err := base64.RawStdEncoding.DecodeString(value); err == nil && len(raw) == 32 {
		return raw, nil
	}
	if raw, err := base64.StdEncoding.DecodeString(value); err == nil && len(raw) == 32 {
		return raw, nil
	}
	return nil, errors.New("cluster secret master key must be 32 bytes encoded as hex or base64")
}

func coordinatorAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

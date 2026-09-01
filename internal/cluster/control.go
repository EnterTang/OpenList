package cluster

import (
	"bytes"
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

// SecretMigrationResult describes an all-or-nothing encryption-key rotation.
// A row is skipped when it already authenticates with the current key.
type SecretMigrationResult struct {
	Total    int `json:"total"`
	Migrated int `json:"migrated"`
	Skipped  int `json:"skipped"`
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

// NodeConfigView is the Coordinator-owned, non-secret desired configuration
// that can be safely shown to an administrator. qB credentials are never
// included; QBClientConfig only contains the local secret reference.
type NodeConfigView struct {
	NodeID           string                       `json:"node_id"`
	Revision         uint64                       `json:"revision"`
	DesiredHash      string                       `json:"desired_hash"`
	Config           protocol.WorkerDesiredConfig `json:"config"`
	Status           string                       `json:"status"`
	ObservedRevision uint64                       `json:"observed_revision"`
	ObservedHash     string                       `json:"observed_hash"`
	ObservedAt       *time.Time                   `json:"observed_at,omitempty"`
	LastError        string                       `json:"last_error,omitempty"`
}

// GetNodeConfig returns the last Coordinator-owned desired config for one
// Worker. The response intentionally contains only secret references, never
// decrypted qB usernames or passwords.
func GetNodeConfig(ctx context.Context, nodeID string) (*NodeConfigView, error) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return nil, errors.New("cluster node id is required")
	}
	var state model.ClusterNodeDesiredConfig
	if err := db.GetDb().WithContext(ctx).First(&state, "node_id = ?", nodeID).Error; err != nil {
		return nil, err
	}
	var config protocol.WorkerDesiredConfig
	if strings.TrimSpace(state.ConfigJSON) != "" {
		if err := json.Unmarshal([]byte(state.ConfigJSON), &config); err != nil {
			return nil, fmt.Errorf("decode desired worker config: %w", err)
		}
	}
	return &NodeConfigView{
		NodeID: state.NodeID, Revision: state.Revision, DesiredHash: state.DesiredHash,
		Config: config, Status: state.Status, ObservedRevision: state.ObservedRevision,
		ObservedHash: state.ObservedHash, ObservedAt: state.ObservedAt, LastError: state.LastError,
	}, nil
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

// MigrateSecrets re-encrypts every ClusterSecret that still uses the previous
// Coordinator master key. The transaction is deliberately all-or-nothing:
// one undecryptable row prevents a partial rotation and leaves the operator
// an actionable error before the previous key is removed.
func MigrateSecrets(ctx context.Context, actor ControlActor) (*SecretMigrationResult, error) {
	keys, err := coordinatorMasterKeys()
	if err != nil {
		return nil, err
	}
	if len(keys) < 2 || bytes.Equal(keys[0], keys[1]) {
		return nil, errors.New("previous cluster secret master key is required for migration")
	}

	result := &SecretMigrationResult{}
	err = db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var secrets []model.ClusterSecret
		if err := tx.Order("id ASC").Find(&secrets).Error; err != nil {
			return err
		}
		result.Total = len(secrets)
		for _, secret := range secrets {
			plaintext, keyIndex, err := decryptCoordinatorSecretWithKeys(secret, keys)
			if err != nil {
				return fmt.Errorf("migrate secret %q: %w", secret.ID, err)
			}
			if keyIndex == 0 {
				result.Skipped++
				continue
			}
			ciphertext, nonce, fingerprint, err := encryptCoordinatorSecretWithKey(plaintext, keys[0])
			if err != nil {
				return fmt.Errorf("re-encrypt secret %q: %w", secret.ID, err)
			}
			updatedAt := time.Now().UTC()
			if err := tx.Model(&model.ClusterSecret{}).Where("id = ?", secret.ID).Updates(map[string]any{
				"updated_at": updatedAt, "ciphertext": ciphertext, "nonce": nonce,
				"fingerprint": fingerprint, "version": secret.Version + 1, "rotated_at": updatedAt,
			}).Error; err != nil {
				return err
			}
			result.Migrated++
		}
		return createControlAudit(tx, actor, "secret.migrate", "secret", "all", uint64(result.Migrated), "succeeded", "re-encrypted with current master key")
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ResolveSecret returns the decrypted JSON payload for an active Coordinator
// secret. Callers must keep the returned bytes in memory only for the
// operation that needs them.
func ResolveSecret(ctx context.Context, id string) ([]byte, string, error) {
	var secret model.ClusterSecret
	if err := db.GetDb().WithContext(ctx).First(&secret, "id = ? AND revoked_at IS NULL", strings.TrimSpace(id)).Error; err != nil {
		return nil, "", err
	}
	plaintext, err := decryptCoordinatorSecret(secret)
	if err != nil {
		return nil, "", err
	}
	return plaintext, secret.Fingerprint, nil
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
	if err := sealNodeConfigQBSecrets(ctx, nodeID, &payload, desired); err != nil {
		return nil, err
	}
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

// ReplayNodeConfig re-sends the current desired configuration without
// advancing its revision. Workers intentionally keep qB credentials only in
// memory, so a process restart needs a fresh node-specific envelope even when
// the desired configuration itself has not changed.
func (r *Runtime) ReplayNodeConfig(ctx context.Context, nodeID string) error {
	r.controlMu.Lock()
	defer r.controlMu.Unlock()
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return errors.New("cluster node id is required")
	}
	var state model.ClusterNodeDesiredConfig
	if err := db.GetDb().WithContext(ctx).First(&state, "node_id = ?", nodeID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}
	apply, err := buildNodeConfigApply(ctx, nodeID, state)
	if err != nil {
		return err
	}
	return r.sendDurableControl(ctx, nodeID, protocol.MessageConfigApply, nodeID, apply, func(tx *gorm.DB) error {
		if err := ensureControlNode(tx, nodeID); err != nil {
			return err
		}
		return createControlAudit(tx, ControlActor{Name: "system"}, "config.replay", "node_config", nodeID, state.Revision, "queued", "replayed desired config after Worker reconnect")
	})
}

func buildNodeConfigApply(ctx context.Context, nodeID string, state model.ClusterNodeDesiredConfig) (protocol.ConfigApply, error) {
	if state.Revision == 0 || strings.TrimSpace(state.DesiredHash) == "" || strings.TrimSpace(state.ConfigJSON) == "" {
		return protocol.ConfigApply{}, errors.New("stored Worker desired configuration is incomplete")
	}
	var desired protocol.WorkerDesiredConfig
	if err := json.Unmarshal([]byte(state.ConfigJSON), &desired); err != nil {
		return protocol.ConfigApply{}, fmt.Errorf("decode desired worker config: %w", err)
	}
	if err := desired.Validate(); err != nil {
		return protocol.ConfigApply{}, err
	}
	hash, err := protocol.HashWorkerDesiredConfig(desired)
	if err != nil {
		return protocol.ConfigApply{}, err
	}
	configJSON := state.ConfigJSON
	if !strings.EqualFold(hash, state.DesiredHash) {
		if !hasLegacyStagingCapacityFields(state.ConfigJSON) {
			return protocol.ConfigApply{}, errors.New("stored Worker desired configuration hash mismatch")
		}
		canonical, marshalErr := json.Marshal(desired)
		if marshalErr != nil {
			return protocol.ConfigApply{}, fmt.Errorf("encode migrated desired worker config: %w", marshalErr)
		}
		configJSON = string(canonical)
		if err := db.GetDb().WithContext(ctx).Model(&model.ClusterNodeDesiredConfig{}).Where("node_id = ? AND revision = ?", state.NodeID, state.Revision).Updates(map[string]any{
			"updated_at": time.Now().UTC(), "desired_hash": hash, "config_json": configJSON,
		}).Error; err != nil {
			return protocol.ConfigApply{}, fmt.Errorf("persist migrated desired worker config: %w", err)
		}
	}
	apply := protocol.ConfigApply{
		Revision: state.Revision, DesiredHash: hash,
		ConfigJSON: configJSON, DesiredConfig: &desired,
	}
	if err := sealNodeConfigQBSecrets(ctx, nodeID, &apply, desired); err != nil {
		return protocol.ConfigApply{}, err
	}
	return apply, nil
}

func hasLegacyStagingCapacityFields(raw string) bool {
	var payload struct {
		Staging map[string]json.RawMessage `json:"staging"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return false
	}
	for key := range payload.Staging {
		switch key {
		case "max_file_bytes", "safety_reserve_bytes", "pause_download_low_watermark_bytes", "resume_download_high_watermark_bytes", "download_disk_low_watermark_gb", "download_disk_high_watermark_gb":
			return true
		}
	}
	return false
}

func sealNodeConfigQBSecrets(ctx context.Context, nodeID string, apply *protocol.ConfigApply, desired protocol.WorkerDesiredConfig) error {
	if len(desired.QBClients) == 0 {
		return nil
	}
	var node model.ClusterNode
	if err := db.GetDb().WithContext(ctx).First(&node, "id = ?", nodeID).Error; err != nil {
		return err
	}
	if node.Disabled || node.Status == model.ClusterNodeStatusRevoked || strings.TrimSpace(node.KeyPublic) == "" {
		return errors.New("cluster node is disabled, revoked, or has no pinned public key")
	}
	apply.QBSecretEnvelopes = make(map[string]string, len(desired.QBClients))
	for _, client := range desired.QBClients {
		secret, _, secretErr := ResolveSecret(ctx, client.SecretRef)
		if secretErr != nil {
			return fmt.Errorf("resolve qB client %q secret: %w", client.ID, secretErr)
		}
		var parameters map[string]any
		if err := json.Unmarshal(secret, &parameters); err != nil || parameters == nil {
			return fmt.Errorf("qB client %q secret payload is invalid", client.ID)
		}
		if _, ok := firstSecretString(parameters, "username", "user"); !ok {
			return fmt.Errorf("qB client %q secret username is required", client.ID)
		}
		if _, ok := firstSecretString(parameters, "password", "pass"); !ok {
			return fmt.Errorf("qB client %q secret password is required", client.ID)
		}
		envelope, sealErr := secure.SealJSON(node.KeyPublic, parameters, protocol.QBSecretApplyAAD(nodeID, *apply, client.ID))
		if sealErr != nil {
			return fmt.Errorf("seal qB client %q secret: %w", client.ID, sealErr)
		}
		apply.QBSecretEnvelopes[strings.TrimSpace(client.ID)] = envelope
	}
	return nil
}

func firstSecretString(values map[string]any, keys ...string) (string, bool) {
	for _, key := range keys {
		value, ok := values[key].(string)
		if ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value), true
		}
	}
	return "", false
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
	return encryptCoordinatorSecretWithKey(plaintext, key)
}

func encryptCoordinatorSecretWithKey(plaintext, key []byte) (string, string, string, error) {
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
	keys, err := coordinatorMasterKeys()
	if err != nil {
		return nil, err
	}
	plaintext, _, err := decryptCoordinatorSecretWithKeys(secret, keys)
	return plaintext, err
}

func decryptCoordinatorSecretWithKeys(secret model.ClusterSecret, keys [][]byte) ([]byte, int, error) {
	if len(keys) == 0 {
		return nil, -1, errors.New("cluster secret master key is not configured")
	}
	nonce, err := base64.RawStdEncoding.DecodeString(secret.Nonce)
	if err != nil {
		return nil, -1, errors.New("stored secret nonce is invalid")
	}
	ciphertext, err := base64.RawStdEncoding.DecodeString(secret.Ciphertext)
	if err != nil {
		return nil, -1, errors.New("stored secret ciphertext is invalid")
	}
	for index, key := range keys {
		aead, err := coordinatorAEAD(key)
		if err != nil {
			continue
		}
		if len(nonce) != aead.NonceSize() {
			return nil, -1, errors.New("stored secret nonce is invalid")
		}
		plaintext, err := aead.Open(nil, nonce, ciphertext, []byte("openlist-cluster-secret-v1"))
		if err == nil {
			return plaintext, index, nil
		}
	}
	return nil, -1, errors.New("stored secret authentication failed")
}

func coordinatorMasterKeys() ([][]byte, error) {
	current, err := coordinatorMasterKey()
	if err != nil {
		return nil, err
	}
	keys := [][]byte{current}
	previousValue := strings.TrimSpace(conf.Conf.Cluster.SecretMasterKeyPrevious)
	if previousValue == "" {
		return keys, nil
	}
	previous, err := decodeCoordinatorMasterKey(previousValue, true)
	if err != nil {
		return nil, errors.New("previous cluster secret master key must be 32 bytes encoded as hex or base64 (16-byte legacy keys are also accepted)")
	}
	return append(keys, previous), nil
}

func coordinatorMasterKey() ([]byte, error) {
	value := strings.TrimSpace(conf.Conf.Cluster.SecretMasterKey)
	if value == "" {
		return nil, errors.New("cluster secret master key is not configured")
	}
	key, err := decodeCoordinatorMasterKey(value, false)
	if err != nil {
		return nil, errors.New("cluster secret master key must be 32 bytes encoded as hex or base64")
	}
	return key, nil
}

func decodeCoordinatorMasterKey(value string, allowLegacy bool) ([]byte, error) {
	decode := func(raw []byte) ([]byte, error) {
		if len(raw) == 32 || (allowLegacy && len(raw) == 16) {
			return raw, nil
		}
		return nil, errors.New("invalid key length")
	}
	if raw, err := hex.DecodeString(value); err == nil {
		return decode(raw)
	}
	if raw, err := base64.RawStdEncoding.DecodeString(value); err == nil {
		return decode(raw)
	}
	if raw, err := base64.StdEncoding.DecodeString(value); err == nil {
		return decode(raw)
	}
	return nil, errors.New("invalid key encoding")
}

func coordinatorAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

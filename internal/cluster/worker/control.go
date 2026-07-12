package worker

import (
	"context"
	"encoding/json"
	"errors"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/secure"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type StorageOperator interface {
	FindByMountPath(string) (*model.Storage, error)
	Create(context.Context, model.Storage) (uint, error)
	Update(context.Context, model.Storage) error
}

type openListStorageOperator struct{}

func (openListStorageOperator) FindByMountPath(mountPath string) (*model.Storage, error) {
	storage, err := db.GetStorageByMountPath(path.Clean(mountPath))
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return storage, err
}
func (openListStorageOperator) Create(ctx context.Context, storage model.Storage) (uint, error) {
	return op.CreateStorage(ctx, storage)
}
func (openListStorageOperator) Update(ctx context.Context, storage model.Storage) error {
	return op.UpdateStorage(ctx, storage)
}

type observedState struct {
	revision uint64
	hash     string
}

type limitGate struct {
	mu     sync.Mutex
	limit  int
	active int
	wake   chan struct{}
}

func newLimitGate(limit int) *limitGate {
	return &limitGate{limit: limit, wake: make(chan struct{})}
}

func (g *limitGate) SetLimit(limit int) {
	if limit < 0 {
		limit = 0
	}
	g.mu.Lock()
	g.limit = limit
	close(g.wake)
	g.wake = make(chan struct{})
	g.mu.Unlock()
}

func (g *limitGate) Acquire(ctx context.Context) (func(), error) {
	for {
		g.mu.Lock()
		if g.limit == 0 || g.active < g.limit {
			g.active++
			g.mu.Unlock()
			return func() {
				g.mu.Lock()
				if g.active > 0 {
					g.active--
				}
				close(g.wake)
				g.wake = make(chan struct{})
				g.mu.Unlock()
			}, nil
		}
		wake := g.wake
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wake:
		}
	}
}

func (s *Service) ConfigureControlPlane(nodeID string, keys *secure.KeyPair, operator StorageOperator) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.controlNodeID = strings.TrimSpace(nodeID)
	s.controlKeys = keys
	if operator == nil {
		operator = openListStorageOperator{}
	}
	s.storageOperator = operator
	var persisted model.ClusterWorkerObservedState
	if db.GetDb() != nil && db.GetDb().First(&persisted, "id = ?", "config").Error == nil {
		var desired protocol.WorkerDesiredConfig
		if json.Unmarshal([]byte(persisted.PayloadJSON), &desired) == nil && desired.Validate() == nil {
			s.applyDesiredConfigMemory(desired, persisted.Revision, persisted.Hash)
		}
	}
	if db.GetDb() != nil {
		var storageStates []model.ClusterWorkerObservedState
		if db.GetDb().Where("resource_type = ?", "storage").Find(&storageStates).Error == nil {
			for _, state := range storageStates {
				mountPath := path.Clean(state.ResourceKey)
				s.storageObserved[mountPath] = observedState{revision: state.Revision, hash: state.Hash}
				if state.Revision > s.observedRevision {
					s.observedRevision = state.Revision
				}
			}
		}
	}
}

func (s *Service) ControlIdentity() (*protocol.NodeKeyAgreement, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var identity *protocol.NodeKeyAgreement
	if s.controlKeys != nil {
		identity = &protocol.NodeKeyAgreement{Algorithm: protocol.KeyAgreementX25519, KeyID: s.controlKeys.KeyID(), PublicKey: s.controlKeys.PublicKey()}
	}
	return identity, s.observedRevision
}

func (s *Service) DecorateInventory(report *protocol.InventoryReport) {
	if report == nil {
		return
	}
	report.KeyAgreement, report.ObservedRevision = s.ControlIdentity()
}

func (s *Service) handleConfigApply(ctx context.Context, message protocol.Envelope) error {
	apply, err := protocol.DecodePayload[protocol.ConfigApply](message)
	if err != nil {
		return err
	}
	observed := protocol.ConfigObserved{Revision: apply.Revision, DesiredHash: apply.DesiredHash, ObservedAt: time.Now().UTC()}
	desired, applyErr := apply.DecodeDesiredConfig()
	if applyErr == nil {
		applyErr = s.applyDesiredConfig(ctx, apply, desired)
	}
	if applyErr != nil {
		observed.Status = "failed"
		observed.ErrorCode = controlErrorCode(applyErr)
		observed.Error = safeControlError(applyErr)
	} else {
		observed.Status = "applied"
		observed.ObservedHash = apply.DesiredHash
	}
	return s.sendControlResult(ctx, message.MessageID, protocol.MessageConfigObserved, observed)
}

func (s *Service) applyDesiredConfig(ctx context.Context, apply protocol.ConfigApply, desired protocol.WorkerDesiredConfig) error {
	s.mu.Lock()
	if apply.Revision < s.configObserved.revision {
		s.mu.Unlock()
		return controlFailure{"stale_revision", "desired config revision is older than the observed revision"}
	}
	if apply.Revision == s.configObserved.revision {
		s.mu.Unlock()
		if strings.EqualFold(apply.DesiredHash, s.configObserved.hash) {
			return nil
		}
		return controlFailure{"revision_conflict", "desired config revision was already applied with a different hash"}
	}
	s.mu.Unlock()
	raw, err := json.Marshal(desired)
	if err != nil {
		return controlFailure{"invalid_config", "desired config could not be persisted"}
	}
	if database := db.GetDb(); database != nil {
		state := model.ClusterWorkerObservedState{ID: "config", ResourceType: "config", ResourceKey: "worker", Revision: apply.Revision, Hash: apply.DesiredHash, PayloadJSON: string(raw)}
		if err := database.WithContext(ctx).Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, DoUpdates: clause.AssignmentColumns([]string{"updated_at", "revision", "hash", "payload_json"})}).Create(&state).Error; err != nil {
			return controlFailure{"config_persist_failed", "desired config could not be persisted"}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyDesiredConfigMemory(desired, apply.Revision, apply.DesiredHash)
	return nil
}

func (s *Service) applyDesiredConfigMemory(desired protocol.WorkerDesiredConfig, revision uint64, hash string) {
	s.desiredConfig = cloneDesiredConfig(desired)
	s.configObserved = observedState{revision: revision, hash: hash}
	if revision > s.observedRevision {
		s.observedRevision = revision
	}
	s.downloadGate.SetLimit(desired.DownloadConcurrency)
	s.uploadGate.SetLimit(desired.UploadConcurrency)
	for name, binding := range desired.TargetBindings {
		gate := s.targetGates[name]
		if gate == nil {
			gate = newLimitGate(defaultTargetConcurrency(binding.MaxConcurrency))
			s.targetGates[name] = gate
		} else {
			gate.SetLimit(defaultTargetConcurrency(binding.MaxConcurrency))
		}
	}
}

func (s *Service) handleStorageApply(ctx context.Context, message protocol.Envelope) error {
	apply, err := protocol.DecodePayload[protocol.StorageApply](message)
	if err != nil {
		return err
	}
	result := protocol.StorageApplyResult{
		Revision: apply.Revision, DesiredHash: apply.DesiredHash, NodeMountID: apply.NodeMountID,
		MountPath: path.Clean(apply.MountPath), AppliedAt: time.Now().UTC(),
	}
	if err := apply.Validate(); err != nil {
		result.Status, result.ErrorCode, result.Error = "failed", "invalid_request", "storage apply request is invalid"
		return s.sendControlResult(ctx, message.MessageID, protocol.MessageStorageApplyResult, result)
	}
	nodeMountID, storageID, applyErr := s.applyStorage(ctx, apply)
	if applyErr != nil {
		result.Status, result.ErrorCode, result.Error = "failed", controlErrorCode(applyErr), safeControlError(applyErr)
	} else {
		result.Status = "applied"
		result.NodeMountID = nodeMountID
		result.StorageID = storageID
	}
	return s.sendControlResult(ctx, message.MessageID, protocol.MessageStorageApplyResult, result)
}

func (s *Service) applyStorage(ctx context.Context, apply protocol.StorageApply) (string, uint, error) {
	s.mu.Lock()
	keys, operator, nodeID := s.controlKeys, s.storageOperator, s.controlNodeID
	state := s.storageObserved[path.Clean(apply.MountPath)]
	s.mu.Unlock()
	if keys == nil || operator == nil || nodeID == "" {
		return "", 0, controlFailure{"control_unavailable", "worker secure storage control is not configured"}
	}
	if apply.Revision < state.revision {
		return "", 0, controlFailure{"stale_revision", "storage revision is older than the observed revision"}
	}
	if apply.Revision == state.revision {
		if strings.EqualFold(apply.DesiredHash, state.hash) {
			if existing, findErr := operator.FindByMountPath(apply.MountPath); findErr == nil && existing != nil {
				return stableMountID(nodeID, existing.ID, existing.MountPath), existing.ID, nil
			}
			return apply.NodeMountID, 0, nil
		}
		return "", 0, controlFailure{"revision_conflict", "storage revision was already applied with a different hash"}
	}
	secretParameters := make(map[string]any)
	if err := keys.OpenJSON(apply.SecretEnvelope, protocol.StorageApplyAAD(nodeID, apply), &secretParameters); err != nil {
		return "", 0, controlFailure{"secret_decryption_failed", "storage credentials could not be authenticated for this worker"}
	}
	parameters := cloneMap(apply.Parameters)
	for key, value := range secretParameters {
		parameters[key] = value
	}
	addition, err := json.Marshal(parameters)
	if err != nil {
		return "", 0, controlFailure{"invalid_parameters", "storage parameters could not be encoded"}
	}
	existing, err := operator.FindByMountPath(apply.MountPath)
	if err != nil {
		return "", 0, controlFailure{"storage_lookup_failed", "worker could not inspect the requested mount path"}
	}
	operation := strings.ToLower(strings.TrimSpace(apply.Operation))
	if operation == "create" && existing != nil {
		return "", 0, controlFailure{"storage_exists", "storage mount path already exists"}
	}
	if operation == "update" && existing == nil {
		return "", 0, controlFailure{"storage_not_found", "storage mount path does not exist"}
	}
	if apply.NodeMountID != "" {
		if existing == nil {
			return "", 0, controlFailure{"mount_identity_mismatch", "a new storage must not claim an existing worker mount identity"}
		}
		if apply.NodeMountID != stableMountID(nodeID, existing.ID, existing.MountPath) {
			return "", 0, controlFailure{"mount_identity_mismatch", "storage mount identity does not match this worker"}
		}
	}
	storage := model.Storage{
		MountPath: path.Clean(apply.MountPath), Driver: strings.TrimSpace(apply.Driver), Addition: string(addition),
		Remark: strings.TrimSpace(apply.Remark), Disabled: apply.Disabled,
	}
	if existing == nil {
		id, createErr := operator.Create(ctx, storage)
		if createErr != nil {
			return "", 0, controlFailure{"storage_create_failed", "storage was saved but could not be initialized"}
		}
		storage.ID = id
	} else {
		if existing.Driver != storage.Driver {
			return "", 0, controlFailure{"driver_mismatch", "an existing mount driver cannot be changed"}
		}
		storage.ID = existing.ID
		storage.Order = existing.Order
		storage.CacheExpiration = existing.CacheExpiration
		storage.CustomCachePolicies = existing.CustomCachePolicies
		storage.Sort = existing.Sort
		storage.Proxy = existing.Proxy
		storage.DisableIndex = existing.DisableIndex
		storage.EnableSign = existing.EnableSign
		if err := operator.Update(ctx, storage); err != nil {
			return "", 0, controlFailure{"storage_update_failed", "storage could not be updated or initialized"}
		}
	}
	s.mu.Lock()
	stateRecord := model.ClusterWorkerObservedState{
		ID: "storage:" + storage.MountPath, ResourceType: "storage", ResourceKey: storage.MountPath,
		Revision: apply.Revision, Hash: apply.DesiredHash,
	}
	if database := db.GetDb(); database != nil {
		if err := database.WithContext(ctx).Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, DoUpdates: clause.AssignmentColumns([]string{"updated_at", "revision", "hash"})}).Create(&stateRecord).Error; err != nil {
			s.mu.Unlock()
			return "", 0, controlFailure{"observed_persist_failed", "storage result could not be persisted"}
		}
	}
	s.storageObserved[storage.MountPath] = observedState{revision: apply.Revision, hash: apply.DesiredHash}
	if apply.Revision > s.observedRevision {
		s.observedRevision = apply.Revision
	}
	s.mu.Unlock()
	return stableMountID(nodeID, storage.ID, storage.MountPath), storage.ID, nil
}

func (s *Service) sendControlResult(ctx context.Context, correlationID string, messageType protocol.MessageType, payload any) error {
	if s.sender == nil {
		return errors.New("cluster worker sender is unavailable")
	}
	message, err := protocol.NewEnvelope(messageType, payload)
	if err != nil {
		return err
	}
	message.CorrelationID = correlationID
	return s.sender.Send(ctx, *message)
}

func (s *Service) providerTempRoot(provider string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	root := strings.TrimSpace(s.desiredConfig.ProviderTempRoots[normalizeControlKey(provider)])
	if root == "" {
		return ""
	}
	return path.Clean(root)
}

func (s *Service) resolveTargetBinding(profile string) (string, string, *limitGate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := normalizeControlKey(profile)
	if binding, ok := s.desiredConfig.TargetBindings[name]; ok {
		gate := s.targetGates[name]
		if gate == nil {
			gate = newLimitGate(defaultTargetConcurrency(binding.MaxConcurrency))
			s.targetGates[name] = gate
		}
		return path.Clean(binding.MountPath), name, gate
	}
	key := path.Clean(strings.TrimSpace(profile))
	gate := s.targetGates[key]
	if gate == nil {
		gate = newLimitGate(1)
		s.targetGates[key] = gate
	}
	return key, key, gate
}

func defaultTargetConcurrency(value int) int {
	if value <= 0 {
		return 1
	}
	return value
}

func normalizeControlKey(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.NewReplacer("-", "_", " ", "_").Replace(value)
	switch value {
	case "aliyundrive", "aliyundriveopen", "aliyun_drive_open":
		return "aliyun_drive"
	case "139", "139yun", "139_cloud":
		return "mobile_139"
	case "115", "115_cloud", "115_open":
		return "pan115"
	case "123", "123pan", "123_open":
		return "pan123"
	default:
		return value
	}
}

func cloneDesiredConfig(config protocol.WorkerDesiredConfig) protocol.WorkerDesiredConfig {
	cloned := config
	cloned.ProviderTempRoots = make(map[string]string, len(config.ProviderTempRoots))
	for key, value := range config.ProviderTempRoots {
		cloned.ProviderTempRoots[normalizeControlKey(key)] = path.Clean(value)
	}
	cloned.TargetBindings = make(map[string]protocol.TargetBinding, len(config.TargetBindings))
	for key, value := range config.TargetBindings {
		value.MountPath = path.Clean(value.MountPath)
		cloned.TargetBindings[normalizeControlKey(key)] = value
	}
	return cloned
}

func cloneMap(values map[string]any) map[string]any {
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

type controlFailure struct{ code, safe string }

func (e controlFailure) Error() string { return e.safe }

func controlErrorCode(err error) string {
	var failure controlFailure
	if errors.As(err, &failure) {
		return failure.code
	}
	return "apply_failed"
}

func safeControlError(err error) string {
	var failure controlFailure
	if errors.As(err, &failure) {
		return failure.safe
	}
	return "worker could not apply the requested state"
}

func (s *Service) acquireDownloadCapacity(ctx context.Context) (func(), error) {
	return s.downloadGate.Acquire(ctx)
}

func (s *Service) acquireUploadCapacity(ctx context.Context) (func(), error) {
	return s.uploadGate.Acquire(ctx)
}

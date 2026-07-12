package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/coordinator"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/resultqueue"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/secure"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/transport"
	clusterworker "github.com/OpenListTeam/OpenList/v4/internal/cluster/worker"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/etfauto"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Runtime struct {
	role Role

	ctx    context.Context
	cancel context.CancelFunc

	hub                *transport.Hub
	coordinatorService *coordinator.Service
	workerClient       *transport.WorkerClient
	workerService      *clusterworker.Service
	redisClient        *redis.Client
	leaseOwner         string

	mu        sync.RWMutex
	controlMu sync.Mutex
	outboxMu  sync.Mutex
	started   bool
}

type DispatchMediaJobRequest struct {
	NodeID               string               `json:"node_id"`
	IdempotencyKey       string               `json:"idempotency_key"`
	Priority             int                  `json:"priority"`
	ExpectedBytes        int64                `json:"expected_bytes"`
	LeaseDuration        time.Duration        `json:"-"`
	TaskContext          protocol.TaskContext `json:"task_context"`
	RequiredCapabilities []string             `json:"required_capabilities,omitempty"`
}

type DispatchShareInspectRequest struct {
	NodeID               string               `json:"node_id,omitempty"`
	IdempotencyKey       string               `json:"idempotency_key"`
	Priority             int                  `json:"priority"`
	LeaseDuration        time.Duration        `json:"-"`
	TaskContext          protocol.TaskContext `json:"task_context"`
	RequiredCapabilities []string             `json:"required_capabilities,omitempty"`
}

type DispatchMediaBatchRequest struct {
	BatchID string                    `json:"batch_id,omitempty"`
	Items   []DispatchMediaJobRequest `json:"items"`
}

type DispatchMediaBatchResult struct {
	BatchID string              `json:"batch_id"`
	Parent  *model.ClusterJob   `json:"parent,omitempty"`
	Jobs    []*model.ClusterJob `json:"jobs"`
	Errors  []string            `json:"errors,omitempty"`
}

var DefaultRuntime = &Runtime{}

func Start() error { return DefaultRuntime.Start() }

func Stop() { DefaultRuntime.Stop() }

func WebSocketHandler() http.Handler { return DefaultRuntime.WebSocketHandler() }

func CoordinatorService() *coordinator.Service { return DefaultRuntime.CoordinatorService() }

func WorkerService() *clusterworker.Service { return DefaultRuntime.WorkerService() }

func DispatchMediaJob(ctx context.Context, req DispatchMediaJobRequest) (*model.ClusterJob, error) {
	return DefaultRuntime.DispatchMediaJob(ctx, req)
}

func DispatchMediaBatch(ctx context.Context, req DispatchMediaBatchRequest) (*DispatchMediaBatchResult, error) {
	return DefaultRuntime.DispatchMediaBatch(ctx, req)
}

func DispatchShareInspect(ctx context.Context, req DispatchShareInspectRequest) (*model.ClusterJob, error) {
	return DefaultRuntime.DispatchShareInspect(ctx, req)
}

func ShareInspectManifest(ctx context.Context, jobID string) (*model.ClusterShareInspectManifest, error) {
	service := DefaultRuntime.CoordinatorService()
	if service == nil {
		return nil, errors.New("cluster coordinator is disabled")
	}
	return service.ShareInspectManifest(ctx, jobID)
}

func SetShareInspectConsumer(consumer coordinator.ShareInspectConsumer) error {
	service := DefaultRuntime.CoordinatorService()
	if service == nil {
		return errors.New("cluster coordinator is disabled")
	}
	service.SetShareInspectConsumer(consumer)
	return nil
}

func QueryNodeInventory(ctx context.Context, nodeID string) error {
	return DefaultRuntime.QueryNodeInventory(ctx, nodeID)
}

func SetNodeState(ctx context.Context, nodeID, state string) error {
	return DefaultRuntime.SetNodeState(ctx, nodeID, state)
}

func (r *Runtime) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return nil
	}
	r.role = ParseRole(conf.Conf.Cluster.Role)
	if r.role == RoleStandalone {
		r.started = true
		return nil
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	if r.role.RunsCoordinator() {
		if strings.TrimSpace(conf.Conf.Cluster.EnrollmentToken) == "" {
			r.stopLocked()
			return errors.New("cluster.enrollment_token is required for coordinator and hybrid roles")
		}
		if strings.TrimSpace(conf.Conf.Cluster.ETFRootPath) == "" {
			r.stopLocked()
			return errors.New("cluster.etf_root_path is required for coordinator and hybrid roles")
		}
		r.leaseOwner = coordinatorID() + ":" + uuid.NewString()
		if err := r.acquireCoordinatorLease(r.ctx, time.Now().UTC()); err != nil {
			r.stopLocked()
			return err
		}
		r.coordinatorService = coordinator.New(db.GetDb(), conf.Conf.Cluster.EnrollmentToken)
		r.coordinatorService.SetShareInspectConsumer(consumeSubscriptionShareInspect)
		r.hub = transport.NewHub(transport.HubOptions{
			CoordinatorID:       coordinatorID(),
			Authenticate:        r.coordinatorService.Authenticate,
			Handler:             r.coordinatorService,
			CheckOrigin:         clusterCheckOrigin,
			RejectDuplicateNode: true,
			OnConnect: func(peer transport.Peer) {
				r.coordinatorService.OnConnect(peer)
				if err := r.coordinatorService.ReplayOutbox(r.ctx, peer); err != nil {
					log.Errorf("replay cluster outbox for node %s: %v", peer.NodeID(), err)
				}
			},
			OnDisconnect:      r.coordinatorService.OnDisconnect,
			HeartbeatInterval: heartbeatInterval(),
		})
		go r.runManifestProcessor()
		go r.runCoordinatorLease()
		subscription.RegisterClusterDispatcher(subscriptionDispatcher{runtime: r})
	}
	if r.role.RunsWorker() {
		if err := r.startWorkerLocked(); err != nil {
			r.stopLocked()
			return err
		}
	}
	r.started = true
	log.Infof("cluster runtime started with role %s", r.role)
	return nil
}

func (r *Runtime) startWorkerLocked() error {
	nodeID := strings.TrimSpace(conf.Conf.Cluster.NodeID)
	if nodeID == "" {
		return errors.New("cluster.node_id is required for worker and hybrid roles")
	}
	coordinatorURL, err := workerCoordinatorURL(r.role)
	if err != nil {
		return err
	}
	redisCfg := conf.Conf.Cluster.Redis
	r.redisClient = redis.NewClient(&redis.Options{
		Addr:     redisCfg.Address,
		Username: redisCfg.Username,
		Password: redisCfg.Password,
		DB:       redisCfg.DB,
	})
	queue := resultqueue.New(r.redisClient, resultqueue.Config{
		Stream:          redisCfg.ResultStream,
		Group:           redisCfg.ConsumerGroup,
		DLQ:             redisCfg.DeadLetterStream,
		Consumer:        nodeID,
		CleanupStream:   redisCfg.CleanupStream,
		CleanupConsumer: nodeID,
	})
	keyFile := strings.TrimSpace(conf.Conf.Cluster.WorkerKeyFile)
	if keyFile == "" {
		base := filepath.Dir(conf.Conf.Database.DBFile)
		if base == "" || base == "." {
			base = "data"
		}
		nodeKeyName := fmt.Sprintf("%x", sha256.Sum256([]byte(nodeID)))[:24]
		keyFile = filepath.Join(base, "cluster", nodeKeyName+".x25519.key")
	}
	keyPair, err := secure.LoadOrCreateKeyPair(keyFile)
	if err != nil {
		return fmt.Errorf("load cluster worker identity: %w", err)
	}
	var workerService *clusterworker.Service
	handler := transport.HandlerFunc(func(ctx context.Context, peer transport.Peer, message protocol.Envelope) error {
		if workerService == nil {
			return nil
		}
		if message.Type == protocol.MessageInventoryQuery {
			redisReady := queue.ValidateDurability(ctx) == nil
			report, inventoryErr := clusterworker.BuildInventory(ctx, nodeID, redisReady)
			if inventoryErr != nil {
				return inventoryErr
			}
			workerService.DecorateInventory(&report)
			return workerService.SendInventory(ctx, report)
		}
		return workerService.HandleMessage(ctx, peer, message)
	})
	r.workerClient, err = transport.NewWorkerClient(transport.WorkerClientOptions{
		URL:               coordinatorURL,
		NodeID:            nodeID,
		NodeName:          nodeID,
		AgentVersion:      conf.Version,
		Role:              string(RoleWorker),
		SupportedVersions: []string{protocol.Version1},
		EnrollmentToken:   conf.Conf.Cluster.EnrollmentToken,
		Handler:           handler,
		HeartbeatInterval: heartbeatInterval(),
		ReconnectMinDelay: reconnectInterval(),
		HelloControlState: func() (*protocol.NodeKeyAgreement, uint64) {
			if workerService != nil {
				return workerService.ControlIdentity()
			}
			return &protocol.NodeKeyAgreement{Algorithm: protocol.KeyAgreementX25519, KeyID: keyPair.KeyID(), PublicKey: keyPair.PublicKey()}, 0
		},
		OnConnect: func(_ transport.Peer, _ protocol.Welcome) {
			redisReady := queue.ValidateDurability(r.ctx) == nil
			report, inventoryErr := clusterworker.BuildInventory(r.ctx, nodeID, redisReady)
			if inventoryErr != nil {
				log.Errorf("build cluster worker inventory: %v", inventoryErr)
				return
			}
			workerService.DecorateInventory(&report)
			if inventoryErr = workerService.SendInventory(r.ctx, report); inventoryErr != nil {
				log.Errorf("send cluster worker inventory: %v", inventoryErr)
			}
		},
		OnDisconnect: func(cause error) {
			if workerService != nil {
				workerService.CancelActive(cause)
			}
		},
	})
	if err != nil {
		return err
	}
	workerService = clusterworker.New(queue, r.workerClient)
	workerService.ConfigureControlPlane(nodeID, keyPair, nil)
	r.workerService = workerService
	clusterworker.SetDefaultService(workerService)
	go func() {
		if err := r.workerClient.Run(r.ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Errorf("cluster worker websocket stopped: %v", err)
		}
	}()
	go r.runReporter(queue)
	go r.runCleanupProcessor()
	return nil
}

func (r *Runtime) runCleanupProcessor() {
	for r.ctx.Err() == nil {
		if err := r.workerService.RunCleanupProcessor(r.ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Errorf("cluster worker cleanup processor stopped: %v", err)
		}
		if !waitContext(r.ctx, 3*time.Second) {
			return
		}
	}
}

func (r *Runtime) runReporter(queue *resultqueue.Queue) {
	for r.ctx.Err() == nil {
		if conf.Conf.Cluster.Redis.RequireAOF {
			if err := queue.ValidateDurability(r.ctx); err != nil {
				log.Errorf("cluster worker result queue is not durable; media deletion must remain blocked: %v", err)
				if !waitContext(r.ctx, 10*time.Second) {
					return
				}
				continue
			}
		}
		if err := r.workerService.RunReporter(r.ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Errorf("cluster worker result reporter stopped: %v", err)
		}
		if !waitContext(r.ctx, 3*time.Second) {
			return
		}
	}
}

func (r *Runtime) runManifestProcessor() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if r.coordinatorService == nil {
				continue
			}
			if _, err := r.coordinatorService.ProcessPendingManifests(r.ctx, 20); err != nil {
				log.Errorf("process cluster ETF manifests: %v", err)
			}
			if _, err := r.coordinatorService.ProcessPendingShareInspects(r.ctx, 20); err != nil {
				log.Errorf("process cluster share inspection manifests: %v", err)
			}
			if _, err := r.coordinatorService.SweepExpiredLeases(r.ctx, time.Now().UTC()); err != nil {
				log.Errorf("sweep expired cluster leases: %v", err)
			}
			if err := r.redispatchQueuedJobs(r.ctx, 20); err != nil {
				log.Errorf("redispatch queued cluster jobs: %v", err)
			}
		}
	}
}

func (r *Runtime) acquireCoordinatorLease(ctx context.Context, now time.Time) error {
	if db.GetDb() == nil {
		return errors.New("cluster coordinator database is unavailable")
	}
	return db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var lease model.ClusterCoordinatorLease
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&lease, "name = ?", "control-plane").Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return tx.Create(&model.ClusterCoordinatorLease{Name: "control-plane", OwnerID: r.leaseOwner, LeaseUntil: now.Add(45 * time.Second)}).Error
		}
		if err != nil {
			return err
		}
		if lease.OwnerID != r.leaseOwner && lease.LeaseUntil.After(now) {
			return fmt.Errorf("cluster coordinator lease is held by %s until %s", lease.OwnerID, lease.LeaseUntil.Format(time.RFC3339))
		}
		return tx.Model(&lease).Updates(map[string]any{"owner_id": r.leaseOwner, "lease_until": now.Add(45 * time.Second)}).Error
	})
}

func (r *Runtime) runCoordinatorLease() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case now := <-ticker.C:
			result := db.GetDb().WithContext(r.ctx).Model(&model.ClusterCoordinatorLease{}).
				Where("name = ? AND owner_id = ?", "control-plane", r.leaseOwner).
				Updates(map[string]any{"lease_until": now.UTC().Add(45 * time.Second)})
			if result.Error != nil || result.RowsAffected != 1 {
				log.Errorf("cluster coordinator lease lost; stopping control-plane schedulers: error=%v rows=%d", result.Error, result.RowsAffected)
				subscription.StopScheduler()
				etfauto.StopWorker()
				r.fenceLostCoordinator()
				return
			}
		}
	}
}

func (r *Runtime) fenceLostCoordinator() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancel != nil {
		r.cancel()
	}
	if r.hub != nil {
		_ = r.hub.Close()
	}
	if r.workerClient != nil {
		_ = r.workerClient.Close()
	}
	if r.redisClient != nil {
		_ = r.redisClient.Close()
	}
	clusterworker.SetDefaultService(nil)
	r.hub = nil
	r.coordinatorService = nil
	r.workerClient = nil
	r.workerService = nil
	r.redisClient = nil
	r.started = false
}

func (r *Runtime) redispatchQueuedJobs(ctx context.Context, limit int) error {
	r.mu.RLock()
	hub := r.hub
	r.mu.RUnlock()
	if hub == nil {
		return nil
	}
	connected := hub.ConnectedNodes()
	if len(connected) == 0 {
		return nil
	}
	var nodes []model.ClusterNode
	if err := db.GetDb().WithContext(ctx).
		Where("id IN ? AND disabled = ? AND drain = ?", connected, false, false).
		Find(&nodes).Error; err != nil {
		return err
	}
	if limit <= 0 {
		limit = 20
	}
	var jobs []model.ClusterJob
	if err := db.GetDb().WithContext(ctx).
		Where("status = ? AND available_at <= ? AND current_attempt_id = ?", model.ClusterJobStatusQueued, time.Now().UTC(), "").
		Order("priority DESC, available_at ASC").Limit(limit).Find(&jobs).Error; err != nil {
		return err
	}
	for i := range jobs {
		available, err := compatibleNodeIDs(ctx, hub, nodes, &jobs[i])
		if err != nil {
			log.Errorf("match cluster job %s to nodes: %v", jobs[i].ID, err)
			continue
		}
		if len(available) == 0 {
			_ = db.GetDb().WithContext(ctx).Model(&model.ClusterJob{}).Where("id = ?", jobs[i].ID).Updates(map[string]any{
				"last_error_code": "no_compatible_worker", "last_error": "no connected worker satisfies provider, target mount, Redis, and capability requirements",
			}).Error
			continue
		}
		nodeID := available[i%len(available)]
		if err := r.redispatchJob(ctx, hub, &jobs[i], nodeID); err != nil && !errors.Is(err, transport.ErrNotConnected) {
			log.Errorf("redispatch cluster job %s: %v", jobs[i].ID, err)
		}
	}
	return nil
}

func compatibleNodeIDs(ctx context.Context, hub *transport.Hub, nodes []model.ClusterNode, job *model.ClusterJob) ([]string, error) {
	var taskContext protocol.TaskContext
	if err := json.Unmarshal([]byte(job.TaskContextJSON), &taskContext); err != nil {
		return nil, err
	}
	var required []string
	if strings.TrimSpace(job.RequiredCapabilitiesJSON) != "" {
		if err := json.Unmarshal([]byte(job.RequiredCapabilitiesJSON), &required); err != nil {
			return nil, err
		}
	}
	matched := make([]string, 0, len(nodes))
	for i := range nodes {
		if _, online := hub.Session(nodes[i].ID); !online {
			continue
		}
		ok, err := nodeInventorySupports(ctx, nodes[i].ID, taskContext, required, job.ExpectedBytes)
		if err != nil {
			return nil, err
		}
		if ok {
			matched = append(matched, nodes[i].ID)
		}
	}
	return matched, nil
}

func nodeInventorySupports(ctx context.Context, nodeID string, taskContext protocol.TaskContext, required []string, expectedBytes int64) (bool, error) {
	var inventory model.ClusterNodeInventory
	if err := db.GetDb().WithContext(ctx).Where("node_id = ?", nodeID).Order("revision DESC").First(&inventory).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	var capabilities protocol.NodeCapabilities
	if err := json.Unmarshal([]byte(inventory.CapabilitiesJSON), &capabilities); err != nil {
		return false, err
	}
	if !capabilities.RedisDurabilityReady || !containsFold(capabilities.SupportedProviders, taskContext.Share.Provider) {
		return false, nil
	}
	for _, operation := range required {
		if !containsFold(capabilities.SupportedOperations, operation) {
			return false, nil
		}
	}
	// share.inspect is metadata-only: it needs provider credentials and the
	// operation capability, but no upload-capable target mount or free space.
	if containsFold(required, model.ClusterJobTypeShareInspect) && strings.TrimSpace(taskContext.TargetProfile) == "" {
		return true, nil
	}
	var mounts []protocol.MountInventory
	if err := json.Unmarshal([]byte(inventory.MountsJSON), &mounts); err != nil {
		return false, err
	}
	targetPath := strings.TrimSpace(taskContext.TargetProfile)
	if targetPath != "" && !path.IsAbs(targetPath) {
		var desired model.ClusterNodeDesiredConfig
		if err := db.GetDb().WithContext(ctx).First(&desired, "node_id = ? AND status = ? AND observed_revision >= revision", nodeID, model.ClusterDesiredStatusApplied).Error; err != nil {
			return false, nil
		}
		var config protocol.WorkerDesiredConfig
		if json.Unmarshal([]byte(desired.ConfigJSON), &config) != nil {
			return false, nil
		}
		binding, ok := config.TargetBindings[targetPath]
		if !ok {
			return false, nil
		}
		targetPath = binding.MountPath
	}
	for _, mount := range mounts {
		mountPath := strings.TrimRight(path.Clean(mount.MountPath), "/")
		resolvedTargetPath := path.Clean(targetPath)
		if resolvedTargetPath != mountPath && !strings.HasPrefix(resolvedTargetPath, mountPath+"/") {
			continue
		}
		if !mount.CanUpload || !mount.SupportsETF {
			continue
		}
		if expectedBytes > 0 && mount.FreeBytes > 0 && mount.FreeBytes < expectedBytes {
			continue
		}
		return true, nil
	}
	return false, nil
}

func containsFold(values []string, expected string) bool {
	expected = strings.TrimSpace(expected)
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), expected) {
			return true
		}
	}
	return false
}

func (r *Runtime) redispatchJob(ctx context.Context, hub *transport.Hub, job *model.ClusterJob, nodeID string) error {
	if job == nil {
		return errors.New("cluster job is nil")
	}
	var taskContext protocol.TaskContext
	if err := json.Unmarshal([]byte(job.TaskContextJSON), &taskContext); err != nil {
		return fmt.Errorf("decode cluster job task context: %w", err)
	}
	var required []string
	if strings.TrimSpace(job.RequiredCapabilitiesJSON) != "" {
		if err := json.Unmarshal([]byte(job.RequiredCapabilitiesJSON), &required); err != nil {
			return fmt.Errorf("decode cluster job capabilities: %w", err)
		}
	}
	now := time.Now().UTC()
	leaseUntil := now.Add(time.Minute)
	attemptID := uuid.NewString()
	leaseToken := uuid.NewString()
	generation := job.CurrentGeneration + 1
	attemptRef := protocol.AttemptRef{JobID: job.ID, AttemptID: attemptID, Generation: generation, LeaseToken: leaseToken}
	offer := protocol.JobOffer{
		AttemptRef:           attemptRef,
		IdempotencyKey:       job.IdempotencyKey,
		JobType:              job.Type,
		LeaseUntil:           leaseUntil,
		RequiredCapabilities: required,
		TaskContext:          taskContext,
		TaskContextHash:      job.TaskContextHash,
	}
	message, err := protocol.NewEnvelope(protocol.MessageJobOffer, offer)
	if err != nil {
		return err
	}
	message.CorrelationID = job.ID
	attempt := model.ClusterJobAttempt{
		ID: attemptID, JobID: job.ID, NodeID: nodeID, Generation: generation,
		Status: model.ClusterAttemptStatusOffered, LeaseTokenHash: fmt.Sprintf("%x", sha256.Sum256([]byte(leaseToken))),
		LeaseUntil: leaseUntil, OfferedAt: now,
	}
	outbox := model.ClusterOutbox{
		ID: uuid.NewString(), MessageID: message.MessageID, PeerNodeID: nodeID, CorrelationID: job.ID,
		MessageType: string(message.Type), PayloadJSON: string(message.Payload),
		PayloadHash: fmt.Sprintf("%x", sha256.Sum256(message.Payload)), Status: model.ClusterMessageStatusPending, AvailableAt: now,
	}
	r.outboxMu.Lock()
	err = db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		claim := tx.Model(&model.ClusterJob{}).
			Where("id = ? AND status = ? AND current_attempt_id = ? AND current_generation = ?", job.ID, model.ClusterJobStatusQueued, "", job.CurrentGeneration).
			Updates(map[string]any{
				"status": model.ClusterJobStatusLeased, "assigned_node_id": nodeID,
				"current_attempt_id": attemptID, "current_generation": generation,
				"last_error_code": "", "last_error": "",
			})
		if claim.Error != nil {
			return claim.Error
		}
		if claim.RowsAffected == 0 {
			return nil
		}
		if err := tx.Create(&attempt).Error; err != nil {
			return err
		}
		var lastSeq uint64
		if err := tx.Model(&model.ClusterOutbox{}).Where("peer_node_id = ?", nodeID).Select("COALESCE(MAX(seq), 0)").Scan(&lastSeq).Error; err != nil {
			return err
		}
		outbox.Seq = lastSeq + 1
		return tx.Create(&outbox).Error
	})
	r.outboxMu.Unlock()
	if err != nil {
		return err
	}
	if outbox.Seq == 0 {
		return nil
	}
	if err := hub.Send(ctx, nodeID, *message); err != nil {
		_ = db.GetDb().WithContext(ctx).Model(&model.ClusterOutbox{}).Where("id = ?", outbox.ID).Update("last_error", err.Error()).Error
		return err
	}
	sentAt := time.Now().UTC()
	return db.GetDb().WithContext(ctx).Model(&model.ClusterOutbox{}).Where("id = ?", outbox.ID).Updates(map[string]any{
		"status": model.ClusterMessageStatusSending, "last_sent_at": sentAt, "attempt_count": 1,
	}).Error
}

func (r *Runtime) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopLocked()
}

func (r *Runtime) stopLocked() {
	subscription.RegisterClusterDispatcher(nil)
	if r.cancel != nil {
		r.cancel()
	}
	if r.workerClient != nil {
		_ = r.workerClient.Close()
	}
	if r.hub != nil {
		_ = r.hub.Close()
	}
	if r.redisClient != nil {
		_ = r.redisClient.Close()
	}
	if r.leaseOwner != "" && db.GetDb() != nil {
		_ = db.GetDb().Model(&model.ClusterCoordinatorLease{}).
			Where("name = ? AND owner_id = ?", "control-plane", r.leaseOwner).
			Update("lease_until", time.Now().UTC()).Error
	}
	r.leaseOwner = ""
	clusterworker.SetDefaultService(nil)
	r.started = false
}

func (r *Runtime) WebSocketHandler() http.Handler {
	r.mu.RLock()
	hub := r.hub
	r.mu.RUnlock()
	if hub == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "cluster coordinator is disabled", http.StatusNotFound)
		})
	}
	return hub
}

func (r *Runtime) CoordinatorService() *coordinator.Service {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.coordinatorService
}

func (r *Runtime) WorkerService() *clusterworker.Service {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.workerService
}

func (r *Runtime) QueryNodeInventory(ctx context.Context, nodeID string) error {
	r.mu.RLock()
	hub := r.hub
	r.mu.RUnlock()
	if hub == nil {
		return errors.New("cluster coordinator is disabled")
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return errors.New("cluster node id is required")
	}
	message, err := protocol.NewEnvelope(protocol.MessageInventoryQuery, map[string]any{"requested_at": time.Now().UTC()})
	if err != nil {
		return err
	}
	return hub.Send(ctx, nodeID, *message)
}

func (r *Runtime) SetNodeState(ctx context.Context, nodeID, state string) error {
	r.mu.RLock()
	hub := r.hub
	service := r.coordinatorService
	r.mu.RUnlock()
	if hub == nil || service == nil {
		return errors.New("cluster coordinator is disabled")
	}
	if err := service.SetNodeState(ctx, nodeID, state); err != nil {
		return err
	}
	if state == model.ClusterNodeStatusDisabled || state == model.ClusterNodeStatusRevoked {
		hub.Disconnect(nodeID, errors.New("cluster node disabled by coordinator"))
	}
	return nil
}

// DispatchShareInspect durably offers a metadata-only share inspection. The
// caller owns the idempotency key (normally subscription + source cursor), so
// the same share URL can be inspected again when a later source observation
// may contain updated objects.
func (r *Runtime) DispatchShareInspect(ctx context.Context, req DispatchShareInspectRequest) (*model.ClusterJob, error) {
	r.mu.RLock()
	hub := r.hub
	r.mu.RUnlock()
	if hub == nil {
		return nil, errors.New("cluster coordinator is disabled")
	}
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		return nil, errors.New("share inspection idempotency key is required")
	}
	if strings.TrimSpace(req.TaskContext.Share.URL) == "" || strings.TrimSpace(req.TaskContext.WorkflowVersion) == "" || strings.TrimSpace(req.TaskContext.SealedManifestVersion) == "" {
		return nil, errors.New("share inspection requires share URL, workflow version, and sealed manifest version")
	}
	if len(req.RequiredCapabilities) == 0 {
		req.RequiredCapabilities = []string{model.ClusterJobTypeShareInspect}
	}
	contextHash, err := protocol.HashTaskContext(req.TaskContext)
	if err != nil {
		return nil, err
	}
	idempotencyKey := strings.TrimSpace(req.IdempotencyKey)
	var existing model.ClusterJob
	if err := db.GetDb().WithContext(ctx).First(&existing, "idempotency_key = ?", idempotencyKey).Error; err == nil {
		return &existing, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	if strings.TrimSpace(req.NodeID) == "" {
		nodeID, err := r.selectCompatibleNode(ctx, DispatchMediaJobRequest{
			TaskContext: req.TaskContext, RequiredCapabilities: req.RequiredCapabilities,
		}, 0)
		if err != nil {
			return nil, err
		}
		req.NodeID = nodeID
	}
	if _, ok := hub.Session(req.NodeID); !ok {
		return nil, transport.ErrNotConnected
	}
	var node model.ClusterNode
	if err := db.GetDb().WithContext(ctx).First(&node, "id = ?", req.NodeID).Error; err != nil {
		return nil, err
	}
	if node.Disabled || node.Drain || node.Status == model.ClusterNodeStatusRevoked {
		return nil, errors.New("target cluster node is disabled, draining, or revoked")
	}
	compatible, err := nodeInventorySupports(ctx, req.NodeID, req.TaskContext, req.RequiredCapabilities, 0)
	if err != nil {
		return nil, err
	}
	if !compatible {
		return nil, errors.New("target cluster node inventory does not support share inspection")
	}
	now := time.Now().UTC()
	leaseDuration := req.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = time.Minute
	}
	jobID, attemptID, leaseToken := uuid.NewString(), uuid.NewString(), uuid.NewString()
	leaseUntil := now.Add(leaseDuration)
	taskContextJSON, err := json.Marshal(req.TaskContext)
	if err != nil {
		return nil, err
	}
	requiredJSON, err := json.Marshal(req.RequiredCapabilities)
	if err != nil {
		return nil, err
	}
	job := &model.ClusterJob{
		ID: jobID, Type: model.ClusterJobTypeShareInspect, Status: model.ClusterJobStatusLeased,
		NotificationStatus: model.ClusterNotificationStatusNotRequired, WorkerCleanupStatus: model.ClusterCleanupStatusSucceeded,
		ResultDeliveryStatus: model.ClusterResultDeliveryStatusQueued, IdempotencyKey: idempotencyKey,
		WorkflowVersion: req.TaskContext.WorkflowVersion, Priority: req.Priority,
		SubscriptionID: req.TaskContext.Subscription.SubscriptionID,
		SourceProvider: req.TaskContext.Share.Provider, SourceURL: req.TaskContext.Share.URL,
		TaskContextJSON: string(taskContextJSON), TaskContextHash: contextHash,
		RequiredCapabilitiesJSON: string(requiredJSON), AssignedNodeID: req.NodeID,
		CurrentAttemptID: attemptID, CurrentGeneration: 1, AvailableAt: now,
	}
	attempt := &model.ClusterJobAttempt{
		ID: attemptID, JobID: jobID, NodeID: req.NodeID, Generation: 1,
		Status:         model.ClusterAttemptStatusOffered,
		LeaseTokenHash: fmt.Sprintf("%x", sha256.Sum256([]byte(leaseToken))), LeaseUntil: leaseUntil, OfferedAt: now,
	}
	offer := protocol.JobOffer{
		AttemptRef:     protocol.AttemptRef{JobID: jobID, AttemptID: attemptID, Generation: 1, LeaseToken: leaseToken},
		IdempotencyKey: idempotencyKey, JobType: model.ClusterJobTypeShareInspect,
		LeaseUntil: leaseUntil, RequiredCapabilities: req.RequiredCapabilities,
		TaskContext: req.TaskContext, TaskContextHash: contextHash,
	}
	if err := offer.Validate(); err != nil {
		return nil, err
	}
	message, err := protocol.NewEnvelope(protocol.MessageJobOffer, offer)
	if err != nil {
		return nil, err
	}
	message.CorrelationID = jobID
	outbox := &model.ClusterOutbox{
		ID: uuid.NewString(), MessageID: message.MessageID, PeerNodeID: req.NodeID,
		CorrelationID: jobID, MessageType: string(message.Type), PayloadJSON: string(message.Payload),
		PayloadHash: fmt.Sprintf("%x", sha256.Sum256(message.Payload)), Status: model.ClusterMessageStatusPending, AvailableAt: now,
	}
	r.outboxMu.Lock()
	persistErr := db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(job).Error; err != nil {
			return err
		}
		if err := tx.Create(attempt).Error; err != nil {
			return err
		}
		var lastSeq uint64
		if err := tx.Model(&model.ClusterOutbox{}).Where("peer_node_id = ?", req.NodeID).Select("COALESCE(MAX(seq), 0)").Scan(&lastSeq).Error; err != nil {
			return err
		}
		outbox.Seq = lastSeq + 1
		return tx.Create(outbox).Error
	})
	r.outboxMu.Unlock()
	if persistErr != nil {
		return job, fmt.Errorf("persist share inspection offer: %w", persistErr)
	}
	if err := hub.Send(ctx, req.NodeID, *message); err != nil {
		_ = db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			_ = tx.Model(attempt).Updates(map[string]any{"status": model.ClusterAttemptStatusLost, "finished_at": time.Now().UTC(), "error": err.Error()}).Error
			_ = tx.Model(job).Updates(map[string]any{"status": model.ClusterJobStatusQueued, "assigned_node_id": "", "current_attempt_id": "", "last_error": err.Error()}).Error
			return tx.Model(outbox).Updates(map[string]any{"status": model.ClusterMessageStatusDeadLetter, "last_error": err.Error()}).Error
		})
		return job, err
	}
	sentAt := time.Now().UTC()
	_ = db.GetDb().WithContext(ctx).Model(outbox).Updates(map[string]any{"status": model.ClusterMessageStatusSending, "last_sent_at": sentAt, "attempt_count": 1}).Error
	return job, nil
}

func (r *Runtime) DispatchMediaJob(ctx context.Context, req DispatchMediaJobRequest) (*model.ClusterJob, error) {
	r.mu.RLock()
	hub := r.hub
	r.mu.RUnlock()
	if hub == nil {
		return nil, errors.New("cluster coordinator is disabled")
	}
	if strings.TrimSpace(req.NodeID) == "" {
		return nil, errors.New("target cluster node is required")
	}
	if err := req.TaskContext.Validate(); err != nil {
		return nil, err
	}
	if len(req.TaskContext.SourceObjects) != 1 {
		return nil, errors.New("cluster media dispatch requires exactly one source object; dispatch each media file as a separate child job")
	}
	if len(req.RequiredCapabilities) == 0 {
		req.RequiredCapabilities = []string{"share.save", "mobile.upload", "result.report"}
	}
	contextHash, err := protocol.HashTaskContext(req.TaskContext)
	if err != nil {
		return nil, err
	}
	idempotencyKey := strings.TrimSpace(req.IdempotencyKey)
	if idempotencyKey == "" {
		sourceFingerprint, err := mediaSourceFingerprint(req.TaskContext)
		if err != nil {
			return nil, err
		}
		idempotencyKey = fmt.Sprintf("%d:%s:%s:%s", req.TaskContext.Subscription.SubscriptionID, req.TaskContext.MediaItemID, req.TaskContext.WorkflowVersion, sourceFingerprint)
	}
	var existing model.ClusterJob
	if err := db.GetDb().WithContext(ctx).First(&existing, "idempotency_key = ?", idempotencyKey).Error; err == nil {
		return &existing, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	if _, ok := hub.Session(req.NodeID); !ok {
		return nil, transport.ErrNotConnected
	}
	var node model.ClusterNode
	if err := db.GetDb().WithContext(ctx).First(&node, "id = ?", req.NodeID).Error; err != nil {
		return nil, err
	}
	if node.Disabled || node.Drain || node.Status == model.ClusterNodeStatusRevoked {
		return nil, errors.New("target cluster node is disabled, draining, or revoked")
	}
	compatible, err := nodeInventorySupports(ctx, req.NodeID, req.TaskContext, req.RequiredCapabilities, req.ExpectedBytes)
	if err != nil {
		return nil, err
	}
	if !compatible {
		return nil, errors.New("target cluster node inventory does not satisfy the media task requirements")
	}
	jobID := uuid.NewString()
	attemptID := uuid.NewString()
	leaseToken := uuid.NewString()
	leaseDuration := req.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = time.Minute
	}
	now := time.Now().UTC()
	leaseUntil := now.Add(leaseDuration)
	taskContextJSON, _ := json.Marshal(req.TaskContext)
	requiredJSON, _ := json.Marshal(req.RequiredCapabilities)
	job := &model.ClusterJob{
		ID:                       jobID,
		ParentJobID:              req.TaskContext.ParentBatchID,
		Type:                     model.ClusterJobTypeMediaTransfer,
		Status:                   model.ClusterJobStatusLeased,
		NotificationStatus:       model.ClusterNotificationStatusPending,
		WorkerCleanupStatus:      model.ClusterCleanupStatusPending,
		ResultDeliveryStatus:     model.ClusterResultDeliveryStatusQueued,
		IdempotencyKey:           idempotencyKey,
		WorkflowVersion:          req.TaskContext.WorkflowVersion,
		Priority:                 req.Priority,
		SubscriptionID:           req.TaskContext.Subscription.SubscriptionID,
		SubscriptionItemID:       req.TaskContext.Subscription.SubscriptionItemID,
		MediaItemID:              req.TaskContext.MediaItemID,
		SourceProvider:           req.TaskContext.Share.Provider,
		SourceURL:                req.TaskContext.Share.URL,
		TaskContextJSON:          string(taskContextJSON),
		TaskContextHash:          contextHash,
		RequiredCapabilitiesJSON: string(requiredJSON),
		ExpectedBytes:            req.ExpectedBytes,
		AssignedNodeID:           req.NodeID,
		CurrentAttemptID:         attemptID,
		CurrentGeneration:        1,
		AvailableAt:              now,
	}
	attempt := &model.ClusterJobAttempt{
		ID:             attemptID,
		JobID:          jobID,
		NodeID:         req.NodeID,
		Generation:     1,
		Status:         model.ClusterAttemptStatusOffered,
		LeaseTokenHash: fmt.Sprintf("%x", sha256.Sum256([]byte(leaseToken))),
		LeaseUntil:     leaseUntil,
		OfferedAt:      now,
	}
	attemptRef := protocol.AttemptRef{JobID: jobID, AttemptID: attemptID, Generation: 1, LeaseToken: leaseToken}
	offer := protocol.JobOffer{
		AttemptRef:           attemptRef,
		IdempotencyKey:       idempotencyKey,
		JobType:              model.ClusterJobTypeMediaTransfer,
		LeaseUntil:           leaseUntil,
		RequiredCapabilities: req.RequiredCapabilities,
		TaskContext:          req.TaskContext,
		TaskContextHash:      contextHash,
	}
	message, err := protocol.NewEnvelope(protocol.MessageJobOffer, offer)
	if err != nil {
		return nil, err
	}
	payloadHash := fmt.Sprintf("%x", sha256.Sum256(message.Payload))
	outbox := &model.ClusterOutbox{
		ID:            uuid.NewString(),
		MessageID:     message.MessageID,
		PeerNodeID:    req.NodeID,
		CorrelationID: jobID,
		MessageType:   string(message.Type),
		PayloadJSON:   string(message.Payload),
		PayloadHash:   payloadHash,
		Status:        model.ClusterMessageStatusPending,
		AvailableAt:   now,
	}
	message.CorrelationID = jobID
	r.outboxMu.Lock()
	persistErr := db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(job).Error; err != nil {
			return err
		}
		if err := tx.Create(attempt).Error; err != nil {
			return err
		}
		var lastSeq uint64
		if err := tx.Model(&model.ClusterOutbox{}).Where("peer_node_id = ?", req.NodeID).Select("COALESCE(MAX(seq), 0)").Scan(&lastSeq).Error; err != nil {
			return err
		}
		outbox.Seq = lastSeq + 1
		return tx.Create(outbox).Error
	})
	r.outboxMu.Unlock()
	if persistErr != nil {
		return job, fmt.Errorf("persist cluster job offer: %w", persistErr)
	}
	if err := hub.Send(ctx, req.NodeID, *message); err != nil {
		_ = db.GetDb().Transaction(func(tx *gorm.DB) error {
			_ = tx.Model(&model.ClusterJobAttempt{}).Where("id = ?", attemptID).Updates(map[string]any{"status": model.ClusterAttemptStatusLost, "finished_at": time.Now().UTC(), "error": err.Error()}).Error
			_ = tx.Model(&model.ClusterJob{}).Where("id = ?", jobID).Updates(map[string]any{"status": model.ClusterJobStatusQueued, "assigned_node_id": "", "current_attempt_id": "", "last_error": err.Error()}).Error
			return tx.Model(&model.ClusterOutbox{}).Where("id = ?", outbox.ID).Updates(map[string]any{"status": model.ClusterMessageStatusDeadLetter, "last_error": err.Error()}).Error
		})
		return job, err
	}
	sentAt := time.Now().UTC()
	_ = db.GetDb().Model(&model.ClusterOutbox{}).Where("id = ?", outbox.ID).Updates(map[string]any{
		"status":        model.ClusterMessageStatusSending,
		"last_sent_at":  sentAt,
		"attempt_count": 1,
	}).Error
	return job, nil
}

func mediaSourceFingerprint(task protocol.TaskContext) (string, error) {
	raw, err := json.Marshal(struct {
		SubscriptionID        uint                    `json:"subscription_id"`
		MediaItemID           string                  `json:"media_item_id"`
		WorkflowVersion       string                  `json:"workflow_version"`
		SealedManifestVersion string                  `json:"sealed_manifest_version"`
		SourceObjects         []protocol.SourceObject `json:"source_objects"`
		TargetProfile         string                  `json:"target_profile"`
	}{
		SubscriptionID: task.Subscription.SubscriptionID, MediaItemID: task.MediaItemID,
		WorkflowVersion: task.WorkflowVersion, SealedManifestVersion: task.SealedManifestVersion,
		SourceObjects: task.SourceObjects, TargetProfile: task.TargetProfile,
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(raw)), nil
}

func (r *Runtime) DispatchMediaBatch(ctx context.Context, req DispatchMediaBatchRequest) (*DispatchMediaBatchResult, error) {
	if len(req.Items) == 0 || len(req.Items) > 100 {
		return nil, errors.New("cluster media batch requires 1 to 100 child jobs")
	}
	batchID := strings.TrimSpace(req.BatchID)
	if batchID == "" {
		batchID = uuid.NewString()
	}
	parent := &model.ClusterJob{
		ID: batchID, Type: model.ClusterJobTypeShareBatch, Status: model.ClusterJobStatusPlanning,
		NotificationStatus:  model.ClusterNotificationStatusNotRequired,
		WorkerCleanupStatus: model.ClusterCleanupStatusPending, ResultDeliveryStatus: model.ClusterResultDeliveryStatusQueued,
		IdempotencyKey: "cluster-batch:" + batchID, WorkflowVersion: "cluster-share-batch/v1", ExpectedItems: len(req.Items), AvailableAt: time.Now().UTC(),
	}
	for i := range req.Items {
		parent.ExpectedBytes += req.Items[i].ExpectedBytes
	}
	if err := db.GetDb().WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(parent).Error; err != nil {
		return nil, err
	}
	if err := db.GetDb().WithContext(ctx).First(parent, "id = ?", batchID).Error; err != nil {
		return nil, err
	}
	result := &DispatchMediaBatchResult{BatchID: batchID, Parent: parent, Jobs: make([]*model.ClusterJob, 0, len(req.Items))}
	for i := range req.Items {
		item := req.Items[i]
		item.TaskContext.ParentBatchID = batchID
		if strings.TrimSpace(item.NodeID) == "" {
			nodeID, err := r.selectCompatibleNode(ctx, item, i)
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("item %d: %v", i, err))
				continue
			}
			item.NodeID = nodeID
		}
		job, err := r.DispatchMediaJob(ctx, item)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("item %d: %v", i, err))
			continue
		}
		result.Jobs = append(result.Jobs, job)
	}
	if len(result.Jobs) == 0 {
		now := time.Now().UTC()
		_ = db.GetDb().WithContext(ctx).Model(parent).Updates(map[string]any{
			"status": model.ClusterJobStatusPartialFailed, "finished_at": now, "last_error": strings.Join(result.Errors, "; "),
		}).Error
		return result, errors.New("no cluster media child job was dispatched")
	}
	status := model.ClusterJobStatusRunning
	if len(result.Errors) > 0 {
		status = model.ClusterJobStatusPartialFailed
	}
	_ = db.GetDb().WithContext(ctx).Model(parent).Updates(map[string]any{"status": status, "last_error": strings.Join(result.Errors, "; ")}).Error
	_ = db.GetDb().WithContext(ctx).First(parent, "id = ?", batchID).Error
	return result, nil
}

func (r *Runtime) selectCompatibleNode(ctx context.Context, req DispatchMediaJobRequest, offset int) (string, error) {
	r.mu.RLock()
	hub := r.hub
	r.mu.RUnlock()
	if hub == nil {
		return "", errors.New("cluster coordinator is disabled")
	}
	if len(req.RequiredCapabilities) == 0 {
		req.RequiredCapabilities = []string{"share.save", "mobile.upload", "result.report"}
	}
	taskJSON, err := json.Marshal(req.TaskContext)
	if err != nil {
		return "", err
	}
	requiredJSON, err := json.Marshal(req.RequiredCapabilities)
	if err != nil {
		return "", err
	}
	temporary := model.ClusterJob{TaskContextJSON: string(taskJSON), RequiredCapabilitiesJSON: string(requiredJSON), ExpectedBytes: req.ExpectedBytes}
	connected := hub.ConnectedNodes()
	if len(connected) == 0 {
		return "", transport.ErrNotConnected
	}
	var nodes []model.ClusterNode
	if err := db.GetDb().WithContext(ctx).Where("id IN ? AND disabled = ? AND drain = ?", connected, false, false).Find(&nodes).Error; err != nil {
		return "", err
	}
	matched, err := compatibleNodeIDs(ctx, hub, nodes, &temporary)
	if err != nil {
		return "", err
	}
	if len(matched) == 0 {
		return "", errors.New("no compatible cluster worker is connected")
	}
	return matched[offset%len(matched)], nil
}

func coordinatorID() string {
	if nodeID := strings.TrimSpace(conf.Conf.Cluster.NodeID); nodeID != "" {
		return nodeID
	}
	return "openlist-coordinator"
}

func heartbeatInterval() time.Duration {
	seconds := conf.Conf.Cluster.HeartbeatIntervalSecond
	if seconds <= 0 {
		seconds = 15
	}
	return time.Duration(seconds) * time.Second
}

func reconnectInterval() time.Duration {
	seconds := conf.Conf.Cluster.ReconnectIntervalSecond
	if seconds <= 0 {
		seconds = 5
	}
	return time.Duration(seconds) * time.Second
}

func workerCoordinatorURL(role Role) (string, error) {
	raw := strings.TrimSpace(conf.Conf.Cluster.CoordinatorURL)
	if raw == "" && role == RoleHybrid {
		raw = fmt.Sprintf("http://127.0.0.1:%d", conf.Conf.Scheme.HttpPort)
	}
	if raw == "" {
		return "", errors.New("cluster.coordinator_url is required for worker role")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse cluster coordinator URL: %w", err)
	}
	switch parsed.Scheme {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("unsupported cluster coordinator URL scheme %q", parsed.Scheme)
	}
	if parsed.Scheme != "wss" && !isLoopbackHost(parsed.Hostname()) {
		return "", errors.New("remote cluster coordinator connections must use wss")
	}
	if parsed.Path == "" || parsed.Path == "/" {
		basePath := ""
		if conf.URL != nil {
			basePath = conf.URL.Path
		}
		parsed.Path = strings.TrimRight(basePath, "/") + conf.Conf.Cluster.WebSocketPath
	}
	return parsed.String(), nil
}

func isLoopbackHost(host string) bool {
	host = strings.TrimSpace(strings.ToLower(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func clusterCheckOrigin(request *http.Request) bool {
	origin := strings.TrimSpace(request.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	parsed, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return strings.EqualFold(parsed.Host, request.Host)
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

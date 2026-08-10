package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/resultqueue"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/secure"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/transport"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/plugin"
	"github.com/OpenListTeam/OpenList/v4/internal/setting"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
	"github.com/OpenListTeam/OpenList/v4/internal/task_group"
	"github.com/OpenListTeam/OpenList/v4/pkg/singleflight"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/tache"
	log "github.com/sirupsen/logrus"
)

type Sender interface {
	Send(context.Context, protocol.Envelope) error
}

type clusterErrorCoder interface {
	ClusterErrorCode() string
}

type resultQueue interface {
	ValidateDurability(context.Context) error
	EnqueueDurably(context.Context, any) (string, error)
	EnqueueResultAndCleanupDurably(context.Context, any, resultqueue.CleanupRequest) (string, string, error)
	ClaimAttempt(context.Context, string, time.Duration) (bool, error)
	ReleaseAttempt(context.Context, string) error
	CleanupBacklog(context.Context) (int64, error)
	EnsureGroup(context.Context) error
	Reclaim(context.Context, time.Duration, string, int64) ([]resultqueue.Result, string, error)
	Read(context.Context, int64, time.Duration) ([]resultqueue.Result, error)
	AckAndDelete(context.Context, ...string) error
	MoveToDLQ(context.Context, resultqueue.Result, string) error
	EnsureCleanupGroup(context.Context) error
	ReclaimCleanup(context.Context, time.Duration, string, int64) ([]resultqueue.Result, string, error)
	ReadCleanup(context.Context, int64, time.Duration) ([]resultqueue.Result, error)
	AckAndDeleteCleanup(context.Context, ...string) error
	MoveCleanupToDLQ(context.Context, resultqueue.Result, string) error
	Stats(context.Context) (resultqueue.Stats, error)
}

var cleanupLookupDelay = 2 * time.Second

var (
	getCleanupStorageAndActualPath = op.GetStorageAndActualPath
	getCleanupObject               = getFreshCleanupObject
	removeCleanupObjectExact       = op.RemoveExact
)

func getFreshCleanupObject(ctx context.Context, storage driver.Driver, actualPath string, _ ...bool) (model.Obj, error) {
	actualPath = path.Clean(strings.TrimSpace(actualPath))
	objects, err := op.List(ctx, storage, path.Dir(actualPath), model.ListArgs{Refresh: true})
	if err != nil {
		return nil, err
	}
	name := path.Base(actualPath)
	for _, object := range objects {
		if object.GetName() == name {
			return object, nil
		}
	}
	return nil, errs.ObjectNotFound
}

type activeTask struct {
	attempt       protocol.AttemptRef
	offer         protocol.JobOffer
	ctx           context.Context
	cancel        context.CancelCauseFunc
	stagingMount  string
	deliveryMount string
}

type Service struct {
	queue  resultQueue
	sender Sender

	mu      sync.Mutex
	pending map[string]resultqueue.Result
	active  map[string]*activeTask
	control map[string]chan error
	permits map[string]chan protocol.StagePermit

	controlNodeID         string
	controlKeys           *secure.KeyPair
	storageOperator       StorageOperator
	desiredConfig         protocol.WorkerDesiredConfig
	configObserved        observedState
	storageObserved       map[string]observedState
	observedRevision      uint64
	downloadGate          *limitGate
	uploadGate            *limitGate
	targetGates           map[string]*limitGate
	mediaTransferBoundary func(context.Context, protocol.JobOffer, resolvedMediaTransferTargets) error
	shareSaveSaver        func(context.Context, string, string, string, []string) ([]string, error)
	shareSaveBatchSaver   func(context.Context, string, string, string, []string) ([]string, error)
	stagedSourceFinder    func(context.Context, string, protocol.SourceObject) (string, bool)
	shareSaveFlights      singleflight.Group[[]string]
}

type resolvedMediaTransferTargets struct {
	StagingRoot   string
	DeliveryRoot  string
	DeliveryMount string
}

func New(queue resultQueue, sender Sender) *Service {
	concurrency := effectiveMediaConcurrency()
	return &Service{
		queue: queue, sender: sender,
		pending: make(map[string]resultqueue.Result), active: make(map[string]*activeTask), control: make(map[string]chan error), permits: make(map[string]chan protocol.StagePermit),
		storageOperator: openListStorageOperator{}, storageObserved: make(map[string]observedState),
		downloadGate: newLimitGate(concurrency), uploadGate: newLimitGate(concurrency), targetGates: make(map[string]*limitGate),
		shareSaveSaver: subscription.SaveClusterShareSelection, shareSaveBatchSaver: subscription.SaveClusterShareSelectionBatch, stagedSourceFinder: findExistingStagedSource,
	}
}

func effectiveMediaConcurrency() int {
	limit := defaultMediaConcurrency()
	if limit < 1 {
		return 1
	}
	return limit
}

func defaultMediaConcurrency() int {
	defaultWorkers := 5
	if conf.Conf != nil && conf.Conf.Tasks.Move.Workers > 0 {
		defaultWorkers = conf.Conf.Tasks.Move.Workers
	}
	if conf.Conf == nil {
		return defaultWorkers
	}
	if _, cached := op.Cache.GetSetting(conf.TaskMoveThreadsNum); !cached && db.GetDb() == nil {
		return defaultWorkers
	}
	return setting.GetInt(conf.TaskMoveThreadsNum, defaultWorkers)
}

// EnqueueUploadResult is the deletion barrier for Worker media. Callers may
// remove the uploaded media only after this method succeeds.
func (s *Service) EnqueueUploadResult(ctx context.Context, manifest protocol.UploadETFManifest) (string, error) {
	if s.queue == nil {
		return "", resultqueue.ErrUnavailable
	}
	if err := manifest.Validate(); err != nil {
		return "", err
	}
	return s.queue.EnqueueDurably(ctx, manifest)
}

// EnqueueThenCleanup atomically persists the ETF result and a restart-safe,
// exact-path cleanup request before attempting the destructive cleanup.
func (s *Service) EnqueueThenCleanup(ctx context.Context, manifest protocol.UploadETFManifest, cleanup resultqueue.CleanupRequest) (string, error) {
	persistCtx, cancelPersist := detachedFinalizationContext(ctx)
	if err := manifest.Validate(); err != nil {
		cancelPersist()
		return "", err
	}
	s.reportCleanupStatus(ctx, cleanup, model.ClusterStageStatusRunning, "")
	id, cleanupID, err := s.queue.EnqueueResultAndCleanupDurably(persistCtx, manifest, cleanup)
	cancelPersist()
	if err != nil {
		s.reportCleanupStatus(ctx, cleanup, model.ClusterStageStatusFailed, err.Error())
		return "", err
	}
	cleanupCtx, cancelCleanup := detachedFinalizationContext(ctx)
	defer cancelCleanup()
	if err := executeCleanup(cleanupCtx, cleanup); err != nil {
		s.reportCleanupStatus(ctx, cleanup, model.ClusterStageStatusFailed, err.Error())
		// The durable ETF result is the point of no return for the upload.
		// Returning the cleanup error would make the transfer task upload the
		// same media again. Keep the result queued and surface cleanup as an
		// operational warning instead.
		log.Errorf("cluster upload result %s persisted but media cleanup failed: %v", id, err)
	} else {
		s.reportCleanupStatus(ctx, cleanup, model.ClusterStageStatusSucceeded, "")
		if err := s.queue.AckAndDeleteCleanup(cleanupCtx, cleanupID); err != nil {
			log.Warnf("cluster upload result %s cleaned but cleanup receipt %s could not be removed: %v", id, cleanupID, err)
		}
	}
	return id, nil
}

func (s *Service) RunCleanupProcessor(ctx context.Context) error {
	if s.queue == nil {
		return errors.New("worker cleanup queue is not configured")
	}
	if err := s.queue.EnsureCleanupGroup(ctx); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		requests, _, err := s.queue.ReclaimCleanup(ctx, 5*time.Second, "0-0", 20)
		if err == nil && len(requests) == 0 {
			requests, err = s.queue.ReadCleanup(ctx, 20, 5*time.Second)
		}
		if err != nil {
			if !sleepContext(ctx, time.Second) {
				return ctx.Err()
			}
			continue
		}
		for _, queued := range requests {
			var request resultqueue.CleanupRequest
			if err := json.Unmarshal(queued.Payload, &request); err != nil {
				_ = s.queue.MoveCleanupToDLQ(ctx, queued, "invalid_cleanup_json: "+err.Error())
				continue
			}
			if err := request.Validate(); err != nil {
				_ = s.queue.MoveCleanupToDLQ(ctx, queued, "invalid_cleanup: "+err.Error())
				continue
			}
			s.reportCleanupStatus(ctx, request, model.ClusterStageStatusRunning, "")
			if err := executeCleanup(ctx, request); err != nil {
				s.reportCleanupStatus(ctx, request, model.ClusterStageStatusFailed, err.Error())
				log.Warnf("retry cluster cleanup %s: %v", queued.ID, err)
				continue
			}
			s.reportCleanupStatus(ctx, request, model.ClusterStageStatusSucceeded, "")
			if err := s.queue.AckAndDeleteCleanup(ctx, queued.ID); err != nil {
				log.Warnf("ack cluster cleanup %s: %v", queued.ID, err)
			}
		}
	}
}

func executeCleanup(ctx context.Context, request resultqueue.CleanupRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	targets := []resultqueue.CleanupTarget{{
		OpenListPath: request.OpenListPath, StorageMountPath: request.StorageMountPath,
		RemoteFileID: request.RemoteFileID, Name: request.Name, EmptyRecycleBin: request.EmptyRecycleBin,
	}}
	targets = append(targets, request.AdditionalTargets...)
	for _, target := range targets {
		if err := executeCleanupTarget(ctx, target); err != nil {
			return err
		}
	}
	return nil
}

func executeCleanupTarget(ctx context.Context, target resultqueue.CleanupTarget) error {
	storage, actualPath, err := getCleanupStorageAndActualPath(target.OpenListPath)
	if err != nil {
		return fmt.Errorf("resolve cleanup storage: %w", err)
	}
	if path.Clean(storage.GetStorage().MountPath) != path.Clean(target.StorageMountPath) {
		return errors.New("cleanup storage mount changed; refusing deletion")
	}
	// Brief delay to allow cloud API eventual-consistency to surface the
	// freshly-uploaded file in directory listings.
	select {
	case <-time.After(cleanupLookupDelay):
	case <-ctx.Done():
		return ctx.Err()
	}
	cleanupObj, err := getCleanupObject(ctx, storage, actualPath, true)
	if err != nil {
		if errs.IsObjectNotFound(err) {
			// The file may have already been removed, or the cloud API may
			// not yet reflect the upload in directory listings. If we have a
			// remote file ID, attempt a direct removal by ID before falling
			// back to recycle-bin-only cleanup.
			if strings.TrimSpace(target.RemoteFileID) != "" {
				if remover, ok := storage.(driver.Remove); ok {
					directObj := &model.Object{ID: target.RemoteFileID, Name: target.Name}
					if removeErr := remover.Remove(ctx, directObj); removeErr != nil && !errs.IsNotFoundError(removeErr) {
						log.Warnf("cleanup direct remove by remote id %s failed: %v", target.RemoteFileID, removeErr)
					}
				}
			}
			if target.EmptyRecycleBin {
				cleaner, ok := storage.(driver.RecycleEntryCleaner)
				if !ok {
					return errors.New("cleanup storage does not support recycle-bin cleanup")
				}
				if strings.TrimSpace(target.RemoteFileID) == "" {
					return errors.New("cleanup request is missing remote id for recycle-bin cleanup")
				}
				if err := cleaner.ClearRecycleEntry(ctx, &model.Object{ID: target.RemoteFileID, Name: target.Name}); err != nil {
					return fmt.Errorf("clear missing cleanup recycle entry: %w", err)
				}
			}
			return nil
		}
		return fmt.Errorf("read cleanup target: %w", err)
	}
	if target.RemoteFileID != "" && cleanupObj.GetID() != target.RemoteFileID {
		return errors.New("cleanup target remote id changed; refusing deletion")
	}
	if target.ExactFile && strings.TrimSpace(target.RemoteFileID) == "" {
		return errors.New("exact cleanup target is missing its remote id")
	}
	if err := removeCleanupObjectExact(ctx, storage, actualPath, cleanupObj); err != nil && !errs.IsNotFoundError(err) {
		return err
	}
	if target.EmptyRecycleBin {
		cleaner, ok := storage.(driver.RecycleEntryCleaner)
		if !ok {
			return errors.New("cleanup storage does not support recycle-bin cleanup")
		}
		if target.RemoteFileID == "" {
			return errors.New("cleanup request is missing remote id for recycle-bin cleanup")
		}
		if err := cleaner.ClearRecycleEntry(ctx, cleanupObj); err != nil {
			return fmt.Errorf("clear cleanup recycle entry: %w", err)
		}
	}
	return nil
}

func (s *Service) RunReporter(ctx context.Context) error {
	if s.queue == nil || s.sender == nil {
		return errors.New("worker result reporter is not configured")
	}
	if err := s.queue.EnsureGroup(ctx); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		results, _, err := s.queue.Reclaim(ctx, 5*time.Second, "0-0", 20)
		if err != nil {
			if !sleepContext(ctx, time.Second) {
				return ctx.Err()
			}
			continue
		}
		if len(results) == 0 {
			results, err = s.queue.Read(ctx, 20, 5*time.Second)
			if err != nil {
				if !sleepContext(ctx, time.Second) {
					return ctx.Err()
				}
				continue
			}
		}
		for _, result := range results {
			if err := s.sendResult(ctx, result); err != nil {
				log.Warnf("cluster worker result %s send failed: %v", result.ID, err)
				break
			}
		}
	}
}

func (s *Service) HandleMessage(ctx context.Context, _ transport.Peer, message protocol.Envelope) error {
	if message.Type == protocol.MessageConfigApply {
		return s.handleConfigApply(ctx, message)
	}
	if message.Type == protocol.MessageStorageApply {
		return s.handleStorageApply(ctx, message)
	}
	if message.Type == protocol.MessageAck || message.Type == protocol.MessageNack {
		return s.handleControlResponse(message)
	}
	if message.Type == protocol.MessageStagePermit {
		permit, err := protocol.DecodePayload[protocol.StagePermit](message)
		if err != nil {
			return err
		}
		s.mu.Lock()
		waiter := s.permits[message.CorrelationID]
		if waiter != nil {
			delete(s.permits, message.CorrelationID)
		}
		s.mu.Unlock()
		if waiter != nil {
			select {
			case waiter <- permit:
			default:
			}
		}
		return nil
	}
	if message.Type == protocol.MessageJobOffer {
		offer, err := protocol.DecodePayload[protocol.JobOffer](message)
		if err != nil {
			return err
		}
		if err := offer.Validate(); err != nil {
			return err
		}
		return s.acceptJob(ctx, offer)
	}
	if message.Type == protocol.MessageJobCancel {
		cancel, err := protocol.DecodePayload[protocol.JobCancel](message)
		if err != nil {
			return err
		}
		s.cancelAttempt(cancel.AttemptRef, errors.New("cluster job cancelled by coordinator"))
		return nil
	}
	if message.Type != protocol.MessageUploadETFManifestAck {
		return nil
	}
	ack, err := protocol.DecodePayload[protocol.UploadETFManifestAck](message)
	if err != nil {
		return err
	}
	s.mu.Lock()
	result, ok := s.pending[ack.PayloadHash]
	if ok {
		delete(s.pending, ack.PayloadHash)
	}
	s.mu.Unlock()
	if !ok {
		return nil
	}
	switch ack.Outcome {
	case protocol.ManifestAckAccepted, protocol.ManifestAckDuplicate, protocol.ManifestAckAdopted:
		return s.queue.AckAndDelete(ctx, result.ID)
	case protocol.ManifestAckConflict, protocol.ManifestAckContextMismatch:
		return s.queue.MoveToDLQ(ctx, result, ack.ErrorCode+": "+ack.Error)
	default:
		s.mu.Lock()
		s.pending[ack.PayloadHash] = result
		s.mu.Unlock()
		return nil
	}
}

func (s *Service) handleControlResponse(message protocol.Envelope) error {
	messageID := strings.TrimSpace(message.CorrelationID)
	var responseErr error
	if message.Type == protocol.MessageAck {
		ack, err := protocol.DecodePayload[protocol.Ack](message)
		if err != nil {
			return err
		}
		if strings.TrimSpace(ack.MessageID) != "" {
			messageID = strings.TrimSpace(ack.MessageID)
		}
	} else {
		nack, err := protocol.DecodePayload[protocol.Nack](message)
		if err != nil {
			return err
		}
		if strings.TrimSpace(nack.MessageID) != "" {
			messageID = strings.TrimSpace(nack.MessageID)
		}
		responseErr = fmt.Errorf("coordinator rejected cluster command %s: %s", nack.Code, nack.Error)
	}
	if messageID == "" {
		return nil
	}
	s.mu.Lock()
	waiter := s.control[messageID]
	if waiter != nil {
		delete(s.control, messageID)
	}
	permitWaiter := s.permits[messageID]
	if responseErr != nil && permitWaiter != nil {
		delete(s.permits, messageID)
		close(permitWaiter)
	}
	s.mu.Unlock()
	if waiter != nil {
		select {
		case waiter <- responseErr:
		default:
		}
	}
	return nil
}

func (s *Service) cleanupBacklogBlocksOffer(ctx context.Context, offer protocol.JobOffer) error {
	if offer.JobType == model.ClusterJobTypeShareInspect {
		return nil
	}
	cleanupBacklog, err := s.queue.CleanupBacklog(ctx)
	if err != nil {
		return fmt.Errorf("read worker cleanup backlog: %w", err)
	}
	if cleanupBacklog > 0 {
		return fmt.Errorf("worker has %d pending media cleanup request(s); refusing a new upload until capacity is reclaimed", cleanupBacklog)
	}
	return nil
}

func (s *Service) acceptJob(ctx context.Context, offer protocol.JobOffer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !offer.LeaseUntil.After(time.Now()) {
		return errors.New("cluster job lease has already expired")
	}
	if err := s.queue.ValidateDurability(ctx); err != nil {
		return fmt.Errorf("worker result queue is not durable: %w", err)
	}
	if err := s.cleanupBacklogBlocksOffer(ctx, offer); err != nil {
		return err
	}
	attemptKey := executionAttemptKey(offer.AttemptRef)
	claimed, err := s.queue.ClaimAttempt(ctx, attemptKey, 7*24*time.Hour)
	if err != nil {
		return fmt.Errorf("journal cluster job attempt: %w", err)
	}
	if !claimed {
		return s.sendJobAccept(ctx, offer)
	}
	// Detach execution from the websocket session context so transport loss
	// cannot cancel an in-flight share-save / move. Explicit job.cancel and
	// generation supersession still cancel via cancelCause.
	jobCtx, cancelCause := context.WithCancelCause(context.WithoutCancel(ctx))
	current := &activeTask{attempt: offer.AttemptRef, offer: offer, ctx: jobCtx, cancel: cancelCause}

	s.mu.Lock()
	if running, exists := s.active[offer.JobID]; exists {
		if sameAttempt(running.attempt, offer.AttemptRef) {
			s.mu.Unlock()
			cancelCause(nil)
			if running.ctx.Err() != nil {
				return fmt.Errorf("cluster job %s previous execution is still stopping", offer.JobID)
			}
			return s.sendJobAccept(ctx, offer)
		}
		if offer.Generation <= running.attempt.Generation {
			s.mu.Unlock()
			cancelCause(nil)
			return fmt.Errorf("cluster job %s generation %d is already active", offer.JobID, running.attempt.Generation)
		}
		running.cancel(errors.New("cluster job superseded by a newer generation"))
	}
	s.active[offer.JobID] = current
	s.mu.Unlock()
	if err := s.sendJobAccept(ctx, offer); err != nil {
		_ = s.queue.ReleaseAttempt(context.WithoutCancel(ctx), attemptKey)
		s.finishActive(offer.JobID, current)
		cancelCause(err)
		return err
	}
	go func() {
		defer cancelCause(nil)
		defer s.finishActive(offer.JobID, current)
		go s.maintainLease(jobCtx, cancelCause, offer)

		var result map[string]any
		var err error
		if offer.JobType == "share.inspect" {
			result, err = s.executeShareInspect(jobCtx, offer)
		} else {
			err = s.executeMediaTransfer(jobCtx, offer)
		}
		log.Infof("cluster job %s execution finished, err=%v, sending result", offer.JobID, err)
		resultCtx, cancelResult := context.WithTimeout(context.WithoutCancel(jobCtx), 30*time.Second)
		defer cancelResult()
		if resultErr := s.sendJobResult(resultCtx, offer, result, err); resultErr != nil {
			log.Errorf("send cluster job %s result: %v", offer.JobID, resultErr)
		} else {
			log.Infof("cluster job %s result sent successfully", offer.JobID)
		}
	}()
	return nil
}

func (s *Service) maintainLease(ctx context.Context, cancel context.CancelCauseFunc, offer protocol.JobOffer) {
	leaseWindow := time.Until(offer.LeaseUntil)
	if leaseWindow < time.Minute {
		leaseWindow = time.Minute
	}
	if leaseWindow > 25*time.Minute {
		leaseWindow = 25 * time.Minute
	}
	renewEvery := leaseWindow / 3
	if renewEvery < 5*time.Second {
		renewEvery = 5 * time.Second
	}
	tryRenew := func() bool {
		requestedUntil := time.Now().UTC().Add(leaseWindow)
		if err := s.sendLeaseRenew(context.WithoutCancel(ctx), offer, requestedUntil); err != nil {
			if isLeaseReassigned(err) {
				cancel(fmt.Errorf("renew cluster job lease: %w", err))
				return false
			}
			// Transport blips must not cancel the move. Keep executing and
			// retry renewal; OnTransportReconnected also forces a renew.
			log.Warnf("cluster job %s lease renew deferred: %v", offer.JobID, err)
		}
		return true
	}
	if !tryRenew() {
		return
	}
	ticker := time.NewTicker(renewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !tryRenew() {
				return
			}
		}
	}
}

func isLeaseReassigned(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "stale_lease") || strings.Contains(msg, "reassigned") || strings.Contains(msg, "superseded")
}

func (s *Service) sendLeaseRenew(ctx context.Context, offer protocol.JobOffer, requestedUntil time.Time) error {
	message, err := protocol.NewEnvelope(protocol.MessageLeaseRenew, protocol.LeaseRenew{
		AttemptRef:     offer.AttemptRef,
		RequestedUntil: requestedUntil,
	})
	if err != nil {
		return err
	}
	waiter := make(chan error, 1)
	s.mu.Lock()
	s.control[message.MessageID] = waiter
	s.mu.Unlock()
	if err := s.sender.Send(ctx, *message); err != nil {
		s.mu.Lock()
		delete(s.control, message.MessageID)
		s.mu.Unlock()
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	select {
	case <-waitCtx.Done():
		s.mu.Lock()
		delete(s.control, message.MessageID)
		s.mu.Unlock()
		return waitCtx.Err()
	case err := <-waiter:
		return err
	}
}

func executionAttemptKey(ref protocol.AttemptRef) string {
	return fmt.Sprintf("%s:%s:%d", safeClusterPathSegment(ref.JobID), safeClusterPathSegment(ref.AttemptID), ref.Generation)
}

// CancelActive explicitly aborts every active worker job. It is reserved for
// coordinator-driven cancellation paths and tests. Transport disconnect must
// NOT call this — media moves continue offline and renew after reconnect.
func (s *Service) CancelActive(cause error) {
	if cause == nil {
		cause = transport.ErrSessionClosed
	}
	s.mu.Lock()
	tasks := make([]*activeTask, 0, len(s.active))
	for _, task := range s.active {
		tasks = append(tasks, task)
	}
	s.mu.Unlock()
	for _, task := range tasks {
		task.cancel(cause)
	}
}

// OnTransportReconnected renews leases for jobs that kept running while the
// websocket was down so the coordinator does not sweep them as lease_expired.
func (s *Service) OnTransportReconnected(ctx context.Context) {
	s.mu.Lock()
	tasks := make([]*activeTask, 0, len(s.active))
	for _, task := range s.active {
		if task == nil || task.ctx.Err() != nil {
			continue
		}
		copied := *task
		tasks = append(tasks, &copied)
	}
	s.mu.Unlock()
	if len(tasks) == 0 {
		return
	}
	log.Infof("cluster worker reconnected with %d active transfer(s); renewing leases", len(tasks))
	for _, task := range tasks {
		requestedUntil := time.Now().UTC().Add(25 * time.Minute)
		if err := s.sendLeaseRenew(ctx, task.offer, requestedUntil); err != nil {
			log.Warnf("cluster job %s post-reconnect lease renew failed: %v", task.offer.JobID, err)
		}
	}
}

func (s *Service) cancelAttempt(attempt protocol.AttemptRef, cause error) {
	s.mu.Lock()
	task := s.active[attempt.JobID]
	s.mu.Unlock()
	if task != nil && sameAttempt(task.attempt, attempt) {
		task.cancel(cause)
	}
}

func (s *Service) finishActive(jobID string, task *activeTask) {
	s.mu.Lock()
	if s.active[jobID] == task {
		delete(s.active, jobID)
	}
	s.mu.Unlock()
}

func (s *Service) recordActiveAccountBindings(jobID, stagingMount, deliveryMount string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if task := s.active[jobID]; task != nil {
		task.stagingMount = path.Clean(strings.TrimSpace(stagingMount))
		task.deliveryMount = path.Clean(strings.TrimSpace(deliveryMount))
	}
}

func sameAttempt(left, right protocol.AttemptRef) bool {
	return left.JobID == right.JobID &&
		left.AttemptID == right.AttemptID &&
		left.Generation == right.Generation &&
		left.LeaseToken == right.LeaseToken
}

func detachedFinalizationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	} else {
		ctx = context.WithoutCancel(ctx)
	}
	return context.WithTimeout(ctx, 2*time.Minute)
}

func NewCleanupRequest(manifest protocol.UploadETFManifest, storageMountPath string, additionalTargets ...resultqueue.CleanupTarget) (resultqueue.CleanupRequest, error) {
	request := resultqueue.CleanupRequest{
		Version:          "v1",
		JobID:            safeClusterPathSegment(manifest.JobID),
		MediaItemID:      safeClusterPathSegment(manifest.MediaItemID),
		AttemptID:        manifest.AttemptID,
		Generation:       manifest.Generation,
		LeaseToken:       manifest.LeaseToken,
		OpenListPath:     path.Clean(strings.TrimSpace(manifest.RemotePath)),
		StorageMountPath: path.Clean(storageMountPath),
		RemoteFileID:     manifest.RemoteFileID,
		Name:             manifest.Name,
		EmptyRecycleBin:  true,
		CreatedAt:        time.Now().UTC(),
	}
	if len(additionalTargets) > 0 {
		request.AdditionalTargets = append(request.AdditionalTargets, additionalTargets...)
	}
	if err := request.Validate(); err != nil {
		return resultqueue.CleanupRequest{}, err
	}
	return request, nil
}

func NewSourceCleanupTarget(ctx context.Context, manifest protocol.UploadETFManifest, ownedRoot, sourcePath string) (resultqueue.CleanupTarget, error) {
	ownedRoot = path.Clean(strings.TrimSpace(ownedRoot))
	sourcePath = path.Clean(strings.TrimSpace(sourcePath))
	if path.Dir(sourcePath) != ownedRoot {
		return resultqueue.CleanupTarget{}, errors.New("cluster source cleanup must target a direct file in the staging root")
	}
	storage, actualPath, err := getCleanupStorageAndActualPath(sourcePath)
	if err != nil {
		return resultqueue.CleanupTarget{}, fmt.Errorf("resolve cluster source cleanup storage: %w", err)
	}
	target := resultqueue.CleanupTarget{
		OpenListPath: path.Clean(sourcePath), StorageMountPath: path.Clean(storage.GetStorage().MountPath),
		OwnedRootPath: ownedRoot, Name: path.Base(sourcePath), EmptyRecycleBin: false, ExactFile: true,
	}
	obj, err := getCleanupObject(ctx, storage, actualPath, true)
	if err != nil {
		return resultqueue.CleanupTarget{}, fmt.Errorf("read cluster source cleanup object: %w", err)
	}
	if obj == nil || obj.IsDir() || strings.TrimSpace(obj.GetID()) == "" {
		return resultqueue.CleanupTarget{}, errors.New("cluster source cleanup requires an exact remote file id")
	}
	target.RemoteFileID = obj.GetID()
	probe := resultqueue.CleanupRequest{
		Version: "v1", JobID: safeClusterPathSegment(manifest.JobID), MediaItemID: safeClusterPathSegment(manifest.MediaItemID),
		OpenListPath:     path.Join(target.StorageMountPath, "cleanup-validation", target.Name),
		StorageMountPath: target.StorageMountPath, RemoteFileID: target.RemoteFileID, Name: target.Name, CreatedAt: time.Now().UTC(),
		AdditionalTargets: []resultqueue.CleanupTarget{target},
	}
	if err := probe.Validate(); err != nil {
		return resultqueue.CleanupTarget{}, err
	}
	return target, nil
}

func mapClusterDeliveryPath(deliveryRoot, logicalMediaRoot, logicalTargetPath string) (string, error) {
	deliveryRoot = path.Clean(strings.TrimSpace(deliveryRoot))
	logicalMediaRoot = strings.TrimSpace(logicalMediaRoot)
	if logicalMediaRoot == "" {
		logicalMediaRoot = "/"
	} else {
		logicalMediaRoot = path.Clean(logicalMediaRoot)
	}
	logicalTargetPath = path.Clean(strings.TrimSpace(logicalTargetPath))
	if !strings.HasPrefix(deliveryRoot, "/") || deliveryRoot == "/" {
		return "", errors.New("cluster delivery root must be an absolute non-root path")
	}
	if !strings.HasPrefix(logicalMediaRoot, "/") || !strings.HasPrefix(logicalTargetPath, "/") {
		return "", errors.New("cluster logical media paths must be absolute")
	}
	if strings.Contains(logicalMediaRoot, `\`) || strings.Contains(logicalTargetPath, `\`) {
		return "", errors.New("cluster logical media paths must use slash separators")
	}
	prefix := strings.TrimSuffix(logicalMediaRoot, "/") + "/"
	if logicalMediaRoot == "/" {
		prefix = "/"
	}
	if logicalTargetPath == logicalMediaRoot || !strings.HasPrefix(logicalTargetPath, prefix) {
		return "", errors.New("cluster logical target must be a file below the logical media root")
	}
	relativeTarget := strings.TrimPrefix(logicalTargetPath, prefix)
	mapped := path.Join(deliveryRoot, relativeTarget)
	if mapped == deliveryRoot || !strings.HasPrefix(mapped, strings.TrimSuffix(deliveryRoot, "/")+"/") {
		return "", errors.New("cluster delivery target escapes the worker delivery root")
	}
	return mapped, nil
}

func trustedSourceSHA256(source protocol.SourceObject) (string, bool) {
	hash := strings.ToUpper(strings.TrimSpace(source.ContentSHA256))
	decoded, err := hex.DecodeString(hash)
	if err != nil || len(decoded) != sha256.Size || source.Size <= 0 {
		return "", false
	}
	return hash, true
}

func resolveClusterAdoptPath(targetFilePath, expectedPath, targetName, expectedName string) string {
	if expectedName != targetName {
		return expectedPath
	}
	return targetFilePath
}

func postPluginAdoptMatches(existingName string, existingSize int64, expectedName string, expectedSize int64) bool {
	return existingName == expectedName && (expectedSize <= 0 || existingSize == expectedSize)
}

func clusterMoveContext(ctx context.Context, binding task_group.ClusterTransferBinding, creator *model.User) context.Context {
	ctx = task_group.WithClusterTransferBinding(ctx, binding)
	ctx = context.WithValue(ctx, conf.ForceTaskKey, struct{}{})
	if creator != nil {
		ctx = context.WithValue(ctx, conf.UserKey, creator)
	}
	return ctx
}

type nativeTransferTask interface {
	GetID() string
	GetState() tache.State
	GetErr() error
}

var cancelNativeMoveTask = func(id string) {
	if fs.MoveTaskManager != nil && strings.TrimSpace(id) != "" {
		fs.MoveTaskManager.Cancel(id)
	}
}

func waitNativeTransferTask(ctx context.Context, transfer nativeTransferTask) error {
	if transfer == nil {
		return errors.New("native cluster move task was not created")
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	done := ctx.Done()
	var canceled error
	var cancelDeadline <-chan time.Time
	for {
		switch transfer.GetState() {
		case tache.StateSucceeded:
			return nil
		case tache.StateFailed, tache.StateCanceled:
			if canceled != nil {
				return canceled
			}
			if err := transfer.GetErr(); err != nil {
				return err
			}
			return fmt.Errorf("native cluster move task ended in state %d", transfer.GetState())
		}
		select {
		case <-done:
			canceled = context.Cause(ctx)
			if canceled == nil {
				canceled = ctx.Err()
			}
			cancelNativeMoveTask(transfer.GetID())
			done = nil
			cancelDeadline = time.After(10 * time.Second)
		case <-cancelDeadline:
			return fmt.Errorf("cancel native cluster move task %s: terminal state timeout: %w", transfer.GetID(), canceled)
		case <-ticker.C:
		}
	}
}

func safeClusterPathSegment(value string) string {
	value = strings.TrimSpace(value)
	if value != "" && value != "." && value != ".." {
		safe := true
		for _, r := range value {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
				continue
			}
			safe = false
			break
		}
		if safe {
			return value
		}
	}
	sum := sha256.Sum256([]byte(value))
	return "id-" + hex.EncodeToString(sum[:8])
}

func (s *Service) executeMediaTransfer(ctx context.Context, offer protocol.JobOffer) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if offer.JobType != "media.transfer" {
		return fmt.Errorf("unsupported cluster job type %q", offer.JobType)
	}
	releaseExec, err := s.acquireDownloadCapacity(ctx)
	if err != nil {
		return fmt.Errorf("wait for cluster media concurrency slot: %w", err)
	}
	defer releaseExec()
	targetProfileRef := strings.TrimSpace(offer.TaskContext.TargetProfile)
	if strings.TrimSpace(offer.TaskContext.DeliveryTarget.Provider) == "" && (targetProfileRef == "" || targetProfileRef == "/") {
		return errors.New("cluster target profile must be a mounted destination path")
	}
	targetRootBase, targetBindingMount, err := s.resolveDeliveryTargetRoot(ctx, offer.TaskContext)
	if err != nil {
		return fmt.Errorf("resolve cluster delivery target root: %w", err)
	}
	var targetStorage driver.Driver
	if s.mediaTransferBoundary == nil {
		targetStorage, _, err = op.GetStorageAndActualPath(targetBindingMount)
		if err != nil {
			return fmt.Errorf("resolve cluster target profile: %w", err)
		}
		if !strings.Contains(strings.ToLower(targetStorage.GetStorage().Driver), "139") {
			return errors.New("cluster media target must use a 139 driver with ETF upload support")
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	primary := primarySourceObject(offer.TaskContext.SourceObjects)
	if primary.SourceFileID == "" {
		return errors.New("cluster media task has no source object")
	}
	if _, err := s.requestStagePermitWithRetry(ctx, offer, model.ClusterStageSavingShare); err != nil {
		return err
	}
	s.reportStageStatus(ctx, offer, model.ClusterStageSavingShare, model.ClusterStageStatusRunning, "")
	requestedTempRoot, err := s.resolveStagingTempRoot(ctx, offer.TaskContext)
	if err != nil {
		return fmt.Errorf("resolve cluster staging temp root: %w", err)
	}
	if s.mediaTransferBoundary != nil {
		return s.mediaTransferBoundary(ctx, offer, resolvedMediaTransferTargets{
			StagingRoot: requestedTempRoot, DeliveryRoot: targetRootBase, DeliveryMount: targetBindingMount,
		})
	}
	stagingStorage, _, err := op.GetStorageAndActualPath(requestedTempRoot)
	if err != nil {
		return fmt.Errorf("resolve cluster staging account: %w", err)
	}
	s.recordActiveAccountBindings(offer.JobID, stagingStorage.GetStorage().MountPath, targetBindingMount)
	stagedSource, reused, err := s.prepareMediaTransferShareSave(ctx, offer, requestedTempRoot)
	if err != nil {
		s.reportStageStatus(ctx, offer, model.ClusterStageSavingShare, model.ClusterStageStatusFailed, err.Error())
		return err
	}
	s.reportStageStatus(ctx, offer, model.ClusterStageSavingShare, model.ClusterStageStatusSucceeded, "")
	if reused {
		log.Infof("cluster job %s continuing with existing staged source %s", offer.JobID, stagedSource)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	targetFilePath, err := mapClusterDeliveryPath(targetRootBase, offer.TaskContext.Media.LogicalMediaRoot, offer.TaskContext.Media.LogicalTargetPath)
	if err != nil {
		return fmt.Errorf("map cluster delivery target: %w", err)
	}
	targetRoot := path.Dir(targetFilePath)
	targetName := path.Base(targetFilePath)
	if err := fs.MakeDir(ctx, targetRoot); err != nil {
		return fmt.Errorf("create cluster mobile target: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	uploadPermit, err := s.requestStagePermitWithRetry(ctx, offer, model.ClusterStageUploadingMobile)
	if err != nil {
		return err
	}
	uploadStageFinished := false
	finishUploadStage := func(status, stageError string) {
		if uploadStageFinished {
			return
		}
		s.reportStageStatus(ctx, offer, model.ClusterStageUploadingMobile, status, stageError)
		uploadStageFinished = true
	}
	defer func() {
		if uploadStageFinished {
			return
		}
		stageError := "cluster upload stage ended without a terminal status"
		if err != nil {
			stageError = err.Error()
		}
		finishUploadStage(model.ClusterStageStatusFailed, stageError)
	}()
	manifest := protocol.UploadETFManifest{
		AttemptRef:            offer.AttemptRef,
		ParentBatchID:         offer.TaskContext.ParentBatchID,
		MediaItemID:           offer.TaskContext.MediaItemID,
		OperationKey:          offer.IdempotencyKey,
		StagePermitToken:      uploadPermit.PermitToken,
		TaskContextHash:       offer.TaskContextHash,
		WorkflowVersion:       offer.TaskContext.WorkflowVersion,
		SealedManifestVersion: offer.TaskContext.SealedManifestVersion,
		TargetProfile:         offer.TaskContext.TargetProfile,
		WorkerTargetRoot:      targetRootBase,
		StagingTarget:         offer.TaskContext.StagingTarget,
		DeliveryTarget:        offer.TaskContext.DeliveryTarget,
		Subscription:          offer.TaskContext.Subscription,
		Share:                 offer.TaskContext.Share,
		Media:                 offer.TaskContext.Media,
		SourceObjects:         offer.TaskContext.SourceObjects,
		ShareSaveKey:          offer.TaskContext.ShareSaveKey,
		ShareSaveObjects:      offer.TaskContext.ShareSaveObjects,
		MobileAccountBinding:  targetStorage.GetStorage().MountPath,
	}
	sourceCleanup, err := NewSourceCleanupTarget(ctx, manifest, requestedTempRoot, stagedSource)
	if err != nil {
		return fmt.Errorf("build cluster source cleanup request: %w", err)
	}
	pluginOpts := plugin.ProcessOptions{
		AntiHash:  setting.GetBool(conf.PluginAntiHashEnabled),
		ISORename: setting.GetBool(conf.PluginISORenameEnabled),
		Whitelist: setting.GetStr(conf.PluginExtensionWhitelist),
	}
	expectedName := plugin.ApplyUploadName(targetName, pluginOpts)
	expectedSize := plugin.ExpectedUploadSize(primary.Size, targetName, pluginOpts)
	expectedPath := path.Join(targetRoot, expectedName)
	adoptPath := resolveClusterAdoptPath(targetFilePath, expectedPath, targetName, expectedName)
	existing, getErr := fs.Get(ctx, adoptPath, &fs.GetArgs{NoLog: true})
	if getErr != nil && !errs.IsNotFoundError(getErr) {
		return fmt.Errorf("inspect cluster upload reconciliation target: %w", getErr)
	}
	if getErr == nil && existing != nil && !existing.IsDir() {
		existingSHA256 := strings.ToUpper(strings.TrimSpace(existing.GetHash().GetHash(utils.SHA256)))
		if existingSHA256 == "" {
			return errors.New("cluster target already contains an owned media object without SHA256 metadata; manual reconciliation is required")
		}
		pluginApplied := plugin.ShouldProcessUpload(targetName, pluginOpts)
		adoptOK := false
		if pluginApplied {
			adoptOK = postPluginAdoptMatches(existing.GetName(), existing.GetSize(), expectedName, expectedSize)
			if !adoptOK {
				log.Warnf("cluster job %s target %s does not match post-plugin name/size; continuing with upload", offer.JobID, adoptPath)
			}
		} else {
			expectedSHA256, trusted := trustedSourceSHA256(primary)
			if !trusted {
				return errors.New("cluster source object lacks a trusted size/SHA256 fingerprint; existing target requires manual reconciliation")
			}
			if existing.GetName() != expectedName || existing.GetSize() != primary.Size || !strings.EqualFold(existingSHA256, expectedSHA256) {
				return errors.New("cluster target object does not match the source name, size, and SHA256; refusing automatic adoption")
			}
			adoptOK = true
		}
		if adoptOK {
			manifest.Name = existing.GetName()
			manifest.Size = existing.GetSize()
			manifest.SHA256 = existingSHA256
			manifest.HashSource = "remote_object_metadata"
			manifest.RemoteFileID = existing.GetID()
			manifest.RemotePath = adoptPath
			manifest.UploadReceipt = existing.GetID()
			cleanup, cleanupErr := NewCleanupRequest(manifest, targetStorage.GetStorage().MountPath, sourceCleanup)
			if cleanupErr != nil {
				return cleanupErr
			}
			s.reportStageStatus(ctx, offer, model.ClusterStageUploadingMobile, model.ClusterStageStatusRunning, "")
			if _, enqueueErr := s.EnqueueThenCleanup(ctx, manifest, cleanup); enqueueErr != nil {
				finishUploadStage(model.ClusterStageStatusFailed, enqueueErr.Error())
				return fmt.Errorf("reconcile existing cluster upload: %w", enqueueErr)
			}
			finishUploadStage(model.ClusterStageStatusSucceeded, "")
			return nil
		}
	}
	finalizePayload := task_group.TransferFinalizePayload{
		SubscriptionID: offer.TaskContext.Subscription.SubscriptionID, SubscriptionItemID: offer.TaskContext.Subscription.SubscriptionItemID,
		SourceKey: offer.TaskContext.Subscription.SourceKey, FileHash: primary.Hash,
		TargetDir: targetRoot, FileName: path.Base(stagedSource), TargetName: expectedName,
	}
	binding := task_group.ClusterTransferBinding{
		UploadManifest: &manifest, AdditionalCleanupTargets: []resultqueue.CleanupTarget{sourceCleanup}, FinalizePayload: &finalizePayload,
	}
	creator, err := op.GetAdmin()
	if err != nil {
		return fmt.Errorf("resolve cluster move task creator: %w", err)
	}
	if creator == nil {
		return errors.New("resolve cluster move task creator: admin user is unavailable")
	}
	taskCtx := clusterMoveContext(ctx, binding, creator)
	s.reportStageStatus(ctx, offer, model.ClusterStageUploadingMobile, model.ClusterStageStatusRunning, "")
	transferTask, err := fs.Move(taskCtx, stagedSource, targetRoot, true)
	if err != nil {
		finishUploadStage(model.ClusterStageStatusFailed, err.Error())
		return fmt.Errorf("transfer cluster media: %w", err)
	}
	if err := waitNativeTransferTask(ctx, transferTask); err != nil {
		finishUploadStage(model.ClusterStageStatusFailed, err.Error())
		return fmt.Errorf("native cluster move task: %w", err)
	}
	finishUploadStage(model.ClusterStageStatusSucceeded, "")
	return nil
}

func (s *Service) prepareMediaTransferShareSave(ctx context.Context, offer protocol.JobOffer, requestedTempRoot string) (string, bool, error) {
	primary := primarySourceObject(offer.TaskContext.SourceObjects)
	if primary.SourceFileID == "" {
		return "", false, errors.New("cluster media task has no source object")
	}
	if stagedSource, reused := s.findStagedSource(ctx, requestedTempRoot, primary); reused {
		return stagedSource, true, nil
	}

	shareSaveObjects := mediaTransferShareSaveObjects(offer.TaskContext, primary)
	if strings.TrimSpace(offer.TaskContext.ShareSaveKey) != "" && len(shareSaveObjects) > 0 {
		if !s.allShareSaveObjectsStaged(ctx, requestedTempRoot, shareSaveObjects) {
			key := mediaTransferShareSaveSingleflightKey(offer.TaskContext, requestedTempRoot)
			if _, saveErr, _ := s.shareSaveFlights.Do(key, func() ([]string, error) {
				return s.saveClusterShareSelectionBatch(ctx, offer.TaskContext.Share.URL, offer.TaskContext.Share.Passcode, requestedTempRoot, mediaTransferShareSaveFileIDs(shareSaveObjects))
			}); saveErr != nil {
				// A previous cancelled attempt may have left the object in staging.
				if fallback, ok := s.findStagedSource(ctx, requestedTempRoot, primary); ok {
					log.Warnf("cluster job %s share-save batch reported %v; reusing existing staged object %s", offer.JobID, saveErr, fallback)
					return fallback, true, nil
				}
				return "", false, fmt.Errorf("save cluster share selection batch: %w", saveErr)
			}
		}
		if stagedSource, ok := s.findStagedSource(ctx, requestedTempRoot, primary); ok {
			return stagedSource, false, nil
		}
		return "", false, fmt.Errorf("cluster staged source for %s was not found after share-save batch preparation", primary.SourceFileID)
	}

	saved, saveErr := s.saveClusterShareSelection(ctx, offer.TaskContext.Share.URL, offer.TaskContext.Share.Passcode, requestedTempRoot, []string{primary.SourceFileID})
	if saveErr != nil {
		// A previous cancelled attempt may have left the object in staging.
		if fallback, ok := s.findStagedSource(ctx, requestedTempRoot, primary); ok {
			log.Warnf("cluster job %s share-save reported %v; reusing existing staged object %s", offer.JobID, saveErr, fallback)
			return fallback, true, nil
		}
		return "", false, fmt.Errorf("save cluster share selection: %w", saveErr)
	}
	if len(saved) != 1 {
		return "", false, fmt.Errorf("cluster media task saved %d files, want 1", len(saved))
	}
	return saved[0], false, nil
}

func (s *Service) saveClusterShareSelection(ctx context.Context, rawURL, passcode, tempRoot string, selectedFileIDs []string) ([]string, error) {
	if s.shareSaveSaver != nil {
		return s.shareSaveSaver(ctx, rawURL, passcode, tempRoot, selectedFileIDs)
	}
	return subscription.SaveClusterShareSelection(ctx, rawURL, passcode, tempRoot, selectedFileIDs)
}

func (s *Service) saveClusterShareSelectionBatch(ctx context.Context, rawURL, passcode, tempRoot string, selectedFileIDs []string) ([]string, error) {
	if s.shareSaveBatchSaver != nil {
		return s.shareSaveBatchSaver(ctx, rawURL, passcode, tempRoot, selectedFileIDs)
	}
	return subscription.SaveClusterShareSelectionBatch(ctx, rawURL, passcode, tempRoot, selectedFileIDs)
}

func (s *Service) findStagedSource(ctx context.Context, tempRoot string, primary protocol.SourceObject) (string, bool) {
	if s.stagedSourceFinder != nil {
		return s.stagedSourceFinder(ctx, tempRoot, primary)
	}
	return findExistingStagedSource(ctx, tempRoot, primary)
}

func (s *Service) allShareSaveObjectsStaged(ctx context.Context, tempRoot string, objects []protocol.SourceObject) bool {
	if len(objects) == 0 {
		return false
	}
	for _, object := range objects {
		if _, ok := s.findStagedSource(ctx, tempRoot, object); !ok {
			return false
		}
	}
	return true
}

func mediaTransferShareSaveObjects(task protocol.TaskContext, primary protocol.SourceObject) []protocol.SourceObject {
	objects := make([]protocol.SourceObject, 0, len(task.ShareSaveObjects)+1)
	seen := make(map[string]struct{}, len(task.ShareSaveObjects)+1)
	appendObject := func(object protocol.SourceObject) {
		id := strings.TrimSpace(object.SourceFileID)
		if id == "" {
			return
		}
		object.SourceFileID = id
		if _, exists := seen[id]; exists {
			return
		}
		seen[id] = struct{}{}
		objects = append(objects, object)
	}
	for _, object := range task.ShareSaveObjects {
		appendObject(object)
	}
	appendObject(primary)
	return objects
}

func mediaTransferShareSaveFileIDs(objects []protocol.SourceObject) []string {
	selectedFileIDs := make([]string, 0, len(objects))
	for _, object := range objects {
		if id := strings.TrimSpace(object.SourceFileID); id != "" {
			selectedFileIDs = append(selectedFileIDs, id)
		}
	}
	return selectedFileIDs
}

func mediaTransferShareSaveSingleflightKey(task protocol.TaskContext, requestedTempRoot string) string {
	return strings.Join([]string{
		"share-save-batch",
		strings.TrimSpace(task.ShareSaveKey),
		path.Clean(strings.TrimSpace(requestedTempRoot)),
		strings.TrimSpace(task.StagingTarget.Provider),
		task.StagingTarget.Folder,
		fmt.Sprintf("%d", task.StagingTarget.StorageID),
		strings.TrimSpace(task.StagingTarget.NodeMountID),
		strings.TrimSpace(task.StagingTarget.AccountFingerprint),
	}, "|")
}

func findExistingStagedSource(ctx context.Context, tempRoot string, primary protocol.SourceObject) (string, bool) {
	name := path.Base(strings.TrimSpace(primary.SourceRelativePath))
	if name == "" || name == "." || name == "/" {
		return "", false
	}
	candidate := path.Join(tempRoot, name)
	existing, err := fs.Get(ctx, candidate, &fs.GetArgs{NoLog: true})
	if err != nil || existing == nil || existing.IsDir() {
		return "", false
	}
	if primary.Size > 0 && existing.GetSize() > 0 && existing.GetSize() != primary.Size {
		return "", false
	}
	return candidate, true
}

func (s *Service) reportStageStatus(ctx context.Context, offer protocol.JobOffer, stage, status, stageError string) {
	if err := s.sendStageStatus(ctx, offer, stage, status, stageError); err != nil {
		log.Warnf("cluster job %s stage %s/%s notify failed: %v", offer.JobID, stage, status, err)
	}
}

func (s *Service) reportCleanupStatus(ctx context.Context, cleanup resultqueue.CleanupRequest, status, stageError string) {
	if cleanup.JobID == "" || cleanup.AttemptID == "" || cleanup.Generation == 0 || cleanup.LeaseToken == "" {
		return
	}
	update := protocol.StageStatusUpdate{
		AttemptRef: protocol.AttemptRef{JobID: cleanup.JobID, AttemptID: cleanup.AttemptID, Generation: cleanup.Generation, LeaseToken: cleanup.LeaseToken},
		Stage:      model.ClusterStageWorkerMediaCleanup,
		Status:     status,
		Error:      stageError,
	}
	if status == model.ClusterStageStatusSucceeded || status == model.ClusterStageStatusFailed {
		update.FinishedAt = time.Now().UTC()
	}
	message, err := protocol.NewEnvelope(protocol.MessageStageStatus, update)
	if err != nil {
		log.Warnf("cluster cleanup %s status %s envelope failed: %v", cleanup.JobID, status, err)
		return
	}
	if s.sender == nil {
		return
	}
	if err := s.sender.Send(ctx, *message); err != nil {
		log.Warnf("cluster cleanup %s status %s notify failed: %v", cleanup.JobID, status, err)
	}
}

func (s *Service) sendStageStatus(ctx context.Context, offer protocol.JobOffer, stage, status, stageError string) error {
	update := protocol.StageStatusUpdate{AttemptRef: offer.AttemptRef, Stage: stage, Status: status, Error: stageError}
	if status == model.ClusterStageStatusSucceeded || status == model.ClusterStageStatusFailed {
		update.FinishedAt = time.Now().UTC()
	}
	message, err := protocol.NewEnvelope(protocol.MessageStageStatus, update)
	if err != nil {
		return err
	}
	return s.sender.Send(ctx, *message)
}

func (s *Service) requestStagePermitWithRetry(ctx context.Context, offer protocol.JobOffer, stage string) (protocol.StagePermit, error) {
	backoff := time.Second
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return protocol.StagePermit{}, fmt.Errorf("%w (last permit error: %v)", err, lastErr)
			}
			return protocol.StagePermit{}, err
		}
		permit, err := s.requestStagePermit(ctx, offer, stage)
		if err == nil {
			return permit, nil
		}
		lastErr = err
		if isLeaseReassigned(err) {
			return protocol.StagePermit{}, err
		}
		log.Warnf("cluster job %s stage %s permit deferred: %v", offer.JobID, stage, err)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return protocol.StagePermit{}, ctx.Err()
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (s *Service) requestStagePermit(ctx context.Context, offer protocol.JobOffer, stage string) (protocol.StagePermit, error) {
	request := protocol.StagePermitRequest{
		AttemptRef: offer.AttemptRef, Stage: stage, OperationKey: offer.IdempotencyKey + ":" + stage,
	}
	message, err := protocol.NewEnvelope(protocol.MessageStagePermitRequest, request)
	if err != nil {
		return protocol.StagePermit{}, err
	}
	waiter := make(chan protocol.StagePermit, 1)
	s.mu.Lock()
	s.permits[message.MessageID] = waiter
	s.mu.Unlock()
	if err := s.sender.Send(ctx, *message); err != nil {
		s.mu.Lock()
		delete(s.permits, message.MessageID)
		s.mu.Unlock()
		return protocol.StagePermit{}, err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	select {
	case <-waitCtx.Done():
		s.mu.Lock()
		delete(s.permits, message.MessageID)
		s.mu.Unlock()
		return protocol.StagePermit{}, waitCtx.Err()
	case permit := <-waiter:
		if !sameAttempt(permit.AttemptRef, offer.AttemptRef) || permit.Stage != stage || permit.OperationKey != request.OperationKey || permit.PermitToken == "" || !permit.PermitExpiresAt.After(time.Now().UTC()) {
			return protocol.StagePermit{}, errors.New("coordinator returned an invalid or expired stage permit")
		}
		return permit, nil
	}
}

func (s *Service) sendJobAccept(ctx context.Context, offer protocol.JobOffer) error {
	payload := protocol.JobAccept{AttemptRef: offer.AttemptRef, AcceptedAt: time.Now().UTC()}
	message, err := protocol.NewEnvelope(protocol.MessageJobAccept, payload)
	if err != nil {
		return err
	}
	return s.sender.Send(ctx, *message)
}

func (s *Service) sendJobResult(ctx context.Context, offer protocol.JobOffer, result map[string]any, runErr error) error {
	payload := protocol.JobResult{AttemptRef: offer.AttemptRef, FinishedAt: time.Now().UTC()}
	if runErr == nil {
		payload.Status = "succeeded"
		payload.Result = result
	} else {
		payload.Status = "failed"
		payload.ErrorCode = "worker_execution_failed"
		var coded clusterErrorCoder
		if errors.As(runErr, &coded) && strings.TrimSpace(coded.ClusterErrorCode()) != "" {
			payload.ErrorCode = strings.TrimSpace(coded.ClusterErrorCode())
		}
		payload.Error = runErr.Error()
	}
	message, err := protocol.NewEnvelope(protocol.MessageJobResult, payload)
	if err != nil {
		return err
	}
	return s.sender.Send(ctx, *message)
}

func primarySourceObject(objects []protocol.SourceObject) protocol.SourceObject {
	var selected protocol.SourceObject
	for _, object := range objects {
		ext := strings.ToLower(path.Ext(object.SourceRelativePath))
		isSidecar := ext == ".srt" || ext == ".ass" || ext == ".ssa" || ext == ".nfo" || ext == ".jpg" || ext == ".png"
		if isSidecar {
			continue
		}
		if selected.SourceFileID == "" || object.Size > selected.Size {
			selected = object
		}
	}
	if selected.SourceFileID == "" && len(objects) > 0 {
		selected = objects[0]
	}
	return selected
}

func (s *Service) QueueStats(ctx context.Context) (resultqueue.Stats, error) {
	if s.queue == nil {
		return resultqueue.Stats{}, resultqueue.ErrUnavailable
	}
	return s.queue.Stats(ctx)
}

func (s *Service) SendInventory(ctx context.Context, report protocol.InventoryReport) error {
	if s.sender == nil {
		return transport.ErrNotConnected
	}
	message, err := protocol.NewEnvelope(protocol.MessageInventoryReport, report)
	if err != nil {
		return err
	}
	return s.sender.Send(ctx, *message)
}

func (s *Service) sendResult(ctx context.Context, result resultqueue.Result) error {
	var manifest protocol.UploadETFManifest
	if err := json.Unmarshal(result.Payload, &manifest); err != nil {
		return s.queue.MoveToDLQ(ctx, result, "invalid_manifest_json: "+err.Error())
	}
	if err := manifest.Validate(); err != nil {
		return s.queue.MoveToDLQ(ctx, result, "invalid_manifest: "+err.Error())
	}
	payloadHash, err := protocol.HashUploadETFManifest(manifest)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.pending[payloadHash] = result
	s.mu.Unlock()
	message, err := protocol.NewEnvelope(protocol.MessageUploadETFManifest, manifest)
	if err != nil {
		return err
	}
	message.NodeID = ""
	if err := s.sender.Send(ctx, *message); err != nil {
		s.mu.Lock()
		delete(s.pending, payloadHash)
		s.mu.Unlock()
		return err
	}
	return nil
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

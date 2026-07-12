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
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type Sender interface {
	Send(context.Context, protocol.Envelope) error
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

type activeTask struct {
	attempt protocol.AttemptRef
	ctx     context.Context
	cancel  context.CancelCauseFunc
}

type Service struct {
	queue  resultQueue
	sender Sender

	mu      sync.Mutex
	pending map[string]resultqueue.Result
	active  map[string]*activeTask
	control map[string]chan error
	permits map[string]chan protocol.StagePermit

	controlNodeID    string
	controlKeys      *secure.KeyPair
	storageOperator  StorageOperator
	desiredConfig    protocol.WorkerDesiredConfig
	configObserved   observedState
	storageObserved  map[string]observedState
	observedRevision uint64
	downloadGate     *limitGate
	uploadGate       *limitGate
	targetGates      map[string]*limitGate
}

func New(queue resultQueue, sender Sender) *Service {
	return &Service{
		queue: queue, sender: sender,
		pending: make(map[string]resultqueue.Result), active: make(map[string]*activeTask), control: make(map[string]chan error), permits: make(map[string]chan protocol.StagePermit),
		storageOperator: openListStorageOperator{}, storageObserved: make(map[string]observedState),
		downloadGate: newLimitGate(0), uploadGate: newLimitGate(0), targetGates: make(map[string]*limitGate),
	}
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
	id, cleanupID, err := s.queue.EnqueueResultAndCleanupDurably(persistCtx, manifest, cleanup)
	cancelPersist()
	if err != nil {
		return "", err
	}
	cleanupCtx, cancelCleanup := detachedFinalizationContext(ctx)
	defer cancelCleanup()
	if err := executeCleanup(cleanupCtx, cleanup); err != nil {
		// The durable ETF result is the point of no return for the upload.
		// Returning the cleanup error would make the transfer task upload the
		// same media again. Keep the result queued and surface cleanup as an
		// operational warning instead.
		log.Errorf("cluster upload result %s persisted but media cleanup failed: %v", id, err)
	} else if err := s.queue.AckAndDeleteCleanup(cleanupCtx, cleanupID); err != nil {
		log.Warnf("cluster upload result %s cleaned but cleanup receipt %s could not be removed: %v", id, cleanupID, err)
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
			if err := executeCleanup(ctx, request); err != nil {
				log.Warnf("retry cluster cleanup %s: %v", queued.ID, err)
				continue
			}
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
	storage, _, err := op.GetStorageAndActualPath(target.OpenListPath)
	if err != nil {
		return fmt.Errorf("resolve cleanup storage: %w", err)
	}
	if path.Clean(storage.GetStorage().MountPath) != path.Clean(target.StorageMountPath) {
		return errors.New("cleanup storage mount changed; refusing deletion")
	}
	var cleanupObj model.Obj = &model.Object{ID: target.RemoteFileID, Name: target.Name}
	if found, getErr := fs.Get(ctx, target.OpenListPath, &fs.GetArgs{NoLog: true}); getErr == nil {
		if target.RemoteFileID != "" && found.GetID() != target.RemoteFileID {
			return errors.New("cleanup target remote id changed; refusing deletion")
		}
		cleanupObj = found
	}
	if err := fs.Remove(ctx, target.OpenListPath); err != nil && !errs.IsNotFoundError(err) {
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
	cleanupBacklog, err := s.queue.CleanupBacklog(ctx)
	if err != nil {
		return fmt.Errorf("read worker cleanup backlog: %w", err)
	}
	if cleanupBacklog > 0 {
		return fmt.Errorf("worker has %d pending media cleanup request(s); refusing a new upload until capacity is reclaimed", cleanupBacklog)
	}
	attemptKey := executionAttemptKey(offer.AttemptRef)
	claimed, err := s.queue.ClaimAttempt(ctx, attemptKey, 7*24*time.Hour)
	if err != nil {
		return fmt.Errorf("journal cluster job attempt: %w", err)
	}
	if !claimed {
		return s.sendJobAccept(ctx, offer)
	}
	jobCtx, cancelCause := context.WithCancelCause(ctx)
	current := &activeTask{attempt: offer.AttemptRef, ctx: jobCtx, cancel: cancelCause}

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
			release, capacityErr := s.acquireTargetCapacity(jobCtx, offer.TaskContext.TargetProfile)
			if capacityErr != nil {
				err = capacityErr
			} else {
				defer release()
				err = s.executeMediaTransfer(jobCtx, offer)
			}
		}
		resultCtx, cancelResult := context.WithTimeout(jobCtx, 10*time.Second)
		defer cancelResult()
		if resultErr := s.sendJobResult(resultCtx, offer, result, err); resultErr != nil && jobCtx.Err() == nil {
			log.Errorf("send cluster job %s result: %v", offer.JobID, resultErr)
		}
	}()
	return nil
}

func (s *Service) acquireTargetCapacity(ctx context.Context, targetProfile string) (func(), error) {
	_, _, gate := s.resolveTargetBinding(targetProfile)
	release, err := gate.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	for {
		backlog, err := s.queue.CleanupBacklog(ctx)
		if err != nil {
			release()
			return nil, err
		}
		if backlog == 0 {
			return release, nil
		}
		if !sleepContext(ctx, 2*time.Second) {
			release()
			return nil, ctx.Err()
		}
	}
}

func (s *Service) maintainLease(ctx context.Context, cancel context.CancelCauseFunc, offer protocol.JobOffer) {
	initialRemaining := time.Until(offer.LeaseUntil)
	if initialRemaining <= 0 {
		cancel(errors.New("cluster job lease expired"))
		return
	}
	leaseWindow := initialRemaining
	if leaseWindow > 25*time.Minute {
		leaseWindow = 25 * time.Minute
	}
	renewEvery := leaseWindow / 3
	if renewEvery < 5*time.Second {
		renewEvery = 5 * time.Second
	}
	ticker := time.NewTicker(renewEvery)
	defer ticker.Stop()
	expires := time.NewTimer(initialRemaining)
	defer expires.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-expires.C:
			cancel(errors.New("cluster job lease expired before renewal"))
			return
		case <-ticker.C:
			requestedUntil := time.Now().UTC().Add(leaseWindow)
			if err := s.sendLeaseRenew(ctx, offer, requestedUntil); err != nil {
				cancel(fmt.Errorf("renew cluster job lease: %w", err))
				return
			}
			if !expires.Stop() {
				select {
				case <-expires.C:
				default:
				}
			}
			expires.Reset(time.Until(requestedUntil))
		}
	}
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

// CancelActive cancels every task bound to the current Worker connection.
// Runtime calls this when the WebSocket disconnects so stale leased work
// cannot continue consuming bandwidth after reconnect or reassignment.
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

func clusterTaskNamespace(jobID, mediaItemID string) string {
	return path.Join(".openlist-cluster", safeClusterPathSegment(jobID), safeClusterPathSegment(mediaItemID))
}

func NewCleanupRequest(manifest protocol.UploadETFManifest, storageMountPath string) (resultqueue.CleanupRequest, error) {
	targetRoot := strings.TrimSpace(manifest.WorkerTargetRoot)
	if targetRoot == "" {
		targetRoot = manifest.TargetProfile
	}
	request := resultqueue.CleanupRequest{
		Version:          "v1",
		JobID:            safeClusterPathSegment(manifest.JobID),
		MediaItemID:      safeClusterPathSegment(manifest.MediaItemID),
		OpenListPath:     path.Join(targetRoot, clusterTaskNamespace(manifest.JobID, manifest.MediaItemID), manifest.Name),
		StorageMountPath: path.Clean(storageMountPath),
		RemoteFileID:     manifest.RemoteFileID,
		Name:             manifest.Name,
		EmptyRecycleBin:  true,
		CreatedAt:        time.Now().UTC(),
	}
	if err := request.Validate(); err != nil {
		return resultqueue.CleanupRequest{}, err
	}
	return request, nil
}

func NewSourceCleanupTarget(ctx context.Context, manifest protocol.UploadETFManifest, sourcePath string) (resultqueue.CleanupTarget, error) {
	storage, _, err := op.GetStorageAndActualPath(sourcePath)
	if err != nil {
		return resultqueue.CleanupTarget{}, fmt.Errorf("resolve cluster source cleanup storage: %w", err)
	}
	target := resultqueue.CleanupTarget{
		OpenListPath: path.Clean(sourcePath), StorageMountPath: path.Clean(storage.GetStorage().MountPath),
		Name: path.Base(sourcePath), EmptyRecycleBin: false,
	}
	if obj, getErr := fs.Get(ctx, sourcePath, &fs.GetArgs{NoLog: true}); getErr == nil && obj != nil {
		target.RemoteFileID = obj.GetID()
	}
	probe := resultqueue.CleanupRequest{
		Version: "v1", JobID: safeClusterPathSegment(manifest.JobID), MediaItemID: safeClusterPathSegment(manifest.MediaItemID),
		OpenListPath: target.OpenListPath, StorageMountPath: target.StorageMountPath,
		RemoteFileID: target.RemoteFileID, Name: target.Name, CreatedAt: time.Now().UTC(),
	}
	if err := probe.Validate(); err != nil {
		return resultqueue.CleanupTarget{}, err
	}
	return target, nil
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

func (s *Service) executeMediaTransfer(ctx context.Context, offer protocol.JobOffer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if offer.JobType != "media.transfer" {
		return fmt.Errorf("unsupported cluster job type %q", offer.JobType)
	}
	targetProfileRef := strings.TrimSpace(offer.TaskContext.TargetProfile)
	if targetProfileRef == "" || targetProfileRef == "/" {
		return errors.New("cluster target profile must be a mounted destination path")
	}
	targetProfile, _, _ := s.resolveTargetBinding(targetProfileRef)
	targetStorage, _, err := op.GetStorageAndActualPath(targetProfile)
	if err != nil {
		return fmt.Errorf("resolve cluster target profile: %w", err)
	}
	if !strings.Contains(strings.ToLower(targetStorage.GetStorage().Driver), "139") {
		return errors.New("cluster media target must use a 139 driver with ETF upload support")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	primary := primarySourceObject(offer.TaskContext.SourceObjects)
	if primary.SourceFileID == "" {
		return errors.New("cluster media task has no source object")
	}
	namespace := clusterTaskNamespace(offer.JobID, offer.TaskContext.MediaItemID)
	if _, err := s.requestStagePermit(ctx, offer, model.ClusterStageSavingShare); err != nil {
		return err
	}
	requestedTempRoot := namespace
	if configuredRoot := s.providerTempRoot(offer.TaskContext.Share.Provider); configuredRoot != "" {
		requestedTempRoot = path.Join(configuredRoot, namespace)
	}
	releaseDownload, err := s.acquireDownloadCapacity(ctx)
	if err != nil {
		return err
	}
	saved, err := subscription.SaveClusterShareSelection(ctx, offer.TaskContext.Share.URL, offer.TaskContext.Share.Passcode, requestedTempRoot, []string{primary.SourceFileID})
	releaseDownload()
	if err != nil {
		return fmt.Errorf("save cluster share selection: %w", err)
	}
	if len(saved) != 1 {
		return fmt.Errorf("cluster media task saved %d files, want 1", len(saved))
	}
	stagedSource := saved[0]
	if err := ctx.Err(); err != nil {
		return err
	}
	releaseUpload, err := s.acquireUploadCapacity(ctx)
	if err != nil {
		return err
	}
	defer releaseUpload()
	targetRoot := path.Join(targetProfile, namespace)
	if err := fs.MakeDir(ctx, targetRoot); err != nil {
		return fmt.Errorf("create cluster mobile target: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	uploadPermit, err := s.requestStagePermit(ctx, offer, model.ClusterStageUploadingMobile)
	if err != nil {
		return err
	}
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
		WorkerTargetRoot:      targetProfile,
		Subscription:          offer.TaskContext.Subscription,
		Share:                 offer.TaskContext.Share,
		Media:                 offer.TaskContext.Media,
		SourceObjects:         offer.TaskContext.SourceObjects,
		MobileAccountBinding:  targetStorage.GetStorage().MountPath,
	}
	sourceCleanup, err := NewSourceCleanupTarget(ctx, manifest, stagedSource)
	if err != nil {
		return fmt.Errorf("build cluster source cleanup request: %w", err)
	}
	targetFilePath := path.Join(targetRoot, path.Base(stagedSource))
	existing, getErr := fs.Get(ctx, targetFilePath, &fs.GetArgs{NoLog: true})
	if getErr != nil && !errs.IsNotFoundError(getErr) {
		return fmt.Errorf("inspect cluster upload reconciliation target: %w", getErr)
	}
	if getErr == nil && existing != nil && !existing.IsDir() {
		existingSHA256 := strings.ToUpper(strings.TrimSpace(existing.GetHash().GetHash(utils.SHA256)))
		if existingSHA256 == "" {
			return errors.New("cluster target already contains an owned media object without SHA256 metadata; manual reconciliation is required")
		}
		expectedSHA256 := strings.ToUpper(strings.TrimSpace(primary.Hash))
		if decoded, decodeErr := hex.DecodeString(expectedSHA256); decodeErr != nil || len(decoded) != sha256.Size || primary.Size <= 0 {
			return errors.New("cluster source object lacks a trusted size/SHA256 fingerprint; existing target requires manual reconciliation")
		}
		expectedName := path.Base(primary.SourceRelativePath)
		if expectedName == "." || expectedName == "" {
			expectedName = path.Base(stagedSource)
		}
		if existing.GetName() != expectedName || existing.GetSize() != primary.Size || !strings.EqualFold(existingSHA256, expectedSHA256) {
			return errors.New("cluster target object does not match the source name, size, and SHA256; refusing automatic adoption")
		}
		manifest.Name = existing.GetName()
		manifest.Size = existing.GetSize()
		manifest.SHA256 = existingSHA256
		manifest.HashSource = "remote_object_metadata"
		manifest.RemoteFileID = existing.GetID()
		manifest.RemotePath = targetFilePath
		manifest.UploadReceipt = existing.GetID()
		cleanup, cleanupErr := NewCleanupRequest(manifest, targetStorage.GetStorage().MountPath)
		if cleanupErr != nil {
			return cleanupErr
		}
		cleanup.AdditionalTargets = append(cleanup.AdditionalTargets, sourceCleanup)
		if _, enqueueErr := s.EnqueueThenCleanup(ctx, manifest, cleanup); enqueueErr != nil {
			return fmt.Errorf("reconcile existing cluster upload: %w", enqueueErr)
		}
		return nil
	}
	taskCtx := WithUploadManifest(ctx, manifest)
	taskCtx = WithAdditionalCleanupTargets(taskCtx, sourceCleanup)
	taskCtx = context.WithValue(taskCtx, conf.NoTaskKey, struct{}{})
	taskCtx = context.WithValue(taskCtx, conf.ForceTaskKey, struct{}{})
	if _, err := fs.Copy(taskCtx, stagedSource, targetRoot, true); err != nil {
		return fmt.Errorf("transfer cluster media: %w", err)
	}
	return nil
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

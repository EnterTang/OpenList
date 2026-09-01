package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
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
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
	"github.com/OpenListTeam/OpenList/v4/internal/task_group"
	"github.com/OpenListTeam/OpenList/v4/pkg/qbittorrent"
	"github.com/OpenListTeam/OpenList/v4/pkg/singleflight"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/tache"
	"github.com/shirou/gopsutil/v4/disk"
	log "github.com/sirupsen/logrus"
)

type Sender interface {
	Send(context.Context, protocol.Envelope) error
}

type clusterErrorCoder interface {
	ClusterErrorCode() string
}

// postPluginHashFileStreamer preserves the content hash computed from the
// exact bytes that will be uploaded. Some upload drivers compute a hash
// internally but expose only an error from Put, so that value cannot be
// recovered from the upload call itself.
type postPluginHashFileStreamer struct {
	model.FileStreamer
	hashInfo utils.HashInfo
}

func (s *postPluginHashFileStreamer) GetHash() utils.HashInfo {
	return s.hashInfo
}

func annotatePostPluginSHA256(file model.FileStreamer) (model.FileStreamer, string, error) {
	_, hash, err := stream.CacheFullAndHash(file, nil, utils.SHA256)
	if err != nil {
		return file, "", err
	}
	hash = strings.ToUpper(strings.TrimSpace(hash))
	if hash == "" {
		return file, "", errors.New("processed qB file has no SHA256")
	}
	return &postPluginHashFileStreamer{
		FileStreamer: file,
		hashInfo:     utils.NewHashInfo(utils.SHA256, hash),
	}, hash, nil
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

const sourceCleanupLookupAttempts = 3

const moviePilotStagingCapacityCheckInterval = time.Minute

const shareSaveBatchCollisionKeyPrefix = "share-save-batch-collision:"

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

func getFreshUploadedObject(ctx context.Context, expectedPath string) (model.Obj, error) {
	storage, actualPath, err := getCleanupStorageAndActualPath(expectedPath)
	if err != nil {
		return nil, err
	}
	return getCleanupObject(ctx, storage, actualPath)
}

type activeTask struct {
	attempt         protocol.AttemptRef
	offer           protocol.JobOffer
	eventSeq        uint64
	stage           string
	stageStatus     string
	completedBytes  int64
	totalBytes      int64
	bytesPerSecond  int64
	progressMessage string
	progressAt      time.Time
	ctx             context.Context
	cancel          context.CancelCauseFunc
	stagingMount    string
	deliveryMount   string
	capacityRelease func()
}

func (t *activeTask) releaseCapacity() {
	if t == nil || t.capacityRelease == nil {
		return
	}
	t.capacityRelease()
}

type Service struct {
	queue  resultQueue
	sender Sender

	mu      sync.Mutex
	pending map[string]resultqueue.Result
	active  map[string]*activeTask
	control map[string]chan error
	permits map[string]chan protocol.StagePermit

	controlNodeID          string
	controlKeys            *secure.KeyPair
	storageOperator        StorageOperator
	desiredConfig          protocol.WorkerDesiredConfig
	configObserved         observedState
	storageObserved        map[string]observedState
	qbSecrets              map[string]map[string]any
	qbHealth               map[string]string
	observedRevision       uint64
	downloadGate           *limitGate
	uploadGate             *limitGate
	moviePilotUploadGate   *limitGate
	targetGates            map[string]*limitGate
	qbClientFactory        func(protocol.QBClientConfig) (qbittorrent.Client, error)
	inventoryRefresh       func(context.Context) error
	stagingFreeSpace       func(context.Context, string) (uint64, error)
	downloadFreeSpace      func(context.Context, string) (uint64, error)
	stagingReservationMu   sync.Mutex
	stagingReservations    map[string]int64
	moviePilotTorrents     map[string]moviePilotTorrentRegistryEntry
	capacityPausedTorrents map[string]qbCapacityPauseEntry
	mediaTransferBoundary  func(context.Context, protocol.JobOffer, resolvedMediaTransferTargets) error
	shareSaveSaver         func(context.Context, string, string, string, []string) ([]string, error)
	shareSaveBatchSaver    func(context.Context, string, string, string, []string) ([]string, error)
	stagedSourceFinder     func(context.Context, string, protocol.SourceObject) (string, bool)
	shareSaveFlights       singleflight.Group[mediaTransferShareSaveBatchResult]
	shareSaveFlightMu      sync.Mutex
	shareSaveFlightCalls   map[string]int
	shareSaveFlightJoined  func(string)
}

// SetInventoryRefresh installs the runtime callback used to publish an
// immediate capability snapshot after a control-plane config apply. This is
// needed because qB health is discovered from credentials held only in memory.
func (s *Service) SetInventoryRefresh(refresh func(context.Context) error) {
	s.mu.Lock()
	s.inventoryRefresh = refresh
	s.mu.Unlock()
}

func (s *Service) refreshInventory(ctx context.Context) {
	s.mu.Lock()
	refresh := s.inventoryRefresh
	s.mu.Unlock()
	if refresh == nil {
		return
	}
	if err := refresh(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, transport.ErrNotConnected) {
		log.Warnf("cluster worker inventory refresh after config apply failed: %v", err)
	}
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
		qbSecrets: make(map[string]map[string]any), qbHealth: make(map[string]string),
		downloadGate: newLimitGate(concurrency), uploadGate: newLimitGate(concurrency), moviePilotUploadGate: newLimitGate(moviePilotDefaultUploadConcurrency), targetGates: make(map[string]*limitGate),
		// Leave the factory unset so discoverTorrentClient can select the
		// Worker-local credential-aware constructor. Tests and deployments that
		// need a custom qB client may still inject an explicit factory.
		qbClientFactory: nil,
		shareSaveSaver:  subscription.SaveClusterShareSelection, shareSaveBatchSaver: subscription.SaveClusterShareSelectionBatch, stagedSourceFinder: findExistingStagedSource,
		stagingFreeSpace: func(ctx context.Context, root string) (uint64, error) {
			usage, err := disk.UsageWithContext(ctx, root)
			if err != nil {
				return 0, err
			}
			return usage.Free, nil
		},
		downloadFreeSpace: func(ctx context.Context, root string) (uint64, error) {
			usage, err := disk.UsageWithContext(ctx, root)
			if err != nil {
				return 0, err
			}
			return usage.Free, nil
		},
		stagingReservations:    make(map[string]int64),
		moviePilotTorrents:     make(map[string]moviePilotTorrentRegistryEntry),
		capacityPausedTorrents: make(map[string]qbCapacityPauseEntry),
	}
}

// reserveMoviePilotStaging reserves only the bytes needed by an in-flight
// qB-to-staging copy. The reservation is released immediately after the copy
// finishes because the actual staged file then accounts for its own disk
// usage. Keeping the reservation while copying closes the race between two
// upload workers that both observe the same free-space value.
func (s *Service) reserveMoviePilotStaging(ctx context.Context, stagingRoot string, bytes int64) (func(), error) {
	if bytes <= 0 {
		return func() {}, nil
	}
	root := filepath.Clean(strings.TrimSpace(stagingRoot))
	if root == "." || root == "" {
		return nil, errors.New("qB staging root is required")
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create qB staging root: %w", err)
	}
	usage := s.stagingFreeSpace
	s.mu.Lock()
	safetyReserve := stagingSafetyReserveBytes(s.desiredConfig.Staging)
	s.mu.Unlock()
	if usage == nil {
		usage = func(ctx context.Context, root string) (uint64, error) {
			value, err := disk.UsageWithContext(ctx, root)
			if err != nil {
				return 0, err
			}
			return value.Free, nil
		}
	}
	s.stagingReservationMu.Lock()
	defer s.stagingReservationMu.Unlock()
	free, err := usage(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("inspect qB staging free space: %w", err)
	}
	reserved := s.stagingReservations[root]
	if safetyReserve < 0 || reserved < 0 || uint64(bytes) > free || uint64(reserved) > free-uint64(bytes) || uint64(safetyReserve) > free-uint64(bytes)-uint64(reserved) {
		return nil, fmt.Errorf("%w: free=%d reserved=%d required=%d safety_reserve=%d", ErrQBStagingInsufficientSpace, free, reserved, bytes, safetyReserve)
	}
	s.stagingReservations[root] = reserved + bytes
	var once sync.Once
	return func() {
		once.Do(func() {
			s.stagingReservationMu.Lock()
			defer s.stagingReservationMu.Unlock()
			remaining := s.stagingReservations[root] - bytes
			if remaining <= 0 {
				delete(s.stagingReservations, root)
			} else {
				s.stagingReservations[root] = remaining
			}
		})
	}, nil
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
	if strings.TrimSpace(target.LocalPath) != "" {
		return resultqueue.ExecuteLocalCleanupTarget(ctx, target)
	}
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
		return s.sendJobReject(ctx, offer, "worker_lease_expired", "cluster job lease has already expired", true)
	}
	if err := s.queue.ValidateDurability(ctx); err != nil {
		return s.sendJobReject(ctx, offer, "worker_queue_unavailable", err.Error(), true)
	}
	if err := s.cleanupBacklogBlocksOffer(ctx, offer); err != nil {
		return s.sendJobReject(ctx, offer, "worker_cleanup_backlog", err.Error(), true)
	}
	attemptKey := executionAttemptKey(offer.AttemptRef)
	claimed, err := s.queue.ClaimAttempt(ctx, attemptKey, 7*24*time.Hour)
	if err != nil {
		return s.sendJobReject(ctx, offer, "worker_journal_unavailable", err.Error(), true)
	}
	if !claimed {
		s.mu.Lock()
		running, active := s.active[offer.JobID]
		s.mu.Unlock()
		if active && running != nil && sameAttempt(running.attempt, offer.AttemptRef) {
			return s.sendJobAccept(ctx, offer)
		}
		// The durable claim can survive a Worker restart. Do not acknowledge a
		// replay unless this process still owns the active execution; the
		// coordinator will retry the offer or recover it after the lease ends.
		return fmt.Errorf("cluster attempt %s is already claimed without an active execution", offer.AttemptRef.AttemptID)
	}
	if offer.TaskContext.Torrent != nil {
		if err := s.rememberMoviePilotTorrentWithSubscription(ctx, offer.TaskContext.Torrent, offer.TaskContext.Subscription); err != nil {
			_ = s.queue.ReleaseAttempt(context.WithoutCancel(ctx), attemptKey)
			return s.sendJobReject(ctx, offer, "worker_torrent_registry_unavailable", err.Error(), true)
		}
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
			_ = s.queue.ReleaseAttempt(context.WithoutCancel(ctx), attemptKey)
			return fmt.Errorf("cluster job %s generation %d is already active", offer.JobID, running.attempt.Generation)
		}
		running.cancel(errors.New("cluster job superseded by a newer generation"))
	}
	s.mu.Unlock()

	if offer.JobType == model.ClusterJobTypeMediaTransfer {
		acquire := s.tryAcquireMediaCapacity
		capacityName := "worker media"
		if offer.TaskContext.Torrent != nil {
			acquire = s.tryAcquireMoviePilotUploadCapacity
			capacityName = "MoviePilot qB upload"
		}
		release, ok := acquire()
		if !ok {
			active, limit := s.downloadGate.Snapshot()
			if offer.TaskContext.Torrent != nil && s.moviePilotUploadGate != nil {
				active, limit = s.moviePilotUploadGate.Snapshot()
			}
			log.Warnf("cluster job %s admission rejected attempt=%s generation=%d code=worker_capacity_unavailable capacity=%s active=%d limit=%d", offer.JobID, offer.AttemptID, offer.Generation, capacityName, active, limit)
			cancelCause(nil)
			_ = s.queue.ReleaseAttempt(context.WithoutCancel(ctx), attemptKey)
			return s.sendJobReject(ctx, offer, "worker_capacity_unavailable", capacityName+" concurrency limit is full", true)
		}
		var releaseOnce sync.Once
		current.capacityRelease = func() { releaseOnce.Do(release) }
	}

	s.mu.Lock()
	if running, exists := s.active[offer.JobID]; exists {
		if sameAttempt(running.attempt, offer.AttemptRef) {
			s.mu.Unlock()
			current.releaseCapacity()
			cancelCause(nil)
			if running.ctx.Err() != nil {
				return fmt.Errorf("cluster job %s previous execution is still stopping", offer.JobID)
			}
			return s.sendJobAccept(ctx, offer)
		}
		if offer.Generation <= running.attempt.Generation {
			s.mu.Unlock()
			current.releaseCapacity()
			cancelCause(nil)
			_ = s.queue.ReleaseAttempt(context.WithoutCancel(ctx), attemptKey)
			return fmt.Errorf("cluster job %s generation %d is already active", offer.JobID, running.attempt.Generation)
		}
	}
	s.active[offer.JobID] = current
	s.mu.Unlock()
	if err := s.sendJobAccept(ctx, offer); err != nil {
		_ = s.queue.ReleaseAttempt(context.WithoutCancel(ctx), attemptKey)
		s.finishActive(offer.JobID, current)
		current.releaseCapacity()
		cancelCause(err)
		return err
	}
	go func() {
		defer cancelCause(nil)
		defer s.finishActive(offer.JobID, current)
		defer current.releaseCapacity()
		go s.maintainLease(jobCtx, cancelCause, offer)

		var result map[string]any
		var err error
		if offer.JobType == "share.inspect" {
			result, err = s.executeShareInspect(jobCtx, offer)
		} else if offer.JobType == model.ClusterJobTypeTorrentObserve {
			result, err = s.executeTorrentObserve(jobCtx, offer)
		} else if offer.JobType == model.ClusterJobTypeTorrentRetention {
			result, err = s.executeTorrentRetention(jobCtx, offer)
		} else if offer.JobType == "media.transfer" && offer.TaskContext.DeliveryMode == model.SubscriptionDeliveryModeDirectDownload {
			result, err = s.executeMediaDirectFirst(jobCtx, offer)
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

func (s *Service) executeTorrentObserve(ctx context.Context, offer protocol.JobOffer) (map[string]any, error) {
	if offer.JobType != model.ClusterJobTypeTorrentObserve || offer.TaskContext.Torrent == nil {
		return nil, fmt.Errorf("unsupported torrent observation offer")
	}
	s.reportStageStatus(ctx, offer, model.ClusterStageQBObserving, model.ClusterStageStatusRunning, "")
	_, client, _, err := s.discoverTorrentClient(offer.TaskContext.Torrent)
	if err != nil {
		s.reportStageStatus(ctx, offer, model.ClusterStageQBObserving, model.ClusterStageStatusFailed, err.Error())
		return nil, err
	}
	for {
		info, queryErr := client.GetTorrentByHash(ctx, offer.TaskContext.Torrent.TorrentHash)
		if queryErr != nil {
			s.reportStageStatus(ctx, offer, model.ClusterStageQBObserving, model.ClusterStageStatusFailed, queryErr.Error())
			return nil, fmt.Errorf("query qB torrent %q: %w", offer.TaskContext.Torrent.TorrentHash, queryErr)
		}
		completedBytes, totalBytes := torrentProgressBytes(info)
		if progressErr := s.sendJobProgress(ctx, offer, model.ClusterStageQBObserving, completedBytes, totalBytes, int64(info.Dlspeed), string(info.State)); progressErr != nil {
			log.Warnf("cluster job %s qB progress notify failed: %v", offer.JobID, progressErr)
		}
		if info.Progress >= 0.999999 && info.AmountLeft <= 0 {
			files, discoverErr := s.DiscoverTorrentFiles(ctx, offer.TaskContext.Torrent)
			if discoverErr != nil {
				s.reportStageStatus(ctx, offer, model.ClusterStageQBObserving, model.ClusterStageStatusFailed, discoverErr.Error())
				return nil, discoverErr
			}
			s.reportStageStatus(ctx, offer, model.ClusterStageQBObserving, model.ClusterStageStatusSucceeded, "")
			return map[string]any{
				"files":           files,
				"torrent_hash":    offer.TaskContext.Torrent.TorrentHash,
				"content_path":    info.ContentPath,
				"qb_state":        string(info.State),
				"progress":        info.Progress,
				"ratio":           info.Ratio,
				"seeding_seconds": int64(info.SeedingTime),
				"observed_at":     time.Now().UTC(),
			}, nil
		}
		timer := time.NewTimer(10 * time.Second)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func torrentProgressBytes(info qbittorrent.TorrentInfo) (int64, int64) {
	total := info.TotalSize
	if total <= 0 {
		total = info.Size
	}
	if total <= 0 && info.Completed > 0 {
		total = info.Completed + max(info.AmountLeft, 0)
	}
	completed := info.Completed
	if completed <= 0 && total > 0 {
		completed = total - max(info.AmountLeft, 0)
	}
	if completed < 0 {
		completed = 0
	}
	if total > 0 && completed > total {
		completed = total
	}
	return completed, total
}

func (s *Service) executeTorrentRetention(ctx context.Context, offer protocol.JobOffer) (map[string]any, error) {
	if offer.JobType != model.ClusterJobTypeTorrentRetention || offer.TaskContext.Torrent == nil {
		return nil, fmt.Errorf("unsupported torrent retention offer")
	}
	torrent := offer.TaskContext.Torrent
	_, client, _, err := s.discoverTorrentClient(torrent)
	if err != nil {
		return nil, err
	}
	action := strings.ToLower(strings.TrimSpace(torrent.Action))
	if action == "" {
		action = "delete"
	}
	if action == "delete" {
		s.reportStageStatus(ctx, offer, model.ClusterStageQBDeleting, model.ClusterStageStatusRunning, "")
		_, queryErr := client.GetTorrentByHash(ctx, torrent.TorrentHash)
		if isQBTorrentMissing(queryErr) {
			s.forgetMoviePilotTorrent(ctx, torrent)
			s.reportStageStatus(ctx, offer, model.ClusterStageQBDeleting, model.ClusterStageStatusSucceeded, "")
			return map[string]any{"action": action, "torrent_hash": torrent.TorrentHash, "completed_at": time.Now().UTC()}, nil
		}
		if queryErr != nil {
			s.reportStageStatus(ctx, offer, model.ClusterStageQBDeleting, model.ClusterStageStatusFailed, queryErr.Error())
			return nil, fmt.Errorf("query qB torrent before deletion: %w", queryErr)
		}
		if err = client.StopByHash(ctx, torrent.TorrentHash); err != nil {
			s.reportStageStatus(ctx, offer, model.ClusterStageQBDeleting, model.ClusterStageStatusFailed, err.Error())
			return nil, fmt.Errorf("stop qB torrent before deletion: %w", err)
		}
		err = client.DeleteByHash(ctx, torrent.TorrentHash, true)
		if err != nil {
			s.reportStageStatus(ctx, offer, model.ClusterStageQBDeleting, model.ClusterStageStatusFailed, err.Error())
			return nil, err
		}
		_, queryErr = client.GetTorrentByHash(ctx, torrent.TorrentHash)
		if queryErr == nil {
			err = fmt.Errorf("qB torrent %q still exists after deletion", torrent.TorrentHash)
			s.reportStageStatus(ctx, offer, model.ClusterStageQBDeleting, model.ClusterStageStatusFailed, err.Error())
			return nil, err
		}
		if !isQBTorrentMissing(queryErr) {
			err = fmt.Errorf("confirm qB torrent deletion: %w", queryErr)
			s.reportStageStatus(ctx, offer, model.ClusterStageQBDeleting, model.ClusterStageStatusFailed, err.Error())
			return nil, err
		}
		s.forgetMoviePilotTorrent(ctx, torrent)
		s.reportStageStatus(ctx, offer, model.ClusterStageQBDeleting, model.ClusterStageStatusSucceeded, "")
	} else if action == "inspect" {
		s.reportStageStatus(ctx, offer, model.ClusterStageRetentionCheck, model.ClusterStageStatusRunning, "")
		info, queryErr := client.GetTorrentByHash(ctx, torrent.TorrentHash)
		if queryErr != nil {
			s.reportStageStatus(ctx, offer, model.ClusterStageRetentionCheck, model.ClusterStageStatusFailed, queryErr.Error())
			return nil, fmt.Errorf("inspect qB torrent retention state: %w", queryErr)
		}
		s.reportStageStatus(ctx, offer, model.ClusterStageRetentionCheck, model.ClusterStageStatusSucceeded, "")
		return map[string]any{
			"action": action, "torrent_hash": torrent.TorrentHash, "qb_state": string(info.State),
			"progress": info.Progress, "ratio": info.Ratio, "seeding_seconds": int64(info.SeedingTime),
			"observed_at": time.Now().UTC(),
		}, nil
	} else if action == "pause" {
		err = client.StopByHash(ctx, torrent.TorrentHash)
	} else if action == "resume" {
		err = client.StartByHash(ctx, torrent.TorrentHash)
	} else {
		return nil, fmt.Errorf("unsupported torrent retention action %q", action)
	}
	if err != nil {
		return nil, err
	}
	return map[string]any{"action": action, "torrent_hash": torrent.TorrentHash, "completed_at": time.Now().UTC()}, nil
}

func isQBTorrentMissing(err error) bool {
	var notFound qbittorrent.InfoNotFoundError
	return errors.As(err, &notFound)
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
		LastEventSeq:   s.currentJobEventSeq(offer.JobID),
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

func (s *Service) sendJobProgress(ctx context.Context, offer protocol.JobOffer, stage string, completedBytes, totalBytes, bytesPerSecond int64, progressMessage string) error {
	if s.sender == nil {
		return errors.New("cluster sender is unavailable")
	}
	if strings.TrimSpace(stage) == "" {
		return errors.New("cluster progress stage is required")
	}
	if completedBytes < 0 || totalBytes < 0 || (totalBytes > 0 && completedBytes > totalBytes) {
		return errors.New("cluster progress byte counts are invalid")
	}
	eventSeq, ok := s.nextJobEventSeq(offer.JobID)
	if !ok {
		return fmt.Errorf("cluster job %s is not active", offer.JobID)
	}
	s.mu.Lock()
	if active := s.active[offer.JobID]; active != nil && sameAttempt(active.attempt, offer.AttemptRef) {
		active.stage = strings.TrimSpace(stage)
		active.stageStatus = model.ClusterStageStatusRunning
		active.completedBytes = completedBytes
		active.totalBytes = totalBytes
		active.bytesPerSecond = bytesPerSecond
		active.progressMessage = strings.TrimSpace(progressMessage)
		active.progressAt = time.Now().UTC()
	}
	s.mu.Unlock()
	if offer.TaskContext.Torrent != nil {
		s.syncMoviePilotTorrentStatus(ctx, offer.TaskContext.Torrent, offer.JobID, stage, model.ClusterStageStatusRunning, completedBytes, totalBytes, progressMessage, "", "", "", totalBytes > 0 && completedBytes >= totalBytes)
	}
	payload := protocol.JobProgress{
		AttemptRef:     offer.AttemptRef,
		Stage:          strings.TrimSpace(stage),
		EventSeq:       eventSeq,
		CompletedBytes: completedBytes,
		TotalBytes:     totalBytes,
		BytesPerSecond: bytesPerSecond,
		ObservedAt:     time.Now().UTC(),
		Message:        strings.TrimSpace(progressMessage),
	}
	message, err := protocol.NewEnvelope(protocol.MessageJobProgress, payload)
	if err != nil {
		return err
	}
	return s.sender.Send(ctx, *message)
}

func (s *Service) nextJobEventSeq(jobID string) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	active, ok := s.active[jobID]
	if !ok || active == nil {
		return 0, false
	}
	active.eventSeq++
	return active.eventSeq, true
}

func (s *Service) currentJobEventSeq(jobID string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if active := s.active[jobID]; active != nil {
		return active.eventSeq
	}
	return 0
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
	s.ResumeMoviePilotTorrents(ctx)
	s.ReconcileMoviePilotStagingCapacity(ctx)
	s.ReconcileQBDiskCapacity(ctx)
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

// PauseMoviePilotTorrents stops qB torrents referenced by active cluster jobs
// before the Worker goes offline. The qB files remain untouched; the
// Coordinator can safely redispatch the transfer after reconnection.
func (s *Service) PauseMoviePilotTorrents(ctx context.Context) {
	s.mu.Lock()
	torrents := make(map[string]protocol.TorrentTaskContext, len(s.moviePilotTorrents)+len(s.active))
	for key, entry := range s.moviePilotTorrents {
		torrents[key] = entry.Torrent
	}
	for _, task := range s.active {
		if task == nil || task.offer.TaskContext.Torrent == nil {
			continue
		}
		torrent := *task.offer.TaskContext.Torrent
		torrents[moviePilotTorrentRegistryKey(&torrent)] = torrent
	}
	s.mu.Unlock()
	for _, torrentValue := range torrents {
		torrent := torrentValue
		_, client, _, err := s.discoverTorrentClient(&torrent)
		if err != nil {
			log.Warnf("pause MoviePilot qB torrent %s on disconnect: %v", torrent.TorrentHash, err)
			continue
		}
		pauseCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		info, infoErr := client.GetTorrentByHash(pauseCtx, torrent.TorrentHash)
		if infoErr != nil {
			if isMoviePilotTorrentMissing(infoErr) {
				s.forgetMoviePilotTorrent(ctx, &torrent)
			}
			log.Warnf("inspect MoviePilot qB torrent %s on disconnect: %v", torrent.TorrentHash, infoErr)
			cancel()
			continue
		}
		if info.Progress >= 0.999999 && info.AmountLeft <= 0 {
			cancel()
			continue
		}
		if err := client.StopByHash(pauseCtx, torrent.TorrentHash); err != nil {
			log.Warnf("pause MoviePilot qB torrent %s on disconnect: %v", torrent.TorrentHash, err)
		} else if err := s.setMoviePilotTorrentDisconnectPaused(ctx, &torrent, true); err != nil {
			log.Warnf("persist MoviePilot qB torrent %s disconnect pause: %v", torrent.TorrentHash, err)
		}
		cancel()
	}
}

// ResumeMoviePilotTorrents resumes only torrents paused by the disconnect
// safeguard. Capacity and retention pauses are intentionally left untouched.
func (s *Service) ResumeMoviePilotTorrents(ctx context.Context) {
	s.mu.Lock()
	torrents := make([]protocol.TorrentTaskContext, 0, len(s.moviePilotTorrents))
	for _, entry := range s.moviePilotTorrents {
		if entry.PausedByDisconnect {
			torrents = append(torrents, entry.Torrent)
		}
	}
	s.mu.Unlock()
	for i := range torrents {
		torrent := &torrents[i]
		_, client, _, err := s.discoverTorrentClient(torrent)
		if err != nil {
			log.Warnf("resume MoviePilot qB torrent %s after reconnect: %v", torrent.TorrentHash, err)
			continue
		}
		resumeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		info, infoErr := client.GetTorrentByHash(resumeCtx, torrent.TorrentHash)
		if infoErr != nil {
			if isMoviePilotTorrentMissing(infoErr) {
				s.forgetMoviePilotTorrent(ctx, torrent)
			}
			log.Warnf("inspect MoviePilot qB torrent %s before reconnect resume: %v", torrent.TorrentHash, infoErr)
			cancel()
			continue
		}
		if info.Progress >= 0.999999 && info.AmountLeft <= 0 {
			if err := s.setMoviePilotTorrentDisconnectPaused(ctx, torrent, false); err != nil {
				log.Warnf("clear completed MoviePilot qB torrent %s disconnect pause: %v", torrent.TorrentHash, err)
			}
			cancel()
			continue
		}
		s.mu.Lock()
		pausedByCapacity := s.moviePilotTorrents[moviePilotTorrentRegistryKey(torrent)].PausedByCapacity
		_, globallyPausedByCapacity := s.capacityPausedTorrents[qbCapacityPauseKey(torrent.QBClientID, torrent.TorrentHash)]
		s.mu.Unlock()
		if pausedByCapacity || globallyPausedByCapacity {
			if err := s.setMoviePilotTorrentDisconnectPaused(ctx, torrent, false); err != nil {
				log.Warnf("clear MoviePilot qB torrent %s disconnect pause: %v", torrent.TorrentHash, err)
			}
			cancel()
			continue
		}
		if err := client.StartByHash(resumeCtx, torrent.TorrentHash); err != nil {
			log.Warnf("resume MoviePilot qB torrent %s after reconnect: %v", torrent.TorrentHash, err)
		} else if err := s.setMoviePilotTorrentDisconnectPaused(ctx, torrent, false); err != nil {
			log.Warnf("clear MoviePilot qB torrent %s disconnect pause: %v", torrent.TorrentHash, err)
		}
		cancel()
	}
}

// ReconcileMoviePilotStagingCapacity applies the configured low/high
// watermarks to known incomplete MoviePilot torrents. It records capacity as a
// distinct pause cause so reconnect recovery never restarts a torrent before
// staging space has recovered past the high watermark.
func (s *Service) ReconcileMoviePilotStagingCapacity(ctx context.Context) {
	s.mu.Lock()
	staging := s.desiredConfig.Staging
	entries := make([]moviePilotTorrentRegistryEntry, 0, len(s.moviePilotTorrents))
	for _, entry := range s.moviePilotTorrents {
		entries = append(entries, entry)
	}
	s.mu.Unlock()
	root := strings.TrimSpace(staging.Root)
	low := gigabytesToBytes(staging.StagingPauseDownloadWatermarkGB)
	high := gigabytesToBytes(staging.StagingResumeDownloadWatermarkGB)
	if root == "" || low <= 0 || high <= 0 {
		return
	}
	usage := s.stagingFreeSpace
	if usage == nil {
		usage = func(ctx context.Context, root string) (uint64, error) {
			value, err := disk.UsageWithContext(ctx, root)
			if err != nil {
				return 0, err
			}
			return value.Free, nil
		}
	}
	free, err := usage(ctx, root)
	if err != nil {
		log.Warnf("inspect MoviePilot staging capacity: %v", err)
		return
	}
	if free <= uint64(low) {
		for i := range entries {
			entry := entries[i]
			if entry.PausedByCapacity {
				continue
			}
			s.pauseMoviePilotTorrentForCapacity(ctx, &entry.Torrent)
		}
		return
	}
	if free < uint64(high) {
		return
	}
	for i := range entries {
		entry := entries[i]
		if !entry.PausedByCapacity || entry.PausedByDisconnect {
			continue
		}
		s.resumeMoviePilotTorrentForCapacity(ctx, &entry.Torrent)
	}
}

// RunMoviePilotStagingCapacityMonitor keeps the low/high watermark policy
// effective when free space changes outside an upload attempt, such as after
// an operator frees disk space or a cleanup job completes.
func (s *Service) RunMoviePilotStagingCapacityMonitor(ctx context.Context) error {
	for {
		s.ReconcileMoviePilotStagingCapacity(ctx)
		s.ReconcileQBDiskCapacity(ctx)
		timer := time.NewTimer(moviePilotStagingCapacityCheckInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *Service) pauseMoviePilotTorrentForCapacity(ctx context.Context, torrent *protocol.TorrentTaskContext) {
	_, client, _, err := s.discoverTorrentClient(torrent)
	if err != nil {
		log.Warnf("resolve MoviePilot qB torrent %s for capacity pause: %v", torrent.TorrentHash, err)
		return
	}
	pauseCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	info, err := client.GetTorrentByHash(pauseCtx, torrent.TorrentHash)
	if err != nil {
		if isMoviePilotTorrentMissing(err) {
			s.forgetMoviePilotTorrent(ctx, torrent)
		}
		log.Warnf("inspect MoviePilot qB torrent %s for capacity pause: %v", torrent.TorrentHash, err)
		return
	}
	if info.Progress >= 0.999999 && info.AmountLeft <= 0 {
		return
	}
	if err := client.StopByHash(pauseCtx, torrent.TorrentHash); err != nil {
		log.Warnf("pause MoviePilot qB torrent %s for staging capacity: %v", torrent.TorrentHash, err)
		return
	}
	if err := s.setMoviePilotTorrentCapacityPaused(ctx, torrent, true); err != nil {
		log.Warnf("persist MoviePilot qB torrent %s capacity pause: %v", torrent.TorrentHash, err)
	}
}

func (s *Service) resumeMoviePilotTorrentForCapacity(ctx context.Context, torrent *protocol.TorrentTaskContext) {
	_, client, _, err := s.discoverTorrentClient(torrent)
	if err != nil {
		log.Warnf("resolve MoviePilot qB torrent %s for capacity resume: %v", torrent.TorrentHash, err)
		return
	}
	resumeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	info, err := client.GetTorrentByHash(resumeCtx, torrent.TorrentHash)
	if err != nil {
		if isMoviePilotTorrentMissing(err) {
			s.forgetMoviePilotTorrent(ctx, torrent)
		}
		log.Warnf("inspect MoviePilot qB torrent %s for capacity resume: %v", torrent.TorrentHash, err)
		return
	}
	if info.Progress >= 0.999999 && info.AmountLeft <= 0 {
		if err := s.setMoviePilotTorrentCapacityPaused(ctx, torrent, false); err != nil {
			log.Warnf("clear completed MoviePilot qB torrent %s capacity pause: %v", torrent.TorrentHash, err)
		}
		return
	}
	if err := client.StartByHash(resumeCtx, torrent.TorrentHash); err != nil {
		log.Warnf("resume MoviePilot qB torrent %s after staging capacity recovery: %v", torrent.TorrentHash, err)
		return
	}
	if err := s.setMoviePilotTorrentCapacityPaused(ctx, torrent, false); err != nil {
		log.Warnf("clear MoviePilot qB torrent %s capacity pause: %v", torrent.TorrentHash, err)
	}
}

func isMoviePilotTorrentMissing(err error) bool {
	var missing qbittorrent.InfoNotFoundError
	return errors.As(err, &missing)
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
	obj, err := getSourceCleanupObjectWithRetry(ctx, storage, actualPath)
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

func NewLocalSourceCleanupTarget(ownedRoot, sourcePath string) (resultqueue.CleanupTarget, error) {
	ownedRoot = filepath.Clean(strings.TrimSpace(ownedRoot))
	sourcePath = filepath.Clean(strings.TrimSpace(sourcePath))
	if !filepath.IsAbs(ownedRoot) || ownedRoot == string(filepath.Separator) || !filepath.IsAbs(sourcePath) || filepath.Dir(sourcePath) != ownedRoot {
		return resultqueue.CleanupTarget{}, errors.New("local cluster source cleanup must target a direct file in a non-root staging directory")
	}
	return resultqueue.CleanupTarget{
		LocalPath: sourcePath, OwnedRootPath: ownedRoot, Name: filepath.Base(sourcePath), ExactFile: true,
	}, nil
}

func getSourceCleanupObjectWithRetry(ctx context.Context, storage driver.Driver, actualPath string) (model.Obj, error) {
	var lastErr error
	for attempt := 0; attempt < sourceCleanupLookupAttempts; attempt++ {
		obj, err := getCleanupObject(ctx, storage, actualPath, true)
		if err == nil {
			return obj, nil
		}
		lastErr = err
		if !errs.IsObjectNotFound(err) || attempt+1 == sourceCleanupLookupAttempts {
			return nil, err
		}
		select {
		case <-time.After(cleanupLookupDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
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

// executeMediaTransfer runs after acceptJob has reserved the media slot. The
// direct-download fallback deliberately calls this function with the same
// reservation, so this method must not acquire a second gate slot.
func (s *Service) executeMediaTransfer(ctx context.Context, offer protocol.JobOffer) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if offer.JobType != "media.transfer" {
		return fmt.Errorf("unsupported cluster job type %q", offer.JobType)
	}
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
	isQBTransfer := offer.TaskContext.Torrent != nil
	primary := primarySourceObject(offer.TaskContext.SourceObjects)
	var stagedSource string
	var reused bool
	var stagingTempRoot string
	var qbStagingConfig protocol.StagingConfig
	var qbClient qbittorrent.Client
	if isQBTransfer {
		s.reportStageStatus(ctx, offer, model.ClusterStageQBObserving, model.ClusterStageStatusRunning, "")
		files, discoverErr := s.DiscoverTorrentFiles(ctx, offer.TaskContext.Torrent)
		if discoverErr != nil {
			s.reportStageStatus(ctx, offer, model.ClusterStageQBObserving, model.ClusterStageStatusFailed, discoverErr.Error())
			return discoverErr
		}
		if len(files) != 1 {
			err = fmt.Errorf("torrent transfer requires one selected media file, found %d", len(files))
			s.reportStageStatus(ctx, offer, model.ClusterStageQBObserving, model.ClusterStageStatusFailed, err.Error())
			return err
		}
		file := files[0]
		primary = protocol.SourceObject{
			Provider: "qbittorrent", SourceFileID: "torrent:" + file.Hash + ":" + file.Name,
			SourceRelativePath: file.Name, Size: file.Size,
		}
		s.reportStageStatus(ctx, offer, model.ClusterStageQBObserving, model.ClusterStageStatusSucceeded, "")
		s.mu.Lock()
		stagingConfig := s.desiredConfig.Staging
		s.mu.Unlock()
		qbStagingConfig = stagingConfig
		stagingTempRoot = strings.TrimSpace(stagingConfig.Root)
		if stagingTempRoot == "" {
			return errors.New("MoviePilot qB staging root is not configured")
		}
		_, qbClient, _, err = s.discoverTorrentClient(offer.TaskContext.Torrent)
		if err != nil {
			return fmt.Errorf("resolve qB control client: %w", err)
		}
		releaseStagingReservation, admissionErr := s.reserveMoviePilotStaging(ctx, stagingTempRoot, file.Size)
		if admissionErr != nil {
			if errors.Is(admissionErr, ErrQBStagingInsufficientSpace) && qbClient != nil {
				if pauseErr := qbClient.StopByHash(ctx, offer.TaskContext.Torrent.TorrentHash); pauseErr != nil {
					log.Warnf("pause MoviePilot qB torrent %s after staging space reservation failure: %v", offer.TaskContext.Torrent.TorrentHash, pauseErr)
				} else if pauseErr := s.setMoviePilotTorrentCapacityPaused(ctx, offer.TaskContext.Torrent, true); pauseErr != nil {
					log.Warnf("persist MoviePilot qB torrent %s staging capacity pause: %v", offer.TaskContext.Torrent.TorrentHash, pauseErr)
				}
				s.ReconcileMoviePilotStagingCapacity(ctx)
			}
			s.reportStageStatus(ctx, offer, model.ClusterStageQBCopying, model.ClusterStageStatusFailed, admissionErr.Error())
			return admissionErr
		}
		s.reportStageStatus(ctx, offer, model.ClusterStageQBCopying, model.ClusterStageStatusRunning, "")
		stagedSource, err = CopyQBFileToStaging(ctx, QBSource{
			WorkerPath: file.WorkerPath, DownloadRoot: file.DownloadRoot, Name: file.Name, Size: file.Size,
		}, QBStagingAdmission{
			StagingRoot: stagingTempRoot, DownloadRoot: file.DownloadRoot,
			MaxFileBytes: stagingMaxFileBytes(stagingConfig), ExtensionWhitelist: stagingConfig.ExtensionWhitelist,
		})
		releaseStagingReservation()
		if err != nil {
			if errors.Is(err, ErrQBStagingInsufficientSpace) && qbClient != nil {
				if pauseErr := qbClient.StopByHash(ctx, offer.TaskContext.Torrent.TorrentHash); pauseErr != nil {
					log.Warnf("pause MoviePilot qB torrent %s after staging space failure: %v", offer.TaskContext.Torrent.TorrentHash, pauseErr)
				} else if pauseErr := s.setMoviePilotTorrentCapacityPaused(ctx, offer.TaskContext.Torrent, true); pauseErr != nil {
					log.Warnf("persist MoviePilot qB torrent %s staging capacity pause: %v", offer.TaskContext.Torrent.TorrentHash, pauseErr)
				}
				s.ReconcileMoviePilotStagingCapacity(ctx)
			}
			s.reportStageStatus(ctx, offer, model.ClusterStageQBCopying, model.ClusterStageStatusFailed, err.Error())
			return err
		}
		s.reportStageStatus(ctx, offer, model.ClusterStageQBCopying, model.ClusterStageStatusSucceeded, "")
	} else {
		if primary.SourceFileID == "" {
			return errors.New("cluster media task has no source object")
		}
		if _, err := s.requestStagePermitWithRetry(ctx, offer, model.ClusterStageSavingShare); err != nil {
			return err
		}
		s.reportStageStatus(ctx, offer, model.ClusterStageSavingShare, model.ClusterStageStatusRunning, "")
		requestedTempRoot, resolveErr := s.resolveStagingTempRoot(ctx, offer.TaskContext)
		if resolveErr != nil {
			return fmt.Errorf("resolve cluster staging temp root: %w", resolveErr)
		}
		stagingTempRoot = mediaTransferShareSaveTempRoot(offer.TaskContext, requestedTempRoot)
	}
	if s.mediaTransferBoundary != nil {
		return s.mediaTransferBoundary(ctx, offer, resolvedMediaTransferTargets{
			StagingRoot: stagingTempRoot, DeliveryRoot: targetRootBase, DeliveryMount: targetBindingMount,
		})
	}
	var stagingStorage driver.Driver
	if !isQBTransfer {
		stagingStorage, _, err = op.GetStorageAndActualPath(stagingTempRoot)
		if err != nil {
			return fmt.Errorf("resolve cluster staging account: %w", err)
		}
		s.recordActiveAccountBindings(offer.JobID, stagingStorage.GetStorage().MountPath, targetBindingMount)
		stagedSource, reused, err = s.prepareMediaTransferShareSave(ctx, offer, stagingTempRoot)
		if err != nil {
			s.reportStageStatus(ctx, offer, model.ClusterStageSavingShare, model.ClusterStageStatusFailed, err.Error())
			return err
		}
		s.reportStageStatus(ctx, offer, model.ClusterStageSavingShare, model.ClusterStageStatusSucceeded, "")
		if reused {
			log.Infof("cluster job %s continuing with existing staged source %s", offer.JobID, stagedSource)
		}
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
		DeliveryMode:          offer.TaskContext.DeliveryMode,
		Torrent:               offer.TaskContext.Torrent,
		SourceObjects:         offer.TaskContext.SourceObjects,
		ShareSaveKey:          offer.TaskContext.ShareSaveKey,
		ShareSaveObjects:      offer.TaskContext.ShareSaveObjects,
		MobileAccountBinding:  targetStorage.GetStorage().MountPath,
	}
	var sourceCleanup resultqueue.CleanupTarget
	if isQBTransfer {
		sourceCleanup, err = NewLocalSourceCleanupTarget(stagingTempRoot, stagedSource)
	} else {
		sourceCleanup, err = NewSourceCleanupTarget(ctx, manifest, stagingTempRoot, stagedSource)
	}
	if err != nil {
		return fmt.Errorf("build cluster source cleanup request: %w", err)
	}
	pluginOpts := plugin.ProcessOptions{
		AntiHash:  setting.GetBool(conf.PluginAntiHashEnabled),
		ISORename: setting.GetBool(conf.PluginISORenameEnabled),
		Whitelist: setting.GetStr(conf.PluginExtensionWhitelist),
	}
	if isQBTransfer {
		// qB source files must remain byte-identical for seeding. The Worker
		// applies the configured transformations to the staging copy only.
		pluginOpts.AntiHash = qbStagingConfig.AntiHashEnabled
		pluginOpts.ISORename = qbStagingConfig.ISORenameEnabled
		if len(qbStagingConfig.ExtensionWhitelist) > 0 {
			whitelist := make([]string, 0, len(qbStagingConfig.ExtensionWhitelist))
			for _, extension := range qbStagingConfig.ExtensionWhitelist {
				whitelist = append(whitelist, strings.TrimPrefix(strings.TrimSpace(extension), "."))
			}
			pluginOpts.Whitelist = strings.Join(whitelist, ",")
		}
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
	if isQBTransfer {
		// stagedSource is a Worker-local filesystem path. filepath.Base is
		// required here because path.Base does not recognize Windows '\\'.
		finalizePayload.FileName = filepath.Base(stagedSource)
	}
	binding := task_group.ClusterTransferBinding{
		UploadManifest: &manifest, AdditionalCleanupTargets: []resultqueue.CleanupTarget{sourceCleanup}, FinalizePayload: &finalizePayload,
	}
	creator, err := op.GetAdmin()
	s.reportStageStatus(ctx, offer, model.ClusterStageUploadingMobile, model.ClusterStageStatusRunning, "")
	if isQBTransfer {
		if err != nil {
			return fmt.Errorf("resolve cluster qB upload task creator: %w", err)
		}
		if creator == nil {
			return errors.New("resolve cluster qB upload task creator: admin user is unavailable")
		}
		sourceFile, openErr := os.Open(stagedSource)
		if openErr != nil {
			finishUploadStage(model.ClusterStageStatusFailed, openErr.Error())
			return fmt.Errorf("open staged qB file: %w", openErr)
		}
		localFile := &stream.FileStream{
			Ctx: ctx, Obj: &model.Object{Name: targetName, Size: primary.Size}, Reader: sourceFile,
			Mimetype: utils.GetMimeType(targetName), Closers: utils.NewClosers(sourceFile),
		}
		processedFile, processErr := plugin.ProcessStreamer(localFile, pluginOpts)
		if processErr != nil {
			finishUploadStage(model.ClusterStageStatusFailed, processErr.Error())
			return fmt.Errorf("process staged qB file for upload: %w", processErr)
		}
		processedFile, postPluginSHA256, hashErr := annotatePostPluginSHA256(processedFile)
		if hashErr != nil {
			finishUploadStage(model.ClusterStageStatusFailed, hashErr.Error())
			return fmt.Errorf("hash processed staged qB file: %w", hashErr)
		}
		uploadTotal := processedFile.GetSize()
		if progressErr := s.sendJobProgress(ctx, offer, model.ClusterStageUploadingMobile, 0, uploadTotal, 0, "uploading"); progressErr != nil {
			log.Warnf("cluster job %s upload progress notify failed: %v", offer.JobID, progressErr)
		}
		processedFile = newProgressFileStreamer(processedFile, func(completed, total int64) {
			if progressErr := s.sendJobProgress(ctx, offer, model.ClusterStageUploadingMobile, completed, total, 0, "uploading"); progressErr != nil {
				log.Warnf("cluster job %s upload progress notify failed: %v", offer.JobID, progressErr)
			}
		})
		putCtx := context.WithValue(clusterMoveContext(ctx, binding, creator), conf.SkipPluginKey, struct{}{})
		if putErr := fs.PutDirectly(putCtx, targetRoot, processedFile, true); putErr != nil {
			finishUploadStage(model.ClusterStageStatusFailed, putErr.Error())
			return fmt.Errorf("upload staged qB file: %w", putErr)
		}
		if progressErr := s.sendJobProgress(ctx, offer, model.ClusterStageUploadingMobile, uploadTotal, uploadTotal, 0, "uploaded"); progressErr != nil {
			log.Warnf("cluster job %s final upload progress notify failed: %v", offer.JobID, progressErr)
		}
		// fs.PutDirectly may leave a temporary cache object without a remote ID.
		// Refresh the target directory so cleanup receives the provider's exact
		// file ID instead of the cache placeholder.
		remote, getRemoteErr := getFreshUploadedObject(ctx, expectedPath)
		if getRemoteErr != nil || remote == nil || remote.IsDir() {
			if getRemoteErr == nil {
				getRemoteErr = errors.New("uploaded qB object is missing or is a directory")
			}
			finishUploadStage(model.ClusterStageStatusFailed, getRemoteErr.Error())
			return fmt.Errorf("inspect uploaded qB object: %w", getRemoteErr)
		}
		manifest.Name = remote.GetName()
		manifest.Size = remote.GetSize()
		remoteSHA256 := strings.ToUpper(strings.TrimSpace(remote.GetHash().GetHash(utils.SHA256)))
		if remoteSHA256 != "" && !strings.EqualFold(remoteSHA256, postPluginSHA256) {
			message := fmt.Sprintf("uploaded qB object SHA256 mismatch: worker=%s remote=%s", postPluginSHA256, remoteSHA256)
			finishUploadStage(model.ClusterStageStatusFailed, message)
			return errors.New(message)
		}
		manifest.SHA256 = postPluginSHA256
		if remoteSHA256 != "" {
			manifest.HashSource = "remote_object_metadata"
		} else {
			manifest.HashSource = "worker_post_plugin"
		}
		manifest.RemoteFileID = remote.GetID()
		manifest.RemotePath = expectedPath
		manifest.UploadReceipt = remote.GetID()
		cleanup, cleanupErr := NewCleanupRequest(manifest, targetStorage.GetStorage().MountPath, sourceCleanup)
		if cleanupErr != nil {
			finishUploadStage(model.ClusterStageStatusFailed, cleanupErr.Error())
			return cleanupErr
		}
		if _, enqueueErr := s.EnqueueThenCleanup(ctx, manifest, cleanup); enqueueErr != nil {
			finishUploadStage(model.ClusterStageStatusFailed, enqueueErr.Error())
			return fmt.Errorf("persist qB upload result: %w", enqueueErr)
		}
		finishUploadStage(model.ClusterStageStatusSucceeded, "")
		return nil
	}
	if err != nil {
		return fmt.Errorf("resolve cluster move task creator: %w", err)
	}
	if creator == nil {
		return errors.New("resolve cluster move task creator: admin user is unavailable")
	}
	taskCtx := clusterMoveContext(ctx, binding, creator)
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
	shareSaveObjects := mediaTransferShareSaveObjects(offer.TaskContext, primary)
	if strings.TrimSpace(offer.TaskContext.ShareSaveKey) != "" && len(shareSaveObjects) > 0 {
		key := mediaTransferShareSaveSingleflightKey(offer.TaskContext, requestedTempRoot)
		prepared, saveErr := s.waitMediaTransferShareSaveBatch(ctx, key, offer.TaskContext.Share.URL, offer.TaskContext.Share.Passcode, requestedTempRoot, shareSaveObjects)
		if saveErr != nil {
			if err := ctx.Err(); err != nil {
				return "", false, err
			}
			// A previous cancelled attempt may have left the object in staging.
			if fallback, ok := s.findStagedSource(ctx, requestedTempRoot, primary); ok {
				log.Warnf("cluster job %s share-save batch reported %v; reusing existing staged object %s", offer.JobID, saveErr, fallback)
				return fallback, true, nil
			}
			return "", false, fmt.Errorf("save cluster share selection batch: %w", saveErr)
		}
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		if stagedSource, reused, ok := prepared.stagedSource(requestedTempRoot, primary); ok {
			return stagedSource, reused, nil
		}
		return "", false, fmt.Errorf("cluster staged source for %s was not found after share-save batch preparation", primary.SourceFileID)
	}

	if stagedSource, reused := s.findStagedSource(ctx, requestedTempRoot, primary); reused {
		return stagedSource, true, nil
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

type mediaTransferShareSaveBatchResult struct {
	stagedPaths map[string]string
	savedPaths  []string
}

func (r mediaTransferShareSaveBatchResult) stagedSource(tempRoot string, primary protocol.SourceObject) (string, bool, bool) {
	if stagedPath, ok := r.stagedPaths[strings.TrimSpace(primary.SourceFileID)]; ok {
		return stagedPath, true, true
	}
	if stagedPath, ok := matchMediaTransferStagedPath(tempRoot, primary, r.savedPaths); ok {
		return stagedPath, false, true
	}
	return "", false, false
}

func (s *Service) waitMediaTransferShareSaveBatch(ctx context.Context, key, rawURL, passcode, tempRoot string, objects []protocol.SourceObject) (mediaTransferShareSaveBatchResult, error) {
	if err := ctx.Err(); err != nil {
		return mediaTransferShareSaveBatchResult{}, err
	}
	joined, done := s.beginShareSaveFlightCall(key)
	defer done()
	sharedCtx := context.WithoutCancel(ctx)
	resultCh := s.shareSaveFlights.DoChan(key, func() (mediaTransferShareSaveBatchResult, error) {
		stagedPaths := make(map[string]string, len(objects))
		missingObjects := make([]protocol.SourceObject, 0, len(objects))
		for _, object := range objects {
			if stagedPath, ok := s.findStagedSource(sharedCtx, tempRoot, object); ok {
				stagedPaths[strings.TrimSpace(object.SourceFileID)] = stagedPath
				continue
			}
			missingObjects = append(missingObjects, object)
		}
		if len(missingObjects) == 0 {
			return mediaTransferShareSaveBatchResult{stagedPaths: stagedPaths}, nil
		}
		savedPaths, err := s.saveClusterShareSelectionBatch(sharedCtx, rawURL, passcode, tempRoot, mediaTransferShareSaveFileIDs(missingObjects))
		if err != nil {
			return mediaTransferShareSaveBatchResult{}, err
		}
		return mediaTransferShareSaveBatchResult{stagedPaths: stagedPaths, savedPaths: savedPaths}, nil
	})
	if joined && s.shareSaveFlightJoined != nil {
		s.shareSaveFlightJoined(key)
	}
	select {
	case <-ctx.Done():
		return mediaTransferShareSaveBatchResult{}, ctx.Err()
	case result := <-resultCh:
		if err := ctx.Err(); err != nil {
			return mediaTransferShareSaveBatchResult{}, err
		}
		return result.Val, result.Err
	}
}

func (s *Service) beginShareSaveFlightCall(key string) (bool, func()) {
	s.shareSaveFlightMu.Lock()
	defer s.shareSaveFlightMu.Unlock()
	if s.shareSaveFlightCalls == nil {
		s.shareSaveFlightCalls = make(map[string]int)
	}
	joined := s.shareSaveFlightCalls[key] > 0
	s.shareSaveFlightCalls[key]++
	return joined, func() {
		s.shareSaveFlightMu.Lock()
		defer s.shareSaveFlightMu.Unlock()
		if s.shareSaveFlightCalls[key] <= 1 {
			delete(s.shareSaveFlightCalls, key)
			return
		}
		s.shareSaveFlightCalls[key]--
	}
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

func mediaTransferShareSaveTempRoot(task protocol.TaskContext, requestedTempRoot string) string {
	requestedTempRoot = path.Clean(strings.TrimSpace(requestedTempRoot))
	collisionID, ok := shareSaveBatchCollisionID(task.ShareSaveKey)
	if !ok {
		return requestedTempRoot
	}
	return path.Join(requestedTempRoot, ".share-save-collision-"+collisionID)
}

func shareSaveBatchCollisionID(key string) (string, bool) {
	key = strings.TrimSpace(key)
	if !strings.HasPrefix(key, shareSaveBatchCollisionKeyPrefix) {
		return "", false
	}
	collisionID := strings.TrimSpace(strings.TrimPrefix(key, shareSaveBatchCollisionKeyPrefix))
	if len(collisionID) != sha256.Size*2 {
		return "", false
	}
	if _, err := hex.DecodeString(collisionID); err != nil {
		return "", false
	}
	return collisionID, true
}

func matchMediaTransferStagedPath(tempRoot string, primary protocol.SourceObject, stagedPaths []string) (string, bool) {
	name := path.Base(strings.TrimSpace(primary.SourceRelativePath))
	if name == "" || name == "." || name == "/" {
		return "", false
	}
	candidate := path.Join(tempRoot, name)
	for _, stagedPath := range stagedPaths {
		if path.Clean(strings.TrimSpace(stagedPath)) == candidate {
			return candidate, true
		}
	}
	return "", false
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
	s.mu.Lock()
	if active := s.active[offer.JobID]; active != nil && sameAttempt(active.attempt, offer.AttemptRef) {
		active.stage = strings.TrimSpace(stage)
		active.stageStatus = strings.TrimSpace(status)
		active.progressMessage = strings.TrimSpace(stageError)
		active.progressAt = time.Now().UTC()
	}
	s.mu.Unlock()
	if offer.TaskContext.Torrent != nil {
		s.syncMoviePilotTorrentStatus(ctx, offer.TaskContext.Torrent, offer.JobID, stage, status, 0, 0, "", "", stageError, "", status == model.ClusterStageStatusSucceeded || status == model.ClusterStageStatusFailed)
	}
	if status == model.ClusterStageStatusRunning {
		log.Infof("cluster job %s stage started attempt=%s generation=%d stage=%s", offer.JobID, offer.AttemptID, offer.Generation, stage)
	}
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

func (s *Service) sendJobReject(ctx context.Context, offer protocol.JobOffer, code, reason string, retryable bool) error {
	if strings.TrimSpace(code) != "worker_capacity_unavailable" {
		log.Warnf("cluster job %s admission rejected attempt=%s generation=%d code=%s retryable=%t", offer.JobID, offer.AttemptID, offer.Generation, strings.TrimSpace(code), retryable)
	}
	payload := protocol.JobReject{
		AttemptRef: offer.AttemptRef,
		Code:       strings.TrimSpace(code),
		Reason:     strings.TrimSpace(reason),
		Retryable:  retryable,
	}
	message, err := protocol.NewEnvelope(protocol.MessageJobReject, payload)
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
	if offer.TaskContext.Torrent != nil {
		if runErr == nil {
			phase := ""
			if offer.JobType == model.ClusterJobTypeTorrentObserve {
				phase = model.MoviePilotTaskPhaseDownloadComplete
			} else if offer.JobType == model.ClusterJobTypeMediaTransfer {
				phase = model.MoviePilotTaskPhaseCompleted
			}
			stage := s.currentMoviePilotStage(offer)
			if stage == "" {
				stage = model.ClusterStageUploadingMobile
			}
			s.syncMoviePilotTorrentStatus(context.WithoutCancel(ctx), offer.TaskContext.Torrent, offer.JobID, stage, model.ClusterStageStatusSucceeded, 0, 0, "", "", "", phase, true)
		} else {
			stage := s.currentMoviePilotStage(offer)
			if stage == "" {
				stage = model.ClusterStageUploadingMobile
			}
			s.syncMoviePilotTorrentStatus(context.WithoutCancel(ctx), offer.TaskContext.Torrent, offer.JobID, stage, model.ClusterStageStatusFailed, 0, 0, "", payload.ErrorCode, payload.Error, model.MoviePilotTaskPhaseFailed, true)
		}
	}
	message, err := protocol.NewEnvelope(protocol.MessageJobResult, payload)
	if err != nil {
		return err
	}
	return s.sender.Send(ctx, *message)
}

func (s *Service) currentMoviePilotStage(offer protocol.JobOffer) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if active := s.active[offer.JobID]; active != nil && sameAttempt(active.attempt, offer.AttemptRef) {
		return strings.TrimSpace(active.stage)
	}
	return ""
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

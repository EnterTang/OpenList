package worker

import (
	"context"
	"errors"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/resultqueue"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/task_group"
	"github.com/OpenListTeam/tache"
	"github.com/stretchr/testify/require"
)

type fakeResultQueue struct {
	enqueueID      string
	enqueueErr     error
	enqueued       any
	cleanup        resultqueue.CleanupRequest
	cleanupID      string
	ctxErr         error
	claimed        bool
	claimErr       error
	cleanupBacklog int64
}

type cleanupTestDriver struct {
	storage   model.Storage
	cleared   model.Obj
	removed   model.Obj
	removeErr error
}

func (d *cleanupTestDriver) Config() driver.Config            { return driver.Config{} }
func (d *cleanupTestDriver) GetStorage() *model.Storage       { return &d.storage }
func (d *cleanupTestDriver) SetStorage(storage model.Storage) { d.storage = storage }
func (d *cleanupTestDriver) GetAddition() driver.Additional   { return &struct{}{} }
func (d *cleanupTestDriver) Init(context.Context) error       { return nil }
func (d *cleanupTestDriver) Drop(context.Context) error       { return nil }
func (d *cleanupTestDriver) List(context.Context, model.Obj, model.ListArgs) ([]model.Obj, error) {
	return nil, nil
}
func (d *cleanupTestDriver) Link(context.Context, model.Obj, model.LinkArgs) (*model.Link, error) {
	return nil, nil
}
func (d *cleanupTestDriver) ClearRecycleEntry(_ context.Context, obj model.Obj) error {
	d.cleared = obj
	return nil
}
func (d *cleanupTestDriver) Remove(_ context.Context, obj model.Obj) error {
	if d.removeErr != nil {
		return d.removeErr
	}
	d.removed = obj
	return nil
}

type channelSender chan protocol.Envelope

func (s channelSender) Send(ctx context.Context, message protocol.Envelope) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s <- message:
		return nil
	}
}

func (q *fakeResultQueue) EnqueueResultAndCleanupDurably(ctx context.Context, value any, cleanup resultqueue.CleanupRequest) (string, string, error) {
	q.ctxErr = ctx.Err()
	q.enqueued = value
	q.cleanup = cleanup
	return q.enqueueID, q.cleanupID, q.enqueueErr
}

func (q *fakeResultQueue) ValidateDurability(context.Context) error { return q.claimErr }

func (q *fakeResultQueue) EnqueueDurably(ctx context.Context, value any) (string, error) {
	q.ctxErr = ctx.Err()
	q.enqueued = value
	return q.enqueueID, q.enqueueErr
}

func (q *fakeResultQueue) ClaimAttempt(context.Context, string, time.Duration) (bool, error) {
	if q.claimErr != nil {
		return false, q.claimErr
	}
	if q.claimed {
		return false, nil
	}
	q.claimed = true
	return true, nil
}

func (q *fakeResultQueue) ReleaseAttempt(context.Context, string) error {
	q.claimed = false
	return nil
}
func (q *fakeResultQueue) CleanupBacklog(context.Context) (int64, error) {
	return q.cleanupBacklog, nil
}

func (*fakeResultQueue) EnsureGroup(context.Context) error { return nil }
func (*fakeResultQueue) Reclaim(context.Context, time.Duration, string, int64) ([]resultqueue.Result, string, error) {
	return nil, "0-0", nil
}
func (*fakeResultQueue) Read(context.Context, int64, time.Duration) ([]resultqueue.Result, error) {
	return nil, nil
}
func (*fakeResultQueue) AckAndDelete(context.Context, ...string) error { return nil }
func (*fakeResultQueue) MoveToDLQ(context.Context, resultqueue.Result, string) error {
	return nil
}
func (*fakeResultQueue) EnsureCleanupGroup(context.Context) error { return nil }
func (*fakeResultQueue) ReclaimCleanup(context.Context, time.Duration, string, int64) ([]resultqueue.Result, string, error) {
	return nil, "0-0", nil
}
func (*fakeResultQueue) ReadCleanup(context.Context, int64, time.Duration) ([]resultqueue.Result, error) {
	return nil, nil
}
func (*fakeResultQueue) AckAndDeleteCleanup(context.Context, ...string) error { return nil }
func (*fakeResultQueue) MoveCleanupToDLQ(context.Context, resultqueue.Result, string) error {
	return nil
}
func (*fakeResultQueue) Stats(context.Context) (resultqueue.Stats, error) {
	return resultqueue.Stats{}, nil
}

func TestPrimarySourceObjectPrefersLargestMedia(t *testing.T) {
	got := primarySourceObject([]protocol.SourceObject{
		{SourceFileID: "subtitle", SourceRelativePath: "episode.srt", Size: 1000},
		{SourceFileID: "small-video", SourceRelativePath: "episode-720p.mkv", Size: 100},
		{SourceFileID: "large-video", SourceRelativePath: "episode-1080p.mkv", Size: 200},
	})
	if got.SourceFileID != "large-video" {
		t.Fatalf("primary = %q", got.SourceFileID)
	}
}

func TestEnqueueThenCleanupPersistsCleanupBeforeAttempt(t *testing.T) {
	queue := &fakeResultQueue{enqueueID: "1-0", cleanupID: "2-0"}
	service := New(queue, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	manifest := validUploadManifest(t)
	cleanup, err := NewCleanupRequest(manifest, "/mobile")
	require.NoError(t, err)
	id, err := service.EnqueueThenCleanup(ctx, manifest, cleanup)

	require.NoError(t, err)
	require.Equal(t, "1-0", id)
	require.NoError(t, queue.ctxErr)
	require.NotNil(t, queue.enqueued)
	require.Equal(t, cleanup, queue.cleanup)
}

func TestEnqueueThenCleanupKeepsMediaWhenResultPersistenceFails(t *testing.T) {
	queue := &fakeResultQueue{enqueueErr: resultqueue.ErrUnavailable}
	service := New(queue, nil)
	manifest := validUploadManifest(t)
	cleanup, cleanupErr := NewCleanupRequest(manifest, "/mobile")
	require.NoError(t, cleanupErr)
	_, err := service.EnqueueThenCleanup(context.Background(), manifest, cleanup)

	require.ErrorIs(t, err, resultqueue.ErrUnavailable)
}

func TestNewCleanupRequestUsesExactFinalPath(t *testing.T) {
	manifest := validUploadManifest(t)
	request, err := NewCleanupRequest(manifest, "/mobile")
	require.NoError(t, err)
	require.Equal(t, "/mobile/upload/tv/国产剧/Show/Season 1/Show.S01E01.mkv", request.OpenListPath)
	require.Equal(t, "remote-1", request.RemoteFileID)
	require.True(t, request.EmptyRecycleBin)
}

func TestMapClusterDeliveryPathPreservesLogicalRelativeTarget(t *testing.T) {
	got, err := mapClusterDeliveryPath(
		"/worker-139/upload",
		"/139_60t/上传中转",
		"/139_60t/上传中转/tv/国产剧/小芳 (2026) {tmdb-296003}/Season 1/小芳.2026.S01E01.第1集.mkv",
	)
	require.NoError(t, err)
	require.Equal(t, "/worker-139/upload/tv/国产剧/小芳 (2026) {tmdb-296003}/Season 1/小芳.2026.S01E01.第1集.mkv", got)
	require.NotContains(t, got, ".openlist-cluster")
}

func TestMapClusterDeliveryPathTreatsEmptyLogicalRootAsWorkerRoot(t *testing.T) {
	got, err := mapClusterDeliveryPath(
		"/worker-139/upload",
		"",
		"/tv/国产剧/小芳 (2026) {tmdb-296003}/Season 1/小芳.S01E01.mkv",
	)
	require.NoError(t, err)
	require.Equal(t, "/worker-139/upload/tv/国产剧/小芳 (2026) {tmdb-296003}/Season 1/小芳.S01E01.mkv", got)
}

func TestTrustedSourceSHA256DoesNotTreatIdentityHashAsContent(t *testing.T) {
	identity := protocol.SourceObject{SourceFileID: "file-1", Hash: "file-1:1024:2026-07-16T00:00:00Z", Size: 1024}
	if got, ok := trustedSourceSHA256(identity); ok || got != "" {
		t.Fatalf("identity fingerprint was accepted as content SHA256: %q, %v", got, ok)
	}
	content := protocol.SourceObject{SourceFileID: "file-1", ContentSHA256: strings.Repeat("a", 64), Size: 1024}
	got, ok := trustedSourceSHA256(content)
	require.True(t, ok)
	require.Equal(t, strings.Repeat("A", 64), got)
}

func TestResolveClusterAdoptPathPrefersPostPluginName(t *testing.T) {
	got := resolveClusterAdoptPath("/mobile/Show.S01E01.mkv", "/mobile/Show.S01E01.mkv.iso", "Show.S01E01.mkv", "Show.S01E01.mkv.iso")
	require.Equal(t, "/mobile/Show.S01E01.mkv.iso", got)
	got = resolveClusterAdoptPath("/mobile/Show.S01E01.mkv", "/mobile/Show.S01E01.mkv", "Show.S01E01.mkv", "Show.S01E01.mkv")
	require.Equal(t, "/mobile/Show.S01E01.mkv", got)
}

func TestPostPluginAdoptMatchesIgnoresLegacySibling(t *testing.T) {
	require.False(t, postPluginAdoptMatches("Show.S01E01.mkv", 100, "Show.S01E01.mkv.iso", 116))
	require.True(t, postPluginAdoptMatches("Show.S01E01.mkv.iso", 116, "Show.S01E01.mkv.iso", 116))
	require.False(t, postPluginAdoptMatches("Show.S01E01.mkv.iso", 100, "Show.S01E01.mkv.iso", 116))
}

func TestClusterMoveContextForcesNativeTaskAndCarriesAdminCreator(t *testing.T) {
	creator := &model.User{ID: 1, Username: "admin", Role: model.ADMIN}
	manifest := validUploadManifest(t)
	payload := task_group.TransferFinalizePayload{TargetDir: "/mobile/upload", FileName: "raw.mkv", TargetName: "Show.S01E01.mkv"}
	binding := task_group.ClusterTransferBinding{UploadManifest: &manifest, FinalizePayload: &payload}

	ctx := clusterMoveContext(context.Background(), binding, creator)
	require.NotNil(t, ctx.Value(conf.ForceTaskKey))
	require.Nil(t, ctx.Value(conf.NoTaskKey))
	require.Same(t, creator, ctx.Value(conf.UserKey))
	got, ok := task_group.ClusterTransferBindingFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, payload.TargetName, got.FinalizePayload.TargetName)
}

func TestWaitNativeTransferTaskReturnsTerminalFailure(t *testing.T) {
	task := &fs.FileTransferTask{}
	task.SetState(tache.StateFailed)
	task.SetErr(errors.New("upload failed"))
	err := waitNativeTransferTask(context.Background(), task)
	require.ErrorContains(t, err, "upload failed")
}

func TestWaitNativeTransferTaskCancelsManagerTaskBeforeReturning(t *testing.T) {
	task := &fs.FileTransferTask{}
	task.SetID("move-1")
	task.SetState(tache.StateRunning)
	oldCancel := cancelNativeMoveTask
	cancelNativeMoveTask = func(id string) {
		require.Equal(t, "move-1", id)
		task.SetState(tache.StateCanceled)
		task.SetErr(context.Canceled)
	}
	t.Cleanup(func() { cancelNativeMoveTask = oldCancel })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, waitNativeTransferTask(ctx, task), context.Canceled)
	require.Equal(t, tache.StateCanceled, task.GetState())
}

func TestMapClusterDeliveryPathRejectsLogicalEscape(t *testing.T) {
	for _, target := range []string{
		"/139_60t/other/Episode.mkv",
		"/139_60t/上传中转/../other/Episode.mkv",
		"relative/Episode.mkv",
	} {
		_, err := mapClusterDeliveryPath("/worker-139/upload", "/139_60t/上传中转", target)
		require.Error(t, err, target)
	}
}

func TestNewCleanupRequestIncludesStagedSourceCleanupByDefault(t *testing.T) {
	manifest := validUploadManifest(t)
	source := resultqueue.CleanupTarget{
		OpenListPath:     "/123/转存至移动/Episode.mkv",
		StorageMountPath: "/123",
		OwnedRootPath:    "/123/转存至移动",
		RemoteFileID:     "staged-source",
		Name:             "Episode.mkv",
		ExactFile:        true,
	}

	request, err := NewCleanupRequest(manifest, "/mobile", source)

	require.NoError(t, err)
	require.Len(t, request.AdditionalTargets, 1)
	require.Equal(t, source, request.AdditionalTargets[0])
}

func TestNewSourceCleanupTargetRequiresExactRemoteID(t *testing.T) {
	d := &cleanupTestDriver{storage: model.Storage{MountPath: "/123"}}
	originalStorage := getCleanupStorageAndActualPath
	originalGet := getCleanupObject
	getCleanupStorageAndActualPath = func(string) (driver.Driver, string, error) {
		return d, "/转存至移动/Episode.mkv", nil
	}
	getCleanupObject = func(context.Context, driver.Driver, string, ...bool) (model.Obj, error) {
		return &model.Object{Name: "Episode.mkv"}, nil
	}
	t.Cleanup(func() {
		getCleanupStorageAndActualPath = originalStorage
		getCleanupObject = originalGet
	})

	manifest := validUploadManifest(t)
	_, err := NewSourceCleanupTarget(context.Background(), manifest, "/123/转存至移动", "/123/转存至移动/Episode.mkv")
	require.ErrorContains(t, err, "exact remote file id")

	getCleanupObject = func(context.Context, driver.Driver, string, ...bool) (model.Obj, error) {
		return &model.Object{ID: "source-file", Name: "Episode.mkv"}, nil
	}
	target, err := NewSourceCleanupTarget(context.Background(), manifest, "/123/转存至移动", "/123/转存至移动/Episode.mkv")
	require.NoError(t, err)
	require.Equal(t, "/123/转存至移动/Episode.mkv", target.OpenListPath)
	require.Equal(t, "/123/转存至移动", target.OwnedRootPath)
	require.Equal(t, "source-file", target.RemoteFileID)
	require.True(t, target.ExactFile)
}

func TestExecuteCleanupTargetRefusesRemoteIDMismatch(t *testing.T) {
	d := &cleanupTestDriver{storage: model.Storage{MountPath: "/123"}}
	originalStorage := getCleanupStorageAndActualPath
	originalGet := getCleanupObject
	originalRemove := removeCleanupObjectExact
	originalDelay := cleanupLookupDelay
	cleanupLookupDelay = 0
	removed := false
	getCleanupStorageAndActualPath = func(string) (driver.Driver, string, error) {
		return d, "/转存至移动/Episode.mkv", nil
	}
	getCleanupObject = func(context.Context, driver.Driver, string, ...bool) (model.Obj, error) {
		return &model.Object{ID: "replacement-file", Name: "Episode.mkv"}, nil
	}
	removeCleanupObjectExact = func(context.Context, driver.Driver, string, model.Obj) error {
		removed = true
		return nil
	}
	t.Cleanup(func() {
		getCleanupStorageAndActualPath = originalStorage
		getCleanupObject = originalGet
		removeCleanupObjectExact = originalRemove
		cleanupLookupDelay = originalDelay
	})

	err := executeCleanupTarget(context.Background(), resultqueue.CleanupTarget{
		OpenListPath: "/123/转存至移动/Episode.mkv", StorageMountPath: "/123",
		OwnedRootPath: "/123/转存至移动", RemoteFileID: "source-file", Name: "Episode.mkv", ExactFile: true,
	})
	require.ErrorContains(t, err, "remote id changed")
	require.False(t, removed)
}

func TestExecuteCleanupTargetClearsRecycleEntryAfterFileAlreadyRemoved(t *testing.T) {
	d := &cleanupTestDriver{storage: model.Storage{MountPath: "/mobile"}}
	originalStorage := getCleanupStorageAndActualPath
	originalGet := getCleanupObject
	originalDelay := cleanupLookupDelay
	cleanupLookupDelay = 0
	getCleanupStorageAndActualPath = func(string) (driver.Driver, string, error) {
		return d, "/.openlist-cluster/job-1/media-1/Episode.mkv", nil
	}
	getCleanupObject = func(context.Context, driver.Driver, string, ...bool) (model.Obj, error) {
		return nil, errs.ObjectNotFound
	}
	t.Cleanup(func() {
		getCleanupStorageAndActualPath = originalStorage
		getCleanupObject = originalGet
		cleanupLookupDelay = originalDelay
	})

	err := executeCleanupTarget(context.Background(), resultqueue.CleanupTarget{
		OpenListPath: "/mobile/.openlist-cluster/job-1/media-1/Episode.mkv", StorageMountPath: "/mobile",
		RemoteFileID: "remote-file", Name: "Episode.mkv", EmptyRecycleBin: true,
	})
	require.NoError(t, err)
	require.NotNil(t, d.removed)
	require.Equal(t, "remote-file", d.removed.GetID())
	require.NotNil(t, d.cleared)
	require.Equal(t, "remote-file", d.cleared.GetID())
}

func TestExecuteCleanupTargetDirectRemoveByIDWhenListingMisses(t *testing.T) {
	d := &cleanupTestDriver{storage: model.Storage{MountPath: "/139"}}
	originalStorage := getCleanupStorageAndActualPath
	originalGet := getCleanupObject
	originalDelay := cleanupLookupDelay
	cleanupLookupDelay = 0
	getCleanupStorageAndActualPath = func(string) (driver.Driver, string, error) {
		return d, "/139/上传中转/Episode.mkv", nil
	}
	getCleanupObject = func(context.Context, driver.Driver, string, ...bool) (model.Obj, error) {
		return nil, errs.ObjectNotFound
	}
	t.Cleanup(func() {
		getCleanupStorageAndActualPath = originalStorage
		getCleanupObject = originalGet
		cleanupLookupDelay = originalDelay
	})

	err := executeCleanupTarget(context.Background(), resultqueue.CleanupTarget{
		OpenListPath: "/139/上传中转/Episode.mkv", StorageMountPath: "/139",
		RemoteFileID: "file-id-123", Name: "Episode.mkv", EmptyRecycleBin: false,
	})
	require.NoError(t, err)
	require.NotNil(t, d.removed)
	require.Equal(t, "file-id-123", d.removed.GetID())
	require.Nil(t, d.cleared)
}

func TestCancelActiveCancelsConnectionBoundTasks(t *testing.T) {
	service := New(nil, nil)
	ctx, cancel := context.WithCancelCause(context.Background())
	service.active["job-1"] = &activeTask{
		attempt: protocol.AttemptRef{JobID: "job-1", AttemptID: "attempt-1", Generation: 1, LeaseToken: "lease"},
		offer:   protocol.JobOffer{AttemptRef: protocol.AttemptRef{JobID: "job-1", AttemptID: "attempt-1", Generation: 1, LeaseToken: "lease"}},
		ctx:     ctx,
		cancel:  cancel,
	}
	want := errors.New("connection lost")

	service.CancelActive(want)

	require.ErrorIs(t, context.Cause(ctx), want)
}

func TestMaintainLeaseKeepsRunningWhenRenewTransportFails(t *testing.T) {
	service := New(&fakeResultQueue{}, failSender{})
	offer := protocol.JobOffer{
		AttemptRef: protocol.AttemptRef{JobID: "job-1", AttemptID: "attempt-1", Generation: 1, LeaseToken: "lease"},
		LeaseUntil: time.Now().Add(time.Hour),
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		service.maintainLease(ctx, cancel, offer)
	}()

	select {
	case <-ctx.Done():
		t.Fatalf("transport renew failure cancelled job: %v", context.Cause(ctx))
	case <-time.After(200 * time.Millisecond):
	}
	cancel(nil)
	<-done
}

type failSender struct{}

func (failSender) Send(context.Context, protocol.Envelope) error {
	return errors.New("websocket closed")
}

func TestOnTransportReconnectedRenewsActiveLeases(t *testing.T) {
	sender := make(channelSender, 1)
	service := New(&fakeResultQueue{}, sender)
	offer := protocol.JobOffer{AttemptRef: protocol.AttemptRef{JobID: "job-1", AttemptID: "attempt-1", Generation: 1, LeaseToken: "lease"}}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	service.active["job-1"] = &activeTask{attempt: offer.AttemptRef, offer: offer, ctx: ctx, cancel: cancel}

	done := make(chan struct{})
	go func() {
		defer close(done)
		service.OnTransportReconnected(context.Background())
	}()
	message := <-sender
	require.Equal(t, protocol.MessageLeaseRenew, message.Type)
	ack, err := protocol.NewEnvelope(protocol.MessageAck, protocol.Ack{MessageID: message.MessageID})
	require.NoError(t, err)
	ack.CorrelationID = message.MessageID
	require.NoError(t, service.HandleMessage(context.Background(), nil, *ack))
	<-done
}

func TestLeaseRenewWaitsForCoordinatorAck(t *testing.T) {
	sender := make(channelSender, 1)
	service := New(&fakeResultQueue{}, sender)
	offer := protocol.JobOffer{AttemptRef: protocol.AttemptRef{JobID: "job-1", AttemptID: "attempt-1", Generation: 1, LeaseToken: "lease"}}
	done := make(chan error, 1)
	go func() { done <- service.sendLeaseRenew(context.Background(), offer, time.Now().Add(time.Minute)) }()
	message := <-sender
	select {
	case err := <-done:
		t.Fatalf("lease renewal completed before ACK: %v", err)
	default:
	}
	ack, err := protocol.NewEnvelope(protocol.MessageAck, protocol.Ack{MessageID: message.MessageID})
	require.NoError(t, err)
	ack.CorrelationID = message.MessageID
	require.NoError(t, service.HandleMessage(context.Background(), nil, *ack))
	require.NoError(t, <-done)
}

func TestLeaseRenewStopsOnCoordinatorNack(t *testing.T) {
	sender := make(channelSender, 1)
	service := New(&fakeResultQueue{}, sender)
	offer := protocol.JobOffer{AttemptRef: protocol.AttemptRef{JobID: "job-1", AttemptID: "attempt-1", Generation: 1, LeaseToken: "lease"}}
	done := make(chan error, 1)
	go func() { done <- service.sendLeaseRenew(context.Background(), offer, time.Now().Add(time.Minute)) }()
	message := <-sender
	nack, err := protocol.NewEnvelope(protocol.MessageNack, protocol.Nack{MessageID: message.MessageID, Code: "stale_lease", Error: "reassigned"})
	require.NoError(t, err)
	nack.CorrelationID = message.MessageID
	require.NoError(t, service.HandleMessage(context.Background(), nil, *nack))
	require.ErrorContains(t, <-done, "stale_lease")
}

func TestStagePermitIsRequestedJustInTime(t *testing.T) {
	sender := make(channelSender, 1)
	service := New(&fakeResultQueue{}, sender)
	offer := protocol.JobOffer{
		AttemptRef:     protocol.AttemptRef{JobID: "job-1", AttemptID: "attempt-1", Generation: 1, LeaseToken: "lease"},
		IdempotencyKey: "operation-1",
	}
	done := make(chan error, 1)
	go func() {
		_, err := service.requestStagePermit(context.Background(), offer, model.ClusterStageSavingShare)
		done <- err
	}()
	message := <-sender
	request, err := protocol.DecodePayload[protocol.StagePermitRequest](message)
	require.NoError(t, err)
	require.Equal(t, model.ClusterStageSavingShare, request.Stage)
	permit, err := protocol.NewEnvelope(protocol.MessageStagePermit, protocol.StagePermit{
		AttemptRef: request.AttemptRef, Stage: request.Stage, OperationKey: request.OperationKey,
		PermitToken: "permit", PermitExpiresAt: time.Now().Add(30 * time.Second),
	})
	require.NoError(t, err)
	permit.CorrelationID = message.MessageID
	require.NoError(t, service.HandleMessage(context.Background(), nil, *permit))
	require.NoError(t, <-done)
}

func TestResolveStagingTempRootDoesNotSubstituteConfiguredProviderRoot(t *testing.T) {
	setWorkerSubscriptionConfig(t, model.SubscriptionConfig{})
	oldList := listInventoryStorages
	listInventoryStorages = func() ([]model.Storage, error) { return nil, nil }
	t.Cleanup(func() { listInventoryStorages = oldList })
	service := New(&fakeResultQueue{}, nil)
	service.desiredConfig.ProviderTempRoots = map[string]string{"aliyundrive": "/ali/cluster-temp"}
	task := protocol.TaskContext{
		Share: protocol.ShareTaskContext{Provider: "aliyundrive"},
		StagingTarget: protocol.ProviderTargetRequirement{
			Provider:      "pan123",
			Folder:        "转存至移动",
			NeedShareSave: true,
			RequiredBytes: 8 << 30,
		},
		SourceObjects: []protocol.SourceObject{{Provider: "aliyundrive", SourceFileID: "file-1", Size: 8 << 30}},
	}

	got, err := service.resolveStagingTempRoot(context.Background(), task)
	require.ErrorContains(t, err, "no compatible provider account")
	require.Empty(t, got)
}

func validUploadManifest(t *testing.T) protocol.UploadETFManifest {
	t.Helper()
	taskContext := protocol.TaskContext{
		ParentBatchID:         "batch-1",
		MediaItemID:           "media-1",
		WorkflowVersion:       "v1",
		SealedManifestVersion: "v1",
		Subscription: protocol.SubscriptionTaskContext{
			SubscriptionID:     1,
			SubscriptionItemID: 1,
			SourceKey:          "telegram:channel",
		},
		Share: protocol.ShareTaskContext{Provider: "aliyundrive", URL: "https://example.com/share"},
		Media: protocol.MediaTaskContext{
			MediaType:         "tv",
			LogicalMediaRoot:  "/139_60t/上传中转",
			LogicalTargetPath: "/139_60t/上传中转/tv/国产剧/Show/Season 1/Show.S01E01.mkv",
		},
		SourceObjects: []protocol.SourceObject{{Provider: "aliyundrive", SourceFileID: "file-1"}},
		ShareSaveKey:  "share-save-batch:abc123",
		ShareSaveObjects: []protocol.SourceObject{
			{Provider: "aliyundrive", SourceFileID: "file-1"},
			{Provider: "aliyundrive", SourceFileID: "file-2"},
		},
		TargetProfile: "/mobile",
	}
	hash, err := protocol.HashTaskContext(taskContext)
	require.NoError(t, err)
	return protocol.UploadETFManifest{
		AttemptRef:            protocol.AttemptRef{JobID: "job-1", AttemptID: "attempt-1", Generation: 1, LeaseToken: "lease"},
		ParentBatchID:         taskContext.ParentBatchID,
		MediaItemID:           taskContext.MediaItemID,
		OperationKey:          "operation-1",
		StagePermitToken:      "upload-permit",
		TaskContextHash:       hash,
		WorkflowVersion:       taskContext.WorkflowVersion,
		SealedManifestVersion: taskContext.SealedManifestVersion,
		TargetProfile:         taskContext.TargetProfile,
		Subscription:          taskContext.Subscription,
		Share:                 taskContext.Share,
		Media:                 taskContext.Media,
		SourceObjects:         taskContext.SourceObjects,
		ShareSaveKey:          taskContext.ShareSaveKey,
		ShareSaveObjects:      taskContext.ShareSaveObjects,
		MobileAccountBinding:  "/mobile",
		RemoteFileID:          "remote-1",
		RemotePath:            "/mobile/upload/tv/国产剧/Show/Season 1/Show.S01E01.mkv",
		Name:                  "Show.S01E01.mkv",
		Size:                  1024,
		SHA256:                strings.Repeat("A", 64),
		HashSource:            "mobile_provider_response",
	}
}

func TestValidUploadManifestReconstructsShareSaveTaskContextHash(t *testing.T) {
	manifest := validUploadManifest(t)
	got, err := protocol.HashTaskContext(manifest.TaskContext())
	require.NoError(t, err)
	require.Equal(t, manifest.TaskContextHash, got)
}

func TestMediaTransfer_BatchShareSaveUsesSingleflight(t *testing.T) {
	tempRoot := "/worker-staging/share-save"
	shareSaveObjects := []protocol.SourceObject{
		{Provider: "pan115", SourceFileID: "file-1", SourceRelativePath: "Show.S01E01.mkv", Size: 101},
		{Provider: "pan115", SourceFileID: "file-2", SourceRelativePath: "Show.S01E02.mkv", Size: 102},
		{Provider: "pan115", SourceFileID: "file-3", SourceRelativePath: "Show.S01E03.mkv", Size: 103},
	}
	service := New(&fakeResultQueue{}, nil)

	var (
		mu        sync.Mutex
		callCount int
		gotIDs    []string
		staged    = make(map[string]string, len(shareSaveObjects))
	)
	saverEntered := make(chan struct{})
	releaseSaver := make(chan struct{})
	service.stagedSourceFinder = func(_ context.Context, _ string, primary protocol.SourceObject) (string, bool) {
		mu.Lock()
		defer mu.Unlock()
		stagedPath, ok := staged[primary.SourceFileID]
		return stagedPath, ok
	}
	service.shareSaveBatchSaver = func(_ context.Context, _, _, _ string, selectedFileIDs []string) ([]string, error) {
		mu.Lock()
		callCount++
		gotIDs = append([]string(nil), selectedFileIDs...)
		if callCount == 1 {
			close(saverEntered)
		}
		mu.Unlock()

		<-releaseSaver

		mu.Lock()
		defer mu.Unlock()
		paths := make([]string, 0, len(shareSaveObjects))
		for _, object := range shareSaveObjects {
			stagedPath := testStagedSourcePath(tempRoot, object)
			staged[object.SourceFileID] = stagedPath
			paths = append(paths, stagedPath)
		}
		return paths, nil
	}

	type result struct {
		sourceFileID string
		stagedPath   string
		reused       bool
		err          error
	}
	results := make(chan result, len(shareSaveObjects))
	for _, primary := range shareSaveObjects {
		primary := primary
		go func() {
			stagedPath, reused, err := service.prepareMediaTransferShareSave(
				context.Background(),
				testMediaTransferOffer(primary, shareSaveObjects, "share-save-batch:alpha"),
				tempRoot,
			)
			results <- result{sourceFileID: primary.SourceFileID, stagedPath: stagedPath, reused: reused, err: err}
		}()
	}

	select {
	case <-saverEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("batch saver was not called")
	}
	close(releaseSaver)

	gotPaths := make(map[string]string, len(shareSaveObjects))
	for range shareSaveObjects {
		result := <-results
		require.NoError(t, result.err)
		gotPaths[result.sourceFileID] = result.stagedPath
	}

	mu.Lock()
	require.Equal(t, 1, callCount)
	require.Equal(t, []string{"file-1", "file-2", "file-3"}, gotIDs)
	mu.Unlock()
	for _, object := range shareSaveObjects {
		require.Equal(t, testStagedSourcePath(tempRoot, object), gotPaths[object.SourceFileID])
	}
}

func TestMediaTransfer_BatchShareSaveFailureFansOutAndAllowsRetry(t *testing.T) {
	tempRoot := "/worker-staging/share-save"
	shareSaveObjects := []protocol.SourceObject{
		{Provider: "pan115", SourceFileID: "file-1", SourceRelativePath: "Show.S01E01.mkv", Size: 101},
		{Provider: "pan115", SourceFileID: "file-2", SourceRelativePath: "Show.S01E02.mkv", Size: 102},
	}
	service := New(&fakeResultQueue{}, nil)

	var (
		mu             sync.Mutex
		callCount      int
		staged         = make(map[string]string, len(shareSaveObjects))
		initialLookups = make(map[string]struct{}, len(shareSaveObjects))
	)
	firstRelease := make(chan struct{})
	initialReady := make(chan struct{})
	saverEntered := make(chan struct{})
	service.stagedSourceFinder = func(_ context.Context, _ string, primary protocol.SourceObject) (string, bool) {
		mu.Lock()
		stagedPath, ok := staged[primary.SourceFileID]
		if !ok && len(staged) == 0 {
			if _, seen := initialLookups[primary.SourceFileID]; !seen {
				initialLookups[primary.SourceFileID] = struct{}{}
				if len(initialLookups) == len(shareSaveObjects) {
					close(initialReady)
				}
			}
			mu.Unlock()
			<-initialReady
			return "", false
		}
		mu.Unlock()
		return stagedPath, ok
	}
	service.shareSaveBatchSaver = func(_ context.Context, _, _, _ string, _ []string) ([]string, error) {
		mu.Lock()
		callCount++
		currentCall := callCount
		mu.Unlock()
		if currentCall == 1 {
			close(saverEntered)
			<-firstRelease
			return nil, errors.New("save batch failed")
		}
		mu.Lock()
		defer mu.Unlock()
		paths := make([]string, 0, len(shareSaveObjects))
		for _, object := range shareSaveObjects {
			stagedPath := testStagedSourcePath(tempRoot, object)
			staged[object.SourceFileID] = stagedPath
			paths = append(paths, stagedPath)
		}
		return paths, nil
	}

	errs := make(chan error, len(shareSaveObjects))
	for _, primary := range shareSaveObjects {
		primary := primary
		go func() {
			_, _, err := service.prepareMediaTransferShareSave(
				context.Background(),
				testMediaTransferOffer(primary, shareSaveObjects, "share-save-batch:beta"),
				tempRoot,
			)
			errs <- err
		}()
	}

	select {
	case <-saverEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("batch saver was not entered")
	}
	time.Sleep(50 * time.Millisecond)
	close(firstRelease)
	for range shareSaveObjects {
		err := <-errs
		require.ErrorContains(t, err, "save batch failed")
	}

	mu.Lock()
	require.Equal(t, 1, callCount)
	mu.Unlock()

	stagedPath, reused, err := service.prepareMediaTransferShareSave(
		context.Background(),
		testMediaTransferOffer(shareSaveObjects[0], shareSaveObjects, "share-save-batch:beta"),
		tempRoot,
	)
	require.NoError(t, err)
	require.False(t, reused)
	require.Equal(t, testStagedSourcePath(tempRoot, shareSaveObjects[0]), stagedPath)
	mu.Lock()
	require.Equal(t, 2, callCount)
	mu.Unlock()
}

func TestMediaTransfer_BatchShareSaveSkipsWhenAllFilesAlreadyStaged(t *testing.T) {
	tempRoot := "/worker-staging/share-save"
	shareSaveObjects := []protocol.SourceObject{
		{Provider: "pan115", SourceFileID: "file-1", SourceRelativePath: "Show.S01E01.mkv", Size: 101},
		{Provider: "pan115", SourceFileID: "file-2", SourceRelativePath: "Show.S01E02.mkv", Size: 102},
	}
	service := New(&fakeResultQueue{}, nil)

	staged := make(map[string]string, len(shareSaveObjects))
	for _, object := range shareSaveObjects {
		staged[object.SourceFileID] = testStagedSourcePath(tempRoot, object)
	}
	service.stagedSourceFinder = func(_ context.Context, _ string, primary protocol.SourceObject) (string, bool) {
		stagedPath, ok := staged[primary.SourceFileID]
		return stagedPath, ok
	}

	called := false
	service.shareSaveBatchSaver = func(_ context.Context, _, _, _ string, _ []string) ([]string, error) {
		called = true
		return nil, nil
	}

	stagedPath, reused, err := service.prepareMediaTransferShareSave(
		context.Background(),
		testMediaTransferOffer(shareSaveObjects[0], shareSaveObjects, "share-save-batch:gamma"),
		tempRoot,
	)
	require.NoError(t, err)
	require.True(t, reused)
	require.Equal(t, testStagedSourcePath(tempRoot, shareSaveObjects[0]), stagedPath)
	require.False(t, called)
}

func TestMediaTransfer_SingleShareSaveWithoutBatchUsesPrimaryOnly(t *testing.T) {
	tempRoot := "/worker-staging/share-save"
	service := New(&fakeResultQueue{}, nil)
	objects := []protocol.SourceObject{
		{Provider: "pan115", SourceFileID: "cover", SourceRelativePath: "Show.S01E01.jpg", Size: 20},
		{Provider: "pan115", SourceFileID: "episode", SourceRelativePath: "Show.S01E01.mkv", Size: 200},
		{Provider: "pan115", SourceFileID: "subtitle", SourceRelativePath: "Show.S01E01.ass", Size: 10},
	}

	var (
		gotIDs []string
		staged string
	)
	service.stagedSourceFinder = func(_ context.Context, _ string, primary protocol.SourceObject) (string, bool) {
		if staged == "" {
			return "", false
		}
		return staged, primary.SourceFileID == "episode"
	}
	service.shareSaveBatchSaver = func(_ context.Context, _, _, _ string, _ []string) ([]string, error) {
		t.Fatal("batch saver should not be called when share-save batch context is absent")
		return nil, nil
	}
	service.shareSaveSaver = func(_ context.Context, _, _, _ string, selectedFileIDs []string) ([]string, error) {
		gotIDs = append([]string(nil), selectedFileIDs...)
		staged = testStagedSourcePath(tempRoot, objects[1])
		return []string{staged}, nil
	}

	stagedPath, reused, err := service.prepareMediaTransferShareSave(
		context.Background(),
		testMediaTransferOfferWithoutBatch(objects),
		tempRoot,
	)
	require.NoError(t, err)
	require.False(t, reused)
	require.Equal(t, []string{"episode"}, gotIDs)
	require.Equal(t, staged, stagedPath)
}

func TestDefaultMediaConcurrencyUsesConfiguredMoveWorkers(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	conf.Conf.Tasks.Move.Workers = 5
	op.Cache.SetSetting(conf.TaskMoveThreadsNum, &model.SettingItem{Key: conf.TaskMoveThreadsNum, Value: "5"})
	t.Cleanup(func() {
		op.Cache.ClearAll()
		conf.Conf = original
	})
	if got := defaultMediaConcurrency(); got != 5 {
		t.Fatalf("default media concurrency = %d, want 5", got)
	}
}

func TestEffectiveMediaConcurrencyUsesDynamicMoveSettingAsFinalLimit(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	conf.Conf.Tasks.Move.Workers = 5
	op.Cache.SetSetting(conf.TaskMoveThreadsNum, &model.SettingItem{Key: conf.TaskMoveThreadsNum, Value: "10"})
	t.Cleanup(func() {
		op.Cache.ClearAll()
		conf.Conf = original
	})
	if got := effectiveMediaConcurrency(); got != 10 {
		t.Fatalf("effective media concurrency = %d, want 10", got)
	}
}

func TestMediaCapacityRefreshesDynamicMoveSetting(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	conf.Conf.Tasks.Move.Workers = 5
	op.Cache.SetSetting(conf.TaskMoveThreadsNum, &model.SettingItem{Key: conf.TaskMoveThreadsNum, Value: "5"})
	service := New(nil, nil)
	op.Cache.SetSetting(conf.TaskMoveThreadsNum, &model.SettingItem{Key: conf.TaskMoveThreadsNum, Value: "10"})
	t.Cleanup(func() {
		op.Cache.ClearAll()
		conf.Conf = original
	})

	releaseDownload, err := service.acquireDownloadCapacity(context.Background())
	if err != nil {
		t.Fatalf("acquire download capacity: %v", err)
	}
	releaseDownload()
	releaseUpload, err := service.acquireUploadCapacity(context.Background())
	if err != nil {
		t.Fatalf("acquire upload capacity: %v", err)
	}
	releaseUpload()

	service.downloadGate.mu.Lock()
	downloadLimit := service.downloadGate.limit
	service.downloadGate.mu.Unlock()
	service.uploadGate.mu.Lock()
	uploadLimit := service.uploadGate.limit
	service.uploadGate.mu.Unlock()
	if downloadLimit != 10 || uploadLimit != 10 {
		t.Fatalf("media capacity limits = download:%d upload:%d, want both 10", downloadLimit, uploadLimit)
	}
}

func TestCleanupBacklogBlocksOfferAllowsShareInspect(t *testing.T) {
	queue := &fakeResultQueue{cleanupBacklog: 3}
	service := New(queue, nil)
	offer := protocol.JobOffer{JobType: model.ClusterJobTypeShareInspect}
	if err := service.cleanupBacklogBlocksOffer(context.Background(), offer); err != nil {
		t.Fatalf("share inspect should ignore cleanup backlog: %v", err)
	}
}

func TestCleanupBacklogBlocksOfferRejectsMediaTransfer(t *testing.T) {
	queue := &fakeResultQueue{cleanupBacklog: 2}
	service := New(queue, nil)
	offer := protocol.JobOffer{JobType: model.ClusterJobTypeMediaTransfer}
	if err := service.cleanupBacklogBlocksOffer(context.Background(), offer); err == nil {
		t.Fatal("expected media transfer to reject cleanup backlog")
	}
}

func testMediaTransferOffer(primary protocol.SourceObject, shareSaveObjects []protocol.SourceObject, shareSaveKey string) protocol.JobOffer {
	return protocol.JobOffer{
		AttemptRef:     protocol.AttemptRef{JobID: "job-" + primary.SourceFileID, AttemptID: "attempt-1", Generation: 1, LeaseToken: "lease"},
		IdempotencyKey: "operation-" + primary.SourceFileID,
		JobType:        model.ClusterJobTypeMediaTransfer,
		TaskContext: protocol.TaskContext{
			Share: protocol.ShareTaskContext{
				Provider: "pan115",
				URL:      "https://example.com/share",
				Passcode: "passcode",
			},
			SourceObjects:    []protocol.SourceObject{primary},
			ShareSaveKey:     shareSaveKey,
			ShareSaveObjects: shareSaveObjects,
			StagingTarget: protocol.ProviderTargetRequirement{
				Provider:           "pan115",
				StorageID:          42,
				NodeMountID:        "node-mount-42",
				AccountFingerprint: "account-42",
				NeedShareSave:      true,
			},
		},
	}
}

func testMediaTransferOfferWithoutBatch(sourceObjects []protocol.SourceObject) protocol.JobOffer {
	return protocol.JobOffer{
		AttemptRef:     protocol.AttemptRef{JobID: "job-no-batch", AttemptID: "attempt-1", Generation: 1, LeaseToken: "lease"},
		IdempotencyKey: "operation-no-batch",
		JobType:        model.ClusterJobTypeMediaTransfer,
		TaskContext: protocol.TaskContext{
			Share: protocol.ShareTaskContext{
				Provider: "pan115",
				URL:      "https://example.com/share",
				Passcode: "passcode",
			},
			SourceObjects: sourceObjects,
			StagingTarget: protocol.ProviderTargetRequirement{
				Provider:           "pan115",
				StorageID:          42,
				NodeMountID:        "node-mount-42",
				AccountFingerprint: "account-42",
				NeedShareSave:      true,
			},
		},
	}
}

func testStagedSourcePath(tempRoot string, object protocol.SourceObject) string {
	return path.Join(tempRoot, path.Base(strings.TrimSpace(object.SourceRelativePath)))
}

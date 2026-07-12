package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/resultqueue"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/stretchr/testify/require"
)

type fakeResultQueue struct {
	enqueueID  string
	enqueueErr error
	enqueued   any
	cleanup    resultqueue.CleanupRequest
	cleanupID  string
	ctxErr     error
	claimed    bool
	claimErr   error
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
func (*fakeResultQueue) CleanupBacklog(context.Context) (int64, error) { return 0, nil }

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

func TestNewCleanupRequestUsesExactIsolatedPath(t *testing.T) {
	manifest := validUploadManifest(t)
	request, err := NewCleanupRequest(manifest, "/mobile")
	require.NoError(t, err)
	require.Equal(t, "/mobile/.openlist-cluster/job-1/media-1/Episode.mkv", request.OpenListPath)
	require.Equal(t, "remote-1", request.RemoteFileID)
	require.True(t, request.EmptyRecycleBin)
}

func TestCancelActiveCancelsConnectionBoundTasks(t *testing.T) {
	service := New(nil, nil)
	ctx, cancel := context.WithCancelCause(context.Background())
	service.active["job-1"] = &activeTask{
		attempt: protocol.AttemptRef{JobID: "job-1", AttemptID: "attempt-1", Generation: 1, LeaseToken: "lease"},
		ctx:     ctx,
		cancel:  cancel,
	}
	want := errors.New("connection lost")

	service.CancelActive(want)

	require.ErrorIs(t, context.Cause(ctx), want)
}

func TestClusterTaskNamespaceIsolatedAndPathSafe(t *testing.T) {
	require.Equal(t, ".openlist-cluster/job-1/media-1", clusterTaskNamespace("job-1", "media-1"))
	namespace := clusterTaskNamespace("../job", "season/episode")
	require.NotContains(t, namespace, "..")
	require.NotContains(t, namespace, "season/episode")
	require.True(t, strings.HasPrefix(namespace, ".openlist-cluster/id-"))
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
			LogicalTargetPath: "/TV/Show/Season 01/Episode.mkv",
		},
		SourceObjects: []protocol.SourceObject{{Provider: "aliyundrive", SourceFileID: "file-1"}},
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
		MobileAccountBinding:  "/mobile",
		RemoteFileID:          "remote-1",
		RemotePath:            "/mobile/.openlist-cluster/job-1/media-1/Episode.mkv",
		Name:                  "Episode.mkv",
		Size:                  1024,
		SHA256:                strings.Repeat("A", 64),
		HashSource:            "mobile_provider_response",
	}
}

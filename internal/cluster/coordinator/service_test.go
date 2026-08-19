package coordinator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/transport"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

type testPeer struct {
	nodeID    string
	sessionID string
	sent      []protocol.Envelope
	sendErr   error
}

func (p *testPeer) NodeID() string {
	if p.nodeID == "" {
		return "worker-1"
	}
	return p.nodeID
}
func (p *testPeer) SessionID() string {
	if p.sessionID == "" {
		return "session-1"
	}
	return p.sessionID
}
func (p *testPeer) ConnectionEpoch() uint64 { return 1 }
func (p *testPeer) Send(_ context.Context, message protocol.Envelope) error {
	if p.sendErr != nil {
		return p.sendErr
	}
	p.sent = append(p.sent, message)
	return nil
}

var _ transport.Peer = (*testPeer)(nil)

func TestUploadManifestIsPersistedBeforeAcceptedAck(t *testing.T) {
	database := openCoordinatorTestDB(t)
	ctx := testTaskContext()
	ctx.Media.TMDBName = "Example"
	ctxHash, err := protocol.HashTaskContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(ctx, ctxHash, model.ClusterAttemptStatusAccepted)
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(testUploadStage(attempt)).Error; err != nil {
		t.Fatal(err)
	}
	manifest := testManifest(job, ctx, ctxHash)
	envelope, err := protocol.NewEnvelope(protocol.MessageUploadETFManifest, manifest)
	if err != nil {
		t.Fatal(err)
	}
	envelope.Seq = 2
	peer := &testPeer{}
	service := New(database, "")
	if err := service.HandleMessage(context.Background(), peer, *envelope); err != nil {
		t.Fatal(err)
	}
	var stored model.ClusterUploadManifest
	if err := database.First(&stored, "job_id = ?", job.ID).Error; err != nil {
		t.Fatalf("manifest was not persisted: %v", err)
	}
	if stored.TMDBName != ctx.Media.TMDBName {
		t.Fatalf("manifest TMDB name = %q, want %q", stored.TMDBName, ctx.Media.TMDBName)
	}
	if len(peer.sent) != 1 || peer.sent[0].Type != protocol.MessageUploadETFManifestAck {
		t.Fatalf("sent = %#v, want one manifest ack", peer.sent)
	}
	ack, err := protocol.DecodePayload[protocol.UploadETFManifestAck](peer.sent[0])
	if err != nil {
		t.Fatal(err)
	}
	if ack.Outcome != protocol.ManifestAckAccepted || ack.ManifestID != stored.ID {
		t.Fatalf("ack = %#v, stored id = %s", ack, stored.ID)
	}
}

func TestRequeueNodeAttemptsImmediatelyReleasesRestartedWorkerJobs(t *testing.T) {
	database := openCoordinatorTestDB(t)
	ctx := testTaskContext()
	ctxHash, err := protocol.HashTaskContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(ctx, ctxHash, model.ClusterAttemptStatusRunning)
	job.ID = "restart-job"
	job.IdempotencyKey = job.ID
	attempt.ID = "restart-attempt"
	attempt.JobID = job.ID
	attempt.NodeID = "restarted-worker"
	job.CurrentAttemptID = attempt.ID
	job.AssignedNodeID = attempt.NodeID
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	stage := testUploadStage(attempt)
	stage.ID = "restart-stage"
	stage.Status = model.ClusterStageStatusRunning
	if err := database.Create(stage).Error; err != nil {
		t.Fatal(err)
	}

	requeued, err := New(database, "token").RequeueNodeAttempts(context.Background(), attempt.NodeID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if requeued != 1 {
		t.Fatalf("requeued = %d, want 1", requeued)
	}

	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusQueued || job.AssignedNodeID != "" || job.CurrentAttemptID != "" {
		t.Fatalf("job after recovery = %#v, want queued without assignment", job)
	}
	if err := database.First(&attempt, "id = ?", attempt.ID).Error; err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.ClusterAttemptStatusLost || attempt.ErrorCode != "worker_restarted" {
		t.Fatalf("attempt after recovery = %#v, want lost worker_restarted", attempt)
	}
	if err := database.First(stage, "id = ?", stage.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stage.Status != model.ClusterStageStatusFailed || stage.ErrorCode != "worker_restarted" {
		t.Fatalf("stage after recovery = %#v, want failed worker_restarted", stage)
	}
}

func TestAuthenticateRequiresTokenAndRejectsDisabledNode(t *testing.T) {
	database := openCoordinatorTestDB(t)
	hello := protocol.Hello{NodeID: "worker-1", EnrollmentToken: "secret"}
	if err := New(database, "").Authenticate(context.Background(), nil, hello); err == nil {
		t.Fatal("empty coordinator token unexpectedly allowed authentication")
	}
	service := New(database, "secret")
	if err := service.Authenticate(context.Background(), nil, hello); err != nil {
		t.Fatalf("valid worker authentication failed: %v", err)
	}
	if err := database.Create(&model.ClusterNode{ID: "worker-1", Status: model.ClusterNodeStatusDisabled, Disabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	if err := service.Authenticate(context.Background(), nil, hello); err == nil {
		t.Fatal("disabled worker unexpectedly authenticated")
	}
}

func TestAuthenticateRequiresPinnedNodeKeyOnEveryReconnect(t *testing.T) {
	database := openCoordinatorTestDB(t)
	service := New(database, "secret")
	key := &protocol.NodeKeyAgreement{Algorithm: protocol.KeyAgreementX25519, KeyID: "key-1", PublicKey: "public-1"}
	hello := protocol.Hello{NodeID: "worker-pinned", NodeName: "worker-pinned", EnrollmentToken: "secret", KeyAgreement: key}
	if err := service.Authenticate(context.Background(), nil, hello); err != nil {
		t.Fatal(err)
	}
	hello.KeyAgreement = nil
	if err := service.Authenticate(context.Background(), nil, hello); err == nil {
		t.Fatal("pinned node reconnected without presenting its key")
	}
	hello.KeyAgreement = &protocol.NodeKeyAgreement{Algorithm: protocol.KeyAgreementX25519, KeyID: "key-2", PublicKey: "public-2"}
	if err := service.Authenticate(context.Background(), nil, hello); err == nil {
		t.Fatal("pinned node replaced its key without approval")
	}
}

func TestUploadManifestRejectsInvalidAttemptFencing(t *testing.T) {
	database := openCoordinatorTestDB(t)
	ctx := testTaskContext()
	ctxHash, err := protocol.HashTaskContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(ctx, ctxHash, model.ClusterAttemptStatusAccepted)
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(testUploadStage(attempt)).Error; err != nil {
		t.Fatal(err)
	}
	manifest := testManifest(job, ctx, ctxHash)
	manifest.LeaseToken = "forged-token"
	envelope, err := protocol.NewEnvelope(protocol.MessageUploadETFManifest, manifest)
	if err != nil {
		t.Fatal(err)
	}
	peer := &testPeer{}
	if err := New(database, "").HandleMessage(context.Background(), peer, *envelope); err != nil {
		t.Fatal(err)
	}
	ack, err := protocol.DecodePayload[protocol.UploadETFManifestAck](peer.sent[0])
	if err != nil {
		t.Fatal(err)
	}
	if ack.Outcome != protocol.ManifestAckContextMismatch || ack.ErrorCode != "attempt_fencing_failed" {
		t.Fatalf("ack = %#v", ack)
	}
	var count int64
	if err := database.Model(&model.ClusterUploadManifest{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("stored %d forged manifests", count)
	}
}

func TestUploadManifestAdoptsValidLostAttempt(t *testing.T) {
	database := openCoordinatorTestDB(t)
	ctx := testTaskContext()
	ctxHash, err := protocol.HashTaskContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(ctx, ctxHash, model.ClusterAttemptStatusLost)
	job.CurrentAttemptID = "attempt-2"
	job.CurrentGeneration = 2
	job.AssignedNodeID = "worker-2"
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(testUploadStage(attempt)).Error; err != nil {
		t.Fatal(err)
	}
	envelope, err := protocol.NewEnvelope(protocol.MessageUploadETFManifest, testManifest(job, ctx, ctxHash))
	if err != nil {
		t.Fatal(err)
	}
	peer := &testPeer{}
	if err := New(database, "").HandleMessage(context.Background(), peer, *envelope); err != nil {
		t.Fatal(err)
	}
	ack, err := protocol.DecodePayload[protocol.UploadETFManifestAck](peer.sent[0])
	if err != nil {
		t.Fatal(err)
	}
	if ack.Outcome != protocol.ManifestAckAdopted {
		t.Fatalf("outcome = %q, want adopted", ack.Outcome)
	}
}

func TestJobAcceptAndResultRequireCurrentLease(t *testing.T) {
	database := openCoordinatorTestDB(t)
	ctx := testTaskContext()
	ctxHash, err := protocol.HashTaskContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(ctx, ctxHash, model.ClusterAttemptStatusOffered)
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	service := New(database, "")
	peer := &testPeer{}
	badAccept, _ := protocol.NewEnvelope(protocol.MessageJobAccept, protocol.JobAccept{AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: 1, LeaseToken: "wrong"}})
	badAccept.Seq = 1
	if err := service.HandleMessage(context.Background(), peer, *badAccept); err == nil {
		t.Fatal("forged accept unexpectedly succeeded")
	}
	accept, _ := protocol.NewEnvelope(protocol.MessageJobAccept, protocol.JobAccept{AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: 1, LeaseToken: "lease"}})
	accept.Seq = 2
	if err := service.HandleMessage(context.Background(), peer, *accept); err != nil {
		t.Fatal(err)
	}
	badResult, _ := protocol.NewEnvelope(protocol.MessageJobResult, protocol.JobResult{AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: 1, LeaseToken: "wrong"}, Status: "succeeded", ResultHash: "result-1"})
	badResult.Seq = 3
	if err := service.HandleMessage(context.Background(), peer, *badResult); err == nil {
		t.Fatal("forged result unexpectedly succeeded")
	}
	result, _ := protocol.NewEnvelope(protocol.MessageJobResult, protocol.JobResult{AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: 1, LeaseToken: "lease"}, Status: "succeeded", ResultHash: "result-1"})
	result.Seq = 4
	if err := service.HandleMessage(context.Background(), peer, *result); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&attempt, "id = ?", attempt.ID).Error; err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.ClusterAttemptStatusSucceeded {
		t.Fatalf("attempt status = %q", attempt.Status)
	}
}

func TestJobRejectRequeuesRetryableCapacityFailure(t *testing.T) {
	database := openCoordinatorTestDB(t)
	ctx := testTaskContext()
	ctxHash, err := protocol.HashTaskContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(ctx, ctxHash, model.ClusterAttemptStatusOffered)
	job.Status = model.ClusterJobStatusLeased
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}

	reject, err := protocol.NewEnvelope(protocol.MessageJobReject, protocol.JobReject{
		AttemptRef: attemptRefForTest(attempt), Code: "worker_capacity_unavailable",
		Reason: "media concurrency limit is full", Retryable: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	reject.Seq = 1
	if err := New(database, "").HandleMessage(context.Background(), &testPeer{}, *reject); err != nil {
		t.Fatal(err)
	}

	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusQueued || job.AssignedNodeID != "" || job.CurrentAttemptID != "" || job.LastErrorCode != "worker_capacity_unavailable" {
		t.Fatalf("job after reject = %#v", job)
	}
	if !job.AvailableAt.After(time.Now().UTC()) {
		t.Fatalf("job available_at = %s, want backoff in the future", job.AvailableAt)
	}
	if err := database.First(&attempt, "id = ?", attempt.ID).Error; err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.ClusterAttemptStatusRejected || attempt.ErrorCode != "worker_capacity_unavailable" {
		t.Fatalf("attempt after reject = %#v", attempt)
	}
}

func TestJobRejectDeadLettersAfterMediaAttemptLimit(t *testing.T) {
	database := openCoordinatorTestDB(t)
	ctx := testTaskContext()
	ctxHash, err := protocol.HashTaskContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(ctx, ctxHash, model.ClusterAttemptStatusOffered)
	job.CurrentGeneration = automaticMediaTransferAttemptLimit
	attempt.Generation = automaticMediaTransferAttemptLimit
	attempt.LeaseTokenHash = fmt.Sprintf("%x", sha256.Sum256([]byte("lease")))
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	reject, err := protocol.NewEnvelope(protocol.MessageJobReject, protocol.JobReject{
		AttemptRef: attemptRefForTest(attempt), Code: "worker_capacity_unavailable", Retryable: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	reject.Seq = 1
	if err := New(database, "").HandleMessage(context.Background(), &testPeer{}, *reject); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusDeadLetter || job.LastErrorCode != "retry_limit_exceeded" || job.FinishedAt == nil {
		t.Fatalf("job after exhausted rejection = %#v", job)
	}
}

func TestSweepStalledAttemptsRequeuesAcceptedMediaWithoutStages(t *testing.T) {
	database := openCoordinatorTestDB(t)
	ctx := testTaskContext()
	ctxHash, err := protocol.HashTaskContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(ctx, ctxHash, model.ClusterAttemptStatusAccepted)
	acceptedAt := time.Now().UTC().Add(-20 * time.Minute)
	attempt.AcceptedAt = &acceptedAt
	job.Status = model.ClusterJobStatusRunning
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}

	affected, err := New(database, "").SweepStalledAttempts(context.Background(), time.Now().UTC(), 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if affected != 1 {
		t.Fatalf("affected = %d", affected)
	}
	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusQueued || job.AssignedNodeID != "" || job.CurrentAttemptID != "" || job.LastErrorCode != "worker_start_timeout" {
		t.Fatalf("job after stalled sweep = %#v", job)
	}
	if err := database.First(&attempt, "id = ?", attempt.ID).Error; err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.ClusterAttemptStatusLost || attempt.ErrorCode != "worker_start_timeout" {
		t.Fatalf("attempt after stalled sweep = %#v", attempt)
	}
}

func TestSweepStalledAttemptsRequeuesShareInspectWithoutManifest(t *testing.T) {
	database := openCoordinatorTestDB(t)
	ctx := testTaskContext()
	ctxHash, err := protocol.HashTaskContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(ctx, ctxHash, model.ClusterAttemptStatusAccepted)
	job.Type = model.ClusterJobTypeShareInspect
	acceptedAt := time.Now().UTC().Add(-40 * time.Minute)
	attempt.AcceptedAt = &acceptedAt
	job.Status = model.ClusterJobStatusRunning
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}

	affected, err := New(database, "").SweepStalledAttempts(context.Background(), time.Now().UTC(), 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if affected != 1 {
		t.Fatalf("affected = %d", affected)
	}
	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusQueued || job.AssignedNodeID != "" || job.CurrentAttemptID != "" || job.LastErrorCode != "share_inspect_timeout" {
		t.Fatalf("job after stalled inspect sweep = %#v", job)
	}
	if err := database.First(&attempt, "id = ?", attempt.ID).Error; err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.ClusterAttemptStatusLost || attempt.ErrorCode != "share_inspect_timeout" {
		t.Fatalf("attempt after stalled inspect sweep = %#v", attempt)
	}
}

func TestSweepStalledAttemptsKeepsShareInspectWithManifest(t *testing.T) {
	database := openCoordinatorTestDB(t)
	ctx := testTaskContext()
	ctxHash, err := protocol.HashTaskContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(ctx, ctxHash, model.ClusterAttemptStatusAccepted)
	job.Type = model.ClusterJobTypeShareInspect
	acceptedAt := time.Now().UTC().Add(-40 * time.Minute)
	attempt.AcceptedAt = &acceptedAt
	job.Status = model.ClusterJobStatusRunning
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterShareInspectManifest{
		ID: "inspect-manifest-1", JobID: job.ID, AttemptID: attempt.ID,
		Generation: attempt.Generation, Status: model.ClusterShareInspectStatusPending,
	}).Error; err != nil {
		t.Fatal(err)
	}

	affected, err := New(database, "").SweepStalledAttempts(context.Background(), time.Now().UTC(), 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if affected != 0 {
		t.Fatalf("affected = %d, want 0", affected)
	}
	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusRunning {
		t.Fatalf("job status = %q, want running", job.Status)
	}
}

func TestFailedMediaResultConvergesNotificationAndActiveStages(t *testing.T) {
	database := openCoordinatorTestDB(t)
	task := testTaskContext()
	contextHash, err := protocol.HashTaskContext(task)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(task, contextHash, model.ClusterAttemptStatusRunning)
	job.SubscriptionItemID = 0
	job.NotificationStatus = model.ClusterNotificationStatusPending
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	stage := &model.ClusterJobStage{
		ID: "upload-permitted", JobID: job.ID, AttemptID: attempt.ID,
		Name: model.ClusterStageUploadingMobile, Status: model.ClusterStageStatusPermitted,
	}
	if err := database.Create(stage).Error; err != nil {
		t.Fatal(err)
	}
	result, _ := protocol.NewEnvelope(protocol.MessageJobResult, protocol.JobResult{
		AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: 1, LeaseToken: "lease"},
		Status:     "failed", ErrorCode: "unexpected_eof", Error: "unexpected EOF", FinishedAt: time.Now().UTC(),
	})
	result.Seq = 1
	if err := New(database, "").HandleMessage(context.Background(), &testPeer{}, *result); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusFailed || job.NotificationStatus != model.ClusterNotificationStatusNotStarted {
		t.Fatalf("job terminal state = %#v", job)
	}
	if err := database.First(stage, "id = ?", stage.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stage.Status != model.ClusterStageStatusFailed || stage.ErrorCode != "unexpected_eof" || stage.Error != "unexpected EOF" || stage.FinishedAt == nil {
		t.Fatalf("stage terminal state = %#v", stage)
	}
}

func TestRetryableMediaResultQueuesAnotherAttempt(t *testing.T) {
	database := openCoordinatorTestDB(t)
	task := testTaskContext()
	contextHash, err := protocol.HashTaskContext(task)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(task, contextHash, model.ClusterAttemptStatusRunning)
	job.SubscriptionItemID = 0
	job.NotificationStatus = model.ClusterNotificationStatusPending
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	stage := &model.ClusterJobStage{
		ID: "upload-short-read", JobID: job.ID, AttemptID: attempt.ID,
		Name: model.ClusterStageUploadingMobile, Status: model.ClusterStageStatusRunning,
	}
	if err := database.Create(stage).Error; err != nil {
		t.Fatal(err)
	}
	finishedAt := time.Now().UTC()
	result, _ := protocol.NewEnvelope(protocol.MessageJobResult, protocol.JobResult{
		AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: 1, LeaseToken: "lease"},
		Status:     "failed", ErrorCode: "source_unexpected_eof", Error: "source stream ended early", FinishedAt: finishedAt,
	})
	result.Seq = 1
	if err := New(database, "").HandleMessage(context.Background(), &testPeer{}, *result); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusQueued || job.AssignedNodeID != "" || job.CurrentAttemptID != "" || job.FinishedAt != nil {
		t.Fatalf("retryable job = %#v", job)
	}
	if !job.AvailableAt.Equal(finishedAt.Add(15 * time.Second)) {
		t.Fatalf("available at = %v, want %v", job.AvailableAt, finishedAt.Add(15*time.Second))
	}
	if job.NotificationStatus != model.ClusterNotificationStatusNotStarted {
		t.Fatalf("notification status = %q, want not_started", job.NotificationStatus)
	}
	if err := database.First(&attempt, "id = ?", attempt.ID).Error; err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.ClusterAttemptStatusFailed {
		t.Fatalf("attempt status = %q, want failed", attempt.Status)
	}
}

func TestRetryableMediaResultStopsAfterAttemptLimit(t *testing.T) {
	database := openCoordinatorTestDB(t)
	task := testTaskContext()
	contextHash, err := protocol.HashTaskContext(task)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(task, contextHash, model.ClusterAttemptStatusRunning)
	job.SubscriptionItemID = 0
	job.CurrentGeneration = automaticMediaTransferAttemptLimit
	attempt.Generation = automaticMediaTransferAttemptLimit
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	result, _ := protocol.NewEnvelope(protocol.MessageJobResult, protocol.JobResult{
		AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: automaticMediaTransferAttemptLimit, LeaseToken: "lease"},
		Status:     "failed", ErrorCode: "source_unexpected_eof", Error: "source stream ended early", FinishedAt: time.Now().UTC(),
	})
	result.Seq = 1
	if err := New(database, "").HandleMessage(context.Background(), &testPeer{}, *result); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusFailed || job.FinishedAt == nil {
		t.Fatalf("exhausted retry job = %#v", job)
	}
}

func TestPermanentShareCredentialFailureDoesNotQueueAnotherAttempt(t *testing.T) {
	database := openCoordinatorTestDB(t)
	task := testTaskContext()
	contextHash, err := protocol.HashTaskContext(task)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(task, contextHash, model.ClusterAttemptStatusRunning)
	job.SubscriptionItemID = 0
	job.NotificationStatus = model.ClusterNotificationStatusPending
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	stage := &model.ClusterJobStage{
		ID: "upload-invalid-credential", JobID: job.ID, AttemptID: attempt.ID,
		Name: model.ClusterStageUploadingMobile, Status: model.ClusterStageStatusRunning,
	}
	if err := database.Create(stage).Error; err != nil {
		t.Fatal(err)
	}
	finishedAt := time.Now().UTC()
	result, _ := protocol.NewEnvelope(protocol.MessageJobResult, protocol.JobResult{
		AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: 1, LeaseToken: "lease"},
		Status:     "failed", ErrorCode: "share_save_credentials_invalid", Error: "refresh_token无效", FinishedAt: finishedAt,
	})
	result.Seq = 1
	if err := New(database, "").HandleMessage(context.Background(), &testPeer{}, *result); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusFailed || job.FinishedAt == nil || job.CurrentAttemptID != attempt.ID {
		t.Fatalf("permanent failure job = %#v", job)
	}
	if !job.FinishedAt.Equal(finishedAt) {
		t.Fatalf("finished at = %v, want %v", job.FinishedAt, finishedAt)
	}
	if err := database.First(&attempt, "id = ?", attempt.ID).Error; err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.ClusterAttemptStatusFailed || attempt.ErrorCode != "share_save_credentials_invalid" {
		t.Fatalf("attempt = %#v", attempt)
	}
	if err := database.First(stage, "id = ?", stage.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stage.Status != model.ClusterStageStatusFailed || stage.ErrorCode != "share_save_credentials_invalid" || stage.Error != "refresh_token无效" {
		t.Fatalf("stage = %#v", stage)
	}
}

func TestDuplicateMediaResultDoesNotRegressMaterializedJob(t *testing.T) {
	database := openCoordinatorTestDB(t)
	task := testTaskContext()
	contextHash, err := protocol.HashTaskContext(task)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(task, contextHash, model.ClusterAttemptStatusSucceeded)
	job.Status = model.ClusterJobStatusSucceeded
	job.SubscriptionItemID = 0
	attempt.ResultHash = "result-already-delivered"
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	result, _ := protocol.NewEnvelope(protocol.MessageJobResult, protocol.JobResult{
		AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: 1, LeaseToken: "lease"},
		Status:     "succeeded", ResultHash: attempt.ResultHash, FinishedAt: time.Now().UTC(),
	})
	result.Seq = 1
	if err := New(database, "").HandleMessage(context.Background(), &testPeer{}, *result); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusSucceeded {
		t.Fatalf("duplicate result regressed job to %q", job.Status)
	}
}

func TestShareInspectResultIsSealedBeforeJobSucceeds(t *testing.T) {
	database := openCoordinatorTestDB(t)
	task := protocol.TaskContext{
		WorkflowVersion: "subscription-share-inspect/v1", SealedManifestVersion: "share-inspect/v1",
		Subscription: protocol.SubscriptionTaskContext{SubscriptionID: 42},
		Share:        protocol.ShareTaskContext{Provider: "aliyun_drive", URL: "https://www.alipan.com/s/example"},
	}
	contextHash, err := protocol.HashTaskContext(task)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(task, contextHash, model.ClusterAttemptStatusAccepted)
	job.Type = model.ClusterJobTypeShareInspect
	taskJSON, _ := json.Marshal(task)
	job.TaskContextJSON = string(taskJSON)
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	objects := []protocol.SourceObject{{Provider: "aliyun_drive", SourceFileID: "file-1", SourceRelativePath: "S01E01.mkv", Size: 1024}}
	objectsJSON, _ := json.Marshal(objects)
	manifest := protocol.ShareInspectManifest{
		Version: task.SealedManifestVersion, Share: task.Share, CanonicalRef: "aliyun_drive:example",
		Objects: objects, ObjectHash: fmt.Sprintf("%x", sha256.Sum256(objectsJSON)), InspectedAt: time.Now().UTC(),
	}
	manifestJSON, _ := json.Marshal(manifest)
	var resultPayload map[string]any
	_ = json.Unmarshal(manifestJSON, &resultPayload)
	result, _ := protocol.NewEnvelope(protocol.MessageJobResult, protocol.JobResult{
		AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: 1, LeaseToken: "lease"},
		Status:     "succeeded", Result: resultPayload, FinishedAt: time.Now().UTC(),
	})
	result.Seq = 1
	service := New(database, "")
	if err := service.HandleMessage(context.Background(), &testPeer{}, *result); err != nil {
		t.Fatal(err)
	}
	stored, err := service.ShareInspectManifest(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Version != task.SealedManifestVersion || stored.ObjectHash != manifest.ObjectHash || stored.Status != model.ClusterShareInspectStatusPending {
		t.Fatalf("stored inspect manifest = %#v", stored)
	}
	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusSucceeded || job.FinishedAt == nil {
		t.Fatalf("inspect job = %#v", job)
	}
	called := false
	service.SetShareInspectConsumer(func(_ context.Context, record model.ClusterShareInspectManifest, decoded protocol.ShareInspectManifest) error {
		called = record.JobID == job.ID && decoded.ObjectHash == manifest.ObjectHash
		return nil
	})
	if consumed, err := service.ProcessPendingShareInspects(context.Background(), 10); err != nil || consumed != 1 || !called {
		t.Fatalf("consumed=%d called=%v err=%v", consumed, called, err)
	}
}

func TestShareInspectResultRejectsInvalidObjectHash(t *testing.T) {
	database := openCoordinatorTestDB(t)
	task := protocol.TaskContext{WorkflowVersion: "inspect/v1", SealedManifestVersion: "share-inspect/v1", Share: protocol.ShareTaskContext{Provider: "aliyun_drive", URL: "https://www.alipan.com/s/example"}}
	contextHash, _ := protocol.HashTaskContext(task)
	job, attempt := testJobAndAttempt(task, contextHash, model.ClusterAttemptStatusAccepted)
	job.Type = model.ClusterJobTypeShareInspect
	taskJSON, _ := json.Marshal(task)
	job.TaskContextJSON = string(taskJSON)
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	manifest := protocol.ShareInspectManifest{Version: task.SealedManifestVersion, Share: task.Share, ObjectHash: strings.Repeat("0", 64)}
	raw, _ := json.Marshal(manifest)
	var payload map[string]any
	_ = json.Unmarshal(raw, &payload)
	message, _ := protocol.NewEnvelope(protocol.MessageJobResult, protocol.JobResult{AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: 1, LeaseToken: "lease"}, Status: "succeeded", Result: payload})
	if err := New(database, "").HandleMessage(context.Background(), &testPeer{}, *message); err == nil {
		t.Fatal("invalid share inspection object hash was accepted")
	}
	var count int64
	if err := database.Model(&model.ClusterShareInspectManifest{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("stored invalid manifests=%d err=%v", count, err)
	}
}

func TestProcessPendingShareInspectsLetsLaterRecordsProceedAfterAnError(t *testing.T) {
	database := openCoordinatorTestDB(t)
	now := time.Now().UTC()
	createRecord := func(id string, createdAt time.Time) {
		payload, err := json.Marshal(protocol.ShareInspectManifest{Version: "share-inspect/v1"})
		if err != nil {
			t.Fatal(err)
		}
		if err := database.Create(&model.ClusterShareInspectManifest{
			ID: id, JobID: id, SubscriptionID: 1, ObservationKey: id,
			ObservationExpected: 1, PayloadJSON: string(payload), Status: model.ClusterShareInspectStatusPending,
			CreatedAt: createdAt, UpdatedAt: createdAt,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	createRecord("old-pending", now.Add(-time.Minute))
	createRecord("new-pending", now.Add(-10*time.Second).In(time.FixedZone("CST", 8*60*60)))

	processed := make([]string, 0, 2)
	service := New(database, "")
	service.SetShareInspectConsumer(func(_ context.Context, record model.ClusterShareInspectManifest, _ protocol.ShareInspectManifest) error {
		processed = append(processed, record.ID)
		if record.ID == "old-pending" {
			return fmt.Errorf("temporary consumer error")
		}
		return nil
	})

	if consumed, err := service.ProcessPendingShareInspects(context.Background(), 1); err != nil || consumed != 0 {
		t.Fatalf("first process consumed=%d err=%v, want the old record to be retried", consumed, err)
	}
	if consumed, err := service.ProcessPendingShareInspects(context.Background(), 1); err != nil || consumed != 1 {
		t.Fatalf("second process consumed=%d err=%v processed=%v, want the later record to proceed", consumed, err, processed)
	}
	if got := strings.Join(processed, ","); got != "old-pending,new-pending" {
		t.Fatalf("processed records = %q, want old then new", got)
	}
}

func TestShareInspectFailurePersistsEmptyManifest(t *testing.T) {
	database := openCoordinatorTestDB(t)
	task := protocol.TaskContext{
		WorkflowVersion: "subscription-share-inspect/v1", SealedManifestVersion: "share-inspect/v1",
		Subscription: protocol.SubscriptionTaskContext{SubscriptionID: 42, ObservationKey: "obs-1", ObservationExpected: 2},
		Share:        protocol.ShareTaskContext{Provider: "quark", URL: "https://pan.quark.cn/s/dead"},
	}
	contextHash, err := protocol.HashTaskContext(task)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(task, contextHash, model.ClusterAttemptStatusAccepted)
	job.Type = model.ClusterJobTypeShareInspect
	taskJSON, _ := json.Marshal(task)
	job.TaskContextJSON = string(taskJSON)
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	result, _ := protocol.NewEnvelope(protocol.MessageJobResult, protocol.JobResult{
		AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: 1, LeaseToken: "lease"},
		Status:     "failed", Error: "分享地址已失效", FinishedAt: time.Now().UTC(),
	})
	result.Seq = 1
	service := New(database, "")
	if err := service.HandleMessage(context.Background(), &testPeer{}, *result); err != nil {
		t.Fatal(err)
	}
	stored, err := service.ShareInspectManifest(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != model.ClusterShareInspectStatusPending || stored.LastError != "分享地址已失效" {
		t.Fatalf("stored failed inspect manifest = %#v", stored)
	}
	if stored.ObjectHash == "" || stored.PayloadJSON == "" {
		t.Fatalf("stored failed inspect manifest payload = %#v", stored)
	}
	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusFailed {
		t.Fatalf("inspect job status = %q", job.Status)
	}
}

func TestReplayOutboxAndAck(t *testing.T) {
	database := openCoordinatorTestDB(t)
	now := time.Now().UTC()
	outbox := model.ClusterOutbox{
		ID: "outbox-1", MessageID: "message-1", PeerNodeID: "worker-1", Seq: 1,
		MessageType: string(protocol.MessageJobOffer), PayloadJSON: `{}`, PayloadHash: "hash",
		Status: model.ClusterMessageStatusPending, AvailableAt: now.Add(-time.Minute),
	}
	if err := database.Create(&outbox).Error; err != nil {
		t.Fatal(err)
	}
	peer := &testPeer{sessionID: "session-reconnected"}
	service := New(database, "")
	if err := service.ReplayOutbox(context.Background(), peer); err != nil {
		t.Fatal(err)
	}
	if len(peer.sent) != 1 || peer.sent[0].MessageID != outbox.MessageID || peer.sent[0].Seq != 0 {
		t.Fatalf("replayed = %#v", peer.sent)
	}
	if err := database.First(&outbox, "id = ?", outbox.ID).Error; err != nil {
		t.Fatal(err)
	}
	if outbox.Status != model.ClusterMessageStatusSending || outbox.AttemptCount != 1 || outbox.SessionID != peer.SessionID() {
		t.Fatalf("outbox after replay = %#v", outbox)
	}
	ackEnvelope, _ := protocol.NewEnvelope(protocol.MessageAck, protocol.Ack{MessageID: outbox.MessageID, AckSeq: 1})
	if err := service.HandleMessage(context.Background(), peer, *ackEnvelope); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&outbox, "id = ?", outbox.ID).Error; err != nil {
		t.Fatal(err)
	}
	if outbox.Status != model.ClusterMessageStatusAcked || outbox.AckedAt == nil {
		t.Fatalf("outbox after ack = %#v", outbox)
	}
}

func TestSweepExpiredLeasesRequeuesOnlyCurrentJob(t *testing.T) {
	database := openCoordinatorTestDB(t)
	ctx := testTaskContext()
	ctxHash, err := protocol.HashTaskContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(ctx, ctxHash, model.ClusterAttemptStatusAccepted)
	attempt.LeaseUntil = time.Now().UTC().Add(-time.Minute)
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	affected, err := New(database, "").SweepExpiredLeases(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if affected != 1 {
		t.Fatalf("affected = %d", affected)
	}
	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusQueued || job.AssignedNodeID != "" || job.CurrentAttemptID != "" {
		t.Fatalf("job after sweep = %#v", job)
	}
	if err := database.First(&attempt, "id = ?", attempt.ID).Error; err != nil {
		t.Fatal(err)
	}
	if attempt.Status != model.ClusterAttemptStatusLost || attempt.ErrorCode != "lease_expired" {
		t.Fatalf("attempt after sweep = %#v", attempt)
	}
}

func TestLeaseRenewExtendsCurrentAttempt(t *testing.T) {
	database := openCoordinatorTestDB(t)
	taskContext := testTaskContext()
	contextHash, err := protocol.HashTaskContext(taskContext)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(taskContext, contextHash, model.ClusterAttemptStatusAccepted)
	attempt.LeaseUntil = time.Now().UTC().Add(time.Minute)
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	requestedUntil := time.Now().UTC().Add(10 * time.Minute)
	message, err := protocol.NewEnvelope(protocol.MessageLeaseRenew, protocol.LeaseRenew{
		AttemptRef:     protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: attempt.Generation, LeaseToken: "lease"},
		RequestedUntil: requestedUntil,
		LastEventSeq:   3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := New(database, "token").HandleMessage(context.Background(), &testPeer{}, *message); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&attempt, "id = ?", attempt.ID).Error; err != nil {
		t.Fatal(err)
	}
	if attempt.LeaseUntil.Before(requestedUntil.Add(-time.Second)) || attempt.LastEventSeq != 3 || attempt.Status != model.ClusterAttemptStatusRunning {
		t.Fatalf("attempt after renewal = %#v", attempt)
	}
}

func TestStagePermitRequiresCurrentLeasedAttempt(t *testing.T) {
	database := openCoordinatorTestDB(t)
	taskContext := testTaskContext()
	contextHash, err := protocol.HashTaskContext(taskContext)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(taskContext, contextHash, model.ClusterAttemptStatusAccepted)
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	request, err := protocol.NewEnvelope(protocol.MessageStagePermitRequest, protocol.StagePermitRequest{
		AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: attempt.Generation, LeaseToken: "lease"},
		Stage:      model.ClusterStageUploadingMobile, OperationKey: job.IdempotencyKey + ":" + model.ClusterStageUploadingMobile,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.Seq = 1
	peer := &testPeer{}
	if err := New(database, "token").HandleMessage(context.Background(), peer, *request); err != nil {
		t.Fatal(err)
	}
	if len(peer.sent) != 1 || peer.sent[0].Type != protocol.MessageStagePermit || peer.sent[0].CorrelationID != request.MessageID {
		t.Fatalf("stage permit response=%#v", peer.sent)
	}
	permit, err := protocol.DecodePayload[protocol.StagePermit](peer.sent[0])
	if err != nil {
		t.Fatal(err)
	}
	if permit.PermitToken == "" || permit.Stage != model.ClusterStageUploadingMobile || !permit.PermitExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("permit=%#v", permit)
	}
	var stage model.ClusterJobStage
	if err := database.First(&stage, "attempt_id = ? AND name = ?", attempt.ID, model.ClusterStageUploadingMobile).Error; err != nil {
		t.Fatal(err)
	}
	if stage.PermitTokenHash == "" || stage.Status != model.ClusterStageStatusPermitted {
		t.Fatalf("stage=%#v", stage)
	}
}

func TestStageStatusUpdatesCurrentAttemptAndRejectsStaleAttempt(t *testing.T) {
	database := openCoordinatorTestDB(t)
	taskContext := testTaskContext()
	contextHash, err := protocol.HashTaskContext(taskContext)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(taskContext, contextHash, model.ClusterAttemptStatusAccepted)
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	stage := model.ClusterJobStage{ID: "stage-save", JobID: job.ID, AttemptID: attempt.ID, Name: model.ClusterStageSavingShare, Status: model.ClusterStageStatusPermitted}
	if err := database.Create(&stage).Error; err != nil {
		t.Fatal(err)
	}
	service := New(database, "token")
	running, err := protocol.NewEnvelope(protocol.MessageStageStatus, protocol.StageStatusUpdate{
		AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: attempt.Generation, LeaseToken: "lease"},
		Stage:      model.ClusterStageSavingShare, Status: model.ClusterStageStatusRunning,
	})
	if err != nil {
		t.Fatal(err)
	}
	running.Seq = 1
	if err := service.HandleMessage(context.Background(), &testPeer{}, *running); err != nil {
		t.Fatal(err)
	}
	message, err := protocol.NewEnvelope(protocol.MessageStageStatus, protocol.StageStatusUpdate{AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: attempt.Generation, LeaseToken: "lease"}, Stage: model.ClusterStageSavingShare, Status: model.ClusterStageStatusSucceeded})
	if err != nil {
		t.Fatal(err)
	}
	message.Seq = 2
	if err := service.HandleMessage(context.Background(), &testPeer{}, *message); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&stage, "id = ?", stage.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stage.Status != model.ClusterStageStatusSucceeded || stage.FinishedAt == nil {
		t.Fatalf("updated stage=%#v", stage)
	}
	stale := *message
	stale.MessageID = "stale-stage-status"
	stale.Payload, _ = json.Marshal(protocol.StageStatusUpdate{AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: "old-attempt", Generation: 1, LeaseToken: "lease"}, Stage: model.ClusterStageSavingShare, Status: model.ClusterStageStatusFailed, Error: "stale"})
	stale.Seq = 3
	if err := service.HandleMessage(context.Background(), &testPeer{}, stale); err == nil {
		t.Fatal("stale stage status unexpectedly accepted")
	}
}

func TestWorkerCleanupStageTracksRetriesAndJobStatus(t *testing.T) {
	database := openCoordinatorTestDB(t)
	taskContext := testTaskContext()
	contextHash, err := protocol.HashTaskContext(taskContext)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(taskContext, contextHash, model.ClusterAttemptStatusAccepted)
	job.WorkerCleanupStatus = model.ClusterCleanupStatusPending
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	service := New(database, "token")
	peer := &testPeer{}
	send := func(sequence uint64, status, stageError string) {
		t.Helper()
		message, err := protocol.NewEnvelope(protocol.MessageStageStatus, protocol.StageStatusUpdate{
			AttemptRef: protocol.AttemptRef{JobID: job.ID, AttemptID: attempt.ID, Generation: attempt.Generation, LeaseToken: "lease"},
			Stage:      model.ClusterStageWorkerMediaCleanup, Status: status, Error: stageError,
		})
		if err != nil {
			t.Fatal(err)
		}
		message.Seq = sequence
		if err := service.HandleMessage(context.Background(), peer, *message); err != nil {
			t.Fatal(err)
		}
	}
	send(1, model.ClusterStageStatusRunning, "")
	send(2, model.ClusterStageStatusFailed, "temporary provider error")
	send(3, model.ClusterStageStatusRunning, "")
	send(4, model.ClusterStageStatusSucceeded, "")

	var storedJob model.ClusterJob
	if err := database.First(&storedJob, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedJob.WorkerCleanupStatus != model.ClusterCleanupStatusSucceeded {
		t.Fatalf("cleanup status = %q", storedJob.WorkerCleanupStatus)
	}
	var stage model.ClusterJobStage
	if err := database.Where("attempt_id = ? AND name = ?", attempt.ID, model.ClusterStageWorkerMediaCleanup).First(&stage).Error; err != nil {
		t.Fatal(err)
	}
	if stage.Status != model.ClusterStageStatusSucceeded || stage.FinishedAt == nil {
		t.Fatalf("cleanup stage = %#v", stage)
	}
}

func TestArchiveAndRetryFailedJobs(t *testing.T) {
	database := openCoordinatorTestDB(t)
	job := model.ClusterJob{ID: "failed-job", IdempotencyKey: "failed-job", Status: model.ClusterJobStatusFailed}
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	service := New(database, "token")
	archived, err := service.ArchiveFailedJobs(context.Background())
	if err != nil || archived != 1 {
		t.Fatalf("archived=%d err=%v", archived, err)
	}
	jobs, err := service.ListJobs(context.Background(), "", false, 10)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("visible jobs=%#v err=%v", jobs, err)
	}
	if err := service.RetryJob(context.Background(), job.ID); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusQueued || job.ArchivedAt != nil || job.CurrentAttemptID != "" {
		t.Fatalf("retried job=%#v", job)
	}
}

func TestRetryFailedSubscriptionItemsRequeuesJobsAndRestoresLinks(t *testing.T) {
	database := openCoordinatorTestDB(t)
	if err := database.AutoMigrate(&model.Subscription{}, &model.SubscriptionItem{}, &model.SubscriptionEpisodeSource{}); err != nil {
		t.Fatal(err)
	}
	subscription := model.Subscription{ID: 71, Name: "Retry subscription"}
	if err := database.Create(&subscription).Error; err != nil {
		t.Fatal(err)
	}
	items := []model.SubscriptionItem{
		{ID: 701, SubscriptionID: subscription.ID, SourceKey: "historical-failure", Status: model.SubscriptionItemStatusFailed, Season: 1, Episode: 1, LastError: "worker failed"},
		{ID: 702, SubscriptionID: subscription.ID, SourceKey: "stale-pending", Status: model.SubscriptionItemStatusPending, Season: 1, Episode: 2},
		{ID: 703, SubscriptionID: subscription.ID, SourceKey: "live-failure", Status: model.SubscriptionItemStatusFailed, ClusterJobID: "live-job", LastError: "late callback"},
		{ID: 704, SubscriptionID: subscription.ID, SourceKey: "unmatched-failure", Status: model.SubscriptionItemStatusFailed, LastError: "no durable job"},
		{ID: 705, SubscriptionID: subscription.ID, SourceKey: "transferred", Status: model.SubscriptionItemStatusTransferred, ClusterJobID: "done-job"},
	}
	for i := range items {
		if err := database.Create(&items[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, source := range []model.SubscriptionEpisodeSource{
		{SubscriptionID: subscription.ID, Season: 1, Episode: 1, SourceItemID: 701, Status: model.SubscriptionItemStatusFailed, ClusterJobID: "historical-job"},
		{SubscriptionID: subscription.ID, Season: 1, Episode: 2, SourceItemID: 702, Status: model.SubscriptionItemStatusPending},
	} {
		if err := database.Create(&source).Error; err != nil {
			t.Fatal(err)
		}
	}
	jobs := []model.ClusterJob{
		{ID: "historical-job", IdempotencyKey: "historical-job", Type: model.ClusterJobTypeMediaTransfer, Status: model.ClusterJobStatusFailed, SubscriptionID: subscription.ID, SubscriptionItemID: 701},
		{ID: "pending-job", IdempotencyKey: "pending-job", Type: model.ClusterJobTypeMediaTransfer, Status: model.ClusterJobStatusDeadLetter, SubscriptionID: subscription.ID, SubscriptionItemID: 702},
		{ID: "live-job", IdempotencyKey: "live-job", Type: model.ClusterJobTypeMediaTransfer, Status: model.ClusterJobStatusRunning, SubscriptionID: subscription.ID, SubscriptionItemID: 703},
		{ID: "done-job", IdempotencyKey: "done-job", Type: model.ClusterJobTypeMediaTransfer, Status: model.ClusterJobStatusSucceeded, SubscriptionID: subscription.ID, SubscriptionItemID: 705},
	}
	for i := range jobs {
		if err := database.Create(&jobs[i]).Error; err != nil {
			t.Fatal(err)
		}
	}

	result, err := New(database, "token").RetryFailedSubscriptionItems(context.Background(), subscription.ID)
	if err != nil {
		t.Fatalf("retry failed subscription items: %v", err)
	}
	if result.Requeued != 2 || result.Unmatched != 1 {
		t.Fatalf("retry result = %#v, want requeued=2 unmatched=1", result)
	}

	for _, itemID := range []uint{701, 702} {
		var item model.SubscriptionItem
		if err := database.First(&item, "id = ?", itemID).Error; err != nil {
			t.Fatal(err)
		}
		if item.Status != model.SubscriptionItemStatusPending || item.ClusterJobID == "" || item.LastError != "" {
			t.Fatalf("requeued item %d = %#v", itemID, item)
		}
		var source model.SubscriptionEpisodeSource
		if err := database.Where("subscription_id = ? AND source_item_id = ?", subscription.ID, itemID).First(&source).Error; err != nil {
			t.Fatal(err)
		}
		if source.Status != model.SubscriptionItemStatusPending || source.ClusterJobID != item.ClusterJobID {
			t.Fatalf("requeued source %d = %#v", itemID, source)
		}
	}
	var unmatched model.SubscriptionItem
	if err := database.First(&unmatched, "id = ?", 704).Error; err != nil {
		t.Fatal(err)
	}
	if unmatched.Status != model.SubscriptionItemStatusFailed || unmatched.LastError != "no durable job" {
		t.Fatalf("unmatched item = %#v", unmatched)
	}
	var live model.SubscriptionItem
	if err := database.First(&live, "id = ?", 703).Error; err != nil {
		t.Fatal(err)
	}
	if live.Status != model.SubscriptionItemStatusFailed || live.ClusterJobID != "live-job" {
		t.Fatalf("live item changed = %#v", live)
	}
	for _, jobID := range []string{"historical-job", "pending-job"} {
		var job model.ClusterJob
		if err := database.First(&job, "id = ?", jobID).Error; err != nil {
			t.Fatal(err)
		}
		if job.Status != model.ClusterJobStatusQueued || job.AssignedNodeID != "" || job.CurrentAttemptID != "" || job.FinishedAt != nil {
			t.Fatalf("requeued job %s = %#v", jobID, job)
		}
	}
}

func TestListJobsIncludesStageProgress(t *testing.T) {
	database := openCoordinatorTestDB(t)
	job := model.ClusterJob{
		ID:               "job-with-stages",
		IdempotencyKey:   "job-with-stages",
		Status:           model.ClusterJobStatusRunning,
		CurrentAttemptID: "attempt-1",
	}
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	stages := []model.ClusterJobStage{
		{ID: "stage-old-attempt", JobID: job.ID, AttemptID: "attempt-old", Name: model.ClusterStageSavingShare, Status: model.ClusterStageStatusFailed},
		{ID: "stage-download", JobID: job.ID, AttemptID: "attempt-1", Name: model.ClusterStageDownloading, Status: model.ClusterStageStatusSucceeded},
		{ID: "stage-upload", JobID: job.ID, AttemptID: "attempt-1", Name: model.ClusterStageUploadingMobile, Status: model.ClusterStageStatusRunning, RetryCount: 1},
	}
	for i := range stages {
		if err := database.Create(&stages[i]).Error; err != nil {
			t.Fatal(err)
		}
	}

	jobs, err := New(database, "token").ListJobs(context.Background(), model.ClusterJobStatusRunning, false, 10)
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 || len(jobs[0].Stages) != 2 {
		t.Fatalf("jobs with stages = %#v", jobs)
	}
	if jobs[0].Stages[0].Name != model.ClusterStageDownloading || jobs[0].Stages[0].Status != model.ClusterStageStatusSucceeded {
		t.Fatalf("first stage = %#v", jobs[0].Stages[0])
	}
	if jobs[0].Stages[1].Name != model.ClusterStageUploadingMobile || jobs[0].Stages[1].Status != model.ClusterStageStatusRunning || jobs[0].Stages[1].RetryCount != 1 {
		t.Fatalf("second stage = %#v", jobs[0].Stages[1])
	}
}

func TestListJobsActiveAliasReturnsOnlyRunnableJobs(t *testing.T) {
	database := openCoordinatorTestDB(t)
	jobs := []model.ClusterJob{
		{ID: "active-job", IdempotencyKey: "active-job", Status: model.ClusterJobStatusRunning},
		{ID: "done-job", IdempotencyKey: "done-job", Status: model.ClusterJobStatusSucceeded},
	}
	for i := range jobs {
		if err := database.Create(&jobs[i]).Error; err != nil {
			t.Fatal(err)
		}
	}

	got, err := New(database, "token").ListJobs(context.Background(), "active", false, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "active-job" {
		t.Fatalf("active jobs = %#v, want active-job only", got)
	}
}

func TestReconcileNodeSessionsMarksConnectedSessionsAndOnlineNodesOffline(t *testing.T) {
	database := openCoordinatorTestDB(t)
	now := time.Unix(1721110000, 0).UTC()
	heartbeat := now.Add(-2 * time.Minute)
	if err := database.Create(&model.ClusterNode{
		ID: "ghost-1", Name: "ghost-1", Role: model.ClusterRoleWorker,
		Status: model.ClusterNodeStatusOnline, LastSessionID: "session-1", LastHeartbeatAt: &heartbeat,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNodeSession{
		ID: "session-1", NodeID: "ghost-1", Status: model.ClusterSessionStatusConnected,
		ConnectedAt: now.Add(-10 * time.Minute),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNode{ID: "disabled-1", Status: model.ClusterNodeStatusDisabled, Disabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	service := New(database, "secret")
	affected, err := service.ReconcileNodeSessions(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if affected != 2 {
		t.Fatalf("affected = %d", affected)
	}
	var node model.ClusterNode
	if err := database.First(&node, "id = ?", "ghost-1").Error; err != nil {
		t.Fatal(err)
	}
	if node.Status != model.ClusterNodeStatusOffline {
		t.Fatalf("node status = %q", node.Status)
	}
	var session model.ClusterNodeSession
	if err := database.First(&session, "id = ?", "session-1").Error; err != nil {
		t.Fatal(err)
	}
	if session.Status != model.ClusterSessionStatusDisconnected || session.DisconnectedAt == nil || !strings.Contains(session.DisconnectError, "startup reconciliation") {
		t.Fatalf("session = %#v", session)
	}
	var disabledNode model.ClusterNode
	if err := database.First(&disabledNode, "id = ?", "disabled-1").Error; err != nil {
		t.Fatal(err)
	}
	if disabledNode.Status != model.ClusterNodeStatusDisabled {
		t.Fatalf("disabled node status = %q", disabledNode.Status)
	}
}

func TestSweepExpiredHeartbeatsMarksTimedOutNodesOffline(t *testing.T) {
	database := openCoordinatorTestDB(t)
	service := New(database, "secret")
	now := time.Unix(1721111000, 0).UTC()
	stale := now.Add(-2 * time.Minute)
	fresh := now.Add(-20 * time.Second)
	if err := database.Create(&model.ClusterNode{
		ID: "timed-out", Status: model.ClusterNodeStatusOnline,
		LastSessionID: "session-timeout", LastHeartbeatAt: &stale,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNodeSession{
		ID: "session-timeout", NodeID: "timed-out", Status: model.ClusterSessionStatusConnected,
		ConnectedAt: now.Add(-5 * time.Minute),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNode{
		ID: "fresh", Status: model.ClusterNodeStatusOnline,
		LastSessionID: "session-fresh", LastHeartbeatAt: &fresh,
	}).Error; err != nil {
		t.Fatal(err)
	}
	affected, err := service.SweepExpiredHeartbeats(context.Background(), now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if affected != 1 {
		t.Fatalf("affected = %d", affected)
	}
	var timedOut model.ClusterNode
	if err := database.First(&timedOut, "id = ?", "timed-out").Error; err != nil {
		t.Fatal(err)
	}
	if timedOut.Status != model.ClusterNodeStatusOffline {
		t.Fatalf("timed out node status = %q", timedOut.Status)
	}
	var timedOutSession model.ClusterNodeSession
	if err := database.First(&timedOutSession, "id = ?", "session-timeout").Error; err != nil {
		t.Fatal(err)
	}
	if timedOutSession.Status != model.ClusterSessionStatusDisconnected || timedOutSession.DisconnectedAt == nil || timedOutSession.DisconnectError != "heartbeat timeout" {
		t.Fatalf("timed out session = %#v", timedOutSession)
	}
	var freshNode model.ClusterNode
	if err := database.First(&freshNode, "id = ?", "fresh").Error; err != nil {
		t.Fatal(err)
	}
	if freshNode.Status != model.ClusterNodeStatusOnline {
		t.Fatalf("fresh node status = %q", freshNode.Status)
	}
}

func TestListNodesHidesStaleOfflineByDefaultAndShowsTimedOutNodeOffline(t *testing.T) {
	database := openCoordinatorTestDB(t)
	service := New(database, "secret")
	now := time.Unix(1721112000, 0).UTC()
	service.SetHeartbeatInterval(15 * time.Second)
	staleHeartbeat := now.Add(-8 * 24 * time.Hour)
	timedOutHeartbeat := now.Add(-2 * time.Minute)
	if err := database.Create(&model.ClusterNode{ID: "stale-offline", Status: model.ClusterNodeStatusOffline, LastHeartbeatAt: &staleHeartbeat}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNode{ID: "timed-out-online", Status: model.ClusterNodeStatusOnline, LastHeartbeatAt: &timedOutHeartbeat}).Error; err != nil {
		t.Fatal(err)
	}
	defaultList, err := service.ListNodes(context.Background(), false, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(defaultList) != 1 {
		t.Fatalf("default list len = %d", len(defaultList))
	}
	if defaultList[0].ID != "timed-out-online" || defaultList[0].Status != model.ClusterNodeStatusOffline {
		t.Fatalf("default list = %#v", defaultList)
	}
	fullList, err := service.ListNodes(context.Background(), true, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(fullList) != 2 {
		t.Fatalf("full list len = %d", len(fullList))
	}
}

func TestDeleteNodeRemovesNodeOwnedMetadataButPreservesHistory(t *testing.T) {
	database := openCoordinatorTestDB(t)
	service := New(database, "secret")
	now := time.Unix(1721113000, 0).UTC()
	offlineHeartbeat := now.Add(-time.Hour)
	if err := database.Create(&model.ClusterNode{ID: "offline-delete", Status: model.ClusterNodeStatusOffline, LastHeartbeatAt: &offlineHeartbeat}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNodeSession{ID: "session-delete", NodeID: "offline-delete", Status: model.ClusterSessionStatusDisconnected}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNodeInventory{ID: "inventory-delete", NodeID: "offline-delete", Revision: 1, CollectedAt: offlineHeartbeat}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNodeDesiredConfig{NodeID: "offline-delete", Status: model.ClusterDesiredStatusApplied}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterStorageProfile{ID: "profile-delete", NodeID: "offline-delete", MountPath: "/offline-delete"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterOutbox{ID: "outbox-delete", MessageID: "outbox-delete", PeerNodeID: "offline-delete", Seq: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterInbox{ID: "inbox-delete", MessageID: "inbox-delete", PeerNodeID: "offline-delete", SessionID: "session-delete", Seq: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterJob{ID: "job-delete", IdempotencyKey: "job-delete", AssignedNodeID: "offline-delete", Status: model.ClusterJobStatusQueued}).Error; err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteNode(context.Background(), "offline-delete", now); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := database.Model(&model.ClusterNode{}).Where("id = ?", "offline-delete").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("node count = %d", count)
	}
	for _, owned := range []any{
		&model.ClusterNodeSession{},
		&model.ClusterNodeInventory{},
		&model.ClusterNodeDesiredConfig{},
		&model.ClusterStorageProfile{},
	} {
		if err := database.Model(owned).Where("node_id = ?", "offline-delete").Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%T count = %d", owned, count)
		}
	}
	for _, transportRecord := range []any{&model.ClusterOutbox{}, &model.ClusterInbox{}} {
		if err := database.Model(transportRecord).Where("peer_node_id = ?", "offline-delete").Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%T count = %d", transportRecord, count)
		}
	}
	if err := database.Model(&model.ClusterJob{}).Where("id = ?", "job-delete").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("job count = %d", count)
	}
}

func TestDeleteNodeAllowsInactiveStates(t *testing.T) {
	database := openCoordinatorTestDB(t)
	service := New(database, "secret")
	now := time.Unix(1721113500, 0).UTC()
	for _, state := range []string{
		model.ClusterNodeStatusPending,
		model.ClusterNodeStatusOffline,
		model.ClusterNodeStatusDisabled,
		model.ClusterNodeStatusRevoked,
	} {
		nodeID := "remove-" + state
		if err := database.Create(&model.ClusterNode{ID: nodeID, Status: state, Disabled: state == model.ClusterNodeStatusDisabled || state == model.ClusterNodeStatusRevoked}).Error; err != nil {
			t.Fatal(err)
		}
		if err := service.DeleteNode(context.Background(), nodeID, now); err != nil {
			t.Fatalf("delete %s node: %v", state, err)
		}
	}
}

func TestDeleteNodeRejectsOnlineAndDrainingNodes(t *testing.T) {
	database := openCoordinatorTestDB(t)
	service := New(database, "secret")
	now := time.Unix(1721114000, 0).UTC()
	onlineHeartbeat := now.Add(-10 * time.Second)
	if err := database.Create(&model.ClusterNode{ID: "online-node", Status: model.ClusterNodeStatusOnline, LastHeartbeatAt: &onlineHeartbeat}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNode{ID: "draining-node", Status: model.ClusterNodeStatusDraining, Drain: true}).Error; err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteNode(context.Background(), "online-node", now); err == nil || !strings.Contains(err.Error(), "cannot be removed") {
		t.Fatalf("online node err = %v", err)
	}
	if err := service.DeleteNode(context.Background(), "draining-node", now); err == nil || !strings.Contains(err.Error(), "draining") {
		t.Fatalf("draining node err = %v", err)
	}
}

func openCoordinatorTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.AutoMigrate(&model.ClusterNode{}, &model.ClusterNodeSession{}, &model.ClusterNodeInventory{}, &model.ClusterNodeDesiredConfig{}, &model.ClusterStorageProfile{}, &model.ClusterJob{}, &model.ClusterJobAttempt{}, &model.ClusterJobStage{}, &model.ClusterUploadManifest{}, &model.ClusterShareInspectManifest{}, &model.ClusterInbox{}, &model.ClusterOutbox{}); err != nil {
		t.Fatal(err)
	}
	return database
}

func testJobAndAttempt(ctx protocol.TaskContext, ctxHash, status string) (model.ClusterJob, model.ClusterJobAttempt) {
	job := model.ClusterJob{
		ID: "job-1", Type: model.ClusterJobTypeMediaTransfer, Status: model.ClusterJobStatusRunning,
		IdempotencyKey: "job-1", WorkflowVersion: ctx.WorkflowVersion,
		SubscriptionID: ctx.Subscription.SubscriptionID, SubscriptionItemID: ctx.Subscription.SubscriptionItemID,
		MediaItemID: ctx.MediaItemID, TaskContextHash: ctxHash, CurrentAttemptID: "attempt-1", CurrentGeneration: 1,
		AssignedNodeID: "worker-1",
	}
	attempt := model.ClusterJobAttempt{
		ID: "attempt-1", JobID: job.ID, NodeID: "worker-1", Generation: 1, Status: status,
		LeaseTokenHash: fmt.Sprintf("%x", sha256.Sum256([]byte("lease"))), LeaseUntil: time.Now().UTC().Add(time.Hour),
	}
	return job, attempt
}

func attemptRefForTest(attempt model.ClusterJobAttempt) protocol.AttemptRef {
	return protocol.AttemptRef{JobID: attempt.JobID, AttemptID: attempt.ID, Generation: attempt.Generation, LeaseToken: "lease"}
}

func testManifest(job model.ClusterJob, ctx protocol.TaskContext, ctxHash string) protocol.UploadETFManifest {
	return protocol.UploadETFManifest{
		AttemptRef:    protocol.AttemptRef{JobID: job.ID, AttemptID: "attempt-1", Generation: 1, LeaseToken: "lease"},
		ParentBatchID: ctx.ParentBatchID, MediaItemID: ctx.MediaItemID, OperationKey: "upload-1",
		StagePermitToken: "upload-permit",
		TaskContextHash:  ctxHash, WorkflowVersion: ctx.WorkflowVersion, SealedManifestVersion: ctx.SealedManifestVersion,
		TargetProfile: ctx.TargetProfile, Subscription: ctx.Subscription, Share: ctx.Share, Media: ctx.Media, SourceObjects: ctx.SourceObjects,
		MobileAccountBinding: "mobile-a", RemoteFileID: "remote-1", RemotePath: "/temp/episode.mkv",
		Name: "episode.mkv", Size: 1024, SHA256: strings.Repeat("a", 64), HashSource: "mobile_provider_response",
	}
}

func testUploadStage(attempt model.ClusterJobAttempt) *model.ClusterJobStage {
	return &model.ClusterJobStage{
		ID: "upload-stage-" + attempt.ID, JobID: attempt.JobID, AttemptID: attempt.ID,
		Name: model.ClusterStageUploadingMobile, Status: model.ClusterStageStatusPermitted,
		OperationKey:    "job-1:" + model.ClusterStageUploadingMobile,
		PermitTokenHash: fmt.Sprintf("%x", sha256.Sum256([]byte("upload-permit"))),
	}
}

func testTaskContext() protocol.TaskContext {
	return protocol.TaskContext{
		ParentBatchID: "batch-1", MediaItemID: "media-1", WorkflowVersion: "cluster-media-transfer/v1", SealedManifestVersion: "v1",
		Subscription:  protocol.SubscriptionTaskContext{SubscriptionID: 1, SubscriptionItemID: 2, SubscriptionName: "Example", SourceKey: "source-1", ShareRefFingerprint: "share-1"},
		Share:         protocol.ShareTaskContext{Provider: "aliyun_drive", URL: "https://www.alipan.com/s/example"},
		Media:         protocol.MediaTaskContext{MediaType: "tv", TMDBID: 123, Season: 1, Episode: 1, LogicalMediaRoot: "/TV/Example", LogicalTargetPath: "/TV/Example/Season 01/episode.mkv"},
		SourceObjects: []protocol.SourceObject{{Provider: "aliyun_drive", SourceFileID: "file-1", SourceRelativePath: "episode.mkv", Size: 1024}},
		TargetProfile: "/mobile/temp",
	}
}

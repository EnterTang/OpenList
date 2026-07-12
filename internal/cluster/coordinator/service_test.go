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

func openCoordinatorTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.AutoMigrate(&model.ClusterNode{}, &model.ClusterNodeSession{}, &model.ClusterJob{}, &model.ClusterJobAttempt{}, &model.ClusterJobStage{}, &model.ClusterUploadManifest{}, &model.ClusterShareInspectManifest{}, &model.ClusterInbox{}, &model.ClusterOutbox{}); err != nil {
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

package protocol

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestEnvelopeRoundTripIgnoresUnknownFields(t *testing.T) {
	original, err := NewEnvelope(MessageHeartbeat, Heartbeat{
		ObservedAt:     time.Date(2026, 7, 12, 8, 0, 0, 0, time.UTC),
		ActiveJobCount: 2,
		ResultQueue: ResultQueueStats{
			PendingCount:    3,
			DurabilityReady: true,
		},
	})
	if err != nil {
		t.Fatalf("new envelope: %v", err)
	}
	original.NodeID = "node-1"
	original.SessionID = "session-1"
	original.Seq = 7

	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	raw = []byte(strings.TrimSuffix(string(raw), "}") + `,"future_field":true}`)

	var decoded Envelope
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal envelope with unknown field: %v", err)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatalf("validate envelope: %v", err)
	}
	payload, err := DecodePayload[Heartbeat](decoded)
	if err != nil {
		t.Fatalf("decode heartbeat: %v", err)
	}
	if payload.ActiveJobCount != 2 || payload.ResultQueue.PendingCount != 3 {
		t.Fatalf("decoded heartbeat = %#v", payload)
	}
}

func TestJobOfferValidatesTaskContextHash(t *testing.T) {
	context := testTaskContext()
	hash, err := HashTaskContext(context)
	if err != nil {
		t.Fatalf("hash task context: %v", err)
	}
	offer := JobOffer{
		AttemptRef: AttemptRef{
			JobID:      "job-1",
			AttemptID:  "attempt-1",
			Generation: 1,
			LeaseToken: "lease-token",
		},
		IdempotencyKey:  "subscription:1:episode:13",
		JobType:         "media.transfer",
		LeaseUntil:      time.Now().Add(time.Minute),
		TaskContext:     context,
		TaskContextHash: hash,
	}
	if err := offer.Validate(); err != nil {
		t.Fatalf("validate offer: %v", err)
	}
	offer.TaskContext.SourceObjects = append(offer.TaskContext.SourceObjects, SourceObject{Provider: "aliyun_drive", SourceFileID: "file-2", SourceRelativePath: "episode-2.mkv"})
	offer.TaskContextHash, err = HashTaskContext(offer.TaskContext)
	if err != nil {
		t.Fatal(err)
	}
	if err := offer.Validate(); err == nil || !strings.Contains(err.Error(), "separate child jobs") {
		t.Fatalf("multi-object media offer error = %v", err)
	}
	offer.TaskContext.SourceObjects = offer.TaskContext.SourceObjects[:1]
	offer.TaskContextHash, err = HashTaskContext(offer.TaskContext)
	if err != nil {
		t.Fatal(err)
	}

	offer.TaskContext.Media.Episode = 14
	if err := offer.Validate(); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("tampered offer error = %v, want hash mismatch", err)
	}
}

func TestShareInspectOfferAllowsMetadataOnlyTaskContext(t *testing.T) {
	context := TaskContext{
		WorkflowVersion:       "cluster/v1",
		SealedManifestVersion: "share-inspect/v1",
		Share:                 ShareTaskContext{Provider: "aliyun_drive", URL: "https://www.alipan.com/s/example"},
	}
	hash, err := HashTaskContext(context)
	if err != nil {
		t.Fatalf("hash inspect task context: %v", err)
	}
	offer := JobOffer{
		AttemptRef:     AttemptRef{JobID: "inspect-1", AttemptID: "attempt-1", Generation: 1, LeaseToken: "lease"},
		IdempotencyKey: "inspect:share", JobType: "share.inspect", LeaseUntil: time.Now().Add(time.Minute),
		TaskContext: context, TaskContextHash: hash,
	}
	if err := offer.Validate(); err != nil {
		t.Fatalf("validate inspect offer: %v", err)
	}
}

func TestUploadETFManifestCarriesAndValidatesCoordinatorContext(t *testing.T) {
	context := testTaskContext()
	hash, err := HashTaskContext(context)
	if err != nil {
		t.Fatalf("hash task context: %v", err)
	}
	manifest := UploadETFManifest{
		AttemptRef: AttemptRef{
			JobID:      "job-1",
			AttemptID:  "attempt-1",
			Generation: 2,
			LeaseToken: "lease-token",
		},
		ParentBatchID:         context.ParentBatchID,
		MediaItemID:           context.MediaItemID,
		OperationKey:          "upload:job-1:file-1",
		StagePermitToken:      "upload-permit",
		TaskContextHash:       hash,
		WorkflowVersion:       context.WorkflowVersion,
		SealedManifestVersion: context.SealedManifestVersion,
		TargetProfile:         context.TargetProfile,
		Subscription:          context.Subscription,
		Share:                 context.Share,
		Media:                 context.Media,
		SourceObjects:         context.SourceObjects,
		MobileAccountBinding:  "mobile-worker-a",
		RemoteFileID:          "mobile-file-1",
		RemoteParentID:        "mobile-parent-1",
		RemotePath:            "/.openlist-cluster/job-1/episode.mkv",
		Name:                  "Example.S01E13.mkv",
		Size:                  123456789,
		SHA256:                strings.Repeat("a1", 32),
		HashSource:            "mobile_provider_response",
		UploadReceipt:         "request-1",
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("validate manifest: %v", err)
	}
	payloadHash, err := HashUploadETFManifest(manifest)
	if err != nil {
		t.Fatalf("hash upload manifest: %v", err)
	}
	if len(payloadHash) != 64 {
		t.Fatalf("payload hash length = %d, want 64", len(payloadHash))
	}

	manifest.Subscription.SubscriptionItemID++
	if err := manifest.Validate(); err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("modified manifest error = %v, want hash mismatch", err)
	}
}

func TestUploadETFManifestRejectsUnsafeFileNames(t *testing.T) {
	context := testTaskContext()
	hash, err := HashTaskContext(context)
	if err != nil {
		t.Fatal(err)
	}
	manifest := UploadETFManifest{
		AttemptRef: AttemptRef{
			JobID:      "job-1",
			AttemptID:  "attempt-1",
			Generation: 1,
			LeaseToken: "lease-token",
		},
		ParentBatchID:         context.ParentBatchID,
		MediaItemID:           context.MediaItemID,
		OperationKey:          "upload:job-1:file-1",
		StagePermitToken:      "upload-permit",
		TaskContextHash:       hash,
		WorkflowVersion:       context.WorkflowVersion,
		SealedManifestVersion: context.SealedManifestVersion,
		TargetProfile:         context.TargetProfile,
		Subscription:          context.Subscription,
		Share:                 context.Share,
		Media:                 context.Media,
		SourceObjects:         context.SourceObjects,
		MobileAccountBinding:  "mobile-worker-a",
		RemoteFileID:          "mobile-file-1",
		Size:                  123,
		SHA256:                strings.Repeat("a", 64),
		HashSource:            "mobile_provider_response",
	}

	for _, name := range []string{
		".",
		"..",
		"../episode.mkv",
		"season/episode.mkv",
		`season\episode.mkv`,
		strings.Repeat("a", maxManifestFileNameBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			manifest.Name = name
			if err := manifest.Validate(); err == nil {
				t.Fatalf("file name %q should be rejected", name)
			}
		})
	}
}

func testTaskContext() TaskContext {
	return TaskContext{
		ParentBatchID:         "batch-1",
		MediaItemID:           "media-episode-13",
		WorkflowVersion:       "cluster-media-transfer/v1",
		SealedManifestVersion: "manifest-v3",
		Subscription: SubscriptionTaskContext{
			SubscriptionID:      1001,
			SubscriptionItemID:  2002,
			SubscriptionName:    "Example",
			SourceKey:           "telegram:channel:message:file",
			SourceMessageID:     "123456",
			ShareRefFingerprint: "aliyun:share-id",
		},
		Share: ShareTaskContext{Provider: "aliyun_drive", URL: "https://www.alipan.com/s/example"},
		Media: MediaTaskContext{
			MediaType:         "tv",
			TMDBID:            123,
			Season:            1,
			Episode:           13,
			LogicalMediaRoot:  "/TV/Example",
			LogicalTargetPath: "/TV/Example/Season 01/Example.S01E13.mkv",
		},
		SourceObjects: []SourceObject{
			{
				Provider:           "aliyun_drive",
				SourceFileID:       "share-file-13",
				SourceRelativePath: "Season 01/Example.S01E13.mkv",
				Size:               123456789,
			},
		},
		TargetProfile: "mobile-primary",
	}
}

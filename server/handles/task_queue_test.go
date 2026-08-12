package handles

import (
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/tache"
)

func TestClusterQueueItemProjectsOperationalFields(t *testing.T) {
	now := time.Now().UTC()
	job := &model.ClusterJob{
		ID:                 "job-1",
		Type:               model.ClusterJobTypeMediaTransfer,
		Status:             model.ClusterJobStatusRunning,
		CreatedAt:          now.Add(-time.Minute),
		UpdatedAt:          now,
		StartedAt:          &now,
		ExpectedBytes:      1234,
		LastErrorCode:      "worker_capacity_unavailable",
		LastError:          "worker is at capacity",
		SubscriptionID:     10,
		SubscriptionItemID: 20,
		SourceProvider:     "pan123",
		AssignedNodeID:     "worker-a",
		Stages: []model.ClusterJobStage{{
			Name:         model.ClusterStageUploadingMobile,
			Status:       model.ClusterStageStatusRunning,
			ProgressJSON: `{"progress":42.5}`,
		}},
	}

	got := clusterQueueItem(job)
	if got.Source != "cluster" || got.Kind != model.ClusterJobTypeMediaTransfer {
		t.Fatalf("identity = %#v", got)
	}
	if got.ClusterJobID != job.ID || got.SubscriptionID != 10 || got.SubscriptionItemID != 20 {
		t.Fatalf("links = %#v", got)
	}
	if got.Provider != "pan123" || got.Worker != "worker-a" || got.Stage != model.ClusterStageUploadingMobile {
		t.Fatalf("routing = %#v", got)
	}
	if got.State != tache.StateRunning || got.Progress != 42.5 {
		t.Fatalf("runtime projection = %#v", got)
	}
	if got.ErrorCode != "worker_capacity_unavailable" || got.TotalBytes != 1234 {
		t.Fatalf("diagnostics = %#v", got)
	}
}

func TestActiveClusterQueueStatusExcludesTerminalJobs(t *testing.T) {
	if !isActiveClusterQueueStatus(model.ClusterJobStatusQueued) {
		t.Fatal("queued job should be active")
	}
	if isActiveClusterQueueStatus(model.ClusterJobStatusSucceeded) {
		t.Fatal("succeeded job should not be active")
	}
}

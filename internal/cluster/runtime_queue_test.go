package cluster

import (
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestNoCompatibleWorkerJobUpdatesBacksOff(t *testing.T) {
	now := time.Date(2026, time.August, 13, 1, 0, 0, 0, time.UTC)
	job := &model.ClusterJob{CreatedAt: now.Add(-time.Hour)}

	updates := noCompatibleWorkerJobUpdates(job, now)
	if got := updates["last_error_code"]; got != "no_compatible_worker" {
		t.Fatalf("last_error_code = %v, want no_compatible_worker", got)
	}
	if got := updates["available_at"].(time.Time); !got.Equal(now.Add(noCompatibleWorkerRetryDelay)) {
		t.Fatalf("available_at = %s, want %s", got, now.Add(noCompatibleWorkerRetryDelay))
	}
	if _, blocked := updates["status"]; blocked {
		t.Fatal("freshly blocked job should remain queued during the wait window")
	}
}

func TestNoCompatibleWorkerJobUpdatesStopsIndefiniteQueueing(t *testing.T) {
	now := time.Date(2026, time.August, 13, 1, 0, 0, 0, time.UTC)
	job := &model.ClusterJob{CreatedAt: now.Add(-noCompatibleWorkerMaxWait)}

	updates := noCompatibleWorkerJobUpdates(job, now)
	if got := updates["status"]; got != model.ClusterJobStatusFailed {
		t.Fatalf("status = %v, want failed", got)
	}
	if got := updates["last_error_code"]; got != "no_compatible_worker_timeout" {
		t.Fatalf("last_error_code = %v, want no_compatible_worker_timeout", got)
	}
	if got := updates["finished_at"].(time.Time); !got.Equal(now) {
		t.Fatalf("finished_at = %s, want %s", got, now)
	}
}

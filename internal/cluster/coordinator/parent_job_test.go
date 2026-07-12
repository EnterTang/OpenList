package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestReconcileParentJobAggregatesChildOutcomes(t *testing.T) {
	tests := []struct {
		name           string
		childStatuses  []string
		expectedItems  int
		wantStatus     string
		wantFinishedAt bool
	}{
		{
			name:           "all succeeded",
			childStatuses:  []string{model.ClusterJobStatusSucceeded, model.ClusterJobStatusSucceeded},
			wantStatus:     model.ClusterJobStatusSucceeded,
			wantFinishedAt: true,
		},
		{
			name:           "partial failed",
			childStatuses:  []string{model.ClusterJobStatusSucceeded, model.ClusterJobStatusFailed},
			wantStatus:     model.ClusterJobStatusPartialFailed,
			wantFinishedAt: true,
		},
		{
			name:           "missing child remains partial failed",
			childStatuses:  []string{model.ClusterJobStatusSucceeded},
			expectedItems:  2,
			wantStatus:     model.ClusterJobStatusPartialFailed,
			wantFinishedAt: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			database := openCoordinatorTestDB(t)
			parent := model.ClusterJob{
				ID:             "parent-1",
				Type:           model.ClusterJobTypeShareBatch,
				Status:         model.ClusterJobStatusRunning,
				IdempotencyKey: "parent-1",
				ExpectedItems:  tt.expectedItems,
			}
			if err := database.Create(&parent).Error; err != nil {
				t.Fatal(err)
			}
			for i, status := range tt.childStatuses {
				child := model.ClusterJob{
					ID:             "child-" + string(rune('1'+i)),
					ParentJobID:    parent.ID,
					Type:           model.ClusterJobTypeMediaTransfer,
					Status:         status,
					IdempotencyKey: "child-key-" + string(rune('1'+i)),
				}
				if err := database.Create(&child).Error; err != nil {
					t.Fatal(err)
				}
			}

			finishedAt := time.Date(2026, 7, 12, 13, 0, 0, 0, time.UTC)
			if err := reconcileParentJobTx(database, parent.ID, finishedAt); err != nil {
				t.Fatal(err)
			}
			if err := database.First(&parent, "id = ?", parent.ID).Error; err != nil {
				t.Fatal(err)
			}
			if parent.Status != tt.wantStatus {
				t.Fatalf("parent status = %q, want %q", parent.Status, tt.wantStatus)
			}
			if tt.wantFinishedAt && (parent.FinishedAt == nil || !parent.FinishedAt.Equal(finishedAt)) {
				t.Fatalf("parent finished_at = %v, want %v", parent.FinishedAt, finishedAt)
			}
		})
	}
}

func TestRetryFailedChildReopensAndEventuallyCompletesParent(t *testing.T) {
	database := openCoordinatorTestDB(t)
	oldFinishedAt := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	parent := model.ClusterJob{
		ID:             "parent-1",
		Type:           model.ClusterJobTypeShareBatch,
		Status:         model.ClusterJobStatusPartialFailed,
		IdempotencyKey: "parent-1",
		FinishedAt:     &oldFinishedAt,
	}
	succeededChild := model.ClusterJob{
		ID:             "child-1",
		ParentJobID:    parent.ID,
		Type:           model.ClusterJobTypeMediaTransfer,
		Status:         model.ClusterJobStatusSucceeded,
		IdempotencyKey: "child-1",
	}
	failedChild := model.ClusterJob{
		ID:             "child-2",
		ParentJobID:    parent.ID,
		Type:           model.ClusterJobTypeMediaTransfer,
		Status:         model.ClusterJobStatusFailed,
		IdempotencyKey: "child-2",
		FinishedAt:     &oldFinishedAt,
	}
	for _, value := range []any{&parent, &succeededChild, &failedChild} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}

	service := New(database, "token")
	if err := service.RetryJob(context.Background(), failedChild.ID); err != nil {
		t.Fatal(err)
	}
	var retriedChild model.ClusterJob
	if err := database.First(&retriedChild, "id = ?", failedChild.ID).Error; err != nil {
		t.Fatal(err)
	}
	if retriedChild.Status != model.ClusterJobStatusQueued || retriedChild.FinishedAt != nil {
		t.Fatalf("retried child = %#v", retriedChild)
	}
	var reopenedParent model.ClusterJob
	if err := database.First(&reopenedParent, "id = ?", parent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reopenedParent.Status != model.ClusterJobStatusRunning || reopenedParent.FinishedAt != nil {
		t.Fatalf("parent after child retry = %#v", reopenedParent)
	}

	manifest := model.ClusterUploadManifest{
		ID:          "manifest-2",
		JobID:       failedChild.ID,
		MediaItemID: "media-2",
		PayloadHash: "payload-2",
		Status:      model.ClusterUploadManifestStatusAccepted,
	}
	stage := model.ClusterJobStage{
		ID:        "stage-2",
		JobID:     failedChild.ID,
		AttemptID: "attempt-2",
		Name:      model.ClusterStageETFMaterializing,
		Status:    model.ClusterStageStatusRunning,
	}
	for _, value := range []any{&manifest, &stage} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	completedAt := time.Date(2026, 7, 12, 14, 0, 0, 0, time.UTC)
	if err := service.completeManifestMaterialization(context.Background(), manifest.ID, failedChild.ID, model.ClusterNotificationStatusPending, completedAt); err != nil {
		t.Fatal(err)
	}
	var completedParent model.ClusterJob
	if err := database.First(&completedParent, "id = ?", parent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if completedParent.Status != model.ClusterJobStatusSucceeded || completedParent.FinishedAt == nil || !completedParent.FinishedAt.Equal(completedAt) {
		t.Fatalf("completed parent = %#v", completedParent)
	}
}

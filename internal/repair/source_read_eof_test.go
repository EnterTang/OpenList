package repair

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func TestRepairSourceReadEOFDryRunAndApply(t *testing.T) {
	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: "x_"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.AutoMigrate(&model.ClusterJob{}, &model.ClusterUploadManifest{}, &model.SubscriptionItem{}); err != nil {
		t.Fatal(err)
	}
	finishedAt := time.Now().UTC()
	item := &model.SubscriptionItem{SubscriptionID: 1, SourceKey: "source-eof", Status: model.SubscriptionItemStatusFailed, ClusterJobID: "job-eof"}
	if err := database.Create(item).Error; err != nil {
		t.Fatal(err)
	}
	job := &model.ClusterJob{
		ID: "job-eof", IdempotencyKey: "job-eof", Type: model.ClusterJobTypeMediaTransfer,
		Status: model.ClusterJobStatusFailed, SourceProvider: "pan123", SubscriptionItemID: item.ID,
		CurrentGeneration: 1, CurrentAttemptID: "attempt-1", AssignedNodeID: "worker-1", FinishedAt: &finishedAt,
		LastError: "native cluster move task: failed to read all data: (expect =100, actual =20) unexpected EOF",
	}
	if err := database.Create(job).Error; err != nil {
		t.Fatal(err)
	}

	dryRun, err := RepairSourceReadEOF(context.Background(), database, SourceReadEOFOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if dryRun.Applied || dryRun.Candidates != 1 || dryRun.Queued != 1 {
		t.Fatalf("dry run = %#v", dryRun)
	}
	assertSourceReadRepairState(t, database, job.ID, item.ID, model.ClusterJobStatusFailed, model.SubscriptionItemStatusFailed)

	applied, err := RepairSourceReadEOF(context.Background(), database, SourceReadEOFOptions{Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if !applied.Applied || applied.Queued != 1 {
		t.Fatalf("apply = %#v", applied)
	}
	assertSourceReadRepairState(t, database, job.ID, item.ID, model.ClusterJobStatusQueued, model.SubscriptionItemStatusTransferring)
}

func TestRepairSourceReadEOFSkipsManifestAndAttemptLimit(t *testing.T) {
	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: "x_"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.AutoMigrate(&model.ClusterJob{}, &model.ClusterUploadManifest{}, &model.SubscriptionItem{}); err != nil {
		t.Fatal(err)
	}
	errorText := "failed to read all data: (expect =100, actual =20) unexpected EOF"
	jobs := []model.ClusterJob{
		{ID: "manifest-job", IdempotencyKey: "manifest-job", Type: model.ClusterJobTypeMediaTransfer, Status: model.ClusterJobStatusFailed, SourceProvider: "pan123", CurrentGeneration: 1, LastError: errorText},
		{ID: "exhausted-job", IdempotencyKey: "exhausted-job", Type: model.ClusterJobTypeMediaTransfer, Status: model.ClusterJobStatusFailed, SourceProvider: "pan123", CurrentGeneration: 3, LastError: errorText},
	}
	if err := database.Create(&jobs).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterUploadManifest{ID: "manifest-1", JobID: "manifest-job", MediaItemID: "media-1"}).Error; err != nil {
		t.Fatal(err)
	}
	report, err := RepairSourceReadEOF(context.Background(), database, SourceReadEOFOptions{Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if report.Candidates != 2 || report.Queued != 0 || report.SkippedManifest != 1 || report.SkippedAttemptLimit != 1 {
		t.Fatalf("report = %#v", report)
	}
}

func assertSourceReadRepairState(t *testing.T, database *gorm.DB, jobID string, itemID uint, jobStatus, itemStatus string) {
	t.Helper()
	var job model.ClusterJob
	if err := database.First(&job, "id = ?", jobID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != jobStatus {
		t.Fatalf("job status = %q, want %q", job.Status, jobStatus)
	}
	var item model.SubscriptionItem
	if err := database.First(&item, itemID).Error; err != nil {
		t.Fatal(err)
	}
	if item.Status != itemStatus {
		t.Fatalf("item status = %q, want %q", item.Status, itemStatus)
	}
}

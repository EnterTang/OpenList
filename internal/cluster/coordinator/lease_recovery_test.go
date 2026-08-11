package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestSweepExpiredLeasesDeadLettersMediaTransferAtAttemptLimit(t *testing.T) {
	database := openCoordinatorTestDB(t)
	taskContext := testTaskContext()
	contextHash, err := protocol.HashTaskContext(taskContext)
	if err != nil {
		t.Fatal(err)
	}
	job, attempt := testJobAndAttempt(taskContext, contextHash, model.ClusterAttemptStatusAccepted)
	job.Type = model.ClusterJobTypeMediaTransfer
	job.SubscriptionItemID = 0
	job.CurrentGeneration = automaticMediaTransferAttemptLimit
	attempt.Generation = automaticMediaTransferAttemptLimit
	attempt.LeaseUntil = time.Now().UTC().Add(-time.Minute)
	if err := database.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&attempt).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := New(database, "").SweepExpiredLeases(context.Background(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := database.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != model.ClusterJobStatusDeadLetter || job.FinishedAt == nil {
		t.Fatalf("job after attempt limit = %#v", job)
	}
}

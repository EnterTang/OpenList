package cluster

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/coordinator"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestDecideCoordinatorLeaseRenewal(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	busyErr := errors.New("database is locked")

	tests := []struct {
		name            string
		decisionAt      time.Time
		currentDeadline time.Time
		renewedUntil    time.Time
		rowsAffected    int64
		err             error
		wantDeadline    time.Time
		wantFence       bool
	}{
		{
			name:            "success advances deadline",
			decisionAt:      now.Add(2 * time.Second),
			currentDeadline: now.Add(10 * time.Second),
			renewedUntil:    now.Add(coordinatorLeaseDuration),
			rowsAffected:    1,
			wantDeadline:    now.Add(coordinatorLeaseDuration),
		},
		{
			name:            "zero rows fences immediately",
			decisionAt:      now,
			currentDeadline: now.Add(10 * time.Second),
			renewedUntil:    now.Add(coordinatorLeaseDuration),
			rowsAffected:    0,
			wantDeadline:    now.Add(10 * time.Second),
			wantFence:       true,
		},
		{
			name:            "database error before deadline retries",
			decisionAt:      now,
			currentDeadline: now.Add(time.Second),
			renewedUntil:    now.Add(coordinatorLeaseDuration),
			rowsAffected:    0,
			err:             busyErr,
			wantDeadline:    now.Add(time.Second),
		},
		{
			name:            "database error at deadline fences",
			decisionAt:      now,
			currentDeadline: now,
			renewedUntil:    now.Add(coordinatorLeaseDuration),
			rowsAffected:    0,
			err:             busyErr,
			wantDeadline:    now,
			wantFence:       true,
		},
		{
			name:            "database error after deadline fences",
			decisionAt:      now,
			currentDeadline: now.Add(-time.Second),
			renewedUntil:    now.Add(coordinatorLeaseDuration),
			rowsAffected:    0,
			err:             busyErr,
			wantDeadline:    now.Add(-time.Second),
			wantFence:       true,
		},
		{
			name:            "success at deadline fences",
			decisionAt:      now,
			currentDeadline: now,
			renewedUntil:    now.Add(coordinatorLeaseDuration),
			rowsAffected:    1,
			wantDeadline:    now,
			wantFence:       true,
		},
		{
			name:            "success completing after deadline fences",
			decisionAt:      now,
			currentDeadline: now.Add(-time.Nanosecond),
			renewedUntil:    now.Add(coordinatorLeaseDuration),
			rowsAffected:    1,
			wantDeadline:    now.Add(-time.Nanosecond),
			wantFence:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDeadline, gotFence := decideCoordinatorLeaseRenewal(
				tt.decisionAt,
				tt.currentDeadline,
				tt.renewedUntil,
				tt.rowsAffected,
				tt.err,
			)
			require.Equal(t, tt.wantDeadline, gotDeadline)
			require.Equal(t, tt.wantFence, gotFence)
		})
	}
}

func TestCoordinatorServiceSnapshotRemainsUsableAfterFence(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = original })

	database, err := gorm.Open(sqlite.Open("file:coordinator_snapshot?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, database.AutoMigrate(
		new(model.ClusterUploadManifest),
		new(model.ClusterShareInspectManifest),
		new(model.ClusterJobAttempt),
		new(model.ClusterJob),
		new(model.ClusterOutbox),
	))
	service := coordinator.New(database, "")
	runtime := &Runtime{ctx: context.Background(), coordinatorService: service}

	snapshot := runtime.coordinatorServiceSnapshot()
	runtime.fenceLostCoordinator()

	require.Nil(t, runtime.coordinatorServiceSnapshot())
	require.Same(t, service, snapshot)
	require.NotPanics(t, func() {
		runtime.processManifestProcessorTick(context.Background(), snapshot)
	})
}

func TestStaleCoordinatorGenerationCannotFenceCurrentRuntime(t *testing.T) {
	service := coordinator.New(nil, "")
	runtime := &Runtime{
		coordinatorService: service,
		generation:         2,
		leaseOwner:         "current-owner",
		started:            true,
	}

	require.False(t, runtime.fenceLostCoordinatorIfCurrent(1, "stale-owner"))
	require.Same(t, service, runtime.coordinatorServiceSnapshot())
	require.True(t, runtime.started)
	require.Equal(t, "current-owner", runtime.leaseOwner)
}

func TestCoordinatorFenceWaitsForWorkerBackground(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &Runtime{ctx: ctx, cancel: cancel}
	release := make(chan struct{})
	runtime.workerBackground.Add(1)
	go func() {
		defer runtime.workerBackground.Done()
		<-release
	}()

	fenced := make(chan struct{})
	go func() {
		runtime.fenceLostCoordinator()
		close(fenced)
	}()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("coordinator fence did not reach worker shutdown")
	}
	select {
	case <-fenced:
		t.Fatal("coordinator fence returned while worker background was still running")
	default:
	}
	close(release)
	select {
	case <-fenced:
	case <-time.After(time.Second):
		t.Fatal("coordinator fence did not return after worker background stopped")
	}
}

func TestRuntimeStopWaitsForLeaseRenewalBeforeReleasingLease(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = original })

	database, err := gorm.Open(sqlite.Open("file:coordinator_stop_order?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	db.Init(database)

	leaseOwner := "stopping-owner"
	require.NoError(t, database.Create(&model.ClusterCoordinatorLease{
		Name:       "control-plane",
		OwnerID:    leaseOwner,
		LeaseUntil: time.Now().UTC().Add(coordinatorLeaseDuration),
	}).Error)

	ctx, cancel := context.WithCancel(context.Background())
	runtime := &Runtime{
		ctx:        ctx,
		cancel:     cancel,
		leaseOwner: leaseOwner,
		started:    true,
	}
	runtime.coordinatorLeaseMu.Lock()
	stopped := make(chan struct{})
	go func() {
		runtime.Stop()
		close(stopped)
	}()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("runtime stop did not cancel the lease generation")
	}
	select {
	case <-stopped:
		t.Fatal("runtime stop returned before the in-flight renewal completed")
	default:
	}
	runtime.coordinatorLeaseMu.Unlock()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("runtime stop did not finish after the renewal completed")
	}

	var lease model.ClusterCoordinatorLease
	require.NoError(t, database.First(&lease, "name = ?", "control-plane").Error)
	require.False(t, lease.LeaseUntil.After(time.Now().UTC()))
}

func TestWorkerCoordinatorURLRequiresTLSForRemoteHost(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = original })

	conf.Conf.Cluster.CoordinatorURL = "http://cluster.example.com"
	_, err := workerCoordinatorURL(RoleWorker)
	require.ErrorContains(t, err, "must use wss")

	conf.Conf.Cluster.CoordinatorURL = "https://cluster.example.com"
	got, err := workerCoordinatorURL(RoleWorker)
	require.NoError(t, err)
	require.Contains(t, got, "wss://cluster.example.com")
}

func TestMediaSourceFingerprintTracksContentNotMessageID(t *testing.T) {
	task := protocol.TaskContext{
		MediaItemID: "episode-13", WorkflowVersion: "v1", SealedManifestVersion: "manifest-1",
		Subscription:  protocol.SubscriptionTaskContext{SubscriptionID: 1, SourceMessageID: "100"},
		SourceObjects: []protocol.SourceObject{{Provider: "aliyun_drive", SourceFileID: "file-1", Size: 100, Hash: "hash-1"}},
		TargetProfile: "/mobile",
	}
	first, err := mediaSourceFingerprint(task)
	require.NoError(t, err)
	task.Subscription.SourceMessageID = "101"
	second, err := mediaSourceFingerprint(task)
	require.NoError(t, err)
	require.Equal(t, first, second)
	task.SourceObjects[0].Hash = "hash-2"
	changed, err := mediaSourceFingerprint(task)
	require.NoError(t, err)
	require.NotEqual(t, first, changed)
}

func TestWorkerCoordinatorURLAllowsLoopbackWithoutTLS(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = original })

	conf.Conf.Cluster.CoordinatorURL = "http://127.0.0.1:5244"
	got, err := workerCoordinatorURL(RoleWorker)
	require.NoError(t, err)
	require.Contains(t, got, "ws://127.0.0.1:5244")
}

func TestClusterCheckOrigin(t *testing.T) {
	request := httptest.NewRequest("GET", "http://coordinator.example.com/api/cluster/ws", nil)
	require.True(t, clusterCheckOrigin(request))

	request.Header.Set("Origin", "https://coordinator.example.com")
	require.True(t, clusterCheckOrigin(request))

	request.Header.Set("Origin", "https://attacker.example.com")
	require.False(t, clusterCheckOrigin(request))
}

func TestCoordinatorStartAllowsMissingETFRootPath(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = original })

	database, err := gorm.Open(sqlite.Open("file:cluster_runtime_start?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	db.Init(database)

	conf.Conf.Cluster.Role = string(RoleCoordinator)
	conf.Conf.Cluster.EnrollmentToken = "enrollment-secret"
	conf.Conf.Cluster.WebSocketPath = "/api/cluster/ws"
	conf.Conf.Cluster.ETFRootPath = ""

	runtime := &Runtime{}
	require.NoError(t, runtime.Start())
	t.Cleanup(runtime.Stop)
	if runtime.CoordinatorService() == nil {
		t.Fatal("coordinator service was not initialized")
	}
}

func TestNodeLocalCleanupQueueNamesAreStableAndIsolated(t *testing.T) {
	streamA, groupA, dlqA := nodeLocalCleanupQueueNames("cluster:local-cleanup:v1", "worker-a")
	streamAAgain, groupAAgain, dlqAAgain := nodeLocalCleanupQueueNames("cluster:local-cleanup:v1", "worker-a")
	streamB, groupB, dlqB := nodeLocalCleanupQueueNames("cluster:local-cleanup:v1", "worker-b")

	require.Equal(t, streamA, streamAAgain)
	require.Equal(t, groupA, groupAAgain)
	require.Equal(t, dlqA, dlqAAgain)
	require.NotEqual(t, streamA, streamB)
	require.NotEqual(t, groupA, groupB)
	require.NotEqual(t, dlqA, dlqB)
	require.Equal(t, streamA+":dlq", dlqA)
}

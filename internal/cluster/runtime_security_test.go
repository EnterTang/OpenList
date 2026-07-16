package cluster

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

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

func TestRuntimeStartReconcilesCoordinatorSessionsBeforeServing(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = original })

	database, err := gorm.Open(sqlite.Open("file:runtime_reconcile?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.AutoMigrate(&model.ClusterNode{}, &model.ClusterNodeSession{}, &model.ClusterCoordinatorLease{}); err != nil {
		t.Fatal(err)
	}
	db.Init(database)
	staleHeartbeat := time.Now().UTC().Add(-5 * time.Minute)
	if err := database.Create(&model.ClusterNode{ID: "ghost-1", Status: model.ClusterNodeStatusOnline, LastSessionID: "session-1", LastHeartbeatAt: &staleHeartbeat}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNodeSession{ID: "session-1", NodeID: "ghost-1", Status: model.ClusterSessionStatusConnected, ConnectedAt: time.Now().UTC().Add(-10 * time.Minute)}).Error; err != nil {
		t.Fatal(err)
	}

	conf.Conf.Cluster.Role = string(RoleCoordinator)
	conf.Conf.Cluster.EnrollmentToken = "enrollment-secret"
	conf.Conf.Cluster.WebSocketPath = "/api/cluster/ws"

	runtime := &Runtime{}
	if err := runtime.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Stop)

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
	if session.Status != model.ClusterSessionStatusDisconnected || session.DisconnectedAt == nil {
		t.Fatalf("session = %#v", session)
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

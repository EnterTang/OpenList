package cluster

import (
	"net/http/httptest"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/stretchr/testify/require"
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

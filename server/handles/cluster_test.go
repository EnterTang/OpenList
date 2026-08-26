package handles

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/coordinator"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func setupClusterHandleRuntime(t *testing.T) *gorm.DB {
	t.Helper()
	gin.SetMode(gin.TestMode)
	oldConf := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	oldRuntime := cluster.DefaultRuntime
	runtime := &cluster.Runtime{}
	cluster.DefaultRuntime = runtime
	database, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.Init(database)
	conf.Conf.Cluster.Role = string(cluster.RoleCoordinator)
	conf.Conf.Cluster.EnrollmentToken = "enrollment-secret"
	conf.Conf.Cluster.WebSocketPath = "/api/cluster/ws"
	if err := runtime.Start(); err != nil {
		t.Fatalf("start cluster runtime: %v", err)
	}
	t.Cleanup(func() {
		runtime.Stop()
		cluster.DefaultRuntime = oldRuntime
		conf.Conf = oldConf
		sqlDB, err := database.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})
	return database
}

func TestListClusterNodesHidesStaleOfflineByDefault(t *testing.T) {
	database := setupClusterHandleRuntime(t)
	staleHeartbeat := time.Now().UTC().Add(-8 * 24 * time.Hour)
	if err := database.Create(&model.ClusterNode{ID: "stale-offline", Status: model.ClusterNodeStatusOffline, LastHeartbeatAt: &staleHeartbeat}).Error; err != nil {
		t.Fatal(err)
	}
	c, recorder := newSubscriptionHandleContext(t, http.MethodGet, "/admin/cluster/nodes")
	ListClusterNodes(c)
	resp := decodeHandleResp[[]coordinator.NodeSummary](t, recorder)
	if resp.Code != 200 {
		t.Fatalf("code = %d, want 200: %s", resp.Code, recorder.Body.String())
	}
	if len(resp.Data) != 0 {
		t.Fatalf("nodes = %#v, want empty list", resp.Data)
	}
}

func TestDeleteClusterNodeRemovesOfflineNode(t *testing.T) {
	database := setupClusterHandleRuntime(t)
	freshHeartbeat := time.Now().UTC().Add(-time.Hour)
	if err := database.Create(&model.ClusterNode{ID: "fresh-offline", Status: model.ClusterNodeStatusOffline, LastHeartbeatAt: &freshHeartbeat}).Error; err != nil {
		t.Fatal(err)
	}
	c, recorder := newSubscriptionHandleContext(t, http.MethodPost, "/admin/cluster/nodes/fresh-offline/delete")
	c.Params = gin.Params{{Key: "id", Value: "fresh-offline"}}
	DeleteClusterNode(c)
	resp := decodeHandleResp[any](t, recorder)
	if resp.Code != 200 {
		t.Fatalf("code = %d, want 200: %s", resp.Code, recorder.Body.String())
	}
	var count int64
	if err := database.Model(&model.ClusterNode{}).Where("id = ?", "fresh-offline").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("node count = %d", count)
	}
}

func TestGetClusterNodeConfigReturnsSafeDesiredConfig(t *testing.T) {
	database := setupClusterHandleRuntime(t)
	desired := map[string]any{
		"qb_clients": []map[string]any{{
			"id": "qb-main", "webui_url": "http://qb:8080", "secret_ref": "secret-qb",
			"path_mappings": []map[string]string{{"qb_path": "/downloads", "worker_path": "/srv/downloads"}},
		}},
		"moviepilot_routes": []map[string]string{{"bridge_instance_id": "mp-main", "downloader": "qb-main", "qb_client_id": "qb-main"}},
		"staging":           map[string]any{"root": "/srv/staging", "max_upload_concurrency": 2},
	}
	raw, err := json.Marshal(desired)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNode{ID: "worker-config", Status: model.ClusterNodeStatusOnline}).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNodeDesiredConfig{
		NodeID: "worker-config", Revision: 3, DesiredHash: "hash-3", ConfigJSON: string(raw),
		Status: model.ClusterDesiredStatusApplied, ObservedRevision: 3, ObservedHash: "hash-3",
	}).Error; err != nil {
		t.Fatal(err)
	}
	c, recorder := newSubscriptionHandleContext(t, http.MethodGet, "/admin/cluster/nodes/worker-config/config")
	c.Params = gin.Params{{Key: "id", Value: "worker-config"}}
	GetClusterNodeConfig(c)
	resp := decodeHandleResp[map[string]any](t, recorder)
	if resp.Code != 200 {
		t.Fatalf("code = %d, want 200: %s", resp.Code, recorder.Body.String())
	}
	config, ok := resp.Data["config"].(map[string]any)
	if !ok {
		t.Fatalf("response config = %#v", resp.Data["config"])
	}
	if config["qb_clients"] == nil || config["moviepilot_routes"] == nil || config["staging"] == nil {
		t.Fatalf("response omitted MoviePilot/qB config: %#v", config)
	}
	if resp.Data["revision"] != float64(3) || resp.Data["status"] != model.ClusterDesiredStatusApplied {
		t.Fatalf("response metadata = %#v", resp.Data)
	}
}

func TestMigrateClusterSecretsDoesNotExposePlaintext(t *testing.T) {
	setupClusterHandleRuntime(t)
	oldKey := strings.Repeat("11", 32)
	newKey := strings.Repeat("22", 32)
	conf.Conf.Cluster.SecretMasterKey = oldKey
	secret, err := cluster.WriteSecret(t.Context(), cluster.SecretWriteRequest{
		Alias: "bridge-migration", Kind: "moviepilot_bridge_hmac",
		Value: map[string]any{"hmac_key": "bridge-plaintext-secret"},
	}, cluster.ControlActor{Name: "test"})
	if err != nil {
		t.Fatal(err)
	}
	conf.Conf.Cluster.SecretMasterKey = newKey
	conf.Conf.Cluster.SecretMasterKeyPrevious = oldKey

	c, recorder := newSubscriptionHandleContext(t, http.MethodPost, "/admin/cluster/secrets/migrate")
	MigrateClusterSecrets(c)
	resp := decodeHandleResp[cluster.SecretMigrationResult](t, recorder)
	if resp.Code != 200 {
		t.Fatalf("code = %d, want 200: %s", resp.Code, recorder.Body.String())
	}
	if resp.Data.Total != 1 || resp.Data.Migrated != 1 || resp.Data.Skipped != 0 {
		t.Fatalf("migration result = %+v", resp.Data)
	}
	if strings.Contains(recorder.Body.String(), "bridge-plaintext-secret") {
		t.Fatalf("migration response leaked plaintext: %s", recorder.Body.String())
	}

	conf.Conf.Cluster.SecretMasterKeyPrevious = ""
	recovered, _, err := cluster.ResolveSecret(t.Context(), secret.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(recovered), "bridge-plaintext-secret") {
		t.Fatalf("migrated secret was not recoverable: %s", recovered)
	}
}

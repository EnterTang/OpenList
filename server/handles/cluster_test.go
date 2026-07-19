package handles

import (
	"net/http"
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

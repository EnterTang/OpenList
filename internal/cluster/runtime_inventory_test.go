package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestNodeInventorySupportsHonorsProviderAccountUploadLimit(t *testing.T) {
	database := openClusterRuntimeTestDB(t)
	oldConf := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = oldConf })
	db.Init(database)

	capabilities, err := json.Marshal(protocol.NodeCapabilities{
		SupportedProviders:   []string{"aliyun_drive"},
		SupportedOperations:  []string{"share.save", "mobile.upload", "result.report"},
		RedisDurabilityReady: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	mounts, err := json.Marshal([]protocol.MountInventory{{
		Provider:    "yidong139",
		MountPath:   "/139-a",
		CanUpload:   true,
		SupportsETF: true,
		FreeBytes:   100 << 30,
	}})
	if err != nil {
		t.Fatal(err)
	}
	providerAccounts, err := json.Marshal([]protocol.ProviderAccountInventory{{
		Provider:             "yidong139",
		MountPath:            "/139-a",
		SupportsUpload:       true,
		SupportsETF:          true,
		MaxSingleUploadBytes: 20 << 30,
		FreeBytes:            100 << 30,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNodeInventory{
		ID:                   "inventory-1",
		NodeID:               "worker-1",
		Revision:             1,
		CollectedAt:          time.Now().UTC(),
		InventoryHash:        "hash-1",
		CapabilitiesJSON:     string(capabilities),
		MountsJSON:           string(mounts),
		ProviderAccountsJSON: string(providerAccounts),
	}).Error; err != nil {
		t.Fatal(err)
	}
	configJSON, err := json.Marshal(protocol.WorkerDesiredConfig{
		TargetBindings: map[string]protocol.TargetBinding{
			"mobile-primary": {MountPath: "/139-a"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNodeDesiredConfig{
		NodeID:           "worker-1",
		Status:           model.ClusterDesiredStatusApplied,
		Revision:         1,
		ObservedRevision: 1,
		ConfigJSON:       string(configJSON),
	}).Error; err != nil {
		t.Fatal(err)
	}

	ok, err := nodeInventorySupports(context.Background(), "worker-1", protocol.TaskContext{
		Share:         protocol.ShareTaskContext{Provider: "aliyun_drive"},
		TargetProfile: "mobile-primary",
	}, []string{"share.save", "mobile.upload", "result.report"}, 30<<30)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected worker to be rejected when upload limit is too small")
	}
}

func openClusterRuntimeTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return database
}

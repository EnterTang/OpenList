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
		SupportedProviders:   []string{"aliyun_drive", "yidong139"},
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
		Provider:          "aliyun_drive",
		MountPath:         "/aliyun-a",
		Status:            "work",
		SupportsShareSave: true,
		SupportsDownload:  true,
		FreeBytes:         100 << 30,
	}, {
		Provider:             "yidong139",
		MountPath:            "/139-a",
		Status:               "work",
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
		Share:          protocol.ShareTaskContext{Provider: "aliyun_drive"},
		StagingTarget:  protocol.ProviderTargetRequirement{Provider: "aliyun_drive", NeedShareSave: true, RequiredBytes: 30 << 30},
		DeliveryTarget: protocol.ProviderTargetRequirement{Provider: "yidong139", NeedUpload: true, RequiredBytes: 30 << 30},
		TargetProfile:  "mobile-primary",
	}, []string{"share.save", "mobile.upload", "result.report"}, 30<<30)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected worker to be rejected when upload limit is too small")
	}
}

func TestTorrentInventoryMatchPinsTaskToBoundWorkerAndRoute(t *testing.T) {
	task := protocol.TaskContext{Torrent: &protocol.TorrentTaskContext{
		WorkerNodeID:     "worker-qb",
		BridgeInstanceID: "mp-main",
		Downloader:       "qb-a",
		QBClientID:       "qb-a",
		TorrentHash:      strings.Repeat("a", 40),
	}}
	capabilities := protocol.NodeCapabilities{
		SupportedOperations: []string{"qb.copy"},
		MoviePilotRoutes: []protocol.MoviePilotRouteInventory{{
			BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a",
			UploadConcurrency: 2, QBHealth: "configured",
		}},
	}

	if worker, err := ResolveTorrentWorker(task); err != nil || worker != "worker-qb" {
		t.Fatalf("ResolveTorrentWorker = %q, %v", worker, err)
	}
	if _, ok, reason, err := nodeInventoryTorrentMatch("worker-qb", task, capabilities, 0); err != nil || !ok {
		t.Fatalf("bound worker should match, ok=%v reason=%q err=%v", ok, reason, err)
	}
	if _, ok, reason, err := nodeInventoryTorrentMatch("worker-other", task, capabilities, 0); err != nil || ok || !strings.Contains(reason, "bound to worker") {
		t.Fatalf("other worker should be rejected, ok=%v reason=%q err=%v", ok, reason, err)
	}
	if got := selectRedispatchNodeID([]string{"worker-other"}, task, 0); got != "" {
		t.Fatalf("offline bound worker must not fall back to %q", got)
	}
	if got := selectRedispatchNodeID([]string{"worker-other", "worker-qb"}, task, 0); got != "worker-qb" {
		t.Fatalf("bound worker selection = %q, want worker-qb", got)
	}
}

func TestNodeInventorySupportsRequiresSourceProviderCapability(t *testing.T) {
	database := openClusterRuntimeTestDB(t)
	oldConf := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = oldConf })
	db.Init(database)

	capabilities, err := json.Marshal(protocol.NodeCapabilities{
		SupportedProviders:   []string{"pan123", "yidong139"},
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
		Status:               "work",
		SupportsUpload:       true,
		SupportsETF:          true,
		MaxSingleUploadBytes: 500 << 30,
		FreeBytes:            100 << 30,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNodeInventory{
		ID:                   "inventory-2",
		NodeID:               "worker-2",
		Revision:             1,
		CollectedAt:          time.Now().UTC(),
		InventoryHash:        "hash-2",
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
		NodeID:           "worker-2",
		Status:           model.ClusterDesiredStatusApplied,
		Revision:         1,
		ObservedRevision: 1,
		ConfigJSON:       string(configJSON),
	}).Error; err != nil {
		t.Fatal(err)
	}

	ok, err := nodeInventorySupports(context.Background(), "worker-2", protocol.TaskContext{
		Share:         protocol.ShareTaskContext{Provider: "pan123"},
		TargetProfile: "mobile-primary",
	}, []string{"share.save", "mobile.upload", "result.report"}, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected worker to be rejected when source provider capability is missing")
	}
}

func TestNodeInventorySupportsRequiresMatchingDeliveryProvider(t *testing.T) {
	database := openClusterRuntimeTestDB(t)
	oldConf := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = oldConf })
	db.Init(database)

	capabilities, err := json.Marshal(protocol.NodeCapabilities{
		SupportedProviders:   []string{"pan123", "yidong139"},
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
		Provider:          "pan123",
		MountPath:         "/123-a",
		Status:            "work",
		SupportsShareSave: true,
		SupportsDownload:  true,
		FreeBytes:         100 << 30,
	}, {
		Provider:             "yidong139",
		MountPath:            "/139-a",
		Status:               "work",
		SupportsShareSave:    false,
		SupportsDownload:     false,
		SupportsUpload:       true,
		SupportsETF:          true,
		MaxSingleUploadBytes: 500 << 30,
		FreeBytes:            100 << 30,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Create(&model.ClusterNodeInventory{
		ID:                   "inventory-3",
		NodeID:               "worker-3",
		Revision:             1,
		CollectedAt:          time.Now().UTC(),
		InventoryHash:        "hash-3",
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
		NodeID:           "worker-3",
		Status:           model.ClusterDesiredStatusApplied,
		Revision:         1,
		ObservedRevision: 1,
		ConfigJSON:       string(configJSON),
	}).Error; err != nil {
		t.Fatal(err)
	}

	ok, err := nodeInventorySupports(context.Background(), "worker-3", protocol.TaskContext{
		Share:          protocol.ShareTaskContext{Provider: "pan123"},
		StagingTarget:  protocol.ProviderTargetRequirement{Provider: "pan123", NeedShareSave: true, RequiredBytes: 1 << 30},
		TargetProfile:  "mobile-primary",
		DeliveryTarget: protocol.ProviderTargetRequirement{Provider: "pan115", Folder: "剧集", NeedUpload: true},
	}, []string{"share.save", "mobile.upload", "result.report"}, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected worker to be rejected when delivery target provider does not match target binding provider")
	}
}

func TestProviderAccountEligibilityRejectsUnknown139LimitAndInsufficientStagingSpace(t *testing.T) {
	unknownLimit := protocol.ProviderAccountInventory{
		Provider: "yidong139", Status: "work", SupportsUpload: true, SupportsETF: true, FreeBytes: 100 << 30,
	}
	if providerAccountMatches(unknownLimit, protocol.ProviderTargetRequirement{
		Provider: "yidong139", NeedUpload: true, RequiredBytes: 1 << 30,
	}, "", false) {
		t.Fatal("139 account with an unknown upload limit must not be treated as unlimited")
	}
	staging := protocol.ProviderAccountInventory{
		Provider: "pan123", Status: "work", SupportsShareSave: true, SupportsDownload: true, FreeBytes: 4 << 30,
	}
	if providerAccountMatches(staging, protocol.ProviderTargetRequirement{
		Provider: "pan123", NeedShareSave: true, RequiredBytes: 5 << 30,
	}, "", true) {
		t.Fatal("staging account with insufficient free space must be rejected")
	}
}

func TestSelectProviderAccountOrdersByMembershipSpaceAndLoad(t *testing.T) {
	base := protocol.ProviderAccountInventory{
		Provider: "yidong139", Status: "work", SupportsUpload: true, SupportsETF: true,
		MaxSingleUploadBytes: 500 << 30,
	}
	accounts := []protocol.ProviderAccountInventory{
		func() protocol.ProviderAccountInventory {
			a := base
			a.NodeMountID = "ordinary"
			a.MembershipWeight = 100
			a.FreeBytes = 900 << 30
			return a
		}(),
		func() protocol.ProviderAccountInventory {
			a := base
			a.NodeMountID = "diamond-busy"
			a.MembershipWeight = 400
			a.FreeBytes = 800 << 30
			a.ActiveJobs = 3
			return a
		}(),
		func() protocol.ProviderAccountInventory {
			a := base
			a.NodeMountID = "diamond-idle"
			a.MembershipWeight = 400
			a.FreeBytes = 800 << 30
			a.ActiveJobs = 1
			return a
		}(),
	}
	selected, ok := selectProviderAccount(accounts, protocol.ProviderTargetRequirement{
		Provider: "yidong139", NeedUpload: true, RequiredBytes: 10 << 30,
	}, "", false)
	if !ok {
		t.Fatal("expected an eligible provider account")
	}
	if selected.NodeMountID != "diamond-idle" {
		t.Fatalf("selected account = %q, want diamond-idle", selected.NodeMountID)
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

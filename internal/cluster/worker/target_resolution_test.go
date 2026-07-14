package worker

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestResolveStagingTempRootUsesEnabledStorageForTaskTarget(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = original })
	setWorkerSubscriptionConfig(t, model.SubscriptionConfig{})

	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))), &gorm.Config{})
	require.NoError(t, err)
	db.Init(database)
	require.NoError(t, database.Create(&model.Storage{MountPath: "/worker-pan123", Driver: "123pan", Status: "work"}).Error)
	oldEnsure := ensureResolvedProviderFolder
	ensureResolvedProviderFolder = func(_ context.Context, target subscription.ResolvedProviderTarget) (subscription.ResolvedProviderTarget, error) {
		return target, nil
	}
	t.Cleanup(func() { ensureResolvedProviderFolder = oldEnsure })

	service := New(&fakeResultQueue{}, nil)
	task := protocol.TaskContext{
		Share: protocol.ShareTaskContext{Provider: "aliyundrive"},
		StagingTarget: protocol.ProviderTargetRequirement{
			Provider:      "pan123",
			Folder:        "转存至移动",
			NeedShareSave: true,
			RequiredBytes: 8 << 30,
		},
		SourceObjects: []protocol.SourceObject{{Provider: "aliyundrive", SourceFileID: "file-1", Size: 8 << 30}},
	}

	got, err := service.resolveStagingTempRoot(t.Context(), task)
	require.NoError(t, err)
	require.Equal(t, "/worker-pan123/转存至移动", got)
}

func TestResolveStagingTempRootRejectsMissingBoundAccountWithoutLegacyFallback(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = original })
	setWorkerSubscriptionConfig(t, model.SubscriptionConfig{})

	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))), &gorm.Config{})
	require.NoError(t, err)
	db.Init(database)
	require.NoError(t, database.Create(&model.Storage{ID: 7, MountPath: "/worker-pan123", Driver: "123pan", Status: "work"}).Error)

	service := New(&fakeResultQueue{}, nil)
	_, err = service.resolveStagingTempRoot(t.Context(), protocol.TaskContext{
		StagingTarget: protocol.ProviderTargetRequirement{
			Provider: "pan123", Folder: "转存至移动", StorageID: 999, NeedShareSave: true,
		},
		SourceObjects: []protocol.SourceObject{{Provider: "pan123", SourceFileID: "file-1", Size: 1 << 30}},
	})
	require.ErrorContains(t, err, "no compatible provider account")
}

func TestResolveDeliveryTargetRootUsesBindingMountAndTaskFolder(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = original })
	setWorkerSubscriptionConfig(t, model.SubscriptionConfig{})

	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))), &gorm.Config{})
	require.NoError(t, err)
	db.Init(database)
	require.NoError(t, database.Create(&model.Storage{MountPath: "/worker-139-a", Driver: "139 cloud", Status: "work", Addition: `{"type":"personal_new","cluster_dedicated_account":true,"membership_tier":"diamond"}`}).Error)
	oldEnsure := ensureResolvedProviderFolder
	ensureResolvedProviderFolder = func(_ context.Context, target subscription.ResolvedProviderTarget) (subscription.ResolvedProviderTarget, error) {
		return target, nil
	}
	t.Cleanup(func() { ensureResolvedProviderFolder = oldEnsure })

	service := New(&fakeResultQueue{}, nil)
	service.desiredConfig.TargetBindings = map[string]protocol.TargetBinding{
		"mobile_primary": {MountPath: "/worker-139-a"},
	}
	task := protocol.TaskContext{
		TargetProfile: "mobile-primary",
		DeliveryTarget: protocol.ProviderTargetRequirement{
			Provider:      "yidong139",
			Folder:        "剧集/港台剧",
			NeedUpload:    true,
			RequiredBytes: 8 << 30,
		},
	}

	got, bindingMount, err := service.resolveDeliveryTargetRoot(t.Context(), task)
	require.NoError(t, err)
	require.Equal(t, "/worker-139-a/剧集/港台剧", got)
	require.Equal(t, "/worker-139-a", bindingMount)
}

func TestResolveDeliveryTargetRootRejectsMismatchedProviderBinding(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = original })
	setWorkerSubscriptionConfig(t, model.SubscriptionConfig{})

	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))), &gorm.Config{})
	require.NoError(t, err)
	db.Init(database)
	require.NoError(t, database.Create(&model.Storage{MountPath: "/worker-115", Driver: "115 Cloud", Status: "work"}).Error)

	service := New(&fakeResultQueue{}, nil)
	service.desiredConfig.TargetBindings = map[string]protocol.TargetBinding{
		"mobile_primary": {MountPath: "/worker-115"},
	}
	_, _, err = service.resolveDeliveryTargetRoot(t.Context(), protocol.TaskContext{
		TargetProfile:  "mobile-primary",
		DeliveryTarget: protocol.ProviderTargetRequirement{Provider: "yidong139", Folder: "剧集"},
	})
	require.ErrorContains(t, err, "no compatible provider account")
}

func TestResolveStagingTempRootUsesWorkerConfigFolderInsteadOfTaskFolder(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = original })
	setWorkerSubscriptionConfig(t, model.SubscriptionConfig{
		Telegram: model.SubscriptionTelegramSourceConfig{
			Pan123: model.SubscriptionTelegramPanConfig{
				TempTransferTarget: model.SubscriptionStorageTarget{Provider: "pan123", Folder: "worker-staging"},
			},
		},
	})

	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))), &gorm.Config{})
	require.NoError(t, err)
	db.Init(database)
	require.NoError(t, database.Create(&model.Storage{ID: 7, MountPath: "/worker-pan123", Driver: "123pan", Status: "work"}).Error)
	oldEnsure := ensureResolvedProviderFolder
	ensureResolvedProviderFolder = func(_ context.Context, target subscription.ResolvedProviderTarget) (subscription.ResolvedProviderTarget, error) {
		return target, nil
	}
	t.Cleanup(func() { ensureResolvedProviderFolder = oldEnsure })

	service := New(&fakeResultQueue{}, nil)
	got, err := service.resolveStagingTempRoot(t.Context(), protocol.TaskContext{
		StagingTarget: protocol.ProviderTargetRequirement{
			Provider: "pan123", Folder: "coordinator-staging", StorageID: 7, NeedShareSave: true, RequiredBytes: 8 << 30,
		},
		SourceObjects: []protocol.SourceObject{{Provider: "pan123", SourceFileID: "file-1", Size: 8 << 30}},
	})
	require.NoError(t, err)
	require.Equal(t, "/worker-pan123/worker-staging", got)
}

func TestResolveStagingTempRootRequiresWorkerConfigForProviderOnlyTask(t *testing.T) {
	setWorkerSubscriptionConfig(t, model.SubscriptionConfig{})

	service := New(&fakeResultQueue{}, nil)
	_, err := service.resolveStagingTempRoot(t.Context(), protocol.TaskContext{
		StagingTarget: protocol.ProviderTargetRequirement{Provider: "pan123", NeedShareSave: true},
	})
	require.ErrorContains(t, err, "worker local pan123 staging folder is required")
}

func TestResolveDeliveryTargetRootUsesWorkerConfigFolderAndTaskAccountBinding(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = original })
	setWorkerSubscriptionConfig(t, model.SubscriptionConfig{
		DefaultTarget: model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "worker-delivery"},
	})

	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))), &gorm.Config{})
	require.NoError(t, err)
	db.Init(database)
	storage := model.Storage{
		ID: 8, MountPath: "/worker-139", Driver: "139 cloud", Status: "work",
		Addition: `{"type":"personal_new","cluster_dedicated_account":true,"membership_tier":"diamond"}`,
	}
	require.NoError(t, database.Create(&storage).Error)
	oldEnsure := ensureResolvedProviderFolder
	ensureResolvedProviderFolder = func(_ context.Context, target subscription.ResolvedProviderTarget) (subscription.ResolvedProviderTarget, error) {
		return target, nil
	}
	t.Cleanup(func() { ensureResolvedProviderFolder = oldEnsure })

	service := New(&fakeResultQueue{}, nil)
	service.controlNodeID = "worker-1"
	got, bindingMount, err := service.resolveDeliveryTargetRoot(t.Context(), protocol.TaskContext{
		DeliveryTarget: protocol.ProviderTargetRequirement{
			Provider:           "yidong139",
			Folder:             "coordinator-delivery",
			StorageID:          storage.ID,
			NodeMountID:        stableMountID(service.controlNodeID, storage.ID, storage.MountPath),
			AccountFingerprint: accountFingerprint(storage, "yidong139"),
			NeedUpload:         true,
			RequiredBytes:      8 << 30,
		},
	})
	require.NoError(t, err)
	require.Equal(t, "/worker-139/worker-delivery", got)
	require.Equal(t, "/worker-139", bindingMount)
}

func TestResolveDeliveryTargetRootRequiresWorkerConfigForProviderOnlyTask(t *testing.T) {
	setWorkerSubscriptionConfig(t, model.SubscriptionConfig{})

	service := New(&fakeResultQueue{}, nil)
	_, _, err := service.resolveDeliveryTargetRoot(t.Context(), protocol.TaskContext{
		DeliveryTarget: protocol.ProviderTargetRequirement{Provider: "yidong139", NeedUpload: true},
	})
	require.ErrorContains(t, err, "worker local yidong139 delivery folder is required")
}

func setWorkerSubscriptionConfig(t *testing.T, cfg model.SubscriptionConfig) {
	t.Helper()
	original := getWorkerSubscriptionConfig
	getWorkerSubscriptionConfig = func() (model.SubscriptionConfig, error) { return cfg, nil }
	t.Cleanup(func() { getWorkerSubscriptionConfig = original })
}

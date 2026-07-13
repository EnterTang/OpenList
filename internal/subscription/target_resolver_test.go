package subscription

import (
	"context"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestResolveProviderTargetPrefersHigherFreeSpace(t *testing.T) {
	oldList := listProviderTargetStorages
	oldFree := storageFreeBytesForMountPath
	defer func() {
		listProviderTargetStorages = oldList
		storageFreeBytesForMountPath = oldFree
	}()

	listProviderTargetStorages = func() ([]model.Storage, error) {
		return []model.Storage{
			{ID: 1, MountPath: "/123-a", Driver: "123Pan"},
			{ID: 2, MountPath: "/123-b", Driver: "123Pan"},
		}, nil
	}
	storageFreeBytesForMountPath = func(ctx context.Context, mountPath string) (int64, bool) {
		switch mountPath {
		case "/123-a":
			return 100, true
		case "/123-b":
			return 200, true
		default:
			return 0, false
		}
	}

	resolved, err := ResolveProviderTarget(context.Background(), ResolveProviderTargetRequest{
		Provider: "pan123",
		Folder:   "转存至移动",
	})
	if err != nil {
		t.Fatalf("resolve provider target: %v", err)
	}
	if resolved.StorageID != 2 {
		t.Fatalf("storage id = %d, want 2", resolved.StorageID)
	}
	if resolved.MountPath != "/123-b" {
		t.Fatalf("mount path = %q, want /123-b", resolved.MountPath)
	}
	if resolved.FullPath != "/123-b/转存至移动" {
		t.Fatalf("full path = %q, want /123-b/转存至移动", resolved.FullPath)
	}
}

func TestTelegramPanSourceConfigWithStorageFallbackResolvesTempTransferTarget(t *testing.T) {
	oldList := listProviderTargetStorages
	oldFree := storageFreeBytesForMountPath
	defer func() {
		listProviderTargetStorages = oldList
		storageFreeBytesForMountPath = oldFree
	}()

	listProviderTargetStorages = func() ([]model.Storage, error) {
		return []model.Storage{{ID: 3, MountPath: "/123-main", Driver: "123Pan"}}, nil
	}
	storageFreeBytesForMountPath = func(ctx context.Context, mountPath string) (int64, bool) {
		return 300, true
	}

	cfg := telegramPanSourceConfigWithStorageFallback(ShareProviderPan123, model.SubscriptionTelegramPanConfig{
		TempTransferTarget: model.SubscriptionStorageTarget{
			Provider: "pan123",
			Folder:   "转存至移动",
		},
	})
	if cfg.TempTransferRoot != "/123-main/转存至移动" {
		t.Fatalf("temp root = %q, want /123-main/转存至移动", cfg.TempTransferRoot)
	}
	if cfg.TempTransferTarget.Provider != "pan123" || cfg.TempTransferTarget.Folder != "转存至移动" {
		t.Fatalf("temp transfer target = %#v", cfg.TempTransferTarget)
	}
}

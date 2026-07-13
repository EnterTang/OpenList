package subscription

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestMigrateLegacyPathTarget(t *testing.T) {
	target, ok := MigrateLegacyPathTarget("/123/转存至移动")
	if !ok {
		t.Fatal("expected migration to succeed")
	}
	if target.Provider != "pan123" {
		t.Fatalf("provider = %q, want pan123", target.Provider)
	}
	if target.Folder != "转存至移动" {
		t.Fatalf("folder = %q, want 转存至移动", target.Folder)
	}
}

func TestNormalizeSubscriptionStorageTargetMigratesLegacyFolderPath(t *testing.T) {
	target := NormalizeSubscriptionStorageTarget(model.SubscriptionStorageTarget{
		Provider: "",
		Folder:   "/139_60t/剧集",
	})
	if target.Provider != "yidong139" {
		t.Fatalf("provider = %q, want yidong139", target.Provider)
	}
	if target.Folder != "剧集" {
		t.Fatalf("folder = %q, want 剧集", target.Folder)
	}
}

func TestNormalizeConfigMigratesDefaultTargetRootToProviderTarget(t *testing.T) {
	cfg := normalizeConfig(model.SubscriptionConfig{
		DefaultTargetRoot: "/139_60t/剧集",
	})
	if cfg.DefaultTarget.Provider != "yidong139" {
		t.Fatalf("default target provider = %q, want yidong139", cfg.DefaultTarget.Provider)
	}
	if cfg.DefaultTarget.Folder != "剧集" {
		t.Fatalf("default target folder = %q, want 剧集", cfg.DefaultTarget.Folder)
	}
}

func TestNormalizeTelegramPanConfigMigratesTempTransferRootToTarget(t *testing.T) {
	cfg := normalizeTelegramPanConfig(model.SubscriptionTelegramPanConfig{
		TempTransferRoot: "/123/转存至移动",
	})
	if cfg.TempTransferTarget.Provider != "pan123" {
		t.Fatalf("temp transfer provider = %q, want pan123", cfg.TempTransferTarget.Provider)
	}
	if cfg.TempTransferTarget.Folder != "转存至移动" {
		t.Fatalf("temp transfer folder = %q, want 转存至移动", cfg.TempTransferTarget.Folder)
	}
}

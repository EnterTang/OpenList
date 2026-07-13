package cluster

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
)

func TestSubscriptionMediaTaskContextCarriesProviderTargets(t *testing.T) {
	cfg := model.SubscriptionConfig{
		Telegram: model.SubscriptionTelegramSourceConfig{
			Pan123: model.SubscriptionTelegramPanConfig{
				TempTransferTarget: model.SubscriptionStorageTarget{Provider: "pan123", Folder: "转存至移动"},
			},
		},
	}
	task := subscription.ClusterMediaTask{
		SubscriptionID:        11,
		SubscriptionItemID:    22,
		SubscriptionName:      "Example",
		SourceKey:             "source-1",
		SourceMessageID:       "9001",
		ShareProvider:         "pan123",
		ShareURL:              "https://www.123pan.com/s/example",
		SharePasscode:         "1234",
		ShareRefFingerprint:   "share-ref",
		SourceFileID:          "file-1",
		SourceRelativePath:    "Example.S01E01.mkv",
		SourceSize:            7 << 30,
		SourceHash:            "hash-1",
		MediaItemID:           "media-1",
		MediaType:             "tv",
		TMDBID:                123,
		Season:                1,
		Episode:               1,
		LogicalMediaRoot:      "/139_60t/港台剧",
		LogicalTargetPath:     "/139_60t/港台剧/Example/Season 01/Example.S01E01.mkv",
		WorkflowVersion:       "workflow-1",
		SealedManifestVersion: "manifest-1",
	}

	context := subscriptionMediaTaskContext(cfg, task, "mobile-primary")
	if context.StagingTarget.Provider != "pan123" || context.StagingTarget.Folder != "转存至移动" {
		t.Fatalf("staging target = %#v", context.StagingTarget)
	}
	if context.DeliveryTarget.Provider != "yidong139" || context.DeliveryTarget.Folder != "港台剧" {
		t.Fatalf("delivery target = %#v", context.DeliveryTarget)
	}
	if !context.StagingTarget.NeedShareSave || !context.DeliveryTarget.NeedUpload {
		t.Fatalf("target flags = staging:%#v delivery:%#v", context.StagingTarget, context.DeliveryTarget)
	}
}

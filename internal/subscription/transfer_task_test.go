package subscription

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/OpenListTeam/OpenList/v4/drivers/local"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/sirupsen/logrus"
)

func TestHandleTransferPayloadMarksTransferred(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	item := &model.SubscriptionItem{
		SubscriptionID: 1,
		SourceKey:      "demo-key",
		Status:         model.SubscriptionItemStatusTransferring,
	}
	if _, _, err := db.UpsertSubscriptionItem(item); err != nil {
		t.Fatalf("upsert item: %v", err)
	}
	handleTransferPayload(t.Context(), true, TransferFinalizePayload{
		SubscriptionID: 1,
		SourceKey:      "demo-key",
		TargetDir:      "/media/tv",
		FileName:       "demo.mkv",
		TargetName:     "demo.mkv",
	})
	got, err := db.GetSubscriptionItem(1, "demo-key")
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	if got.Status != model.SubscriptionItemStatusTransferred {
		t.Fatalf("status = %q, want %q", got.Status, model.SubscriptionItemStatusTransferred)
	}
}

func TestHandleTransferPayloadMarksFailed(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	item := &model.SubscriptionItem{
		SubscriptionID: 2,
		SourceKey:      "demo-key-2",
		Status:         model.SubscriptionItemStatusTransferring,
	}
	if _, _, err := db.UpsertSubscriptionItem(item); err != nil {
		t.Fatalf("upsert item: %v", err)
	}
	handleTransferPayload(t.Context(), false, TransferFinalizePayload{
		SubscriptionID: 2,
		SourceKey:      "demo-key-2",
	})
	got, err := db.GetSubscriptionItem(2, "demo-key-2")
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	if got.Status != model.SubscriptionItemStatusFailed {
		t.Fatalf("status = %q, want %q", got.Status, model.SubscriptionItemStatusFailed)
	}
}

func TestFinalizeTransferTreatsGeneratedETFAsTransferredWhenSourceWasDeleted(t *testing.T) {
	setupSubscriptionRuntimeDB(t)

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "media"), 0o755); err != nil {
		t.Fatalf("mkdir media: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "media", "Movie.mkv.etf"), []byte("etf"), 0o644); err != nil {
		t.Fatalf("write etf: %v", err)
	}
	mountPath := "/" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	_, err := op.CreateStorage(context.Background(), model.Storage{
		Driver:    "Local",
		MountPath: mountPath,
		Addition:  fmt.Sprintf(`{"root_folder_path":%q}`, root),
	})
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}

	item := &model.SubscriptionItem{
		SubscriptionID: 3,
		SourceKey:      "demo-key-3",
		Status:         model.SubscriptionItemStatusTransferring,
	}
	if _, _, err := db.UpsertSubscriptionItem(item); err != nil {
		t.Fatalf("upsert item: %v", err)
	}

	var logs bytes.Buffer
	oldOutput := logrus.StandardLogger().Out
	logrus.SetOutput(&logs)
	t.Cleanup(func() {
		logrus.SetOutput(oldOutput)
	})

	finalizeSubscriptionTransfer(context.Background(), TransferFinalizePayload{
		SubscriptionID: 3,
		SourceKey:      "demo-key-3",
		TargetDir:      mountPath + "/media",
		FileName:       "Movie.mkv",
		TargetName:     "Movie.2024.mkv",
	})

	got, err := db.GetSubscriptionItem(3, "demo-key-3")
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	if got.Status != model.SubscriptionItemStatusTransferred {
		t.Fatalf("status = %q error = %q, want transferred", got.Status, got.LastError)
	}
	if strings.Contains(logs.String(), "failed rename") {
		t.Fatalf("finalize logged a failed rename for generated ETF fallback: %s", logs.String())
	}
}

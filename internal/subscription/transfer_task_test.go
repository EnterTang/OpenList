package subscription

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/OpenListTeam/OpenList/v4/drivers/local"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
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
	item.TargetDir = "/media/tv"
	item.FileName = "demo.mkv"
	item.TargetName = "demo.mkv"
	item, _, err := db.UpsertSubscriptionItem(item)
	if err != nil {
		t.Fatalf("upsert item: %v", err)
	}
	handleTransferPayload(t.Context(), true, TransferFinalizePayload{
		SubscriptionID:     1,
		SubscriptionItemID: item.ID,
		SourceKey:          "demo-key",
		FileHash:           item.FileHash,
		TargetDir:          "/media/tv",
		FileName:           "demo.mkv",
		TargetName:         "demo.mkv",
	})
	got, err := db.GetSubscriptionItem(1, "demo-key")
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	if got.Status != model.SubscriptionItemStatusTransferred {
		t.Fatalf("status = %q, want %q", got.Status, model.SubscriptionItemStatusTransferred)
	}
	sources, err := db.ListSubscriptionEpisodeSources(1)
	if err != nil {
		t.Fatalf("list episode sources: %v", err)
	}
	if len(sources) != 0 {
		t.Fatalf("episode sources after finalize success = %#v, want none", sources)
	}
}

func TestRecoverStaleStandaloneTransferMarksExistingTargetTransferred(t *testing.T) {
	setupSubscriptionRuntimeDB(t)

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "delivery"), 0o755); err != nil {
		t.Fatalf("mkdir delivery: %v", err)
	}
	mountPath := "/" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	if _, err := op.CreateStorage(context.Background(), model.Storage{
		Driver:    "Local",
		MountPath: mountPath,
		Addition:  fmt.Sprintf(`{"root_folder_path":%q}`, root),
	}); err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	item, _, err := db.UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 4,
		SourceKey:      "stale-existing-target",
		FileHash:       "stale-hash",
		FileName:       "episode.mkv",
		TargetDir:      mountPath + "/delivery",
		TargetName:     "episode.mkv",
		TargetPath:     mountPath + "/delivery/episode.mkv",
		Status:         model.SubscriptionItemStatusTransferring,
	})
	if err != nil {
		t.Fatalf("upsert item: %v", err)
	}
	if err := db.GetDb().Model(&model.SubscriptionItem{}).Where("id = ?", item.ID).Update("updated_at", time.Now().Add(-time.Hour)).Error; err != nil {
		t.Fatalf("age item: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "delivery", "episode.mkv"), []byte("done"), 0o644); err != nil {
		t.Fatalf("write target: %v", err)
	}

	recovered, err := RecoverStaleStandaloneTransfers(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("recovered = %d, want 1", recovered)
	}
	got, err := db.GetSubscriptionItem(4, item.SourceKey)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.SubscriptionItemStatusTransferred {
		t.Fatalf("status = %q, want transferred", got.Status)
	}
}

func TestRecoverStaleStandaloneTransferResetsMissingTargetToPending(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	root := t.TempDir()
	mountPath := "/" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	if _, err := op.CreateStorage(context.Background(), model.Storage{
		Driver:    "Local",
		MountPath: mountPath,
		Addition:  fmt.Sprintf(`{"root_folder_path":%q}`, root),
	}); err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	item, _, err := db.UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 5,
		SourceKey:      "stale-missing-target",
		FileHash:       "stale-hash-2",
		FileName:       "episode.mkv",
		TargetDir:      mountPath + "/missing/delivery",
		TargetName:     "episode.mkv",
		TargetPath:     mountPath + "/missing/delivery/episode.mkv",
		Status:         model.SubscriptionItemStatusTransferring,
	})
	if err != nil {
		t.Fatalf("upsert item: %v", err)
	}
	if err := db.GetDb().Model(&model.SubscriptionItem{}).Where("id = ?", item.ID).Update("updated_at", time.Now().Add(-time.Hour)).Error; err != nil {
		t.Fatalf("age item: %v", err)
	}

	recovered, err := RecoverStaleStandaloneTransfers(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 {
		t.Fatalf("recovered = %d, want 1", recovered)
	}
	got, err := db.GetSubscriptionItem(5, item.SourceKey)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != model.SubscriptionItemStatusPending || got.LastError == "" {
		t.Fatalf("item = %#v, want pending with recovery reason", got)
	}
}

func TestHandleTransferPayloadMarksFailed(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	item := &model.SubscriptionItem{
		SubscriptionID: 2,
		SourceKey:      "demo-key-2",
		Status:         model.SubscriptionItemStatusTransferring,
	}
	item, _, err := db.UpsertSubscriptionItem(item)
	if err != nil {
		t.Fatalf("upsert item: %v", err)
	}
	handleTransferPayload(t.Context(), false, TransferFinalizePayload{
		SubscriptionID:     2,
		SubscriptionItemID: item.ID,
		SourceKey:          "demo-key-2",
		FileHash:           item.FileHash,
	})
	got, err := db.GetSubscriptionItem(2, "demo-key-2")
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	if got.Status != model.SubscriptionItemStatusFailed {
		t.Fatalf("status = %q, want %q", got.Status, model.SubscriptionItemStatusFailed)
	}
	sources, err := db.ListSubscriptionEpisodeSources(2)
	if err != nil {
		t.Fatalf("list episode sources: %v", err)
	}
	if len(sources) != 0 {
		t.Fatalf("episode sources after finalize failure = %#v, want none", sources)
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
		FileHash:       "movie-hash",
		TargetDir:      mountPath + "/media",
		FileName:       "Movie.mkv",
		TargetName:     "Movie.2024.mkv",
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
		SubscriptionID:     3,
		SubscriptionItemID: item.ID,
		SourceKey:          "demo-key-3",
		FileHash:           item.FileHash,
		TargetDir:          mountPath + "/media",
		FileName:           "Movie.mkv",
		TargetName:         "Movie.2024.mkv",
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

func TestApplyItemTransferSnapshotsAcceptedMovieAtMovieSlot(t *testing.T) {
	setupSubscriptionRuntimeDB(t)

	sub := &model.Subscription{
		Name:       "Standalone movie snapshot",
		SourceType: model.SubscriptionSourceManual,
		MediaType:  "movie",
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "source"), 0o755); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	sourceFile := filepath.Join(root, "source", "Show.S01E02.mkv")
	if err := os.WriteFile(sourceFile, []byte("media"), 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	mountPath := "/" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	if _, err := op.CreateStorage(context.Background(), model.Storage{
		Driver:    "Local",
		MountPath: mountPath,
		Addition:  fmt.Sprintf(`{"root_folder_path":%q}`, root),
	}); err != nil {
		t.Fatalf("create local storage: %v", err)
	}

	item := &model.SubscriptionItem{
		SubscriptionID: sub.ID,
		SourceKey:      "standalone-source",
		SourceProvider: "pan123",
		SourceURL:      "https://www.123pan.com/s/example",
		SourcePath:     mountPath + "/source/Show.S01E02.mkv",
		FileName:       "Show.S01E02.mkv",
		FilePath:       "/source/Show.S01E02.mkv",
		FileSize:       5,
		Season:         1,
		Episode:        7,
		TargetDir:      mountPath + "/delivery",
		TargetName:     "Show.S01E02.mkv",
		TargetPath:     mountPath + "/delivery/Show.S01E02.mkv",
		Status:         model.SubscriptionItemStatusPending,
	}
	stored, _, err := db.UpsertSubscriptionItem(item)
	if err != nil {
		t.Fatalf("upsert subscription item: %v", err)
	}

	ctx := context.WithValue(context.Background(), conf.NoTaskKey, struct{}{})
	updated, delta, err := applyItemTransfer(ctx, sub, stored, false, persistAcceptedSubscriptionItemAndEpisodeSourceSnapshot)
	if err != nil {
		t.Fatalf("apply item transfer: %v", err)
	}
	if delta != 1 {
		t.Fatalf("delta = %d, want 1", delta)
	}
	if updated.Status != model.SubscriptionItemStatusTransferring {
		t.Fatalf("updated status = %q, want transferring", updated.Status)
	}

	sources, err := db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil {
		t.Fatalf("list episode sources: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("episode sources len = %d, want 1: %#v", len(sources), sources)
	}
	got := sources[0]
	if got.SourceItemID != updated.ID ||
		got.SourceType != model.SubscriptionSourceManual ||
		got.SourceProvider != "pan123" ||
		got.ShareURL != "https://www.123pan.com/s/example" ||
		got.FileName != "Show.S01E02.mkv" ||
		got.ClusterJobID != "" ||
		got.Season != 0 ||
		got.Episode != 0 ||
		got.SelectedAt.IsZero() {
		t.Fatalf("episode source = %#v", got)
	}
}

func TestApplyItemTransferRecoversSnapshotAfterAcceptedStateFailure(t *testing.T) {
	setupSubscriptionRuntimeDB(t)

	sub := &model.Subscription{Name: "Standalone snapshot failure", SourceType: model.SubscriptionSourceManual, MediaType: "movie"}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "source"), 0o755); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "source", "Episode.mkv"), []byte("media"), 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	mountPath := "/" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	if _, err := op.CreateStorage(context.Background(), model.Storage{
		Driver:    "Local",
		MountPath: mountPath,
		Addition:  fmt.Sprintf(`{"root_folder_path":%q}`, root),
	}); err != nil {
		t.Fatalf("create local storage: %v", err)
	}

	stored, _, err := db.UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: sub.ID,
		SourceKey:      "standalone-snapshot-failure",
		SourceProvider: "pan123",
		SourceURL:      "https://www.123pan.com/s/example",
		SourcePath:     mountPath + "/source/Episode.mkv",
		FileName:       "Episode.mkv",
		Season:         1,
		Episode:        9,
		TargetDir:      mountPath + "/delivery",
		TargetName:     "Episode.mkv",
		TargetPath:     mountPath + "/delivery/Episode.mkv",
		Status:         model.SubscriptionItemStatusPending,
	})
	if err != nil {
		t.Fatalf("upsert subscription item: %v", err)
	}

	ctx := context.WithValue(context.Background(), conf.NoTaskKey, struct{}{})
	updated, delta, err := applyItemTransfer(ctx, sub, stored, false, func(*model.Subscription, *model.SubscriptionItem) error {
		return errors.New("forced source snapshot persistence failure")
	})
	if err == nil || err.Error() != "forced source snapshot persistence failure" {
		t.Fatalf("apply item transfer error = %v", err)
	}
	if delta != 0 {
		t.Fatalf("delta = %d, want 0", delta)
	}
	if updated == nil {
		t.Fatal("updated item is nil")
	}

	persisted, err := db.GetSubscriptionItem(sub.ID, stored.SourceKey)
	if err != nil {
		t.Fatalf("get persisted item: %v", err)
	}
	if persisted.Status != model.SubscriptionItemStatusPending {
		t.Fatalf("persisted status = %q, want pending", persisted.Status)
	}
	sources, err := db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil {
		t.Fatalf("list episode sources: %v", err)
	}
	if len(sources) != 0 {
		t.Fatalf("episode sources after snapshot failure = %#v, want none", sources)
	}

	handleTransferPayload(ctx, true, TransferFinalizePayload{
		SubscriptionID:     sub.ID,
		SubscriptionItemID: stored.ID,
		SourceKey:          stored.SourceKey,
		FileHash:           stored.FileHash,
		TargetDir:          stored.TargetDir,
		FileName:           stored.FileName,
		TargetName:         stored.TargetName,
	})
	persisted, err = db.GetSubscriptionItem(sub.ID, stored.SourceKey)
	if err != nil {
		t.Fatalf("get recovered item: %v", err)
	}
	if persisted.Status != model.SubscriptionItemStatusTransferred {
		t.Fatalf("recovered status = %q, want transferred", persisted.Status)
	}
	sources, err = db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil {
		t.Fatalf("list recovered episode sources: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("recovered episode sources = %#v, want one", sources)
	}
	source := sources[0]
	if source.Season != 0 ||
		source.Episode != 0 ||
		source.SourceItemID != stored.ID ||
		source.SourceType != model.SubscriptionSourceManual ||
		source.SourceProvider != "pan123" ||
		source.ShareURL != "https://www.123pan.com/s/example" ||
		source.FileName != stored.FileName ||
		source.ClusterJobID != "" {
		t.Fatalf("recovered episode source = %#v", source)
	}
}

func TestHandleTransferPayloadRecoveryDoesNotOverwriteNewerMovieSnapshot(t *testing.T) {
	setupSubscriptionRuntimeDB(t)

	sub := &model.Subscription{Name: "Standalone newer snapshot", SourceType: model.SubscriptionSourceManual, MediaType: "movie"}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "source"), 0o755); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "source", "Movie.mkv"), []byte("media"), 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	mountPath := "/" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	if _, err := op.CreateStorage(context.Background(), model.Storage{
		Driver:    "Local",
		MountPath: mountPath,
		Addition:  fmt.Sprintf(`{"root_folder_path":%q}`, root),
	}); err != nil {
		t.Fatalf("create local storage: %v", err)
	}

	stored, _, err := db.UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: sub.ID,
		SourceKey:      "standalone-newer-snapshot",
		SourceProvider: "old-provider",
		SourceURL:      "https://old.example/s/movie",
		SourcePath:     mountPath + "/source/Movie.mkv",
		FileName:       "Movie.mkv",
		Season:         1,
		Episode:        11,
		TargetDir:      mountPath + "/delivery",
		TargetName:     "Movie.mkv",
		TargetPath:     mountPath + "/delivery/Movie.mkv",
		Status:         model.SubscriptionItemStatusPending,
	})
	if err != nil {
		t.Fatalf("upsert subscription item: %v", err)
	}

	ctx := context.WithValue(context.Background(), conf.NoTaskKey, struct{}{})
	_, _, err = applyItemTransfer(ctx, sub, stored, false, func(*model.Subscription, *model.SubscriptionItem) error {
		return errors.New("forced source snapshot persistence failure")
	})
	if err == nil || err.Error() != "forced source snapshot persistence failure" {
		t.Fatalf("apply item transfer error = %v", err)
	}
	newer := &model.SubscriptionEpisodeSource{
		SubscriptionID: sub.ID,
		Season:         0,
		Episode:        0,
		SourceItemID:   999,
		SourceType:     model.SubscriptionSourceTelegram,
		SourceProvider: "new-provider",
		ShareURL:       "https://new.example/s/movie",
		FileName:       "New.Movie.mkv",
		ClusterJobID:   "newer-job",
	}
	if _, err := db.UpsertSubscriptionEpisodeSource(newer); err != nil {
		t.Fatalf("seed newer episode source: %v", err)
	}

	handleTransferPayload(ctx, false, TransferFinalizePayload{
		SubscriptionID:     sub.ID,
		SubscriptionItemID: stored.ID,
		SourceKey:          stored.SourceKey,
		FileHash:           stored.FileHash,
		TargetDir:          stored.TargetDir,
		FileName:           stored.FileName,
		TargetName:         stored.TargetName,
	})
	persisted, err := db.GetSubscriptionItem(sub.ID, stored.SourceKey)
	if err != nil {
		t.Fatalf("get recovered item: %v", err)
	}
	if persisted.Status != model.SubscriptionItemStatusFailed || persisted.LastError != "transfer task failed" {
		t.Fatalf("recovered item = %#v", persisted)
	}
	sources, err := db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil {
		t.Fatalf("list episode sources: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("episode sources = %#v, want one", sources)
	}
	if got := sources[0]; got.SourceItemID != newer.SourceItemID ||
		got.SourceType != newer.SourceType ||
		got.SourceProvider != newer.SourceProvider ||
		got.ShareURL != newer.ShareURL ||
		got.FileName != newer.FileName ||
		got.ClusterJobID != newer.ClusterJobID ||
		got.Season != 0 ||
		got.Episode != 0 {
		t.Fatalf("newer episode source was replaced: %#v", got)
	}
}

func TestHandleTransferPayloadDoesNotFinalizePendingItemWithoutAcceptedIdentity(t *testing.T) {
	setupSubscriptionRuntimeDB(t)

	sub := &model.Subscription{Name: "Pending item", SourceType: model.SubscriptionSourceManual, MediaType: "movie"}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	item, _, err := db.UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: sub.ID,
		SourceKey:      "pending-never-accepted",
		FileName:       "Movie.mkv",
		Season:         1,
		Episode:        12,
		TargetDir:      "/media/movie",
		TargetName:     "Movie.mkv",
		Status:         model.SubscriptionItemStatusPending,
	})
	if err != nil {
		t.Fatalf("upsert pending item: %v", err)
	}

	handleTransferPayload(t.Context(), true, TransferFinalizePayload{
		SubscriptionID: sub.ID,
		SourceKey:      item.SourceKey,
		TargetDir:      item.TargetDir,
		FileName:       item.FileName,
		TargetName:     item.TargetName,
	})
	persisted, err := db.GetSubscriptionItem(sub.ID, item.SourceKey)
	if err != nil {
		t.Fatalf("get pending item: %v", err)
	}
	if persisted.Status != model.SubscriptionItemStatusPending {
		t.Fatalf("pending item status = %q, want pending", persisted.Status)
	}
	sources, err := db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil {
		t.Fatalf("list episode sources: %v", err)
	}
	if len(sources) != 0 {
		t.Fatalf("episode sources for never-accepted item = %#v, want none", sources)
	}
}

func TestStaleTransferPayloadDoesNotFinalizeNewFileHash(t *testing.T) {
	setupSubscriptionRuntimeDB(t)

	sub := &model.Subscription{Name: "Stale standalone callback", SourceType: model.SubscriptionSourceManual, MediaType: "movie"}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	accepted, _, err := db.PersistAcceptedSubscriptionItemAndEpisodeSource(&model.SubscriptionItem{
		SubscriptionID: sub.ID,
		SourceKey:      "stale-standalone",
		SourceProvider: "old-provider",
		SourceURL:      "https://old.example/s/movie",
		FileName:       "Movie.mkv",
		FileHash:       "hash-old",
		Season:         1,
		Episode:        13,
		TargetDir:      "/media/movie",
		TargetName:     "Movie.mkv",
		TargetPath:     "/media/movie/Movie.mkv",
		Status:         model.SubscriptionItemStatusTransferring,
	}, &model.SubscriptionEpisodeSource{
		SubscriptionID: sub.ID,
		Season:         0,
		Episode:        0,
		SourceType:     model.SubscriptionSourceManual,
		SourceProvider: "old-provider",
		ShareURL:       "https://old.example/s/movie",
		FileName:       "Movie.mkv",
	})
	if err != nil {
		t.Fatalf("persist accepted item: %v", err)
	}
	payload := TransferFinalizePayload{
		SubscriptionID:     sub.ID,
		SubscriptionItemID: accepted.ID,
		SourceKey:          accepted.SourceKey,
		FileHash:           accepted.FileHash,
		TargetDir:          accepted.TargetDir,
		FileName:           accepted.FileName,
		TargetName:         accepted.TargetName,
	}

	newer := *accepted
	newer.FileHash = "hash-new"
	newer.Status = model.SubscriptionItemStatusPending
	if _, _, err := db.UpsertSubscriptionItem(&newer); err != nil {
		t.Fatalf("replace item with newer file hash: %v", err)
	}
	sourcesBefore, err := db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil || len(sourcesBefore) != 1 {
		t.Fatalf("source before stale callbacks = %#v err=%v", sourcesBefore, err)
	}

	handleTransferPayload(t.Context(), true, payload)
	assertStaleTransferPayloadDidNotChangeItemOrSource(t, sub.ID, accepted.SourceKey, sourcesBefore[0])
	handleTransferPayload(t.Context(), false, payload)
	assertStaleTransferPayloadDidNotChangeItemOrSource(t, sub.ID, accepted.SourceKey, sourcesBefore[0])
}

func TestTerminalCallbackDoesNotOverwriteScanThatWinsAfterValidation(t *testing.T) {
	setupSubscriptionRuntimeDB(t)

	sub := &model.Subscription{Name: "Terminal callback race", SourceType: model.SubscriptionSourceManual, MediaType: "movie"}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	accepted, _, err := db.PersistAcceptedSubscriptionItemAndEpisodeSource(&model.SubscriptionItem{
		SubscriptionID: sub.ID,
		SourceKey:      "terminal-callback-race",
		SourceProvider: "old-provider",
		SourceURL:      "https://old.example/s/movie",
		FileName:       "Old.Movie.mkv",
		FilePath:       "/old/Old.Movie.mkv",
		FileHash:       "hash-old",
		Season:         1,
		Episode:        15,
		TargetDir:      "/media/movie",
		TargetName:     "Old.Movie.mkv",
		TargetPath:     "/media/movie/Old.Movie.mkv",
		Status:         model.SubscriptionItemStatusTransferring,
	}, &model.SubscriptionEpisodeSource{
		SubscriptionID: sub.ID,
		Season:         0,
		Episode:        0,
		SourceType:     model.SubscriptionSourceManual,
		SourceProvider: "old-provider",
		ShareURL:       "https://old.example/s/movie",
		FileName:       "Old.Movie.mkv",
	})
	if err != nil {
		t.Fatalf("persist accepted item: %v", err)
	}
	sourcesBefore, err := db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil || len(sourcesBefore) != 1 {
		t.Fatalf("source before callback race = %#v err=%v", sourcesBefore, err)
	}

	oldPersist := persistSubscriptionTerminalItem
	persistSubscriptionTerminalItem = func(request db.SubscriptionTerminalItemRequest) (*model.SubscriptionItem, error) {
		newer := *accepted
		newer.SourceProvider = "new-provider"
		newer.SourceURL = "https://new.example/s/movie"
		newer.FileName = "New.Movie.mkv"
		newer.FilePath = "/new/New.Movie.mkv"
		newer.FileHash = "hash-new"
		newer.TargetDir = "/media/new"
		newer.TargetName = "New.Movie.mkv"
		newer.TargetPath = "/media/new/New.Movie.mkv"
		newer.Status = model.SubscriptionItemStatusPending
		if _, _, err := db.UpsertSubscriptionItem(&newer); err != nil {
			return nil, err
		}
		return oldPersist(request)
	}
	t.Cleanup(func() { persistSubscriptionTerminalItem = oldPersist })

	handleTransferPayload(t.Context(), true, TransferFinalizePayload{
		SubscriptionID:     sub.ID,
		SubscriptionItemID: accepted.ID,
		SourceKey:          accepted.SourceKey,
		FileHash:           accepted.FileHash,
		TargetDir:          accepted.TargetDir,
		FileName:           accepted.FileName,
		TargetName:         accepted.TargetName,
	})
	item, err := db.GetSubscriptionItem(sub.ID, accepted.SourceKey)
	if err != nil {
		t.Fatalf("get newer item: %v", err)
	}
	if item.SourceProvider != "new-provider" ||
		item.SourceURL != "https://new.example/s/movie" ||
		item.FileName != "New.Movie.mkv" ||
		item.FilePath != "/new/New.Movie.mkv" ||
		item.FileHash != "hash-new" ||
		item.TargetDir != "/media/new" ||
		item.TargetName != "New.Movie.mkv" ||
		item.TargetPath != "/media/new/New.Movie.mkv" ||
		item.Status != model.SubscriptionItemStatusPending {
		t.Fatalf("newer item was overwritten by terminal callback: %#v", item)
	}
	sources, err := db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil || len(sources) != 1 {
		t.Fatalf("sources after callback race = %#v err=%v", sources, err)
	}
	if got := sources[0]; got.SourceItemID != sourcesBefore[0].SourceItemID ||
		got.SourceProvider != sourcesBefore[0].SourceProvider ||
		got.ShareURL != sourcesBefore[0].ShareURL ||
		got.FileName != sourcesBefore[0].FileName ||
		got.Season != sourcesBefore[0].Season ||
		got.Episode != sourcesBefore[0].Episode {
		t.Fatalf("source changed by terminal callback race = %#v", got)
	}
}

func assertStaleTransferPayloadDidNotChangeItemOrSource(t *testing.T, subscriptionID uint, sourceKey string, wantSource model.SubscriptionEpisodeSource) {
	t.Helper()
	item, err := db.GetSubscriptionItem(subscriptionID, sourceKey)
	if err != nil {
		t.Fatalf("get newer item: %v", err)
	}
	if item.Status != model.SubscriptionItemStatusPending || item.FileHash != "hash-new" {
		t.Fatalf("newer item = %#v, want pending hash-new", item)
	}
	sources, err := db.ListSubscriptionEpisodeSources(subscriptionID)
	if err != nil || len(sources) != 1 {
		t.Fatalf("sources after stale callback = %#v err=%v", sources, err)
	}
	got := sources[0]
	if got.SourceItemID != wantSource.SourceItemID ||
		got.SourceType != wantSource.SourceType ||
		got.SourceProvider != wantSource.SourceProvider ||
		got.ShareURL != wantSource.ShareURL ||
		got.FileName != wantSource.FileName ||
		got.ClusterJobID != wantSource.ClusterJobID ||
		got.Season != wantSource.Season ||
		got.Episode != wantSource.Episode {
		t.Fatalf("source changed by stale callback = %#v, want %#v", got, wantSource)
	}
}

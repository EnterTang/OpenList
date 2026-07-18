package db

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func setupPrefixedSubscriptionDB(t *testing.T) {
	t.Helper()
	previousConf := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	database, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "subscriptions.db")), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: conf.Conf.Database.TablePrefix},
	})
	if err != nil {
		t.Fatalf("open prefixed sqlite: %v", err)
	}
	Init(database)
	t.Cleanup(func() {
		conf.Conf = previousConf
		sqlDB, err := database.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})
}

func seedSubscriptionRunBoardFixture(t *testing.T) (model.Subscription, model.Subscription, model.Subscription, []model.SubscriptionRun) {
	t.Helper()

	subscriptions := []model.Subscription{
		{
			Name:       "Alpha Board",
			TMDBName:   "Alpha TMDB",
			TargetRoot: "/media/alpha",
			SourceType: model.SubscriptionSourceManual,
		},
		{
			Name:       "Beta Board",
			TMDBName:   "Beta TMDB",
			TargetRoot: "/media/beta",
			SourceType: model.SubscriptionSourceTelegram,
		},
		{
			Name:       "Gamma Board",
			TMDBName:   "Gamma TMDB",
			TargetRoot: "/media/gamma",
			SourceType: model.SubscriptionSourceManual,
		},
	}
	for i := range subscriptions {
		if err := CreateSubscription(&subscriptions[i]); err != nil {
			t.Fatalf("create subscription %d: %v", i, err)
		}
	}

	now := time.Now().UTC()
	runs := []model.SubscriptionRun{
		{
			SubscriptionID: subscriptions[0].ID,
			StartedAt:      now.Add(-6 * time.Minute),
			Status:         model.SubscriptionStatusSuccess,
			AddedCount:     2,
		},
		{
			SubscriptionID: subscriptions[0].ID,
			StartedAt:      now.Add(-5 * time.Minute),
			Status:         model.SubscriptionStatusSuccess,
			ChangedCount:   3,
		},
		{
			SubscriptionID:   subscriptions[0].ID,
			StartedAt:        now.Add(-4 * time.Minute),
			Status:           model.SubscriptionStatusSuccess,
			TransferredCount: 4,
		},
		{
			SubscriptionID: subscriptions[0].ID,
			StartedAt:      now.Add(-3 * time.Minute),
			Status:         model.SubscriptionStatusFailed,
			Error:          "network failure",
		},
		{
			SubscriptionID: subscriptions[0].ID,
			StartedAt:      now.Add(-2 * time.Minute),
			Status:         model.SubscriptionStatusSuccess,
			Error:          "completed with warning",
		},
		{
			SubscriptionID: subscriptions[0].ID,
			StartedAt:      now.Add(-1 * time.Minute),
			Status:         model.SubscriptionStatusSuccess,
		},
		{
			SubscriptionID: subscriptions[1].ID,
			StartedAt:      now.Add(-30 * time.Second),
			Status:         model.SubscriptionStatusSuccess,
			AddedCount:     1,
		},
	}
	for i := range runs {
		if err := CreateSubscriptionRun(&runs[i]); err != nil {
			t.Fatalf("create run %d: %v", i, err)
		}
	}
	return subscriptions[0], subscriptions[1], subscriptions[2], runs
}

func TestSubscriptionTaskBoardHonorsConfiguredTablePrefix(t *testing.T) {
	setupPrefixedSubscriptionDB(t)

	alpha, _, _, _ := seedSubscriptionRunBoardFixture(t)
	board, err := GetSubscriptionBoard(SubscriptionRunFilter{SubscriptionID: alpha.ID})
	if err != nil {
		t.Fatalf("get prefixed subscription board: %v", err)
	}
	if board.SubscriptionCount != 1 || board.ChangedRunCount != 2 || board.AddedCount != 2 || board.ChangedCount != 3 || board.FailureCount != 2 {
		t.Fatalf("prefixed subscription board = %#v", board)
	}

	runs, total, err := ListSubscriptionRuns(SubscriptionRunFilter{
		SubscriptionID: alpha.ID,
		View:           model.SubscriptionRunViewChanges,
		Page:           1,
		PerPage:        20,
	})
	if err != nil {
		t.Fatalf("list prefixed subscription runs: %v", err)
	}
	if total != 2 || len(runs) != 2 || runs[0].SubscriptionName != alpha.Name {
		t.Fatalf("prefixed subscription runs = total %d items %#v", total, runs)
	}

	if _, err := UpsertSubscriptionEpisodeSource(&model.SubscriptionEpisodeSource{
		SubscriptionID: alpha.ID,
		Season:         1,
		Episode:        1,
		SourceType:     model.SubscriptionSourceManual,
		FileName:       "prefixed.mkv",
		Status:         model.SubscriptionItemStatusTransferred,
	}); err != nil {
		t.Fatalf("create prefixed episode source: %v", err)
	}
	details, err := ListSubscriptionEpisodeSourceDetails(alpha.ID)
	if err != nil {
		t.Fatalf("list prefixed episode source details: %v", err)
	}
	if len(details) != 1 || details[0].WorkerName != "本机" {
		t.Fatalf("prefixed episode source details = %#v", details)
	}

	deleted, err := ClearFailedSubscriptionRuns()
	if err != nil {
		t.Fatalf("clear prefixed failed runs: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("cleared prefixed failed runs = %d, want 2", deleted)
	}
}

func TestUpsertSubscriptionItemPreservesTransferredStatusOnUnchangedScan(t *testing.T) {
	setupETFArchiveDB(t)

	item, isNew, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 1,
		SourceKey:      "source-a",
		FileHash:       "hash-a",
		FileName:       "01.iso",
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert initial item: %v", err)
	}
	if !isNew || item.Status != model.SubscriptionItemStatusPending {
		t.Fatalf("initial item = %#v isNew=%v", item, isNew)
	}

	item.Status = model.SubscriptionItemStatusTransferred
	item.LastError = ""
	if _, _, err := UpsertSubscriptionItem(item); err != nil {
		t.Fatalf("mark transferred: %v", err)
	}

	scanned, isNew, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 1,
		SourceKey:      "source-a",
		FileHash:       "hash-a",
		FileName:       "01.iso",
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert unchanged scan: %v", err)
	}
	if isNew {
		t.Fatal("unchanged scan reported new item")
	}
	if scanned.Status != model.SubscriptionItemStatusTransferred {
		t.Fatalf("status = %q, want transferred", scanned.Status)
	}

	changed, _, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 1,
		SourceKey:      "source-a",
		FileHash:       "hash-b",
		FileName:       "01.iso",
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert changed scan: %v", err)
	}
	if changed.Status != model.SubscriptionItemStatusPending {
		t.Fatalf("changed status = %q, want pending", changed.Status)
	}
}

func TestUpsertSubscriptionItemResetsTransferredStatusWhenTargetPathChanges(t *testing.T) {
	setupETFArchiveDB(t)

	item, _, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 1,
		SourceKey:      "source-target-change",
		FileHash:       "hash-a",
		FileName:       "01.mp4",
		TargetPath:     "/media/Season 1/Show.S01E01.mp4",
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert initial item: %v", err)
	}
	item.Status = model.SubscriptionItemStatusTransferred
	if _, _, err := UpsertSubscriptionItem(item); err != nil {
		t.Fatalf("mark transferred: %v", err)
	}

	rescanned, isNew, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 1,
		SourceKey:      "source-target-change",
		FileHash:       "hash-a",
		FileName:       "01.mp4",
		TargetPath:     "/media/Season 2/Show.S02E01.mp4",
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert target-changed item: %v", err)
	}
	if isNew {
		t.Fatal("target-changed scan reported new item")
	}
	if rescanned.Status != model.SubscriptionItemStatusPending {
		t.Fatalf("status = %q, want pending after target path changed", rescanned.Status)
	}
	if rescanned.LastError != "" {
		t.Fatalf("last error = %q, want cleared", rescanned.LastError)
	}
}

func TestUpsertSubscriptionItemPersistsSourceProvider(t *testing.T) {
	setupETFArchiveDB(t)

	item, _, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 2,
		SourceKey:      "provider-source",
		SourceProvider: "pan123",
		FileHash:       "hash-provider",
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert provider item: %v", err)
	}
	if item.SourceProvider != "pan123" {
		t.Fatalf("source provider = %q, want pan123", item.SourceProvider)
	}

	item.SourceProvider = "quark"
	updated, _, err := UpsertSubscriptionItem(item)
	if err != nil {
		t.Fatalf("update provider item: %v", err)
	}
	if updated.SourceProvider != "quark" {
		t.Fatalf("updated source provider = %q, want quark", updated.SourceProvider)
	}
}

func TestUpdateAndDeleteSubscriptionEditableFields(t *testing.T) {
	setupETFArchiveDB(t)

	sub := &model.Subscription{
		Name:                     "Some Show",
		SourceType:               model.SubscriptionSourceManual,
		SourceConfig:             `{"links":["https://pan.quark.cn/s/first"]}`,
		CheckIntervalMinutes:     60,
		MediaType:                "tv",
		Seasons:                  []int{2},
		LatestSeasonEpisodeStart: 5,
	}
	if err := CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	sub.SourceConfig = `{"links":["https://115cdn.com/s/second"]}`
	sub.CheckIntervalMinutes = 180
	sub.LatestSeasonEpisodeStart = 9
	sub.LatestSeasonEpisodeEnd = 12
	if err := UpdateSubscription(sub); err != nil {
		t.Fatalf("update subscription: %v", err)
	}
	got, err := GetSubscriptionByID(sub.ID)
	if err != nil {
		t.Fatalf("get updated subscription: %v", err)
	}
	if got.SourceConfig != sub.SourceConfig || got.CheckIntervalMinutes != 180 || got.LatestSeasonEpisodeStart != 9 || got.LatestSeasonEpisodeEnd != 12 {
		t.Fatalf("updated editable fields = %#v", got)
	}
	if _, _, err := UpsertSubscriptionItem(&model.SubscriptionItem{SubscriptionID: sub.ID, SourceKey: "item", LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("create child item: %v", err)
	}
	if err := CreateSubscriptionRun(&model.SubscriptionRun{SubscriptionID: sub.ID, StartedAt: time.Now(), Status: model.SubscriptionStatusFailed}); err != nil {
		t.Fatalf("create child run: %v", err)
	}
	if err := DeleteSubscription(sub.ID); err != nil {
		t.Fatalf("delete subscription: %v", err)
	}
	if _, err := GetSubscriptionByID(sub.ID); err == nil {
		t.Fatal("deleted subscription still exists")
	}
	items, err := ListSubscriptionItems(sub.ID)
	if err != nil || len(items) != 0 {
		t.Fatalf("deleted subscription items = %#v err=%v", items, err)
	}
	runs, total, err := ListSubscriptionRuns(SubscriptionRunFilter{SubscriptionID: sub.ID, Page: 1, PerPage: 10})
	if err != nil || total != 0 || len(runs) != 0 {
		t.Fatalf("deleted subscription runs = total %d items %#v err=%v", total, runs, err)
	}
}

func TestUpdateSubscriptionTMDBEpisodeEndPreservesRuntimeFields(t *testing.T) {
	setupETFArchiveDB(t)

	checkedAt := time.Date(2026, time.July, 15, 8, 0, 0, 0, time.UTC)
	syncedAt := time.Date(2026, time.July, 15, 9, 0, 0, 0, time.UTC)
	sub := &model.Subscription{
		Name:                     "TMDB scoped update",
		TMDBName:                 "TMDB scoped update",
		TMDBYear:                 2026,
		SourceType:               model.SubscriptionSourceManual,
		SourceConfig:             `{"share_url":"https://example.test/share"}`,
		TargetRoot:               "/media/tmdb-scoped",
		CheckIntervalMinutes:     90,
		LatestSeasonEpisodeStart: 3,
		LastCheckedAt:            &checkedAt,
		LastCursor:               "cursor-before",
		LastTreeHash:             "tree-before",
		LastStatus:               model.SubscriptionStatusFailed,
		LastError:                "last error before refresh",
	}
	if err := CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	discoveredTMDBID := int64(12345)
	applied, err := UpdateSubscriptionTMDBEpisodeEnd(sub, &discoveredTMDBID, 16, syncedAt)
	if err != nil {
		t.Fatalf("update tmdb episode end: %v", err)
	}
	if !applied {
		t.Fatal("tmdb episode update was not applied")
	}

	got, err := GetSubscriptionByID(sub.ID)
	if err != nil {
		t.Fatalf("get subscription: %v", err)
	}
	if got.TMDBID != discoveredTMDBID || got.LatestSeasonEpisodeEnd != 16 || got.TMDBEpisodeSyncedAt == nil || !got.TMDBEpisodeSyncedAt.Equal(syncedAt) {
		t.Fatalf("tmdb fields = %#v, want discovered id and synchronized episode end", got)
	}
	if !got.UpdatedAt.Equal(syncedAt) {
		t.Fatalf("updated at = %v, want %v", got.UpdatedAt, syncedAt)
	}
	if got.LastCheckedAt == nil || !got.LastCheckedAt.Equal(checkedAt) ||
		got.LastCursor != sub.LastCursor ||
		got.LastTreeHash != sub.LastTreeHash ||
		got.LastStatus != sub.LastStatus ||
		got.LastError != sub.LastError {
		t.Fatalf("runtime/check fields were changed: %#v", got)
	}
	if got.Name != sub.Name ||
		got.TMDBName != sub.TMDBName ||
		got.TMDBYear != sub.TMDBYear ||
		got.SourceType != sub.SourceType ||
		got.SourceConfig != sub.SourceConfig ||
		got.TargetRoot != sub.TargetRoot ||
		got.CheckIntervalMinutes != sub.CheckIntervalMinutes ||
		got.LatestSeasonEpisodeStart != sub.LatestSeasonEpisodeStart {
		t.Fatalf("unrelated subscription fields were changed: %#v", got)
	}
}

func TestUpdateSubscriptionTMDBEpisodeEndSkipsStaleIdentity(t *testing.T) {
	setupETFArchiveDB(t)

	oldSyncedAt := time.Date(2026, time.July, 15, 8, 0, 0, 0, time.UTC)
	newSyncedAt := time.Date(2026, time.July, 15, 9, 0, 0, 0, time.UTC)
	sub := &model.Subscription{
		Name:                   "TMDB compare and set",
		TMDBID:                 101,
		TMDBName:               "Original title",
		TMDBYear:               2020,
		MediaType:              "tv",
		LatestSeasonEpisodeEnd: 8,
		TMDBEpisodeSyncedAt:    &oldSyncedAt,
	}
	if err := CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	snapshot := *sub

	sub.TMDBID = 202
	sub.TMDBName = "Admin updated title"
	sub.TMDBYear = 2026
	sub.LatestSeasonEpisodeEnd = 14
	sub.TMDBEpisodeSyncedAt = &newSyncedAt
	if err := UpdateSubscription(sub); err != nil {
		t.Fatalf("save updated identity: %v", err)
	}

	discoveredTMDBID := int64(303)
	applied, err := UpdateSubscriptionTMDBEpisodeEnd(&snapshot, &discoveredTMDBID, 20, oldSyncedAt)
	if err != nil {
		t.Fatalf("apply stale tmdb update: %v", err)
	}
	if applied {
		t.Fatal("stale tmdb update was applied")
	}

	got, err := GetSubscriptionByID(sub.ID)
	if err != nil {
		t.Fatalf("get subscription: %v", err)
	}
	if got.TMDBID != sub.TMDBID ||
		got.TMDBName != sub.TMDBName ||
		got.TMDBYear != sub.TMDBYear ||
		got.LatestSeasonEpisodeEnd != sub.LatestSeasonEpisodeEnd ||
		got.TMDBEpisodeSyncedAt == nil ||
		!got.TMDBEpisodeSyncedAt.Equal(newSyncedAt) {
		t.Fatalf("stale update overwrote current tmdb state: %#v", got)
	}
}

func TestUpdateSubscriptionTMDBEpisodeEndSkipsStaleSeasonSelection(t *testing.T) {
	setupETFArchiveDB(t)

	oldSyncedAt := time.Date(2026, time.July, 15, 8, 0, 0, 0, time.UTC)
	newSyncedAt := time.Date(2026, time.July, 15, 9, 0, 0, 0, time.UTC)
	sub := &model.Subscription{
		Name:                   "TMDB season compare and set",
		TMDBID:                 101,
		TMDBName:               "Same title",
		TMDBYear:               2020,
		MediaType:              "tv",
		Season:                 1,
		Seasons:                []int{1},
		LatestSeasonEpisodeEnd: 8,
		TMDBEpisodeSyncedAt:    &oldSyncedAt,
	}
	if err := CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	snapshot := *sub

	sub.Season = 2
	sub.Seasons = []int{2}
	sub.LatestSeasonEpisodeEnd = 14
	sub.TMDBEpisodeSyncedAt = &newSyncedAt
	sub.UpdatedAt = snapshot.UpdatedAt.Add(time.Hour)
	if err := UpdateSubscription(sub); err != nil {
		t.Fatalf("save updated season selection: %v", err)
	}

	applied, err := UpdateSubscriptionTMDBEpisodeEnd(&snapshot, nil, snapshot.LatestSeasonEpisodeEnd, oldSyncedAt)
	if err != nil {
		t.Fatalf("apply stale season update: %v", err)
	}
	if applied {
		t.Fatal("stale season update was applied")
	}

	got, err := GetSubscriptionByID(sub.ID)
	if err != nil {
		t.Fatalf("get subscription: %v", err)
	}
	if got.Season != 2 || len(got.Seasons) != 1 || got.Seasons[0] != 2 ||
		got.LatestSeasonEpisodeEnd != 14 ||
		got.TMDBEpisodeSyncedAt == nil ||
		!got.TMDBEpisodeSyncedAt.Equal(newSyncedAt) {
		t.Fatalf("stale update overwrote current season state: %#v", got)
	}
}

func TestUpdateSubscriptionTMDBEpisodeEndSkipsStaleSelectionAtSameUpdatedAt(t *testing.T) {
	setupETFArchiveDB(t)

	oldSyncedAt := time.Date(2026, time.July, 15, 8, 0, 0, 0, time.UTC)
	newSyncedAt := time.Date(2026, time.July, 15, 9, 0, 0, 0, time.UTC)
	sub := &model.Subscription{
		Name:                     "TMDB same timestamp compare and set",
		TMDBID:                   101,
		TMDBName:                 "Same title",
		TMDBYear:                 2020,
		MediaType:                "tv",
		Season:                   1,
		Seasons:                  []int{1},
		LatestSeasonEpisodeStart: 1,
		LatestSeasonEpisodeEnd:   8,
		TMDBEpisodeSyncedAt:      &oldSyncedAt,
	}
	if err := CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	snapshot := *sub

	serializedSeasons, err := json.Marshal([]int{2})
	if err != nil {
		t.Fatalf("serialize seasons: %v", err)
	}
	result := db.Model(&model.Subscription{}).
		Where("id = ?", sub.ID).
		UpdateColumns(map[string]any{
			"season":                      2,
			"seasons":                     string(serializedSeasons),
			"latest_season_episode_start": 3,
			"latest_season_episode_end":   14,
			"tmdb_episode_synced_at":      newSyncedAt,
		})
	if result.Error != nil {
		t.Fatalf("update selection without timestamp: %v", result.Error)
	}

	applied, err := UpdateSubscriptionTMDBEpisodeEnd(&snapshot, nil, snapshot.LatestSeasonEpisodeEnd, oldSyncedAt)
	if err != nil {
		t.Fatalf("apply stale same-timestamp update: %v", err)
	}
	if applied {
		t.Fatal("stale same-timestamp update was applied")
	}

	got, err := GetSubscriptionByID(sub.ID)
	if err != nil {
		t.Fatalf("get subscription: %v", err)
	}
	if !got.UpdatedAt.Equal(snapshot.UpdatedAt) {
		t.Fatalf("updated at = %v, want unchanged %v", got.UpdatedAt, snapshot.UpdatedAt)
	}
	if got.Season != 2 || len(got.Seasons) != 1 || got.Seasons[0] != 2 ||
		got.LatestSeasonEpisodeStart != 3 ||
		got.LatestSeasonEpisodeEnd != 14 ||
		got.TMDBEpisodeSyncedAt == nil ||
		!got.TMDBEpisodeSyncedAt.Equal(newSyncedAt) {
		t.Fatalf("stale update overwrote same-timestamp selection state: %#v", got)
	}
}

func TestUpsertSubscriptionEpisodeSourceReplacesExistingAndDeletesWithSubscription(t *testing.T) {
	setupETFArchiveDB(t)

	sub := &model.Subscription{
		Name:       "Episode source replacement",
		SourceType: model.SubscriptionSourceManual,
	}
	if err := CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	firstSelectedAt := time.Unix(1700000000, 0).UTC()
	first, err := UpsertSubscriptionEpisodeSource(&model.SubscriptionEpisodeSource{
		SubscriptionID: sub.ID,
		Season:         1,
		Episode:        2,
		SourceItemID:   11,
		SourceType:     model.SubscriptionSourceManual,
		SourceProvider: "pan123",
		ShareURL:       "https://www.123pan.com/s/first",
		FileName:       "Show.S01E02.1080p.mkv",
		FileHash:       "first-hash",
		Status:         model.SubscriptionItemStatusPending,
		SelectedAt:     firstSelectedAt,
	})
	if err != nil {
		t.Fatalf("upsert first episode source: %v", err)
	}
	if first.SourceItemID != 11 {
		t.Fatalf("first source item id = %d, want 11", first.SourceItemID)
	}

	secondSelectedAt := firstSelectedAt.Add(5 * time.Minute)
	replaced, err := UpsertSubscriptionEpisodeSource(&model.SubscriptionEpisodeSource{
		SubscriptionID: sub.ID,
		Season:         1,
		Episode:        2,
		SourceItemID:   22,
		SourceType:     model.SubscriptionSourceTelegram,
		SourceProvider: "quark",
		ShareURL:       "https://pan.quark.cn/s/replacement",
		FileName:       "Show.S01E02.REPACK.mkv",
		FileHash:       "replacement-hash",
		Status:         model.SubscriptionItemStatusTransferring,
		ClusterJobID:   "job-episode-source",
		SelectedAt:     secondSelectedAt,
	})
	if err != nil {
		t.Fatalf("upsert replacement episode source: %v", err)
	}

	sources, err := ListSubscriptionEpisodeSources(sub.ID)
	if err != nil {
		t.Fatalf("list episode sources: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("episode sources len = %d, want 1: %#v", len(sources), sources)
	}
	got := sources[0]
	if got.ID != replaced.ID ||
		got.SourceItemID != 22 ||
		got.SourceType != model.SubscriptionSourceTelegram ||
		got.SourceProvider != "quark" ||
		got.ShareURL != "https://pan.quark.cn/s/replacement" ||
		got.FileName != "Show.S01E02.REPACK.mkv" ||
		got.FileHash != "replacement-hash" ||
		got.Status != model.SubscriptionItemStatusTransferring ||
		got.ClusterJobID != "job-episode-source" ||
		!got.SelectedAt.Equal(secondSelectedAt) {
		t.Fatalf("replaced episode source = %#v", got)
	}

	if err := DeleteSubscription(sub.ID); err != nil {
		t.Fatalf("delete subscription: %v", err)
	}
	sources, err = ListSubscriptionEpisodeSources(sub.ID)
	if err != nil {
		t.Fatalf("list deleted episode sources: %v", err)
	}
	if len(sources) != 0 {
		t.Fatalf("episode sources after delete = %#v, want none", sources)
	}
}

func TestPersistAcceptedSubscriptionItemAndEpisodeSourceRollsBackItemWhenSnapshotWriteFails(t *testing.T) {
	setupETFArchiveDB(t)

	item, _, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 1,
		SourceKey:      "accepted-snapshot-failure",
		FileName:       "Episode.mkv",
		Season:         1,
		Episode:        2,
		Status:         model.SubscriptionItemStatusPending,
	})
	if err != nil {
		t.Fatalf("create pending item: %v", err)
	}
	if err := db.Callback().Create().Before("gorm:create").Register("test:fail_episode_source_snapshot", func(tx *gorm.DB) {
		if tx.Statement.Table == "subscription_episode_sources" {
			tx.AddError(errors.New("forced source snapshot persistence failure"))
		}
	}); err != nil {
		t.Fatalf("register source snapshot failure callback: %v", err)
	}

	item.Status = model.SubscriptionItemStatusTransferring
	item.LastError = ""
	_, _, err = PersistAcceptedSubscriptionItemAndEpisodeSource(item, &model.SubscriptionEpisodeSource{
		SubscriptionID: item.SubscriptionID,
		Season:         item.Season,
		Episode:        item.Episode,
		SourceType:     model.SubscriptionSourceManual,
		SourceProvider: "pan123",
		ShareURL:       "https://www.123pan.com/s/example",
		FileName:       item.FileName,
	})
	if err == nil || err.Error() == "" {
		t.Fatalf("persist accepted item error = %v", err)
	}

	persisted, err := GetSubscriptionItem(item.SubscriptionID, item.SourceKey)
	if err != nil {
		t.Fatalf("get persisted item: %v", err)
	}
	if persisted.Status != model.SubscriptionItemStatusPending {
		t.Fatalf("persisted status = %q, want pending", persisted.Status)
	}
	sources, err := ListSubscriptionEpisodeSources(item.SubscriptionID)
	if err != nil {
		t.Fatalf("list episode sources: %v", err)
	}
	if len(sources) != 0 {
		t.Fatalf("episode sources after failed transaction = %#v, want none", sources)
	}
}

func TestRecoverSubscriptionEpisodeSourceAndTerminalItemRollsBackTerminalStateWhenSnapshotWriteFails(t *testing.T) {
	setupETFArchiveDB(t)

	item, _, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 1,
		SourceKey:      "recovery-snapshot-failure",
		FileName:       "Episode.mkv",
		Season:         1,
		Episode:        2,
		Status:         model.SubscriptionItemStatusPending,
	})
	if err != nil {
		t.Fatalf("create pending item: %v", err)
	}
	if err := db.Callback().Create().Before("gorm:create").Register("test:fail_recovery_episode_source_snapshot", func(tx *gorm.DB) {
		if tx.Statement.Table == "subscription_episode_sources" {
			tx.AddError(errors.New("forced recovery source snapshot persistence failure"))
		}
	}); err != nil {
		t.Fatalf("register source snapshot failure callback: %v", err)
	}

	item.Status = model.SubscriptionItemStatusTransferred
	item.LastError = ""
	_, err = RecoverSubscriptionEpisodeSourceAndTerminalItem(item, &model.SubscriptionEpisodeSource{
		SubscriptionID: item.SubscriptionID,
		Season:         item.Season,
		Episode:        item.Episode,
		SourceType:     model.SubscriptionSourceManual,
		SourceProvider: "pan123",
		ShareURL:       "https://www.123pan.com/s/example",
		FileName:       item.FileName,
	})
	if err == nil {
		t.Fatal("recover terminal item unexpectedly succeeded")
	}

	persisted, err := GetSubscriptionItem(item.SubscriptionID, item.SourceKey)
	if err != nil {
		t.Fatalf("get persisted item: %v", err)
	}
	if persisted.Status != model.SubscriptionItemStatusPending {
		t.Fatalf("persisted status = %q, want pending", persisted.Status)
	}
	sources, err := ListSubscriptionEpisodeSources(item.SubscriptionID)
	if err != nil {
		t.Fatalf("list episode sources: %v", err)
	}
	if len(sources) != 0 {
		t.Fatalf("episode sources after failed recovery = %#v, want none", sources)
	}
}

func TestPersistSubscriptionTerminalItemRejectsStaleFileHashWithoutWritingSnapshot(t *testing.T) {
	setupETFArchiveDB(t)

	newer, _, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 1,
		SourceKey:      "terminal-stale-file-hash",
		SourceProvider: "new-provider",
		SourceURL:      "https://new.example/s/movie",
		FileName:       "New.Movie.mkv",
		FilePath:       "/new/New.Movie.mkv",
		FileHash:       "hash-new",
		Season:         1,
		Episode:        2,
		TargetDir:      "/media/new",
		TargetName:     "New.Movie.mkv",
		TargetPath:     "/media/new/New.Movie.mkv",
		Status:         model.SubscriptionItemStatusPending,
	})
	if err != nil {
		t.Fatalf("create newer item: %v", err)
	}

	_, err = PersistSubscriptionTerminalItem(SubscriptionTerminalItemRequest{
		ItemID:            newer.ID,
		SubscriptionID:    newer.SubscriptionID,
		SourceKey:         newer.SourceKey,
		ExpectedFileHash:  "hash-old",
		ExpectedStatus:    model.SubscriptionItemStatusTransferring,
		TerminalStatus:    model.SubscriptionItemStatusTransferred,
		TerminalLastError: "",
		RecoverySource: &model.SubscriptionEpisodeSource{
			SubscriptionID: newer.SubscriptionID,
			Season:         1,
			Episode:        2,
			SourceType:     model.SubscriptionSourceManual,
			SourceProvider: "old-provider",
			ShareURL:       "https://old.example/s/movie",
			FileName:       "Old.Movie.mkv",
		},
	})
	if !errors.Is(err, ErrStaleSubscriptionTerminalCallback) {
		t.Fatalf("terminal write error = %v, want stale callback", err)
	}

	persisted, err := GetSubscriptionItem(newer.SubscriptionID, newer.SourceKey)
	if err != nil {
		t.Fatalf("get newer item: %v", err)
	}
	if persisted.SourceProvider != newer.SourceProvider ||
		persisted.SourceURL != newer.SourceURL ||
		persisted.FileName != newer.FileName ||
		persisted.FilePath != newer.FilePath ||
		persisted.FileHash != newer.FileHash ||
		persisted.TargetDir != newer.TargetDir ||
		persisted.TargetName != newer.TargetName ||
		persisted.TargetPath != newer.TargetPath ||
		persisted.Status != model.SubscriptionItemStatusPending {
		t.Fatalf("newer item was changed by stale terminal write: %#v", persisted)
	}
	sources, err := ListSubscriptionEpisodeSources(newer.SubscriptionID)
	if err != nil {
		t.Fatalf("list episode sources: %v", err)
	}
	if len(sources) != 0 {
		t.Fatalf("stale recovery source was written: %#v", sources)
	}
}

func TestListSubscriptionRunsFiltersSuccessfulNoopRuns(t *testing.T) {
	setupETFArchiveDB(t)

	sub := &model.Subscription{
		Name:       "Legacy runs filter",
		TMDBName:   "Legacy runs filter",
		SourceType: model.SubscriptionSourceManual,
	}
	if err := CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	now := time.Now()
	runs := []model.SubscriptionRun{
		{
			SubscriptionID: sub.ID,
			StartedAt:      now.Add(-3 * time.Minute),
			Status:         model.SubscriptionStatusSuccess,
		},
		{
			SubscriptionID: sub.ID,
			StartedAt:      now.Add(-2 * time.Minute),
			Status:         model.SubscriptionStatusSuccess,
			AddedCount:     1,
		},
		{
			SubscriptionID: sub.ID,
			StartedAt:      now.Add(-1 * time.Minute),
			Status:         model.SubscriptionStatusFailed,
			Error:          "temporary failure",
		},
	}
	for i := range runs {
		if err := CreateSubscriptionRun(&runs[i]); err != nil {
			t.Fatalf("create run %d: %v", i, err)
		}
	}

	items, total, err := ListSubscriptionRuns(SubscriptionRunFilter{Page: 1, PerPage: 10})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("runs total/len = %d/%d, want 2/2: %#v", total, len(items), items)
	}
	for _, item := range items {
		if item.Status == model.SubscriptionStatusSuccess && item.AddedCount == 0 && item.ChangedCount == 0 && item.TransferredCount == 0 {
			t.Fatalf("successful noop run was returned: %#v", item)
		}
	}

	items, total, err = ListSubscriptionRuns(SubscriptionRunFilter{Status: model.SubscriptionStatusSuccess, Page: 1, PerPage: 10})
	if err != nil {
		t.Fatalf("list success runs: %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].AddedCount != 1 {
		t.Fatalf("success runs = total %d items %#v, want only changed success run", total, items)
	}
}

func TestDeleteAndClearFailedSubscriptionRuns(t *testing.T) {
	setupETFArchiveDB(t)

	subscriptions := []model.Subscription{
		{Name: "Delete failure sub 1", TMDBName: "Delete failure sub 1", SourceType: model.SubscriptionSourceManual},
		{Name: "Delete failure sub 2", TMDBName: "Delete failure sub 2", SourceType: model.SubscriptionSourceTelegram},
	}
	for i := range subscriptions {
		if err := CreateSubscription(&subscriptions[i]); err != nil {
			t.Fatalf("create subscription %d: %v", i, err)
		}
	}

	runs := []model.SubscriptionRun{
		{SubscriptionID: subscriptions[0].ID, StartedAt: time.Now().Add(-3 * time.Minute), Status: model.SubscriptionStatusFailed, Error: "first failure"},
		{SubscriptionID: subscriptions[0].ID, StartedAt: time.Now().Add(-2 * time.Minute), Status: model.SubscriptionStatusSuccess, AddedCount: 1},
		{SubscriptionID: subscriptions[1].ID, StartedAt: time.Now().Add(-time.Minute), Status: model.SubscriptionStatusSuccess, Error: "completed with an error"},
	}
	for i := range runs {
		if err := CreateSubscriptionRun(&runs[i]); err != nil {
			t.Fatalf("create run %d: %v", i, err)
		}
	}

	if err := DeleteSubscriptionRun(runs[0].ID); err != nil {
		t.Fatalf("delete failed run: %v", err)
	}
	deleted, err := ClearFailedSubscriptionRuns()
	if err != nil {
		t.Fatalf("clear failed runs: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("cleared runs = %d, want 1", deleted)
	}

	items, total, err := ListSubscriptionRuns(SubscriptionRunFilter{Page: 1, PerPage: 10})
	if err != nil {
		t.Fatalf("list remaining runs: %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].ID != runs[1].ID {
		t.Fatalf("remaining runs = total %d items %#v, want successful run %d", total, items, runs[1].ID)
	}
}

func TestListSubscriptionRunsViewsAndProjection(t *testing.T) {
	setupETFArchiveDB(t)

	alpha, _, _, runs := seedSubscriptionRunBoardFixture(t)

	items, total, err := ListSubscriptionRuns(SubscriptionRunFilter{
		SubscriptionID: alpha.ID,
		View:           model.SubscriptionRunViewChanges,
		Page:           1,
		PerPage:        20,
	})
	if err != nil {
		t.Fatalf("list change runs: %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("change runs total/len = %d/%d, want 2/2: %#v", total, len(items), items)
	}
	for _, item := range items {
		if item.Status != model.SubscriptionStatusSuccess || (item.AddedCount == 0 && item.ChangedCount == 0) {
			t.Fatalf("unexpected change run: %#v", item)
		}
		if item.SubscriptionName != alpha.Name || item.SubscriptionSourceType != alpha.SourceType {
			t.Fatalf("projection fields missing from run: %#v", item)
		}
	}

	items, total, err = ListSubscriptionRuns(SubscriptionRunFilter{
		SubscriptionID: alpha.ID,
		View:           model.SubscriptionRunViewFailures,
		Page:           1,
		PerPage:        20,
	})
	if err != nil {
		t.Fatalf("list failure runs: %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("failure runs total/len = %d/%d, want 2/2: %#v", total, len(items), items)
	}
	for _, item := range items {
		if item.Status != model.SubscriptionStatusFailed && item.Error == "" {
			t.Fatalf("unexpected failure run: %#v", item)
		}
	}

	items, total, err = ListSubscriptionRuns(SubscriptionRunFilter{
		SubscriptionID: alpha.ID,
		View:           model.SubscriptionRunViewFailures,
		Status:         model.SubscriptionStatusSuccess,
		Page:           1,
		PerPage:        20,
	})
	if err != nil {
		t.Fatalf("list success failure runs: %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].ID != runs[4].ID {
		t.Fatalf("success failure runs = total %d items %#v, want success-with-error run %d", total, items, runs[4].ID)
	}

	items, total, err = ListSubscriptionRuns(SubscriptionRunFilter{
		Keyword:    "alpha",
		Page:       1,
		PerPage:    20,
		SourceType: model.SubscriptionSourceManual,
	})
	if err != nil {
		t.Fatalf("list keyword runs: %v", err)
	}
	if total != 5 || len(items) != 5 {
		t.Fatalf("keyword runs total/len = %d/%d, want 5/5: %#v", total, len(items), items)
	}
	for _, item := range items {
		if item.ID == runs[5].ID {
			t.Fatalf("legacy list included noop success run: %#v", item)
		}
	}
}

func TestGetSubscriptionBoardUsesSubscriptionFilters(t *testing.T) {
	setupETFArchiveDB(t)

	alpha, beta, gamma, _ := seedSubscriptionRunBoardFixture(t)

	board, err := GetSubscriptionBoard(SubscriptionRunFilter{
		SourceType: model.SubscriptionSourceManual,
		Page:       1,
		PerPage:    1,
	})
	if err != nil {
		t.Fatalf("get manual board: %v", err)
	}
	if board.SubscriptionCount != 2 || board.ChangedRunCount != 2 || board.AddedCount != 2 || board.ChangedCount != 3 || board.FailureCount != 2 {
		t.Fatalf("manual board = %#v", board)
	}

	board, err = GetSubscriptionBoard(SubscriptionRunFilter{Keyword: "beta"})
	if err != nil {
		t.Fatalf("get beta board: %v", err)
	}
	if board.SubscriptionCount != 1 || board.ChangedRunCount != 1 || board.AddedCount != 1 || board.ChangedCount != 0 || board.FailureCount != 0 {
		t.Fatalf("beta board = %#v", board)
	}

	board, err = GetSubscriptionBoard(SubscriptionRunFilter{SubscriptionID: gamma.ID})
	if err != nil {
		t.Fatalf("get gamma board: %v", err)
	}
	if board.SubscriptionCount != 1 || board.ChangedRunCount != 0 || board.AddedCount != 0 || board.ChangedCount != 0 || board.FailureCount != 0 {
		t.Fatalf("gamma board = %#v", board)
	}

	board, err = GetSubscriptionBoard(SubscriptionRunFilter{SubscriptionID: alpha.ID})
	if err != nil {
		t.Fatalf("get alpha board: %v", err)
	}
	if board.SubscriptionCount != 1 || board.ChangedRunCount != 2 || board.AddedCount != 2 || board.ChangedCount != 3 || board.FailureCount != 2 {
		t.Fatalf("alpha board = %#v", board)
	}

	board, err = GetSubscriptionBoard(SubscriptionRunFilter{SubscriptionID: beta.ID})
	if err != nil {
		t.Fatalf("get beta id board: %v", err)
	}
	if board.SubscriptionCount != 1 || board.ChangedRunCount != 1 || board.AddedCount != 1 || board.ChangedCount != 0 || board.FailureCount != 0 {
		t.Fatalf("beta id board = %#v", board)
	}

	board, err = GetSubscriptionBoard(SubscriptionRunFilter{
		SubscriptionID: alpha.ID,
		SourceType:     model.SubscriptionSourceManual,
		Keyword:        "alpha",
		Status:         model.SubscriptionStatusSuccess,
	})
	if err != nil {
		t.Fatalf("get successful alpha board: %v", err)
	}
	if board.SubscriptionCount != 1 || board.ChangedRunCount != 2 || board.AddedCount != 2 || board.ChangedCount != 3 || board.FailureCount != 1 {
		t.Fatalf("successful alpha board = %#v", board)
	}

	board, err = GetSubscriptionBoard(SubscriptionRunFilter{
		SubscriptionID: alpha.ID,
		SourceType:     model.SubscriptionSourceManual,
		Keyword:        "alpha",
		Status:         model.SubscriptionStatusFailed,
	})
	if err != nil {
		t.Fatalf("get failed alpha board: %v", err)
	}
	if board.SubscriptionCount != 1 || board.ChangedRunCount != 0 || board.AddedCount != 0 || board.ChangedCount != 0 || board.FailureCount != 1 {
		t.Fatalf("failed alpha board = %#v", board)
	}
}

func TestClearFailedSubscriptionRunsUsesFailurePredicate(t *testing.T) {
	setupETFArchiveDB(t)

	alpha, beta, _, runs := seedSubscriptionRunBoardFixture(t)

	deleted, err := ClearFailedSubscriptionRuns()
	if err != nil {
		t.Fatalf("clear failed runs: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted runs = %d, want 2", deleted)
	}

	failures, total, err := ListSubscriptionRuns(SubscriptionRunFilter{
		View:    model.SubscriptionRunViewFailures,
		Page:    1,
		PerPage: 20,
	})
	if err != nil {
		t.Fatalf("list failures after clear: %v", err)
	}
	if total != 0 || len(failures) != 0 {
		t.Fatalf("failures after clear = total %d items %#v, want none", total, failures)
	}

	changes, total, err := ListSubscriptionRuns(SubscriptionRunFilter{
		View:    model.SubscriptionRunViewChanges,
		Page:    1,
		PerPage: 20,
	})
	if err != nil {
		t.Fatalf("list changes after clear: %v", err)
	}
	if total != 3 || len(changes) != 3 {
		t.Fatalf("changes after clear = total %d items %#v, want 3 changed runs", total, changes)
	}
	for _, item := range changes {
		if item.ID == runs[2].ID || item.ID == runs[3].ID || item.ID == runs[4].ID || item.ID == runs[5].ID {
			t.Fatalf("unexpected run remained in changes view: %#v", item)
		}
	}

	board, err := GetSubscriptionBoard(SubscriptionRunFilter{SubscriptionID: alpha.ID})
	if err != nil {
		t.Fatalf("get alpha board after clear: %v", err)
	}
	if board.FailureCount != 0 || board.ChangedRunCount != 2 {
		t.Fatalf("alpha board after clear = %#v", board)
	}

	board, err = GetSubscriptionBoard(SubscriptionRunFilter{SubscriptionID: beta.ID})
	if err != nil {
		t.Fatalf("get beta board after clear: %v", err)
	}
	if board.FailureCount != 0 || board.ChangedRunCount != 1 {
		t.Fatalf("beta board after clear = %#v", board)
	}
}

func TestListSubscriptionEpisodeSourceDetailsResolvesWorkerNames(t *testing.T) {
	setupETFArchiveDB(t)

	sub := &model.Subscription{
		Name:       "Worker detail subscription",
		TMDBName:   "Worker detail subscription",
		SourceType: model.SubscriptionSourceTelegram,
	}
	if err := CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	items := []model.SubscriptionItem{
		{SubscriptionID: sub.ID, SourceKey: "assigned", Season: 1, Episode: 1, Status: model.SubscriptionItemStatusTransferred, LastSeenAt: time.Now().UTC()},
		{SubscriptionID: sub.ID, SourceKey: "attempt", Season: 1, Episode: 2, Status: model.SubscriptionItemStatusFailed, LastSeenAt: time.Now().UTC()},
		{SubscriptionID: sub.ID, SourceKey: "unassigned", Season: 1, Episode: 3, Status: model.SubscriptionItemStatusPending, LastSeenAt: time.Now().UTC()},
		{SubscriptionID: sub.ID, SourceKey: "standalone", Season: 1, Episode: 4, Status: model.SubscriptionItemStatusTransferred, LastSeenAt: time.Now().UTC()},
	}
	for i := range items {
		saved, _, err := UpsertSubscriptionItem(&items[i])
		if err != nil {
			t.Fatalf("upsert item %d: %v", i, err)
		}
		items[i] = *saved
	}

	if err := db.Create(&model.ClusterNode{ID: "node-assigned", Name: "已指派节点"}).Error; err != nil {
		t.Fatalf("create assigned node: %v", err)
	}
	if err := db.Create(&model.ClusterNode{ID: "node-attempt", Name: "重试节点"}).Error; err != nil {
		t.Fatalf("create attempt node: %v", err)
	}
	if err := db.Create(&model.ClusterJob{ID: "job-assigned", IdempotencyKey: "job-assigned", SubscriptionID: sub.ID, SubscriptionItemID: items[0].ID, AssignedNodeID: "node-assigned"}).Error; err != nil {
		t.Fatalf("create assigned job: %v", err)
	}
	if err := db.Create(&model.ClusterJob{ID: "job-attempt", IdempotencyKey: "job-attempt", SubscriptionID: sub.ID, SubscriptionItemID: items[1].ID}).Error; err != nil {
		t.Fatalf("create attempt job: %v", err)
	}
	if err := db.Create(&model.ClusterJob{ID: "job-unassigned", IdempotencyKey: "job-unassigned", SubscriptionID: sub.ID, SubscriptionItemID: items[2].ID}).Error; err != nil {
		t.Fatalf("create unassigned job: %v", err)
	}
	if err := db.Create(&model.ClusterJobAttempt{
		ID:         "attempt-old",
		JobID:      "job-attempt",
		NodeID:     "node-assigned",
		Generation: 1,
		CreatedAt:  time.Unix(1700000600, 0).UTC(),
	}).Error; err != nil {
		t.Fatalf("create old attempt: %v", err)
	}
	if err := db.Create(&model.ClusterJobAttempt{
		ID:         "attempt-new",
		JobID:      "job-attempt",
		NodeID:     "node-attempt",
		Generation: 2,
		CreatedAt:  time.Unix(1700000000, 0).UTC(),
	}).Error; err != nil {
		t.Fatalf("create new attempt: %v", err)
	}

	snapshots := []model.SubscriptionEpisodeSource{
		{SubscriptionID: sub.ID, Season: 1, Episode: 1, SourceItemID: items[0].ID, SourceType: model.SubscriptionSourceTelegram, SourceProvider: "quark", FileName: "assigned.mkv", Status: model.SubscriptionItemStatusTransferring, ClusterJobID: "job-assigned"},
		{SubscriptionID: sub.ID, Season: 1, Episode: 2, SourceItemID: items[1].ID, SourceType: model.SubscriptionSourceTelegram, SourceProvider: "quark", FileName: "attempt.mkv", Status: model.SubscriptionItemStatusFailed, ClusterJobID: "job-attempt"},
		{SubscriptionID: sub.ID, Season: 1, Episode: 3, SourceItemID: items[2].ID, SourceType: model.SubscriptionSourceTelegram, SourceProvider: "quark", FileName: "unassigned.mkv", Status: model.SubscriptionItemStatusPending, ClusterJobID: "job-unassigned"},
		{SubscriptionID: sub.ID, Season: 1, Episode: 4, SourceItemID: items[3].ID, SourceType: model.SubscriptionSourceManual, SourceProvider: "pan123", FileName: "standalone.mkv", Status: model.SubscriptionItemStatusTransferred},
	}
	for i := range snapshots {
		if _, err := UpsertSubscriptionEpisodeSource(&snapshots[i]); err != nil {
			t.Fatalf("upsert snapshot %d: %v", i, err)
		}
	}

	details, err := ListSubscriptionEpisodeSourceDetails(sub.ID)
	if err != nil {
		t.Fatalf("list episode source details: %v", err)
	}
	if len(details) != 4 {
		t.Fatalf("detail count = %d, want 4: %#v", len(details), details)
	}
	wantWorkers := []string{"已指派节点", "重试节点", "未指派", "本机"}
	wantStatuses := []string{
		model.SubscriptionItemStatusTransferred,
		model.SubscriptionItemStatusFailed,
		model.SubscriptionItemStatusPending,
		model.SubscriptionItemStatusTransferred,
	}
	for i, detail := range details {
		if detail.Episode != i+1 {
			t.Fatalf("detail %d episode = %d, want %d", i, detail.Episode, i+1)
		}
		if detail.WorkerName != wantWorkers[i] {
			t.Fatalf("detail %d worker = %q, want %q", i, detail.WorkerName, wantWorkers[i])
		}
		if detail.Status != wantStatuses[i] {
			t.Fatalf("detail %d status = %q, want %q", i, detail.Status, wantStatuses[i])
		}
		if detail.SourceItemID != items[i].ID {
			t.Fatalf("detail %d source item id = %d, want %d", i, detail.SourceItemID, items[i].ID)
		}
	}

	legacyOnly := &model.Subscription{
		Name:       "Legacy only",
		TMDBName:   "Legacy only",
		SourceType: model.SubscriptionSourceManual,
	}
	if err := CreateSubscription(legacyOnly); err != nil {
		t.Fatalf("create legacy subscription: %v", err)
	}
	if _, _, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: legacyOnly.ID,
		SourceKey:      "legacy",
		Status:         model.SubscriptionItemStatusTransferred,
		LastSeenAt:     time.Now().UTC(),
	}); err != nil {
		t.Fatalf("create legacy item: %v", err)
	}
	details, err = ListSubscriptionEpisodeSourceDetails(legacyOnly.ID)
	if err != nil {
		t.Fatalf("list legacy details: %v", err)
	}
	if len(details) != 0 {
		t.Fatalf("legacy subscription details = %#v, want none without snapshots", details)
	}
}

func TestListSubscriptionEpisodeSourceDetailsKeepsTerminalStatusAfterSourceItemReuse(t *testing.T) {
	setupETFArchiveDB(t)

	sub := &model.Subscription{
		Name:       "Snapshot status subscription",
		TMDBName:   "Snapshot status subscription",
		SourceType: model.SubscriptionSourceTelegram,
	}
	if err := CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	accepted, _, err := PersistAcceptedSubscriptionItemAndEpisodeSource(&model.SubscriptionItem{
		SubscriptionID: sub.ID,
		SourceKey:      "reused-source",
		FileHash:       "accepted-hash",
		Season:         1,
		Episode:        1,
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     time.Now().UTC(),
	}, &model.SubscriptionEpisodeSource{
		SubscriptionID: sub.ID,
		Season:         1,
		Episode:        1,
		SourceType:     model.SubscriptionSourceTelegram,
		SourceProvider: "quark",
		ShareURL:       "https://pan.quark.cn/s/accepted",
		FileName:       "accepted.mkv",
	})
	if err != nil {
		t.Fatalf("persist accepted item and source: %v", err)
	}

	if _, err := PersistSubscriptionTerminalItem(SubscriptionTerminalItemRequest{
		ItemID:            accepted.ID,
		SubscriptionID:    sub.ID,
		SourceKey:         accepted.SourceKey,
		ExpectedFileHash:  accepted.FileHash,
		ExpectedStatus:    model.SubscriptionItemStatusPending,
		TerminalStatus:    model.SubscriptionItemStatusTransferred,
		TerminalLastError: "",
	}); err != nil {
		t.Fatalf("persist accepted terminal status: %v", err)
	}

	reused, _, err := UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: sub.ID,
		SourceKey:      accepted.SourceKey,
		FileHash:       "new-pending-hash",
		Season:         1,
		Episode:        1,
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("reuse source item row: %v", err)
	}
	if reused.ID != accepted.ID || reused.Status != model.SubscriptionItemStatusPending {
		t.Fatalf("reused item = %#v, want same id with pending status", reused)
	}

	details, err := ListSubscriptionEpisodeSourceDetails(sub.ID)
	if err != nil {
		t.Fatalf("list episode source details: %v", err)
	}
	if len(details) != 1 {
		t.Fatalf("detail count = %d, want 1: %#v", len(details), details)
	}
	if details[0].Status != model.SubscriptionItemStatusTransferred {
		t.Fatalf("detail status = %q, want accepted snapshot status %q", details[0].Status, model.SubscriptionItemStatusTransferred)
	}
}

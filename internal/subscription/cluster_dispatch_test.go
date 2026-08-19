package subscription

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type recordingClusterDispatcher struct {
	inspectTasks []ClusterInspectTask
	tasks        []ClusterMediaTask
	results      []ClusterDispatchResult
	err          error
}

type retryClusterDispatcher struct {
	recordingClusterDispatcher
	result ClusterRetryResult
	err    error
}

func (d *retryClusterDispatcher) RetryFailedSubscriptionItems(context.Context, uint) (ClusterRetryResult, error) {
	return d.result, d.err
}

func (d *recordingClusterDispatcher) DispatchSubscriptionInspect(_ context.Context, task ClusterInspectTask) (string, error) {
	if d.err != nil {
		return "", d.err
	}
	d.inspectTasks = append(d.inspectTasks, task)
	return "inspect-" + task.IdempotencyKey, nil
}

func (d *recordingClusterDispatcher) DispatchSubscriptionMedia(_ context.Context, tasks []ClusterMediaTask) ([]ClusterDispatchResult, error) {
	d.tasks = append(d.tasks, tasks...)
	if d.err != nil {
		return nil, d.err
	}
	if d.results != nil {
		return append([]ClusterDispatchResult(nil), d.results...), nil
	}
	results := make([]ClusterDispatchResult, 0, len(tasks))
	for _, task := range tasks {
		results = append(results, ClusterDispatchResult{SourceKey: task.SourceKey, JobID: "job-" + task.SourceKey})
	}
	return results, nil
}

func TestRetryFailedForRoleUsesClusterJobReplayWithoutDiscovery(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{Name: "Cluster retry", TMDBName: "Cluster retry", TransferEnabled: true}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	dispatcher := &retryClusterDispatcher{result: ClusterRetryResult{Requeued: 3}}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	result, err := RetryFailedForRole(context.Background(), sub.ID, model.ClusterRoleHybrid)
	if err != nil {
		t.Fatalf("retry failed for role: %v", err)
	}
	if result == nil || result.Run == nil || result.Run.Status != model.SubscriptionStatusRunning || result.Run.QueuedCount != 3 || result.Run.TransferredCount != 0 {
		t.Fatalf("retry result = %#v, want a run with three requeued tasks", result)
	}
	if result.Subscription == nil || result.Subscription.ID != sub.ID {
		t.Fatalf("retry subscription = %#v", result.Subscription)
	}
}

func TestRetryOrphanedClusterSubscriptionItemsRebuildsMissingJob(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{Name: "orphan retry", TMDBName: "orphan retry", TransferEnabled: true}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	item := &model.SubscriptionItem{
		SubscriptionID: sub.ID, SourceKey: "orphan-source", SourceProvider: "quark",
		SourceURL: "https://pan.quark.cn/s/orphan-share", FileID: "file-1", FileName: "episode.mkv",
		FileHash: "hash-1", Status: model.SubscriptionItemStatusFailed, LastSeenAt: time.Now().UTC(),
	}
	if err := db.GetDb().Create(item).Error; err != nil {
		t.Fatal(err)
	}
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	result, err := RetryOrphanedClusterSubscriptionItems(context.Background(), sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Requeued != 1 || len(dispatcher.tasks) != 1 {
		t.Fatalf("retry result = %#v tasks=%#v", result, dispatcher.tasks)
	}
	var got model.SubscriptionItem
	if err := db.GetDb().First(&got, "id = ?", item.ID).Error; err != nil {
		t.Fatal(err)
	}
	if got.Status != model.SubscriptionItemStatusTransferring || got.ClusterJobID == "" {
		t.Fatalf("replayed item = %#v", got)
	}
}

func TestRetryShareRefFallsBackToPersistedSourceMessage(t *testing.T) {
	item := &model.SubscriptionItem{
		SourceMessageText: "Example S01E01 https://pan.quark.cn/s/replay-share 提取码: pass",
	}
	ref, err := retryShareRef(item)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Provider != ShareProviderQuark || ref.ShareID != "replay-share" || ref.Passcode != "pass" {
		t.Fatalf("recovered ref = %#v", ref)
	}
}

func TestRunTelegramClusterSkipsMessagesWithoutSubscriptionTitle(t *testing.T) {
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	oldSearch := builtinTelegramSearch
	builtinTelegramSearch = func(context.Context, *model.Subscription, model.SubscriptionTelegramSourceConfig) ([]telegramCommandRow, error) {
		return []telegramCommandRow{
			{MsgID: int64(19575), Channel: "@shows", Text: "君九龄.2021.S01E04 https://pan.quark.cn/s/bc18e4ea5fb8"},
			{MsgID: int64(19576), Channel: "@shows", Text: "小芳.2026.S01E04 https://pan.quark.cn/s/bc18e4ea5fb9"},
		}, nil
	}
	t.Cleanup(func() { builtinTelegramSearch = oldSearch })

	sub := &model.Subscription{
		ID:           88,
		Name:         "小芳",
		TMDBName:     "小芳",
		SourceType:   model.SubscriptionSourceTelegram,
		SourceConfig: `{"api_id":1,"api_hash":"hash"}`,
	}
	_, _, _, _, dispatched, err := runTelegramCluster(context.Background(), sub)
	if err != nil {
		t.Fatalf("run Telegram cluster: %v", err)
	}
	if dispatched != 1 {
		t.Fatalf("dispatched = %d, want 1", dispatched)
	}
	if len(dispatcher.inspectTasks) != 1 {
		t.Fatalf("inspect tasks = %#v, want one matching message", dispatcher.inspectTasks)
	}
	if got := dispatcher.inspectTasks[0].SourceMessageID; got != "19576" {
		t.Fatalf("source message ID = %q, want 19576", got)
	}
}

func TestRunTelegramClusterGroupsAllMatchingMessagesIntoOneObservation(t *testing.T) {
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	oldSearch := builtinTelegramSearch
	builtinTelegramSearch = func(context.Context, *model.Subscription, model.SubscriptionTelegramSourceConfig) ([]telegramCommandRow, error) {
		return []telegramCommandRow{
			{
				MsgID: int64(20001), Channel: "@shows",
				Text: "小芳.2026.S01E04 https://pan.quark.cn/s/bc18e4ea5fb8",
			},
			{
				MsgID: int64(20002), Channel: "@shows",
				Text: "小芳.2026.S01E04 https://www.123pan.com/s/example",
			},
		}, nil
	}
	t.Cleanup(func() { builtinTelegramSearch = oldSearch })

	sub := &model.Subscription{ID: 89, Name: "小芳", TMDBName: "小芳", SourceType: model.SubscriptionSourceTelegram, SourceConfig: `{"api_id":1,"api_hash":"hash"}`}
	_, _, _, _, dispatched, err := runTelegramCluster(context.Background(), sub)
	if err != nil {
		t.Fatalf("run Telegram cluster: %v", err)
	}
	if dispatched != 2 || len(dispatcher.inspectTasks) != 2 {
		t.Fatalf("dispatched=%d tasks=%#v, want two inspections", dispatched, dispatcher.inspectTasks)
	}
	if dispatcher.inspectTasks[0].ObservationKey == "" || dispatcher.inspectTasks[0].ObservationKey != dispatcher.inspectTasks[1].ObservationKey {
		t.Fatalf("observation keys = %q/%q", dispatcher.inspectTasks[0].ObservationKey, dispatcher.inspectTasks[1].ObservationKey)
	}
	for _, task := range dispatcher.inspectTasks {
		if task.ObservationExpected != 2 {
			t.Fatalf("observation expected = %d, want 2", task.ObservationExpected)
		}
	}
}

func TestClusterInspectTransferPriorityUsesGlobalPriorityForPanSou(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	if _, err := SaveConfig(model.SubscriptionConfig{
		Telegram: model.SubscriptionTelegramSourceConfig{
			TransferPriority: []string{"quark", "pan123", "pan115", "aliyun_drive"},
		},
	}); err != nil {
		t.Fatalf("save subscription config: %v", err)
	}

	got := clusterInspectTransferPriority(&model.Subscription{SourceType: model.SubscriptionSourcePanSou})
	want := []string{"quark", "pan123", "pan115", "aliyun_drive", "guangyapan"}
	if !stringSlicesEqual(got, want) {
		t.Fatalf("PanSou cluster priority = %#v, want %#v", got, want)
	}
}

func TestRunPanSouClusterGroupsAllResultsIntoOneObservation(t *testing.T) {
	body, err := json.Marshal(map[string]any{
		"data": []map[string]any{
			{
				"title": "Example S01E01 Quark",
				"links": []map[string]any{{"url": "https://pan.quark.cn/s/bc18e4ea5fb8"}},
			},
			{
				"title": "Example S01E01 123",
				"links": []map[string]any{{"url": "https://www.123pan.com/s/example"}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	config, err := json.Marshal(model.SubscriptionPanSouSourceConfig{BaseURL: server.URL, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}

	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })
	sub := &model.Subscription{ID: 90, Name: "Example", TMDBName: "Example", SourceType: model.SubscriptionSourcePanSou, SourceConfig: string(config)}
	_, _, _, _, dispatched, err := runPanSouCluster(context.Background(), sub)
	if err != nil {
		t.Fatalf("run PanSou cluster: %v", err)
	}
	if dispatched != 2 || len(dispatcher.inspectTasks) != 2 {
		t.Fatalf("dispatched=%d tasks=%#v, want two inspections", dispatched, dispatcher.inspectTasks)
	}
	if dispatcher.inspectTasks[0].ObservationKey == "" || dispatcher.inspectTasks[0].ObservationKey != dispatcher.inspectTasks[1].ObservationKey {
		t.Fatalf("observation keys = %q/%q", dispatcher.inspectTasks[0].ObservationKey, dispatcher.inspectTasks[1].ObservationKey)
	}
	for _, task := range dispatcher.inspectTasks {
		if task.ObservationExpected != 2 {
			t.Fatalf("observation expected = %d, want 2", task.ObservationExpected)
		}
	}
}

func TestSourceMessageFromTelegramRowPreservesAllTitleFields(t *testing.T) {
	message := sourceMessageFromTelegramRow(telegramCommandRow{
		MsgID:   int64(19576),
		Channel: "Pan123Movie",
		Text:    "S01E04",
		RawText: "小芳.2026.S01E04",
	})
	if !subscriptionTitleMatches(&model.Subscription{Name: "小芳", TMDBName: "小芳"}, message.Text) {
		t.Fatalf("source message lost the title-bearing raw text: %q", message.Text)
	}
	if message.URL != "https://t.me/Pan123Movie/19576" {
		t.Fatalf("source message URL = %q, want Telegram permalink", message.URL)
	}
}

func TestClusterObservationKeyChangesAcrossSubscriptionRuns(t *testing.T) {
	items := []clusterInspectObservationItem{{
		ref:     ShareRef{Provider: ShareProviderPan123, ShareID: "share-1"},
		message: clusterSourceMessage{ID: "94705", Channel: "Pan123Movie"},
	}}
	first := clusterObservationKeyWithRunID("run-1", 182, "telegram", items)
	second := clusterObservationKeyWithRunID("run-2", 182, "telegram", items)
	if first == second {
		t.Fatalf("observation key reused across runs: %q", first)
	}
}

func TestApplyClusterInspectManifestRequiresTelegramMessageTitleButAllowsEpisodeOnlyFile(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{
		Name:                     "小芳",
		TMDBName:                 "小芳",
		SourceType:               model.SubscriptionSourceTelegram,
		TransferEnabled:          true,
		MediaType:                "tv",
		Season:                   1,
		TargetRoot:               "/tv",
		LatestSeasonEpisodeStart: 4,
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	objects := []ClusterInspectObject{{FileID: "episode-4", RelativePath: "S01E04.mp4", Size: 1024}}
	baseTask := ClusterInspectTask{
		SubscriptionID: sub.ID,
		ShareProvider:  string(ShareProviderQuark),
		ShareURL:       "https://pan.quark.cn/s/bc18e4ea5fb8",
	}

	badTask := baseTask
	badTask.SourceMessageID = "19575"
	badTask.SourceMessageText = "君九龄.2021.S01E04"
	count, err := ApplyClusterInspectManifest(context.Background(), badTask, objects)
	if err != nil {
		t.Fatalf("apply unrelated Telegram manifest: %v", err)
	}
	if count != 0 || len(dispatcher.tasks) != 0 {
		t.Fatalf("unrelated Telegram message dispatched %d tasks: %#v", count, dispatcher.tasks)
	}

	goodTask := baseTask
	goodTask.SourceMessageID = "19576"
	goodTask.SourceMessageText = "小芳.2026.S01E04"
	count, err = ApplyClusterInspectManifest(context.Background(), goodTask, objects)
	if err != nil {
		t.Fatalf("apply matching Telegram manifest: %v", err)
	}
	if count != 1 || len(dispatcher.tasks) != 1 {
		t.Fatalf("matching message dispatched %d tasks: %#v", count, dispatcher.tasks)
	}
	if dispatcher.tasks[0].SourceRelativePath != "S01E04.mp4" {
		t.Fatalf("source file = %q, want episode-only filename", dispatcher.tasks[0].SourceRelativePath)
	}
}

func TestApplyClusterInspectObservationPrefersProviderPriorityAcrossShares(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{
		Name: "Example", TMDBName: "Example", SourceType: model.SubscriptionSourceManual,
		TransferEnabled: true, MediaType: "tv", TargetRoot: "/tv",
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	inputs := []ClusterInspectManifestInput{
		{
			Task: ClusterInspectTask{SubscriptionID: sub.ID, ShareProvider: string(ShareProviderAliyunDrive), ShareURL: "https://www.alipan.com/s/example"},
			Objects: []ClusterInspectObject{
				{FileID: "aliyun-largest", RelativePath: "Example.S01E01.aliyun.mkv", Size: 1200},
			},
		},
		{
			Task: ClusterInspectTask{SubscriptionID: sub.ID, ShareProvider: string(ShareProviderQuark), ShareURL: "https://pan.quark.cn/s/bc18e4ea5fb8"},
			Objects: []ClusterInspectObject{
				{FileID: "quark-large", RelativePath: "Example.S01E01.large.mkv", Size: 900},
				{FileID: "quark-season-two", RelativePath: "Example.S02E01.mkv", Size: 700},
				{FileID: "quark-special-a", RelativePath: "Example.Special.A.mkv", Size: 100},
			},
		},
		{
			Task: ClusterInspectTask{SubscriptionID: sub.ID, ShareProvider: string(ShareProviderPan123), ShareURL: "https://www.123pan.com/s/example"},
			Objects: []ClusterInspectObject{
				{FileID: "pan123-small", RelativePath: "Example.S01E01.small.mkv", Size: 600},
				{FileID: "pan123-special-b", RelativePath: "Example.Special.B.mkv", Size: 110},
			},
		},
		{
			Task: ClusterInspectTask{SubscriptionID: sub.ID, ShareProvider: string(ShareProviderPan115), ShareURL: "https://115.com/s/swssal13zrk?password=t58d"},
			Objects: []ClusterInspectObject{
				{FileID: "pan115-larger", RelativePath: "Example.S01E01.pan115.mkv", Size: 1000},
			},
		},
	}

	count, err := ApplyClusterInspectObservation(context.Background(), inputs)
	if err != nil {
		t.Fatalf("apply observation: %v", err)
	}
	if count != 4 || len(dispatcher.tasks) != 4 {
		t.Fatalf("dispatched=%d tasks=%#v, want largest S01E01, S02E01, and two unknown episodes", count, dispatcher.tasks)
	}
	var seasonOne *ClusterMediaTask
	unknown := 0
	for i := range dispatcher.tasks {
		task := &dispatcher.tasks[i]
		if task.Season == 1 && task.Episode == 1 {
			seasonOne = task
		}
		if task.Episode <= 0 {
			unknown++
		}
	}
	if seasonOne == nil || seasonOne.ShareProvider != string(ShareProviderPan123) || seasonOne.SourceFileID != "pan123-small" || seasonOne.SourceSize != 600 {
		t.Fatalf("season one winner = %#v, want preferred pan123 file", seasonOne)
	}
	if unknown != 2 {
		t.Fatalf("unknown episode tasks = %d, want 2", unknown)
	}
}

func TestApplyClusterInspectObservationDoesNotRedispatchAcceptedEpisode(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{
		Name: "Example", TMDBName: "Example", SourceType: model.SubscriptionSourceManual,
		TransferEnabled: true, MediaType: "tv", TargetRoot: "/tv",
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	small := ClusterInspectManifestInput{
		Task: ClusterInspectTask{
			SubscriptionID: sub.ID, ShareProvider: string(ShareProviderQuark),
			ShareURL: "https://pan.quark.cn/s/small", SourceMessageID: "1",
		},
		Objects: []ClusterInspectObject{{FileID: "small", RelativePath: "Example.S01E01.small.mkv", Size: 600}},
	}
	large := ClusterInspectManifestInput{
		Task: ClusterInspectTask{
			SubscriptionID: sub.ID, ShareProvider: string(ShareProviderPan123),
			ShareURL: "https://www.123pan.com/s/large", SourceMessageID: "2",
		},
		Objects: []ClusterInspectObject{{FileID: "large", RelativePath: "Example.S01E01.large.mkv", Size: 900}},
	}

	count, err := ApplyClusterInspectObservation(context.Background(), []ClusterInspectManifestInput{small})
	if err != nil {
		t.Fatalf("apply small observation: %v", err)
	}
	if count != 1 || len(dispatcher.tasks) != 1 || dispatcher.tasks[0].SourceFileID != "small" {
		t.Fatalf("small observation dispatched=%d tasks=%#v", count, dispatcher.tasks)
	}

	count, err = ApplyClusterInspectObservation(context.Background(), []ClusterInspectManifestInput{large})
	if err != nil {
		t.Fatalf("apply large observation: %v", err)
	}
	if count != 0 {
		t.Fatalf("large observation dispatched=%d, want accepted episode to stay locked", count)
	}
	if len(dispatcher.tasks) != 1 || dispatcher.tasks[0].SourceFileID != "small" {
		t.Fatalf("tasks after large = %#v, want only the already accepted task", dispatcher.tasks)
	}

	items, err := db.ListSubscriptionItems(sub.ID)
	if err != nil {
		t.Fatalf("list items: %v", err)
	}
	var smallItem, largeItem *model.SubscriptionItem
	for i := range items {
		switch items[i].FileID {
		case "small":
			smallItem = &items[i]
		case "large":
			largeItem = &items[i]
		}
	}
	if smallItem == nil || largeItem == nil {
		t.Fatalf("items = %#v, want both small and large", items)
	}
	if smallItem.Status != model.SubscriptionItemStatusTransferring {
		t.Fatalf("small status = %q, want accepted transfer to remain active", smallItem.Status)
	}
	if largeItem.Status != model.SubscriptionItemStatusSkipped {
		t.Fatalf("large status = %q, want later duplicate skipped", largeItem.Status)
	}

	tiny := ClusterInspectManifestInput{
		Task: ClusterInspectTask{
			SubscriptionID: sub.ID, ShareProvider: string(ShareProviderQuark),
			ShareURL: "https://pan.quark.cn/s/tiny", SourceMessageID: "3",
		},
		Objects: []ClusterInspectObject{{FileID: "tiny", RelativePath: "Example.S01E01.tiny.mkv", Size: 100}},
	}
	before := len(dispatcher.tasks)
	count, err = ApplyClusterInspectObservation(context.Background(), []ClusterInspectManifestInput{tiny})
	if err != nil {
		t.Fatalf("apply tiny observation: %v", err)
	}
	if count != 0 || len(dispatcher.tasks) != before {
		t.Fatalf("tiny observation should not dispatch: count=%d tasks=%d", count, len(dispatcher.tasks))
	}
}

// TestApplyClusterInspectObservationPrefersProviderPriorityForMovieSlot mirrors
// TestApplyClusterInspectObservationPrefersProviderPriorityAcrossShares but for
// a movie subscription: two candidates land on the same movie slot (same
// TargetPath) from different providers in one batch apply, and only the
// preferred provider should dispatch.
func TestApplyClusterInspectObservationPrefersProviderPriorityForMovieSlot(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{
		Name: "Movie", TMDBName: "Movie", TMDBYear: 2026, SourceType: model.SubscriptionSourceManual,
		TransferEnabled: true, MediaType: "movie", TargetRoot: "/movies",
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	inputs := []ClusterInspectManifestInput{
		{
			Task: ClusterInspectTask{SubscriptionID: sub.ID, ShareProvider: string(ShareProviderAliyunDrive), ShareURL: "https://www.alipan.com/s/example"},
			Objects: []ClusterInspectObject{
				{FileID: "movie-aliyun", RelativePath: "Movie.aliyun.mkv", Size: 900},
			},
		},
		{
			Task: ClusterInspectTask{SubscriptionID: sub.ID, ShareProvider: string(ShareProviderPan123), ShareURL: "https://www.123pan.com/s/example"},
			Objects: []ClusterInspectObject{
				{FileID: "movie-pan123", RelativePath: "Movie.pan123.mkv", Size: 600},
			},
		},
	}

	count, err := ApplyClusterInspectObservation(context.Background(), inputs)
	if err != nil {
		t.Fatalf("apply observation: %v", err)
	}
	if count != 1 || len(dispatcher.tasks) != 1 {
		t.Fatalf("dispatched=%d tasks=%#v, want exactly one movie winner", count, dispatcher.tasks)
	}
	if dispatcher.tasks[0].ShareProvider != string(ShareProviderPan123) || dispatcher.tasks[0].SourceFileID != "movie-pan123" {
		t.Fatalf("movie winner = %#v, want preferred pan123 file even though it is smaller", dispatcher.tasks[0])
	}

	// Both candidates land on the same slot within one batch apply, so
	// selectClusterInspectCandidates resolves the winner before either is
	// persisted: only the preferred pan123 file should ever become a row.
	items, err := db.ListSubscriptionItems(sub.ID)
	if err != nil {
		t.Fatalf("list items: %v", err)
	}
	if len(items) != 1 || items[0].FileID != "movie-pan123" {
		t.Fatalf("items = %#v, want only the single preferred movie winner stored", items)
	}
	if items[0].Status != model.SubscriptionItemStatusTransferring {
		t.Fatalf("pan123 status = %q, want the preferred movie candidate dispatched", items[0].Status)
	}
}

// TestApplyClusterInspectObservationIncrementalKeepsOneWinnerPerMovieSlot
// verifies that the incremental apply path also keeps a single winner per
// movie slot: a weaker provider's manifest arrives first while a stronger
// sibling is still pending (so it must wait), then the stronger sibling
// arrives and becomes the sole winner, with the earlier candidate skipped.
func TestApplyClusterInspectObservationIncrementalKeepsOneWinnerPerMovieSlot(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{
		Name: "Movie", TMDBName: "Movie", TMDBYear: 2026, SourceType: model.SubscriptionSourceManual,
		TransferEnabled: true, MediaType: "movie", TargetRoot: "/movies",
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	weaker := ClusterInspectManifestInput{
		Task:    ClusterInspectTask{SubscriptionID: sub.ID, ShareProvider: string(ShareProviderAliyunDrive), ShareURL: "https://www.alipan.com/s/example"},
		Objects: []ClusterInspectObject{{FileID: "movie-aliyun", RelativePath: "Movie.aliyun.mkv", Size: 900}},
	}
	stronger := ClusterInspectManifestInput{
		Task:    ClusterInspectTask{SubscriptionID: sub.ID, ShareProvider: string(ShareProviderPan123), ShareURL: "https://www.123pan.com/s/example"},
		Objects: []ClusterInspectObject{{FileID: "movie-pan123", RelativePath: "Movie.pan123.mkv", Size: 600}},
	}

	count, err := ApplyClusterInspectObservationIncremental(
		context.Background(),
		[]ClusterInspectManifestInput{weaker},
		ObservationCloseState{PendingProviders: []string{string(ShareProviderPan123)}},
	)
	if err != nil {
		t.Fatalf("apply weaker manifest: %v", err)
	}
	if count != 0 || len(dispatcher.tasks) != 0 {
		t.Fatalf("dispatched=%d tasks=%#v, want the weaker movie candidate to wait for the pending pan123 sibling", count, dispatcher.tasks)
	}

	items, err := db.ListSubscriptionItems(sub.ID)
	if err != nil {
		t.Fatalf("list items after weaker manifest: %v", err)
	}
	if len(items) != 1 || items[0].Status != model.SubscriptionItemStatusPending {
		t.Fatalf("items after weaker manifest = %#v, want the sole candidate left pending", items)
	}

	count, err = ApplyClusterInspectObservationIncremental(
		context.Background(),
		[]ClusterInspectManifestInput{stronger},
		ObservationCloseState{},
	)
	if err != nil {
		t.Fatalf("apply stronger manifest: %v", err)
	}
	if count != 1 || len(dispatcher.tasks) != 1 {
		t.Fatalf("dispatched=%d tasks=%#v, want exactly one winner once the stronger sibling arrives", count, dispatcher.tasks)
	}
	if dispatcher.tasks[0].ShareProvider != string(ShareProviderPan123) || dispatcher.tasks[0].SourceFileID != "movie-pan123" {
		t.Fatalf("winner = %#v, want the preferred pan123 file", dispatcher.tasks[0])
	}

	items, err = db.ListSubscriptionItems(sub.ID)
	if err != nil {
		t.Fatalf("list items after stronger manifest: %v", err)
	}
	var aliyunItem, pan123Item *model.SubscriptionItem
	for i := range items {
		switch items[i].FileID {
		case "movie-aliyun":
			aliyunItem = &items[i]
		case "movie-pan123":
			pan123Item = &items[i]
		}
	}
	if aliyunItem == nil || pan123Item == nil {
		t.Fatalf("items = %#v, want both movie candidates stored", items)
	}
	if aliyunItem.Status != model.SubscriptionItemStatusSkipped {
		t.Fatalf("aliyun status = %q, want the losing candidate skipped once the winner is known", aliyunItem.Status)
	}
	if pan123Item.Status != model.SubscriptionItemStatusTransferring {
		t.Fatalf("pan123 status = %q, want the winning candidate dispatched", pan123Item.Status)
	}
}

func TestClusterTasksPreservePreferredWorkerWithoutChangingMediaIdempotency(t *testing.T) {
	sub := &model.Subscription{
		ID: 41, Name: "Example", PreferredWorkerNodeID: "worker-139",
		TMDBID: 123, TMDBName: "Example", TMDBYear: 2026, MediaType: "tv", TargetRoot: "/TV",
	}
	item := &model.SubscriptionItem{
		ID: 52, SourceKey: "source-1", FileID: "file-1", FilePath: "/Example.S01E01.mkv",
		FileHash: "hash-1", FileSize: 1024, Season: 1, Episode: 1,
		TargetPath: "/TV/Example/Season 1/Example.S01E01.mkv",
	}
	ref := ShareRef{Provider: ShareProviderPan123, RawURL: "https://www.123pan.com/s/example", ShareID: "example"}
	message := clusterSourceMessage{ID: "message-1"}

	inspect := clusterInspectTask(sub, ref, message)
	media := clusterMediaTask(sub, item, ref, message)
	withoutPreference := *sub
	withoutPreference.PreferredWorkerNodeID = ""
	automaticInspect := clusterInspectTask(&withoutPreference, ref, message)
	automatic := clusterMediaTask(&withoutPreference, item, ref, message)

	if inspect.PreferredWorkerNodeID != "worker-139" {
		t.Fatalf("inspect preferred worker = %q", inspect.PreferredWorkerNodeID)
	}
	if media.PreferredWorkerNodeID != "worker-139" {
		t.Fatalf("media preferred worker = %q", media.PreferredWorkerNodeID)
	}
	if inspect.IdempotencyKey != automaticInspect.IdempotencyKey {
		t.Fatalf("worker preference changed inspect idempotency: preferred=%q automatic=%q", inspect.IdempotencyKey, automaticInspect.IdempotencyKey)
	}
	if media.IdempotencyKey != automatic.IdempotencyKey || media.MediaItemID != automatic.MediaItemID {
		t.Fatalf("worker preference changed media identity: preferred=%#v automatic=%#v", media, automatic)
	}
}

func TestClusterInspectObservationTaskScopesIdempotencyToObservation(t *testing.T) {
	sub := &model.Subscription{ID: 41, Name: "Example"}
	ref := ShareRef{Provider: ShareProviderPan123, RawURL: "https://www.123pan.com/s/example", ShareID: "example"}
	message := clusterSourceMessage{ID: "message-1", Channel: "shows"}

	first := clusterInspectObservationTask(sub, ref, message, "observation-1", 2)
	retry := clusterInspectObservationTask(sub, ref, message, "observation-1", 2)
	changedBatch := clusterInspectObservationTask(sub, ref, message, "observation-2", 3)
	if first.IdempotencyKey != retry.IdempotencyKey {
		t.Fatalf("same observation changed idempotency: %q != %q", first.IdempotencyKey, retry.IdempotencyKey)
	}
	if first.IdempotencyKey == changedBatch.IdempotencyKey {
		t.Fatalf("different observations reused inspect idempotency %q", first.IdempotencyKey)
	}
}

func TestApplyClusterInspectObservationKeepsAcceptedSourceVersionLocked(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{
		Name: "Example", TMDBName: "Example", SourceType: model.SubscriptionSourceManual,
		TransferEnabled: true, MediaType: "tv", TargetRoot: "/tv",
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	manifest := func(size int64) ClusterInspectManifestInput {
		return ClusterInspectManifestInput{
			Task: ClusterInspectTask{
				SubscriptionID: sub.ID, ShareProvider: string(ShareProviderQuark),
				ShareURL: "https://pan.quark.cn/s/example", SourceMessageID: "1",
			},
			Objects: []ClusterInspectObject{{FileID: "same-file", RelativePath: "Example.S01E01.mkv", Size: size}},
		}
	}

	count, err := ApplyClusterInspectObservation(context.Background(), []ClusterInspectManifestInput{manifest(600)})
	if err != nil || count != 1 || len(dispatcher.tasks) != 1 {
		t.Fatalf("initial observation count=%d tasks=%d err=%v", count, len(dispatcher.tasks), err)
	}
	accepted, err := db.GetSubscriptionItem(sub.ID, dispatcher.tasks[0].SourceKey)
	if err != nil {
		t.Fatal(err)
	}
	acceptedHash, acceptedJobID := accepted.FileHash, accepted.ClusterJobID

	count, err = ApplyClusterInspectObservation(context.Background(), []ClusterInspectManifestInput{manifest(900)})
	if err != nil {
		t.Fatalf("apply updated source version: %v", err)
	}
	if count != 0 || len(dispatcher.tasks) != 1 {
		t.Fatalf("updated accepted source dispatched=%d tasks=%d, want no second task", count, len(dispatcher.tasks))
	}
	locked, err := db.GetSubscriptionItem(sub.ID, accepted.SourceKey)
	if err != nil {
		t.Fatal(err)
	}
	if locked.Status != model.SubscriptionItemStatusTransferring || locked.ClusterJobID != acceptedJobID || locked.FileHash != acceptedHash {
		t.Fatalf("accepted source was overwritten: %#v", locked)
	}
}

func TestClusterDispatchPersistsContextAndTransitionsStatus(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{ID: 41, Name: "Example", SourceType: model.SubscriptionSourceTelegram, TransferEnabled: true, TMDBID: 123, TMDBName: "Example", TMDBYear: 2026, MediaType: "tv", TargetRoot: "/TV"}
	ref := ShareRef{Provider: ShareProviderAliyunDrive, RawURL: "https://www.alipan.com/s/example", ShareID: "example", Passcode: "1234"}
	message := clusterSourceMessage{ID: "9001", Channel: "shows", URL: "https://t.me/shows/9001", Text: "Example S01E02"}
	item := clusterItemFromShareEntry(sub, ref, TreeEntry{Path: "/Example.S01E02.mkv", Name: "Example.S01E02.mkv", ID: "file-2", Size: 2048, Modified: time.Unix(100, 0)}, message, time.Now())
	stored, _, _, err := upsertClusterItems([]*model.SubscriptionItem{item})
	if err != nil {
		t.Fatalf("upsert cluster item: %v", err)
	}
	count, err := dispatchClusterItems(context.Background(), sub, stored, ref, message)
	if err != nil || count != 1 {
		t.Fatalf("dispatch count=%d err=%v", count, err)
	}
	if len(dispatcher.tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(dispatcher.tasks))
	}
	task := dispatcher.tasks[0]
	if task.SourceFileID != "file-2" || task.Season != 1 || task.Episode != 2 || task.LogicalTargetPath == "" {
		t.Fatalf("task lost media context: %#v", task)
	}
	if task.SourceMessageID != "9001" || task.SourceMessageURL != message.URL || task.SharePasscode != "1234" {
		t.Fatalf("task lost source context: %#v", task)
	}
	got, err := db.GetSubscriptionItem(sub.ID, item.SourceKey)
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	if got.Status != model.SubscriptionItemStatusTransferring || got.ClusterJobID == "" {
		t.Fatalf("item status/job = %q/%q", got.Status, got.ClusterJobID)
	}
	sources, err := db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil {
		t.Fatalf("list episode sources: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("episode sources len = %d, want 1: %#v", len(sources), sources)
	}
	source := sources[0]
	if source.SourceItemID != got.ID ||
		source.SourceType != model.SubscriptionSourceTelegram ||
		source.SourceProvider != string(ShareProviderAliyunDrive) ||
		source.ShareURL != ref.RawURL ||
		source.FileName != item.FileName ||
		source.ClusterJobID != got.ClusterJobID ||
		source.Season != 1 ||
		source.Episode != 2 ||
		source.SelectedAt.IsZero() {
		t.Fatalf("episode source = %#v", source)
	}
	if err := CompleteClusterTransfer(sub.ID, item.SourceKey, got.ClusterJobID); err != nil {
		t.Fatalf("complete transfer: %v", err)
	}
	got, _ = db.GetSubscriptionItem(sub.ID, item.SourceKey)
	if got.Status != model.SubscriptionItemStatusTransferred {
		t.Fatalf("completed status = %q", got.Status)
	}
}

func TestClusterDispatchReplacesMovieSnapshotAtMovieSlot(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{ID: 46, Name: "Movie", SourceType: model.SubscriptionSourceTelegram, TransferEnabled: true, TMDBID: 456, TMDBName: "Movie", TMDBYear: 2026, MediaType: "movie", TargetRoot: "/Movies"}
	ref := ShareRef{Provider: ShareProviderAliyunDrive, RawURL: "https://www.alipan.com/s/movie", ShareID: "movie", Passcode: "1234"}
	message := clusterSourceMessage{ID: "9002", Channel: "movies", URL: "https://t.me/movies/9002", Text: "Movie"}
	item := clusterItemFromShareEntry(sub, ref, TreeEntry{Path: "/Movie.mkv", Name: "Movie.mkv", ID: "movie-file", Size: 2048, Modified: time.Unix(101, 0)}, message, time.Now())
	item.Season = 1
	item.Episode = 8
	stored, _, _, err := upsertClusterItems([]*model.SubscriptionItem{item})
	if err != nil {
		t.Fatalf("upsert cluster item: %v", err)
	}
	_, err = db.UpsertSubscriptionEpisodeSource(&model.SubscriptionEpisodeSource{
		SubscriptionID: sub.ID,
		Season:         0,
		Episode:        0,
		SourceItemID:   999,
		SourceType:     model.SubscriptionSourceManual,
		SourceProvider: "old-provider",
		ShareURL:       "https://old.example/s/movie",
		FileName:       "Old.Movie.mkv",
		SelectedAt:     time.Unix(100, 0),
	})
	if err != nil {
		t.Fatalf("seed movie source snapshot: %v", err)
	}

	count, err := dispatchClusterItems(context.Background(), sub, stored, ref, message)
	if err != nil || count != 1 {
		t.Fatalf("dispatch count=%d err=%v", count, err)
	}
	sources, err := db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil {
		t.Fatalf("list episode sources: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("episode sources len = %d, want 1: %#v", len(sources), sources)
	}
	source := sources[0]
	if source.Season != 0 ||
		source.Episode != 0 ||
		source.SourceItemID != stored[0].ID ||
		source.SourceType != model.SubscriptionSourceTelegram ||
		source.SourceProvider != string(ShareProviderAliyunDrive) ||
		source.ShareURL != ref.RawURL ||
		source.FileName != item.FileName ||
		source.ClusterJobID == "" {
		t.Fatalf("replaced movie source = %#v", source)
	}
}

func TestClusterDispatchRecoversSnapshotWhenAcceptedStatePersistenceFails(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	oldPersist := persistAcceptedSubscriptionItemAndEpisodeSourceSnapshot
	persistAcceptedSubscriptionItemAndEpisodeSourceSnapshot = func(sourceSub *model.Subscription, item *model.SubscriptionItem) error {
		if sourceSub == nil || item == nil {
			t.Fatalf("snapshot input = %#v/%#v", sourceSub, item)
		}
		return errors.New("forced source snapshot persistence failure")
	}
	t.Cleanup(func() { persistAcceptedSubscriptionItemAndEpisodeSourceSnapshot = oldPersist })

	sub := &model.Subscription{ID: 47, Name: "Movie", SourceType: model.SubscriptionSourceTelegram, TransferEnabled: true, TMDBID: 123, TMDBName: "Movie", TMDBYear: 2026, MediaType: "movie", TargetRoot: "/Movies"}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	ref := ShareRef{Provider: ShareProviderAliyunDrive, RawURL: "https://www.alipan.com/s/movie", ShareID: "movie", Passcode: "1234"}
	message := clusterSourceMessage{ID: "9003", Channel: "movies", URL: "https://t.me/movies/9003", Text: "Movie"}
	item := clusterItemFromShareEntry(sub, ref, TreeEntry{Path: "/Movie.mkv", Name: "Movie.mkv", ID: "file-3", Size: 2048, Modified: time.Unix(102, 0)}, message, time.Now())
	item.Season = 1
	item.Episode = 10
	stored, _, _, err := upsertClusterItems([]*model.SubscriptionItem{item})
	if err != nil {
		t.Fatalf("upsert cluster item: %v", err)
	}

	count, err := dispatchClusterItems(context.Background(), sub, stored, ref, message)
	if err == nil || err.Error() != "forced source snapshot persistence failure" {
		t.Fatalf("dispatch error = %v", err)
	}
	if count != 0 {
		t.Fatalf("dispatch count = %d, want 0", count)
	}
	if len(dispatcher.tasks) != 1 {
		t.Fatalf("external dispatch tasks = %d, want 1", len(dispatcher.tasks))
	}
	persisted, err := db.GetSubscriptionItem(sub.ID, item.SourceKey)
	if err != nil {
		t.Fatalf("get persisted item: %v", err)
	}
	if persisted.Status != model.SubscriptionItemStatusPending || persisted.ClusterJobID != "" {
		t.Fatalf("persisted item = %#v, want retryable pending item", persisted)
	}
	sources, err := db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil {
		t.Fatalf("list episode sources: %v", err)
	}
	if len(sources) != 0 {
		t.Fatalf("episode sources after snapshot failure = %#v, want none", sources)
	}

	persistAcceptedSubscriptionItemAndEpisodeSourceSnapshot = oldPersist
	jobID := "job-" + item.SourceKey
	if err := db.GetDb().Create(&model.ClusterJob{
		ID:                 jobID,
		Type:               model.ClusterJobTypeMediaTransfer,
		Status:             model.ClusterJobStatusRunning,
		SubscriptionID:     sub.ID,
		SubscriptionItemID: persisted.ID + 1,
		MediaItemID:        clusterMediaTask(sub, persisted, ref, message).MediaItemID,
	}).Error; err != nil {
		t.Fatalf("create mismatched cluster job: %v", err)
	}
	if err := FailClusterTransfer(sub.ID, item.SourceKey, jobID, errors.New("cluster transfer failed")); err == nil {
		t.Fatal("mismatched cluster job unexpectedly recovered pending item")
	}
	persisted, err = db.GetSubscriptionItem(sub.ID, item.SourceKey)
	if err != nil {
		t.Fatalf("get rejected item: %v", err)
	}
	if persisted.Status != model.SubscriptionItemStatusPending {
		t.Fatalf("rejected item status = %q, want pending", persisted.Status)
	}
	if err := db.GetDb().Model(&model.ClusterJob{}).Where("id = ?", jobID).Update("subscription_item_id", persisted.ID).Error; err != nil {
		t.Fatalf("correct cluster job item id: %v", err)
	}
	if err := FailClusterTransfer(sub.ID, item.SourceKey, jobID, errors.New("cluster transfer failed")); err != nil {
		t.Fatalf("recover failed cluster transfer: %v", err)
	}
	persisted, err = db.GetSubscriptionItem(sub.ID, item.SourceKey)
	if err != nil {
		t.Fatalf("get recovered item: %v", err)
	}
	if persisted.Status != model.SubscriptionItemStatusFailed || persisted.ClusterJobID != jobID || persisted.LastError != "cluster transfer failed" {
		t.Fatalf("recovered item = %#v", persisted)
	}
	sources, err = db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil {
		t.Fatalf("list recovered episode sources: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("recovered episode sources = %#v, want one", sources)
	}
	source := sources[0]
	if source.SourceItemID != persisted.ID ||
		source.SourceType != model.SubscriptionSourceTelegram ||
		source.SourceProvider != string(ShareProviderAliyunDrive) ||
		source.ShareURL != ref.RawURL ||
		source.FileName != item.FileName ||
		source.ClusterJobID != jobID ||
		source.Season != 0 ||
		source.Episode != 0 {
		t.Fatalf("recovered episode source = %#v", source)
	}
}

func TestClusterCallbackRejectsStaleFileHash(t *testing.T) {
	setupSubscriptionRuntimeDB(t)

	sub := &model.Subscription{Name: "Stale cluster callback", SourceType: model.SubscriptionSourceTelegram, MediaType: "movie"}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	accepted, _, err := db.PersistAcceptedSubscriptionItemAndEpisodeSource(&model.SubscriptionItem{
		SubscriptionID: sub.ID,
		SourceKey:      "stale-cluster",
		SourceProvider: string(ShareProviderAliyunDrive),
		SourceURL:      "https://www.alipan.com/s/old",
		FileName:       "Movie.mkv",
		FileHash:       "hash-old",
		Season:         1,
		Episode:        14,
		TargetDir:      "/movies",
		TargetName:     "Movie.mkv",
		TargetPath:     "/movies/Movie.mkv",
		ClusterJobID:   "old-job",
		Status:         model.SubscriptionItemStatusTransferring,
	}, &model.SubscriptionEpisodeSource{
		SubscriptionID: sub.ID,
		Season:         0,
		Episode:        0,
		SourceType:     model.SubscriptionSourceTelegram,
		SourceProvider: string(ShareProviderAliyunDrive),
		ShareURL:       "https://www.alipan.com/s/old",
		FileName:       "Movie.mkv",
		ClusterJobID:   "old-job",
	})
	if err != nil {
		t.Fatalf("persist accepted item: %v", err)
	}
	oldMediaID := clusterMediaTask(sub, accepted, ShareRef{}, clusterSourceMessage{}).MediaItemID
	if err := db.GetDb().Create(&model.ClusterJob{
		ID:                 "old-job",
		Type:               model.ClusterJobTypeMediaTransfer,
		Status:             model.ClusterJobStatusRunning,
		SubscriptionID:     sub.ID,
		SubscriptionItemID: accepted.ID,
		MediaItemID:        oldMediaID,
	}).Error; err != nil {
		t.Fatalf("create accepted cluster job: %v", err)
	}
	newer := *accepted
	newer.FileHash = "hash-new"
	newer.Status = model.SubscriptionItemStatusPending
	if _, _, err := db.UpsertSubscriptionItem(&newer); err != nil {
		t.Fatalf("replace item with newer file hash: %v", err)
	}
	sourcesBefore, err := db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil || len(sourcesBefore) != 1 {
		t.Fatalf("source before stale callback = %#v err=%v", sourcesBefore, err)
	}

	if err := CompleteClusterTransfer(sub.ID, accepted.SourceKey, "old-job"); err == nil {
		t.Fatal("stale cluster callback unexpectedly finalized newer item")
	}
	item, err := db.GetSubscriptionItem(sub.ID, accepted.SourceKey)
	if err != nil {
		t.Fatalf("get newer item: %v", err)
	}
	if item.Status != model.SubscriptionItemStatusPending || item.FileHash != "hash-new" {
		t.Fatalf("newer item = %#v, want pending hash-new", item)
	}
	sources, err := db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil || len(sources) != 1 {
		t.Fatalf("sources after stale callback = %#v err=%v", sources, err)
	}
	if got := sources[0]; got.SourceItemID != sourcesBefore[0].SourceItemID ||
		got.SourceProvider != sourcesBefore[0].SourceProvider ||
		got.ShareURL != sourcesBefore[0].ShareURL ||
		got.ClusterJobID != sourcesBefore[0].ClusterJobID ||
		got.Season != 0 ||
		got.Episode != 0 {
		t.Fatalf("source changed by stale callback = %#v", got)
	}
}

func TestClusterCallbackRequiresMatchingActiveJobID(t *testing.T) {
	setupSubscriptionRuntimeDB(t)

	item, _, err := db.UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: 1,
		SourceKey:      "active-cluster-job",
		ClusterJobID:   "active-job",
		Status:         model.SubscriptionItemStatusTransferring,
	})
	if err != nil {
		t.Fatalf("upsert transferring item: %v", err)
	}
	if err := CompleteClusterTransfer(item.SubscriptionID, item.SourceKey, "stale-job"); err == nil {
		t.Fatal("stale job unexpectedly completed active transfer")
	}
	persisted, err := db.GetSubscriptionItem(item.SubscriptionID, item.SourceKey)
	if err != nil {
		t.Fatalf("get item after stale callback: %v", err)
	}
	if persisted.Status != model.SubscriptionItemStatusTransferring || persisted.ClusterJobID != "active-job" {
		t.Fatalf("item after stale callback = %#v", persisted)
	}

	persisted.ClusterJobID = ""
	if _, _, err := db.UpsertSubscriptionItem(persisted); err != nil {
		t.Fatalf("clear active job id: %v", err)
	}
	if err := FailClusterTransfer(item.SubscriptionID, item.SourceKey, "active-job", errors.New("late failure")); err == nil {
		t.Fatal("callback unexpectedly failed item without an active job id")
	}
	persisted, err = db.GetSubscriptionItem(item.SubscriptionID, item.SourceKey)
	if err != nil {
		t.Fatalf("get item after missing job callback: %v", err)
	}
	if persisted.Status != model.SubscriptionItemStatusTransferring || persisted.ClusterJobID != "" {
		t.Fatalf("item after missing job callback = %#v", persisted)
	}
}

func TestClusterDispatchDoesNotCarrySubscriptionFolders(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{
		ID:              44,
		Name:            "Provider targets",
		TransferEnabled: true,
		TMDBName:        "Example",
		MediaType:       "tv",
		TempTarget: model.SubscriptionStorageTarget{
			Provider: " PAN123 ",
			Folder:   `转存至移动/./`,
		},
		DeliveryTarget: model.SubscriptionStorageTarget{
			Provider: " YIDONG139 ",
			Folder:   `港台剧\热播`,
		},
	}
	ref := ShareRef{Provider: ShareProviderPan123, RawURL: "https://www.123pan.com/s/example", ShareID: "example"}
	item := clusterItemFromShareEntry(sub, ref, TreeEntry{
		Path: "/Example.S01E01.mkv", Name: "Example.S01E01.mkv", ID: "file-1", Size: 1024,
	}, clusterSourceMessage{}, time.Now())
	stored, _, _, err := upsertClusterItems([]*model.SubscriptionItem{item})
	if err != nil {
		t.Fatalf("upsert cluster item: %v", err)
	}
	if _, err := dispatchClusterItems(context.Background(), sub, stored, ref, clusterSourceMessage{}); err != nil {
		t.Fatalf("dispatch cluster item: %v", err)
	}
	if len(dispatcher.tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(dispatcher.tasks))
	}
	task := dispatcher.tasks[0]
	if task.LogicalMediaRoot != "" {
		t.Fatalf("logical media root = %q, want no legacy path dependency", task.LogicalMediaRoot)
	}
}

func TestClusterItemDoesNotRedispatchChangedObjectAfterEpisodeCompleted(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{ID: 42, Name: "Example", TransferEnabled: true, TMDBName: "Example", MediaType: "tv", TargetRoot: "/TV"}
	ref := ShareRef{Provider: ShareProviderQuark, RawURL: "https://pan.quark.cn/s/example", ShareID: "example"}
	first := clusterItemFromShareEntry(sub, ref, TreeEntry{Path: "/Example.S01E03.mkv", Name: "Example.S01E03.mkv", ID: "same-file", Size: 100, Modified: time.Unix(100, 0)}, clusterSourceMessage{ID: "1"}, time.Now())
	stored, _, _, err := upsertClusterItems([]*model.SubscriptionItem{first})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatchClusterItems(context.Background(), sub, stored, ref, clusterSourceMessage{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	jobID := "job-" + first.SourceKey
	if err := CompleteClusterTransfer(sub.ID, first.SourceKey, jobID); err != nil {
		t.Fatal(err)
	}

	unchanged := clusterItemFromShareEntry(sub, ref, TreeEntry{Path: first.FilePath, Name: first.FileName, ID: "same-file", Size: 100, Modified: time.Unix(100, 0)}, clusterSourceMessage{ID: "2"}, time.Now())
	stored, _, _, err = upsertClusterItems([]*model.SubscriptionItem{unchanged})
	if err != nil {
		t.Fatal(err)
	}
	count, err := dispatchClusterItems(context.Background(), sub, stored, ref, clusterSourceMessage{ID: "2"})
	if err != nil || count != 0 {
		t.Fatalf("unchanged repost dispatched count=%d err=%v", count, err)
	}

	changed := clusterItemFromShareEntry(sub, ref, TreeEntry{Path: first.FilePath, Name: first.FileName, ID: "same-file", Size: 200, Modified: time.Unix(200, 0)}, clusterSourceMessage{ID: "3"}, time.Now())
	stored, _, changedCount, err := upsertClusterItems([]*model.SubscriptionItem{changed})
	if err != nil {
		t.Fatal(err)
	}
	if changedCount != 0 {
		t.Fatalf("changed count = %d", changedCount)
	}
	count, err = dispatchClusterItems(context.Background(), sub, stored, ref, clusterSourceMessage{ID: "3"})
	if err != nil || count != 0 {
		t.Fatalf("changed object dispatch count=%d err=%v", count, err)
	}
	if len(dispatcher.tasks) != 1 {
		t.Fatalf("total tasks = %d, want accepted episode to keep one task", len(dispatcher.tasks))
	}
	persisted, err := db.GetSubscriptionItem(sub.ID, first.SourceKey)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Status != model.SubscriptionItemStatusTransferred || persisted.FileHash != first.FileHash {
		t.Fatalf("completed episode was overwritten: %#v", persisted)
	}
}

func TestClusterInspectIdentityChangesByMessageButMediaIdentityUsesCanonicalShare(t *testing.T) {
	sub := &model.Subscription{ID: 99, Name: "Example"}
	firstRef := ShareRef{Provider: ShareProviderAliyunDrive, RawURL: "https://www.alipan.com/s/example?foo=1", ShareID: "example"}
	secondRef := ShareRef{Provider: ShareProviderAliyunDrive, RawURL: "https://www.aliyundrive.com/s/example", ShareID: "example"}
	firstInspect := clusterInspectTask(sub, firstRef, clusterSourceMessage{ID: "100"})
	secondInspect := clusterInspectTask(sub, secondRef, clusterSourceMessage{ID: "101"})
	if firstInspect.IdempotencyKey == secondInspect.IdempotencyKey {
		t.Fatal("different source messages reused a share inspection idempotency key")
	}
	entry := TreeEntry{ID: "file-1", Path: "/Example.S01E01.mkv", Name: "Example.S01E01.mkv", Size: 100}
	firstItem := clusterItemFromShareEntry(sub, firstRef, entry, clusterSourceMessage{ID: "100"}, time.Now())
	secondItem := clusterItemFromShareEntry(sub, secondRef, entry, clusterSourceMessage{ID: "101"}, time.Now())
	if firstItem.SourceKey != secondItem.SourceKey {
		t.Fatal("URL aliases for the same canonical share produced different media identities")
	}
}

func TestClusterDispatchFailureMarksItemFailed(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	RegisterClusterDispatcher(&recordingClusterDispatcher{err: errors.New("no worker")})
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })
	sub := &model.Subscription{ID: 43, TransferEnabled: true, TMDBName: "Movie", MediaType: "movie", TargetRoot: "/Movies"}
	ref := ShareRef{Provider: ShareProviderQuark, RawURL: "https://pan.quark.cn/s/example", ShareID: "example"}
	item := clusterItemFromShareEntry(sub, ref, TreeEntry{Path: "/Movie.mkv", Name: "Movie.mkv", ID: "movie", Size: 100}, clusterSourceMessage{}, time.Now())
	stored, _, _, err := upsertClusterItems([]*model.SubscriptionItem{item})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatchClusterItems(context.Background(), sub, stored, ref, clusterSourceMessage{}); err == nil {
		t.Fatal("expected dispatch error")
	}
	got, _ := db.GetSubscriptionItem(sub.ID, item.SourceKey)
	if got.Status != model.SubscriptionItemStatusFailed || got.LastError != "no worker" {
		t.Fatalf("failed item = %#v", got)
	}
	sources, err := db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil {
		t.Fatalf("list episode sources: %v", err)
	}
	if len(sources) != 0 {
		t.Fatalf("episode sources after dispatch failure = %#v, want none", sources)
	}
}

func TestClusterDispatchEmptyJobIDDoesNotSnapshot(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{ID: 45, SourceType: model.SubscriptionSourceTelegram, TransferEnabled: true, TMDBName: "Movie", MediaType: "movie", TargetRoot: "/Movies"}
	ref := ShareRef{Provider: ShareProviderQuark, RawURL: "https://pan.quark.cn/s/example", ShareID: "example"}
	item := clusterItemFromShareEntry(sub, ref, TreeEntry{Path: "/Movie.mkv", Name: "Movie.mkv", ID: "movie", Size: 100}, clusterSourceMessage{}, time.Now())
	RegisterClusterDispatcher(&recordingClusterDispatcher{
		results: []ClusterDispatchResult{{SourceKey: item.SourceKey, JobID: ""}},
	})
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })
	stored, _, _, err := upsertClusterItems([]*model.SubscriptionItem{item})
	if err != nil {
		t.Fatal(err)
	}

	count, err := dispatchClusterItems(context.Background(), sub, stored, ref, clusterSourceMessage{})
	if err == nil || !strings.Contains(err.Error(), "empty job id") {
		t.Fatalf("dispatch error = %v, want empty job id error", err)
	}
	if count != 0 {
		t.Fatalf("dispatch count = %d, want 0", count)
	}
	got, err := db.GetSubscriptionItem(sub.ID, item.SourceKey)
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	if got.Status != model.SubscriptionItemStatusFailed || !strings.Contains(got.LastError, "empty job id") {
		t.Fatalf("failed item = %#v", got)
	}
	sources, err := db.ListSubscriptionEpisodeSources(sub.ID)
	if err != nil {
		t.Fatalf("list episode sources: %v", err)
	}
	if len(sources) != 0 {
		t.Fatalf("episode sources after empty job id = %#v, want none", sources)
	}
}

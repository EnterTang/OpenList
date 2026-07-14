package subscription

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type recordingClusterDispatcher struct {
	inspectTasks []ClusterInspectTask
	tasks        []ClusterMediaTask
	err          error
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
	results := make([]ClusterDispatchResult, 0, len(tasks))
	for _, task := range tasks {
		results = append(results, ClusterDispatchResult{SourceKey: task.SourceKey, JobID: "job-" + task.SourceKey})
	}
	return results, nil
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

func TestRunTelegramClusterGroupsAllMessageLinksIntoOneObservation(t *testing.T) {
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	oldSearch := builtinTelegramSearch
	builtinTelegramSearch = func(context.Context, *model.Subscription, model.SubscriptionTelegramSourceConfig) ([]telegramCommandRow, error) {
		return []telegramCommandRow{{
			MsgID: int64(20001), Channel: "@shows",
			Text: "小芳.2026.S01E04 https://pan.quark.cn/s/bc18e4ea5fb8 https://www.123pan.com/s/example",
		}}, nil
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

func TestSourceMessageFromTelegramRowPreservesAllTitleFields(t *testing.T) {
	message := sourceMessageFromTelegramRow(telegramCommandRow{
		MsgID:   int64(19576),
		Text:    "S01E04",
		RawText: "小芳.2026.S01E04",
	})
	if !subscriptionTitleMatches(&model.Subscription{Name: "小芳", TMDBName: "小芳"}, message.Text) {
		t.Fatalf("source message lost the title-bearing raw text: %q", message.Text)
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

func TestApplyClusterInspectObservationSelectsLargestEpisodeAcrossShares(t *testing.T) {
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
			Task: ClusterInspectTask{SubscriptionID: sub.ID, ShareProvider: string(ShareProviderQuark), ShareURL: "https://pan.quark.cn/s/bc18e4ea5fb8"},
			Objects: []ClusterInspectObject{
				{FileID: "quark-small", RelativePath: "Example.S01E01.small.mkv", Size: 600},
				{FileID: "quark-season-two", RelativePath: "Example.S02E01.mkv", Size: 700},
				{FileID: "quark-special-a", RelativePath: "Example.Special.A.mkv", Size: 100},
			},
		},
		{
			Task: ClusterInspectTask{SubscriptionID: sub.ID, ShareProvider: string(ShareProviderPan123), ShareURL: "https://www.123pan.com/s/example"},
			Objects: []ClusterInspectObject{
				{FileID: "pan123-large", RelativePath: "Example.S01E01.large.mkv", Size: 900},
				{FileID: "pan123-special-b", RelativePath: "Example.Special.B.mkv", Size: 110},
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
	if seasonOne == nil || seasonOne.SourceFileID != "pan123-large" || seasonOne.SourceSize != 900 {
		t.Fatalf("season one winner = %#v, want largest pan123 file", seasonOne)
	}
	if unknown != 2 {
		t.Fatalf("unknown episode tasks = %d, want 2", unknown)
	}
}

func TestClusterDispatchPersistsContextAndTransitionsStatus(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{ID: 41, Name: "Example", TransferEnabled: true, TMDBID: 123, TMDBName: "Example", TMDBYear: 2026, MediaType: "tv", TargetRoot: "/TV"}
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
	if err := CompleteClusterTransfer(sub.ID, item.SourceKey, got.ClusterJobID); err != nil {
		t.Fatalf("complete transfer: %v", err)
	}
	got, _ = db.GetSubscriptionItem(sub.ID, item.SourceKey)
	if got.Status != model.SubscriptionItemStatusTransferred {
		t.Fatalf("completed status = %q", got.Status)
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

func TestClusterItemIsIdempotentAcrossMessagesButRedispatchesChangedObject(t *testing.T) {
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
	if changedCount != 1 {
		t.Fatalf("changed count = %d", changedCount)
	}
	count, err = dispatchClusterItems(context.Background(), sub, stored, ref, clusterSourceMessage{ID: "3"})
	if err != nil || count != 1 {
		t.Fatalf("changed object dispatch count=%d err=%v", count, err)
	}
	if len(dispatcher.tasks) != 2 {
		t.Fatalf("total tasks = %d, want 2", len(dispatcher.tasks))
	}
	if dispatcher.tasks[0].IdempotencyKey == dispatcher.tasks[1].IdempotencyKey {
		t.Fatal("changed object reused dispatch idempotency key")
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
}

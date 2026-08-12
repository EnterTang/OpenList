package subscription

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/gotd/td/tg"
)

func TestRealtimeTelegramEventIsIdempotentAndUsesClusterInspectWithoutPreferredWorker(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })
	wait := 120
	sub := &model.Subscription{
		Name: "小芳", TMDBName: "小芳", Active: true, SourceType: model.SubscriptionSourceTelegram,
		PreferredWorkerNodeID: "worker-pinned",
		SourceConfig:          `{"api_id":1,"api_hash":"hash","realtime_enabled":true,"realtime_groups":["@quark"],"realtime_candidate_wait_seconds":120}`,
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	row := telegramCommandRow{MsgID: int64(101), Channel: "@quark", Text: "小芳 S01E01 https://pan.quark.cn/s/bc18e4ea5fb8", Links: []string{"https://pan.quark.cn/s/bc18e4ea5fb8"}}
	if !telegramRowMatchesSubscription(sub, row) {
		t.Fatalf("test row does not match subscription: %#v", row)
	}
	if event, created, err := EnqueueRealtimeTelegramRow(sub.ID, row); err != nil || !created || event.ID == 0 {
		t.Fatalf("enqueue first event = %#v created=%v err=%v", event, created, err)
	}
	if _, created, err := EnqueueRealtimeTelegramRow(sub.ID, row); err != nil || created {
		t.Fatalf("duplicate enqueue created=%v err=%v", created, err)
	}
	if processed, err := ProcessPendingRealtimeTelegramEvents(context.Background(), 10); err != nil || processed != 1 {
		t.Fatalf("processed=%d err=%v", processed, err)
	}
	if len(dispatcher.inspectTasks) != 1 {
		t.Fatalf("inspect tasks = %#v, want one", dispatcher.inspectTasks)
	}
	task := dispatcher.inspectTasks[0]
	if task.Trigger != realtimeTelegramTrigger || task.PreferredWorkerNodeID != "" {
		t.Fatalf("realtime inspect task = %#v, want trigger and no preferred worker", task)
	}
	if got := realtimeCandidateWait(model.SubscriptionTelegramSourceConfig{RealtimeCandidateWaitSeconds: &wait}); got != 120*time.Second {
		t.Fatalf("candidate wait = %s, want 120s", got)
	}
}

func TestClaimRealtimeTelegramEventsReclaimsStaleProcessingEvent(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{Name: "Example", TMDBName: "Example", SourceType: model.SubscriptionSourceTelegram}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	event, created, err := EnqueueRealtimeTelegramRow(sub.ID, telegramCommandRow{MsgID: 102, Channel: "@source", Text: "Example"})
	if err != nil || !created {
		t.Fatalf("enqueue event created=%v err=%v", created, err)
	}
	staleAt := time.Now().UTC().Add(-6 * time.Minute)
	if err := db.GetDb().Model(&model.SubscriptionTelegramEvent{}).Where("id = ?", event.ID).Updates(map[string]any{
		"status":     model.SubscriptionTelegramEventStatusProcessing,
		"updated_at": staleAt,
	}).Error; err != nil {
		t.Fatal(err)
	}
	claimed, err := db.ClaimSubscriptionTelegramEvents(10, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != event.ID || claimed[0].Attempts != 1 {
		t.Fatalf("claimed events = %#v", claimed)
	}
}

func TestRealtimeCandidateWaitsForPreferredProviderBeforeClusterMediaDispatch(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })
	sub := &model.Subscription{
		Name: "Example", TMDBName: "Example", Active: true, TransferEnabled: true,
		SourceType: model.SubscriptionSourceTelegram, MediaType: "tv", Season: 1,
		DeliveryTarget: model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "media"}, LatestSeasonEpisodeStart: 1,
		PreferredWorkerNodeID: "worker-pinned",
		SourceConfig:          `{"api_id":1,"api_hash":"hash","realtime_enabled":true,"realtime_candidate_wait_seconds":120,"transfer_priority":["pan123","pan115","quark","aliyun_drive"]}`,
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	quarkTask := ClusterInspectTask{
		SubscriptionID: sub.ID, SubscriptionName: sub.Name, Trigger: realtimeTelegramTrigger,
		ShareProvider: string(ShareProviderQuark), ShareURL: "https://pan.quark.cn/s/bc18e4ea5fb8",
		SourceMessageID: "11", SourceMessageChannel: "quark", SourceMessageText: "Example S01E01",
	}
	if created, err := ApplyRealtimeClusterInspectObservation(context.Background(), []ClusterInspectManifestInput{{
		Task:    quarkTask,
		Objects: []ClusterInspectObject{{FileID: "quark-e1", RelativePath: "Example.S01E01.1080p.mkv", Size: 100, Hash: "quark-hash"}},
	}}); err != nil || created != 1 {
		t.Fatalf("apply Quark realtime inspection created=%d err=%v", created, err)
	}
	if processed, err := ProcessReadyRealtimeCandidates(context.Background(), 10); err != nil || processed != 0 || len(dispatcher.tasks) != 0 {
		t.Fatalf("Quark was dispatched before grace window: processed=%d tasks=%#v err=%v", processed, dispatcher.tasks, err)
	}
	pan123Task := ClusterInspectTask{
		SubscriptionID: sub.ID, SubscriptionName: sub.Name, Trigger: realtimeTelegramTrigger,
		ShareProvider: string(ShareProviderPan123), ShareURL: "https://www.123pan.com/s/7Tx1jv-pVu7v",
		SourceMessageID: "12", SourceMessageChannel: "pan123", SourceMessageText: "Example S01E01",
	}
	if created, err := ApplyRealtimeClusterInspectObservation(context.Background(), []ClusterInspectManifestInput{{
		Task:    pan123Task,
		Objects: []ClusterInspectObject{{FileID: "123-e1", RelativePath: "Example.S01E01.2160p.mkv", Size: 200, Hash: "123-hash"}},
	}}); err != nil || created != 1 {
		t.Fatalf("apply 123 realtime inspection created=%d err=%v", created, err)
	}
	if processed, err := ProcessReadyRealtimeCandidates(context.Background(), 10); err != nil || processed != 1 {
		t.Fatalf("process preferred candidate=%d err=%v", processed, err)
	}
	if len(dispatcher.tasks) != 1 {
		t.Fatalf("media tasks = %#v, want one", dispatcher.tasks)
	}
	if task := dispatcher.tasks[0]; task.ShareProvider != string(ShareProviderPan123) || task.PreferredWorkerNodeID != "" || task.Trigger != realtimeTelegramTrigger {
		t.Fatalf("selected task = %#v, want 123 through unpinned realtime cluster dispatch", task)
	}
	var candidates []model.SubscriptionRealtimeCandidate
	if err := db.GetDb().Order("id ASC").Find(&candidates).Error; err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 2 || candidates[0].Status != model.SubscriptionRealtimeCandidateStatusSkipped || candidates[1].Status != model.SubscriptionRealtimeCandidateStatusSelected {
		t.Fatalf("candidate states = %#v", candidates)
	}
}

func TestRealtimeProviderNeedsWaitOnlyWhenAConfiguredProviderRanksHigher(t *testing.T) {
	wait := 120
	cfg := model.SubscriptionTelegramSourceConfig{
		TransferPriority:             []string{"pan123", "pan115", "quark", "aliyun_drive"},
		RealtimeCandidateWaitSeconds: &wait,
	}
	if !realtimeProviderNeedsWait("quark", cfg) {
		t.Fatal("Quark should wait for higher-priority providers")
	}
	if realtimeProviderNeedsWait("pan123", cfg) {
		t.Fatal("highest-priority provider should not wait")
	}
	cfg.RealtimeExpectedProviders = []string{"quark"}
	if realtimeProviderNeedsWait("quark", cfg) {
		t.Fatal("Quark should not wait when it is the only expected provider")
	}
}

func TestNormalizeRealtimeExpectedProvidersDoesNotAddUnconfiguredSources(t *testing.T) {
	got := normalizeRealtimeExpectedProviders([]string{" quark ", "quark", "unknown"})
	if want := []string{"quark"}; !stringSlicesEqual(got, want) {
		t.Fatalf("providers = %#v, want %#v", got, want)
	}
}

func TestApplyConfigDefaultsMergesRealtimeTelegramSettings(t *testing.T) {
	wait := 75
	sub := &model.Subscription{SourceType: model.SubscriptionSourceTelegram, SourceConfig: `{"api_id":1}`}
	err := ApplyConfigDefaults(sub, model.SubscriptionConfig{Telegram: model.SubscriptionTelegramSourceConfig{
		RealtimeEnabled:              true,
		RealtimeGroups:               []string{"@realtime"},
		RealtimeCandidateWaitSeconds: &wait,
		RealtimeExpectedProviders:    []string{"pan123", "quark"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	var got model.SubscriptionTelegramSourceConfig
	if err := json.Unmarshal([]byte(sub.SourceConfig), &got); err != nil {
		t.Fatal(err)
	}
	if !got.RealtimeEnabled || !stringSlicesEqual(got.RealtimeGroups, []string{"@realtime"}) ||
		got.RealtimeCandidateWaitSeconds == nil || *got.RealtimeCandidateWaitSeconds != wait ||
		!stringSlicesEqual(got.RealtimeExpectedProviders, []string{"pan123", "quark"}) {
		t.Fatalf("merged realtime config = %#v", got)
	}
}

func TestTelegramUpdateChannelSupportsChannelAndBasicGroupIDs(t *testing.T) {
	channelMessage := &tg.Message{PeerID: &tg.PeerChannel{ChannelID: 123}}
	if got := telegramUpdateChannel(tg.Entities{}, channelMessage); got != "-100123" {
		t.Fatalf("channel id = %q, want -100123", got)
	}
	groupMessage := &tg.Message{PeerID: &tg.PeerChat{ChatID: 456}}
	if got := telegramUpdateChannel(tg.Entities{}, groupMessage); got != "-456" {
		t.Fatalf("basic group id = %q, want -456", got)
	}
}

func TestListSubscriptionsWithProgressProjectsRealtimeCardStatus(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{
		Name: "小芳", TMDBName: "小芳", Active: true, SourceType: model.SubscriptionSourceTelegram,
		SourceConfig: `{"api_id":1,"api_hash":"hash","realtime_enabled":true,"realtime_candidate_wait_seconds":120}`,
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	item := &model.SubscriptionItem{SubscriptionID: sub.ID, SourceKey: "realtime-item", FileHash: "hash", Status: model.SubscriptionItemStatusNotifying}
	if _, _, err := db.UpsertSubscriptionItem(item); err != nil {
		t.Fatal(err)
	}
	row := telegramCommandRow{MsgID: int64(301), Channel: "@source", Text: "小芳 S01E01"}
	if _, created, err := EnqueueRealtimeTelegramRow(sub.ID, row); err != nil || !created {
		t.Fatalf("enqueue status event created=%v err=%v", created, err)
	}
	items, total, err := ListSubscriptionsWithProgress(db.SubscriptionFilter{}, "all", time.Now())
	if err != nil || total != 1 || len(items) != 1 {
		t.Fatalf("list items=%#v total=%d err=%v", items, total, err)
	}
	status := items[0].RealtimeStatus
	if !status.Enabled || status.DeliveryStatus != "notifying" || status.ActiveJobCount != 1 || status.LastMessageID != "301" {
		t.Fatalf("realtime status = %#v", status)
	}
}

func TestDeleteSubscriptionRemovesRealtimeInboxAndCandidates(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{Name: "小芳", TMDBName: "小芳", SourceType: model.SubscriptionSourceTelegram}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	if _, _, err := EnqueueRealtimeTelegramRow(sub.ID, telegramCommandRow{MsgID: int64(401), Channel: "@source", Text: "小芳"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.CreateSubscriptionRealtimeCandidate(&model.SubscriptionRealtimeCandidate{
		SubscriptionID: sub.ID, SlotKey: "tv:1:1", SourceKey: "source", FileHash: "hash", ReadyAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteSubscription(sub.ID); err != nil {
		t.Fatal(err)
	}
	var eventCount, candidateCount int64
	if err := db.GetDb().Model(&model.SubscriptionTelegramEvent{}).Where("subscription_id = ?", sub.ID).Count(&eventCount).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.GetDb().Model(&model.SubscriptionRealtimeCandidate{}).Where("subscription_id = ?", sub.ID).Count(&candidateCount).Error; err != nil {
		t.Fatal(err)
	}
	if eventCount != 0 || candidateCount != 0 {
		t.Fatalf("orphaned realtime records event=%d candidate=%d", eventCount, candidateCount)
	}
}

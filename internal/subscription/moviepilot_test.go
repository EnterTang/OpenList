package subscription

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/moviepilotbridge"
)

type fakeMoviePilotBridgeClient struct {
	searches     []moviepilotbridge.ResourceSearchRequest
	intents      []moviepilotbridge.DownloadIntentRequest
	intentModels []model.MoviePilotDownloadIntent
	results      []moviepilotbridge.ResourceSearchResult
	bridges      []string
}

type fakeMoviePilotDownloaderPolicyScheduler struct {
	policy        moviepilotbridge.DownloaderPolicy
	selectionErr  error
	selectedCalls []struct {
		bridgeID     string
		requestID    string
		expectedSize int64
	}
	released []string
}

func (f *fakeMoviePilotDownloaderPolicyScheduler) SelectMoviePilotDownloaderPolicy(_ context.Context, bridgeID, requestID string, expectedSize int64) (moviepilotbridge.DownloaderPolicy, error) {
	f.selectedCalls = append(f.selectedCalls, struct {
		bridgeID     string
		requestID    string
		expectedSize int64
	}{bridgeID: bridgeID, requestID: requestID, expectedSize: expectedSize})
	return f.policy, f.selectionErr
}

func (f *fakeMoviePilotDownloaderPolicyScheduler) ReleaseMoviePilotDownloaderReservation(_ context.Context, requestID string) error {
	f.released = append(f.released, requestID)
	return nil
}

func (f *fakeMoviePilotBridgeClient) SearchResources(_ context.Context, _ string, request moviepilotbridge.ResourceSearchRequest) ([]moviepilotbridge.ResourceSearchResult, error) {
	f.searches = append(f.searches, request)
	return append([]moviepilotbridge.ResourceSearchResult(nil), f.results...), nil
}

func (f *fakeMoviePilotBridgeClient) SubmitIntent(_ context.Context, _ string, intent *model.MoviePilotDownloadIntent, request moviepilotbridge.DownloadIntentRequest) error {
	f.intents = append(f.intents, request)
	if intent != nil {
		f.intentModels = append(f.intentModels, *intent)
	}
	return nil
}

func (f *fakeMoviePilotBridgeClient) ListEnabledInstanceIDs(_ context.Context) ([]string, error) {
	return append([]string(nil), f.bridges...), nil
}

func TestSearchMoviePilotResourcesDoesNotExposeSiteCookie(t *testing.T) {
	projected := projectMoviePilotResult("mp-main", bridgeSearchResult{
		ResourceRef: "r-1", Title: "Show S01E01", SiteCookie: "private",
	})
	if projected.ExternalRef != "r-1" {
		t.Fatalf("external ref = %q", projected.ExternalRef)
	}
	raw, err := json.Marshal(projected)
	if err != nil {
		t.Fatalf("marshal projected result: %v", err)
	}
	if strings.Contains(string(raw), "private") {
		t.Fatalf("projected resource leaked site cookie: %s", raw)
	}
	if projected.SourceType != model.SubscriptionSourceMoviePilot || projected.BridgeInstanceID != "mp-main" {
		t.Fatalf("projected resource = %#v", projected)
	}
}

func TestValidateMoviePilotRetentionPolicyRejectsNegativeThresholds(t *testing.T) {
	for _, policy := range []model.TorrentRetentionPolicy{
		{MinSeedSeconds: -1},
		{MinRatio: -0.1},
	} {
		if err := validateMoviePilotRetentionPolicy(policy); err == nil {
			t.Fatalf("policy %#v was accepted", policy)
		}
	}
	if err := validateMoviePilotRetentionPolicy(model.TorrentRetentionPolicy{MinSeedSeconds: 3600, MinRatio: 1.5}); err != nil {
		t.Fatalf("valid retention policy rejected: %v", err)
	}
}

func TestSearchMoviePilotResourcesProjectsOpaqueRefAndMediaIdentity(t *testing.T) {
	client := &fakeMoviePilotBridgeClient{results: []moviepilotbridge.ResourceSearchResult{{
		ResourceRef: "resource-1", Title: "Show S01E01", Site: "tracker-a", Size: 1024, Seeders: 8,
		Leechers: 2, Grabs: 11, SeasonEpisode: "S01 E01-E24", EpisodeCount: 24,
		Promotion: "免费", HitAndRun: true, Labels: []string{"WEB-DL", "1080p"},
		SelectedFingerprint: "fingerprint-1",
	}}}
	SetMoviePilotBridgeClient(client)
	t.Cleanup(func() { SetMoviePilotBridgeClient(nil) })
	results, err := SearchMoviePilotResources(context.Background(), model.SubscriptionResourceSearchReq{
		Query: "Show", BridgeInstanceID: "mp-main", TMDBID: 123, MediaType: "tv", Season: 2, Episode: 3, Limit: 10,
	})
	if err != nil {
		t.Fatalf("search MoviePilot resources: %v", err)
	}
	if len(results) != 1 || results[0].ExternalRef != "resource-1" || results[0].BridgeInstanceID != "mp-main" {
		t.Fatalf("projected results = %#v", results)
	}
	if results[0].SeasonEpisode != "S01 E01-E24" || results[0].EpisodeCount != 24 || results[0].Promotion != "免费" || !results[0].HitAndRun {
		t.Fatalf("projected resource metadata = %#v", results[0])
	}
	if len(results[0].Labels) != 2 || results[0].Labels[0] != "WEB-DL" || results[0].Grabs != 11 {
		t.Fatalf("projected resource labels/heat = %#v", results[0])
	}
	if len(client.searches) != 1 || client.searches[0].MediaSource != "tmdb" || client.searches[0].MediaID != "123" || client.searches[0].Season != 2 || client.searches[0].Episode != 3 {
		t.Fatalf("search requests = %#v", client.searches)
	}
}

func TestRunMoviePilotAutomaticallyBindsFirstBridgeResult(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	client := &fakeMoviePilotBridgeClient{
		bridges: []string{"mp-main"},
		results: []moviepilotbridge.ResourceSearchResult{{
			ResourceRef: "resource-1", Title: "Auto Movie 1080p", Site: "tracker-a", Size: 1024,
			SelectedFingerprint: "fingerprint-1",
		}},
	}
	SetMoviePilotBridgeClient(client)
	t.Cleanup(func() { SetMoviePilotBridgeClient(nil) })
	sub := &model.Subscription{
		Name: "Auto Movie", TMDBName: "Auto Movie", TMDBID: 123, MediaType: "movie",
		SourceType: model.SubscriptionSourceMoviePilot,
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	items, _, _, _, _, err := runMoviePilot(context.Background(), sub, true)
	if err != nil {
		t.Fatalf("run MoviePilot subscription: %v", err)
	}
	if len(items) != 1 || len(client.intents) != 1 {
		t.Fatalf("automatic MoviePilot run items/intents = %d/%d", len(items), len(client.intents))
	}
	if sub.BoundTorrent == nil || sub.BoundTorrent.ResourceRef != "resource-1" {
		t.Fatalf("bound subscription = %#v", sub.BoundTorrent)
	}
	if client.intents[0].Torrent.ResourceRef != "resource-1" {
		t.Fatalf("submitted intent = %#v", client.intents[0])
	}
	if len(client.searches) != 1 || client.searches[0].MediaID != "123" || client.searches[0].MediaType != "movie" {
		t.Fatalf("automatic search requests = %#v", client.searches)
	}
	reloaded, err := db.GetSubscriptionByID(sub.ID)
	if err != nil {
		t.Fatalf("reload subscription: %v", err)
	}
	if reloaded.BoundTorrent == nil || reloaded.BoundTorrent.SelectedFingerprint != "fingerprint-1" {
		t.Fatalf("persisted binding = %#v", reloaded.BoundTorrent)
	}
}

func TestRunAutoPrefersMoviePilotBeforeFallbackSources(t *testing.T) {
	oldMoviePilot := runMoviePilotForAuto
	oldHDHive := runHDHiveForAuto
	t.Cleanup(func() {
		runMoviePilotForAuto = oldMoviePilot
		runHDHiveForAuto = oldHDHive
	})
	calledFallback := false
	runMoviePilotForAuto = func(_ context.Context, sub *model.Subscription, transfer bool) ([]model.SubscriptionItem, string, int, int, int, error) {
		if !transfer || sub.SourceType != model.SubscriptionSourceMoviePilot {
			t.Fatalf("MoviePilot auto request = source %q transfer %v", sub.SourceType, transfer)
		}
		return []model.SubscriptionItem{{SourcePath: "/staging/auto.mkv", Status: model.SubscriptionItemStatusPending}}, "moviepilot-hash", 1, 0, 0, nil
	}
	runHDHiveForAuto = func(context.Context, *model.Subscription, bool) ([]model.SubscriptionItem, string, int, int, int, error) {
		calledFallback = true
		return nil, "", 0, 0, 0, nil
	}

	sub := &model.Subscription{SourceType: model.SubscriptionSourceAuto}
	items, hash, added, _, _, err := runAuto(context.Background(), sub, true)
	if err != nil {
		t.Fatalf("run auto: %v", err)
	}
	if calledFallback || len(items) != 1 || hash != "moviepilot-hash" || added != 1 {
		t.Fatalf("auto result = items=%#v hash=%q added=%d fallback=%v", items, hash, added, calledFallback)
	}

	clusterSub := &model.Subscription{SourceType: model.SubscriptionSourceAuto}
	clusterItems, clusterHash, clusterAdded, _, _, err := runAutoCluster(context.Background(), clusterSub)
	if err != nil {
		t.Fatalf("run clustered auto: %v", err)
	}
	if calledFallback || len(clusterItems) != 1 || clusterHash != "moviepilot-hash" || clusterAdded != 1 {
		t.Fatalf("cluster auto result = items=%#v hash=%q added=%d fallback=%v", clusterItems, clusterHash, clusterAdded, calledFallback)
	}
}

func TestBindMoviePilotResourcePreservesBoundShareWhenUnbound(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{
		Name: "MoviePilot Show", TMDBID: 123, TMDBName: "Show", MediaType: "tv",
		BoundShare: &model.SubscriptionBoundShare{SourceType: model.SubscriptionSourceManual, ShareURL: "https://pan.example/share", BoundAt: time.Now().UTC()},
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	bound, err := BindMoviePilotResource(context.Background(), model.SubscriptionMoviePilotResourceBindReq{
		SubscriptionID: sub.ID, BridgeInstanceID: "mp-main", ResourceRef: "resource-1",
		SelectedFingerprint: "fingerprint-1", TorrentTitle: "Show.S01E01", MediaType: "tv",
		RetentionPolicy: model.TorrentRetentionPolicy{MinSeedSeconds: 3600},
	})
	if err != nil {
		t.Fatalf("bind MoviePilot resource: %v", err)
	}
	if bound.BoundTorrent == nil || bound.BoundTorrent.ResourceRef != "resource-1" || bound.BoundShare == nil {
		t.Fatalf("bound subscription = %#v", bound)
	}
	unbound, err := UnbindMoviePilotResource(context.Background(), model.SubscriptionMoviePilotResourceUnbindReq{SubscriptionID: sub.ID})
	if err != nil {
		t.Fatalf("unbind MoviePilot resource: %v", err)
	}
	if unbound.BoundTorrent != nil || unbound.BoundShare == nil {
		t.Fatalf("unbound subscription = %#v", unbound)
	}
	if unbound.SourceType != model.SubscriptionSourceManual {
		t.Fatalf("unbound source type = %q, want preserved share source", unbound.SourceType)
	}
}

func TestUnbindMoviePilotResourceFallsBackToAutoWithoutBoundShare(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{
		Name: "MoviePilot Only", TMDBID: 123, MediaType: "tv", SourceType: model.SubscriptionSourceMoviePilot,
		BoundTorrent: &model.SubscriptionBoundTorrent{BridgeInstanceID: "mp-main", ResourceRef: "resource-1"},
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	unbound, err := UnbindMoviePilotResource(context.Background(), model.SubscriptionMoviePilotResourceUnbindReq{SubscriptionID: sub.ID})
	if err != nil {
		t.Fatal(err)
	}
	if unbound.BoundTorrent != nil || unbound.SourceType != model.SubscriptionSourceAuto {
		t.Fatalf("unbound subscription = %#v", unbound)
	}
}

func TestSubmitMoviePilotIntentUsesStableIdempotencyKey(t *testing.T) {
	client := &fakeMoviePilotBridgeClient{}
	SetMoviePilotBridgeClient(client)
	t.Cleanup(func() { SetMoviePilotBridgeClient(nil) })
	sub := &model.Subscription{ID: 7, TMDBID: 123, MediaType: "tv", BoundTorrent: &model.SubscriptionBoundTorrent{
		BridgeInstanceID: "mp-main", ResourceRef: "resource-1", SelectedFingerprint: "fingerprint-1",
		MediaSource: "tmdb", MediaID: "123", MediaType: "tv", RetentionPolicy: model.TorrentRetentionPolicy{MinRatio: 1.5},
	}}
	if err := SubmitMoviePilotIntent(context.Background(), sub); err != nil {
		t.Fatalf("submit first intent: %v", err)
	}
	if err := SubmitMoviePilotIntent(context.Background(), sub); err != nil {
		t.Fatalf("submit second intent: %v", err)
	}
	if len(client.intents) != 2 || client.intents[0].RequestID == "" || client.intents[0].RequestID != client.intents[1].RequestID {
		t.Fatalf("intent requests = %#v", client.intents)
	}
}

func TestSubmitMoviePilotIntentUsesCoordinatorDownloaderPolicy(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	client := &fakeMoviePilotBridgeClient{}
	scheduler := &fakeMoviePilotDownloaderPolicyScheduler{policy: moviepilotbridge.DownloaderPolicy{
		Mode: moviepilotbridge.DownloaderPolicyCoordinatorSelect, Downloader: "qb-win",
		RouteID: "route-win", ReservationID: "reservation-win",
	}}
	SetMoviePilotBridgeClient(client)
	SetMoviePilotDownloaderPolicyScheduler(scheduler)
	t.Cleanup(func() {
		SetMoviePilotBridgeClient(nil)
		SetMoviePilotDownloaderPolicyScheduler(nil)
	})
	sub := &model.Subscription{ID: 41, Name: "Coordinator route", TMDBID: 456, MediaType: "movie", BoundTorrent: &model.SubscriptionBoundTorrent{
		BridgeInstanceID: "mp-main", ResourceRef: "resource-route", SelectedFingerprint: "fingerprint-route",
		MediaSource: "tmdb", MediaID: "456", MediaType: "movie", Size: 99,
	}}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	if err := SubmitMoviePilotIntent(context.Background(), sub); err != nil {
		t.Fatalf("submit intent: %v", err)
	}
	if len(scheduler.selectedCalls) != 1 || scheduler.selectedCalls[0].bridgeID != "mp-main" || scheduler.selectedCalls[0].expectedSize != 99 {
		t.Fatalf("scheduler calls = %#v", scheduler.selectedCalls)
	}
	if len(client.intents) != 1 || client.intents[0].DownloaderPolicy.Mode != scheduler.policy.Mode ||
		client.intents[0].DownloaderPolicy.Downloader != scheduler.policy.Downloader ||
		client.intents[0].DownloaderPolicy.RouteID != scheduler.policy.RouteID ||
		client.intents[0].DownloaderPolicy.ReservationID != scheduler.policy.ReservationID {
		t.Fatalf("submitted policy = %#v", client.intents)
	}
	if len(client.intentModels) != 1 || client.intentModels[0].DownloaderPolicyMode != moviepilotbridge.DownloaderPolicyCoordinatorSelect || client.intentModels[0].SelectedDownloader != "qb-win" || client.intentModels[0].SelectedRouteID != "route-win" || client.intentModels[0].ReservationID != "reservation-win" {
		t.Fatalf("submitted scheduling metadata = %#v", client.intentModels)
	}
}

func TestRunMoviePilotMarksCapacityAsRetryable(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	client := &fakeMoviePilotBridgeClient{}
	scheduler := &fakeMoviePilotDownloaderPolicyScheduler{
		policy:       moviepilotbridge.DownloaderPolicy{Mode: moviepilotbridge.DownloaderPolicyCoordinatorSelect},
		selectionErr: moviepilotbridge.ErrDownloaderCapacityUnavailable,
	}
	SetMoviePilotBridgeClient(client)
	SetMoviePilotDownloaderPolicyScheduler(scheduler)
	t.Cleanup(func() {
		SetMoviePilotBridgeClient(nil)
		SetMoviePilotDownloaderPolicyScheduler(nil)
	})
	sub := &model.Subscription{ID: 42, Name: "Capacity wait", TMDBID: 789, MediaType: "movie", SourceType: model.SubscriptionSourceMoviePilot, BoundTorrent: &model.SubscriptionBoundTorrent{
		BridgeInstanceID: "mp-main", ResourceRef: "resource-capacity", SelectedFingerprint: "fingerprint-capacity", MediaSource: "tmdb", MediaID: "789", MediaType: "movie", Size: 10,
	}}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, _, err := runMoviePilot(context.Background(), sub, true)
	if !errors.Is(err, moviepilotbridge.ErrDownloaderCapacityUnavailable) {
		t.Fatalf("run error = %v, want retryable capacity error", err)
	}
	items, err := db.ListSubscriptionItems(sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Status != model.SubscriptionItemStatusRetryWait || items[0].LastErrorCode != "downloader_capacity_unavailable" || items[0].RetryAt == nil {
		t.Fatalf("capacity item = %#v", items)
	}
	var intent model.MoviePilotDownloadIntent
	if err := db.GetDb().Where("request_id = ?", moviePilotIntentRequestID(sub.ID, sub.BoundTorrent.ResourceRef, sub.BoundTorrent.SelectedFingerprint)).First(&intent).Error; err != nil {
		t.Fatal(err)
	}
	if intent.Status != model.MoviePilotIntentStatusWaitingCapacity || intent.DownloaderPolicyMode != moviepilotbridge.DownloaderPolicyCoordinatorSelect {
		t.Fatalf("waiting intent = %#v", intent)
	}
}

func TestRunClusterKeepsSubscriptionRunningOnCapacityBackpressure(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	client := &fakeMoviePilotBridgeClient{}
	scheduler := &fakeMoviePilotDownloaderPolicyScheduler{selectionErr: moviepilotbridge.ErrDownloaderCapacityUnavailable}
	SetMoviePilotBridgeClient(client)
	SetMoviePilotDownloaderPolicyScheduler(scheduler)
	t.Cleanup(func() {
		SetMoviePilotBridgeClient(nil)
		SetMoviePilotDownloaderPolicyScheduler(nil)
	})
	sub := &model.Subscription{ID: 43, Name: "Cluster capacity wait", TMDBID: 790, MediaType: "movie", SourceType: model.SubscriptionSourceMoviePilot, TransferEnabled: true, BoundTorrent: &model.SubscriptionBoundTorrent{
		BridgeInstanceID: "mp-main", ResourceRef: "resource-cluster-capacity", SelectedFingerprint: "fingerprint-cluster-capacity", MediaSource: "tmdb", MediaID: "790", MediaType: "movie", Size: 10,
	}}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	result, err := RunCluster(context.Background(), sub.ID)
	if !errors.Is(err, moviepilotbridge.ErrDownloaderCapacityUnavailable) {
		t.Fatalf("cluster run error = %v, want capacity backpressure", err)
	}
	if result == nil || result.Run == nil || result.Run.Status != model.SubscriptionStatusRunning || result.Subscription.LastStatus != model.SubscriptionStatusRunning {
		t.Fatalf("cluster capacity result = %#v", result)
	}
	var stored model.Subscription
	if err := db.GetDb().First(&stored, sub.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.LastStatus != model.SubscriptionStatusRunning {
		t.Fatalf("stored subscription status = %q", stored.LastStatus)
	}
}

func TestUpdateMoviePilotRetentionPropagatesToActiveBinding(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{Name: "Retention Show", SourceType: model.SubscriptionSourceMoviePilot, MediaType: "tv", BoundTorrent: &model.SubscriptionBoundTorrent{
		BridgeInstanceID: "mp-main", ResourceRef: "resource-1", RetentionPolicy: model.TorrentRetentionPolicy{MinSeedSeconds: 60}, BoundAt: time.Now().UTC(),
	}}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	intent := &model.MoviePilotDownloadIntent{ID: "intent-retention", RequestID: "request-retention", BridgeInstanceID: "mp-main", SubscriptionID: sub.ID, Status: model.MoviePilotIntentStatusBound, RetentionPolicyJSON: `{"min_seed_seconds":60}`}
	binding := &model.MoviePilotTorrentBinding{ID: "binding-retention", IntentID: intent.ID, BridgeInstanceID: "mp-main", DownloaderAlias: "qb-a", WorkerNodeID: "worker-1", QBClientID: "qb-a", TorrentHash: strings.Repeat("a", 40), Status: model.MoviePilotTorrentStatusSeeding, RetentionStatus: model.MoviePilotRetentionStatusEligible, RetentionPolicyJSON: intent.RetentionPolicyJSON}
	if err := db.GetDb().Create(intent).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.GetDb().Create(binding).Error; err != nil {
		t.Fatal(err)
	}
	updated, err := UpdateMoviePilotRetention(context.Background(), model.SubscriptionMoviePilotRetentionUpdateReq{SubscriptionID: sub.ID, RetentionPolicy: model.TorrentRetentionPolicy{Permanent: true}})
	if err != nil {
		t.Fatal(err)
	}
	if updated.BoundTorrent == nil || !updated.BoundTorrent.RetentionPolicy.Permanent {
		t.Fatalf("updated subscription = %#v", updated.BoundTorrent)
	}
	var gotIntent model.MoviePilotDownloadIntent
	var gotBinding model.MoviePilotTorrentBinding
	if err := db.GetDb().First(&gotIntent, "id = ?", intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.GetDb().First(&gotBinding, "id = ?", binding.ID).Error; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gotIntent.RetentionPolicyJSON, "permanent") || gotBinding.RetentionStatus != model.MoviePilotRetentionStatusHeld || !strings.Contains(gotBinding.RetentionPolicyJSON, "permanent") {
		t.Fatalf("retention snapshots = %#v %#v", gotIntent, gotBinding)
	}
}

func TestUpdateMoviePilotRetentionMarksManualExtensionHeld(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{Name: "Manual Hold Show", SourceType: model.SubscriptionSourceMoviePilot, MediaType: "tv", BoundTorrent: &model.SubscriptionBoundTorrent{
		BridgeInstanceID: "mp-main", ResourceRef: "resource-hold", RetentionPolicy: model.TorrentRetentionPolicy{MinSeedSeconds: 60}, BoundAt: time.Now().UTC(),
	}}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	intent := &model.MoviePilotDownloadIntent{ID: "intent-hold", RequestID: "request-hold", BridgeInstanceID: "mp-main", SubscriptionID: sub.ID, Status: model.MoviePilotIntentStatusBound, RetentionPolicyJSON: `{"min_seed_seconds":60}`}
	eligibleAt := time.Now().UTC().Add(-time.Minute)
	binding := &model.MoviePilotTorrentBinding{
		ID: "binding-hold", IntentID: intent.ID, BridgeInstanceID: "mp-main", DownloaderAlias: "qb-a", WorkerNodeID: "worker-1", QBClientID: "qb-a",
		TorrentHash: strings.Repeat("b", 40), Status: model.MoviePilotTorrentStatusSeeding, RetentionStatus: model.MoviePilotRetentionStatusEligible,
		RetentionEligibleAt: &eligibleAt, RetentionPolicyJSON: intent.RetentionPolicyJSON,
	}
	if err := db.GetDb().Create(intent).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.GetDb().Create(binding).Error; err != nil {
		t.Fatal(err)
	}
	holdUntil := time.Now().UTC().Add(24 * time.Hour)
	if _, err := UpdateMoviePilotRetention(context.Background(), model.SubscriptionMoviePilotRetentionUpdateReq{
		SubscriptionID: sub.ID, RetentionPolicy: model.TorrentRetentionPolicy{MinSeedSeconds: 60, ManualHoldUntil: &holdUntil},
	}); err != nil {
		t.Fatal(err)
	}
	var updatedBinding model.MoviePilotTorrentBinding
	if err := db.GetDb().First(&updatedBinding, "id = ?", binding.ID).Error; err != nil {
		t.Fatal(err)
	}
	if updatedBinding.RetentionStatus != model.MoviePilotRetentionStatusHeld || updatedBinding.RetentionEligibleAt != nil {
		t.Fatalf("manual extension binding = %#v", updatedBinding)
	}
}

func TestUpdateMoviePilotRetentionRejectsAlreadyDeletingBinding(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	sub := &model.Subscription{Name: "Deleting Show", SourceType: model.SubscriptionSourceMoviePilot, MediaType: "tv", BoundTorrent: &model.SubscriptionBoundTorrent{
		BridgeInstanceID: "mp-main", ResourceRef: "resource-deleting", RetentionPolicy: model.TorrentRetentionPolicy{MinSeedSeconds: 60}, BoundAt: time.Now().UTC(),
	}}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	intent := &model.MoviePilotDownloadIntent{ID: "intent-deleting", RequestID: "request-deleting", BridgeInstanceID: "mp-main", SubscriptionID: sub.ID, Status: model.MoviePilotIntentStatusBound, RetentionPolicyJSON: `{"min_seed_seconds":60}`}
	binding := &model.MoviePilotTorrentBinding{
		ID: "binding-deleting", IntentID: intent.ID, BridgeInstanceID: "mp-main", DownloaderAlias: "qb-a", WorkerNodeID: "worker-1", QBClientID: "qb-a",
		TorrentHash: strings.Repeat("c", 40), Status: model.MoviePilotTorrentStatusDeleting, RetentionStatus: model.MoviePilotRetentionStatusDeleting,
		RetentionPolicyJSON: intent.RetentionPolicyJSON,
	}
	if err := db.GetDb().Create(intent).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.GetDb().Create(binding).Error; err != nil {
		t.Fatal(err)
	}
	_, err := UpdateMoviePilotRetention(context.Background(), model.SubscriptionMoviePilotRetentionUpdateReq{
		SubscriptionID: sub.ID, RetentionPolicy: model.TorrentRetentionPolicy{Permanent: true},
	})
	if err == nil || !strings.Contains(err.Error(), "already deleting") {
		t.Fatalf("deleting retention update error = %v", err)
	}
	reloaded, err := db.GetSubscriptionByID(sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.BoundTorrent == nil || reloaded.BoundTorrent.RetentionPolicy.Permanent {
		t.Fatalf("failed update changed subscription policy = %#v", reloaded.BoundTorrent)
	}
}

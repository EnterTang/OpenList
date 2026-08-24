package subscription

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/moviepilotbridge"
)

type fakeMoviePilotBridgeClient struct {
	searches []moviepilotbridge.ResourceSearchRequest
	intents  []moviepilotbridge.DownloadIntentRequest
	results  []moviepilotbridge.ResourceSearchResult
}

func (f *fakeMoviePilotBridgeClient) SearchResources(_ context.Context, _ string, request moviepilotbridge.ResourceSearchRequest) ([]moviepilotbridge.ResourceSearchResult, error) {
	f.searches = append(f.searches, request)
	return append([]moviepilotbridge.ResourceSearchResult(nil), f.results...), nil
}

func (f *fakeMoviePilotBridgeClient) SubmitIntent(_ context.Context, _ string, _ *model.MoviePilotDownloadIntent, request moviepilotbridge.DownloadIntentRequest) error {
	f.intents = append(f.intents, request)
	return nil
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
		SelectedFingerprint: "fingerprint-1",
	}}}
	SetMoviePilotBridgeClient(client)
	t.Cleanup(func() { SetMoviePilotBridgeClient(nil) })
	results, err := SearchMoviePilotResources(context.Background(), model.SubscriptionResourceSearchReq{
		Query: "Show", BridgeInstanceID: "mp-main", TMDBID: 123, MediaType: "tv", Limit: 10,
	})
	if err != nil {
		t.Fatalf("search MoviePilot resources: %v", err)
	}
	if len(results) != 1 || results[0].ExternalRef != "resource-1" || results[0].BridgeInstanceID != "mp-main" {
		t.Fatalf("projected results = %#v", results)
	}
	if len(client.searches) != 1 || client.searches[0].MediaSource != "tmdb" || client.searches[0].MediaID != "123" {
		t.Fatalf("search requests = %#v", client.searches)
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

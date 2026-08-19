package subscription

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/hdhive"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestHDHiveRegularSourcesKeepHDHiveResolutionEnabled(t *testing.T) {
	var received model.SubscriptionTelegramSourceConfig
	oldTelegram := runTelegramForHDHiveSubscription
	runTelegramForHDHiveSubscription = func(_ context.Context, sub *model.Subscription, _ bool) ([]model.SubscriptionItem, string, int, int, int, error) {
		if err := json.Unmarshal([]byte(sub.SourceConfig), &received); err != nil {
			t.Fatalf("decode Telegram fallback config: %v", err)
		}
		return nil, "", 0, 0, 0, nil
	}
	t.Cleanup(func() { runTelegramForHDHiveSubscription = oldTelegram })

	var saved []model.SubscriptionItem
	var hashes []string
	var firstErr error
	_, _ = runHDHiveRegularSources(
		context.Background(),
		&model.Subscription{},
		model.SubscriptionConfig{
			Telegram: model.SubscriptionTelegramSourceConfig{
				SearchCommand: []string{"telegram-search"},
				HDHive:        model.SubscriptionTelegramHDHiveConfig{Enabled: true},
			},
		},
		false,
		&saved,
		&hashes,
		new(int),
		new(int),
		new(int),
		&firstErr,
	)
	if !received.HDHive.Enabled {
		t.Fatal("Telegram fallback disabled HDHive link resolution")
	}
}

func TestHDHiveSubscriptionSkipsPaidUnlockWhenRegularSourceHasCandidate(t *testing.T) {
	var unlockCalls int
	var sourceCalls int
	client := &hdhiveSubscriptionFakeClient{
		resources: []hdhive.Resource{{
			Slug:         "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			PanType:      "115",
			UnlockPoints: intPointer(10),
		}},
		details: hdhive.ResourceDetails{UnlockPoints: intPointer(10)},
	}
	cleanup := installHDHiveRunFakes(t, client)
	defer cleanup()
	runTelegramForHDHiveSubscription = func(context.Context, *model.Subscription, bool) ([]model.SubscriptionItem, string, int, int, int, error) {
		sourceCalls++
		return []model.SubscriptionItem{{SourcePath: "/temp/candidate.mkv", Status: model.SubscriptionItemStatusPending}}, "telegram-hash", 1, 0, 0, nil
	}
	runPanSouForHDHiveSubscription = func(context.Context, *model.Subscription, bool) ([]model.SubscriptionItem, string, int, int, int, error) {
		sourceCalls++
		return nil, "pansou-hash", 0, 0, 0, nil
	}
	unlockHDHiveResourceForSubscription = func(context.Context, string, model.SubscriptionTelegramHDHiveConfig) (model.SubscriptionResourceUnlockResp, error) {
		unlockCalls++
		return model.SubscriptionResourceUnlockResp{}, nil
	}

	sub := &model.Subscription{ID: 1, Name: "Example", TMDBName: "Example", TMDBID: 1399, MediaType: "tv", SourceType: model.SubscriptionSourceHDHive}
	_, _, _, _, _, err := runHDHiveFederated(context.Background(), sub, model.SubscriptionHDHiveSourceConfig{CloudType: "all", Limit: 10}, model.SubscriptionConfig{
		Telegram: model.SubscriptionTelegramSourceConfig{SearchCommand: []string{"telegram"}},
		PanSou:   model.SubscriptionPanSouSourceConfig{BaseURL: "https://pansou.example"},
	}, true)
	if err != nil {
		t.Fatalf("run HDHive subscription: %v", err)
	}
	if sourceCalls != 2 {
		t.Fatalf("regular source calls = %d, want Telegram and PanSou", sourceCalls)
	}
	if unlockCalls != 0 {
		t.Fatalf("paid HDHive unlock calls = %d, want 0", unlockCalls)
	}
}

func TestHDHiveSubscriptionUnlocksPaidResourceOnlyWhenRegularSourcesHaveNoCandidate(t *testing.T) {
	var unlockCalls int
	client := &hdhiveSubscriptionFakeClient{
		resources: []hdhive.Resource{{
			Slug:         "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			PanType:      "115",
			UnlockPoints: intPointer(3),
		}},
		details: hdhive.ResourceDetails{UnlockPoints: intPointer(3)},
	}
	cleanup := installHDHiveRunFakes(t, client)
	defer cleanup()
	runTelegramForHDHiveSubscription = func(context.Context, *model.Subscription, bool) ([]model.SubscriptionItem, string, int, int, int, error) {
		return nil, "telegram-hash", 0, 0, 0, nil
	}
	runPanSouForHDHiveSubscription = func(context.Context, *model.Subscription, bool) ([]model.SubscriptionItem, string, int, int, int, error) {
		return nil, "pansou-hash", 0, 0, 0, nil
	}
	unlockHDHiveResourceForSubscription = func(context.Context, string, model.SubscriptionTelegramHDHiveConfig) (model.SubscriptionResourceUnlockResp, error) {
		unlockCalls++
		return model.SubscriptionResourceUnlockResp{URL: "https://115.com/s/unlocked", AccessCode: "abcd"}, nil
	}

	sub := &model.Subscription{ID: 2, Name: "Example", TMDBName: "Example", TMDBID: 1399, MediaType: "tv", SourceType: model.SubscriptionSourceHDHive}
	_, _, _, _, _, err := runHDHiveFederated(context.Background(), sub, model.SubscriptionHDHiveSourceConfig{CloudType: "all", Limit: 10}, model.SubscriptionConfig{}, true)
	if err != nil {
		t.Fatalf("run HDHive subscription: %v", err)
	}
	if unlockCalls != 1 {
		t.Fatalf("paid HDHive unlock calls = %d, want 1", unlockCalls)
	}
	if sub.BoundShare == nil || sub.BoundShare.ShareURL != "https://115.com/s/unlocked,abcd" {
		t.Fatalf("automatic bound share = %#v", sub.BoundShare)
	}
	if _, _, _, _, _, err := runHDHiveFederated(context.Background(), sub, model.SubscriptionHDHiveSourceConfig{CloudType: "all", Limit: 10}, model.SubscriptionConfig{}, true); err != nil {
		t.Fatalf("rerun HDHive subscription: %v", err)
	}
	if unlockCalls != 1 {
		t.Fatalf("paid HDHive unlock calls after bound rerun = %d, want 1", unlockCalls)
	}
}

func TestHDHiveSubscriptionProcessesFreeResourceEvenWhenRegularSourceHasCandidate(t *testing.T) {
	var unlockCalls int
	client := &hdhiveSubscriptionFakeClient{
		resources: []hdhive.Resource{{
			Slug:    "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			PanType: "115",
		}},
		details: hdhive.ResourceDetails{UnlockPoints: intPointer(0)},
	}
	cleanup := installHDHiveRunFakes(t, client)
	defer cleanup()
	runTelegramForHDHiveSubscription = func(context.Context, *model.Subscription, bool) ([]model.SubscriptionItem, string, int, int, int, error) {
		return []model.SubscriptionItem{{SourcePath: "/temp/candidate.mkv", Status: model.SubscriptionItemStatusPending}}, "telegram-hash", 1, 0, 0, nil
	}
	runPanSouForHDHiveSubscription = func(context.Context, *model.Subscription, bool) ([]model.SubscriptionItem, string, int, int, int, error) {
		return nil, "pansou-hash", 0, 0, 0, nil
	}
	unlockHDHiveResourceForSubscription = func(context.Context, string, model.SubscriptionTelegramHDHiveConfig) (model.SubscriptionResourceUnlockResp, error) {
		unlockCalls++
		return model.SubscriptionResourceUnlockResp{URL: "https://115.com/s/free"}, nil
	}

	sub := &model.Subscription{ID: 5, Name: "Example", TMDBName: "Example", TMDBID: 1399, MediaType: "tv", SourceType: model.SubscriptionSourceHDHive}
	_, _, _, _, _, err := runHDHiveFederated(context.Background(), sub, model.SubscriptionHDHiveSourceConfig{CloudType: "all", Limit: 10}, model.SubscriptionConfig{
		Telegram: model.SubscriptionTelegramSourceConfig{SearchCommand: []string{"telegram"}},
	}, true)
	if err != nil {
		t.Fatalf("run HDHive subscription: %v", err)
	}
	if unlockCalls != 1 {
		t.Fatalf("free HDHive unlock calls = %d, want 1", unlockCalls)
	}
	if sub.BoundShare == nil || sub.BoundShare.ShareURL != "https://115.com/s/free" {
		t.Fatalf("free bound share = %#v", sub.BoundShare)
	}
}

func TestHDHiveRateLimitDoesNotFailFederatedRunWhenRegularSourceHasCandidate(t *testing.T) {
	client := &hdhiveSubscriptionFakeClient{
		searchErr: &hdhive.Error{Code: "HDHIVE_SYMEDIA_RATE_LIMITED", Message: "rate limit exceeded"},
	}
	cleanup := installHDHiveRunFakes(t, client)
	defer cleanup()
	runTelegramForHDHiveSubscription = func(context.Context, *model.Subscription, bool) ([]model.SubscriptionItem, string, int, int, int, error) {
		return []model.SubscriptionItem{{SourcePath: "/temp/pan123-e28.mkv", Status: model.SubscriptionItemStatusPending}}, "telegram-hash", 1, 0, 0, nil
	}

	sub := &model.Subscription{ID: 7, Name: "Example", TMDBName: "Example", TMDBID: 1399, MediaType: "tv", SourceType: model.SubscriptionSourceAuto}
	_, _, _, _, _, err := runHDHiveFederated(context.Background(), sub, model.SubscriptionHDHiveSourceConfig{CloudType: "all", Limit: 10}, model.SubscriptionConfig{
		Telegram: model.SubscriptionTelegramSourceConfig{SearchCommand: []string{"telegram"}},
	}, true)
	if err != nil {
		t.Fatalf("run HDHive subscription: %v, want HDHive rate limit to be non-fatal", err)
	}
}

func TestIsolateHDHiveRateLimitErrorRemovesOnlyRateLimitFailure(t *testing.T) {
	rateLimit := &hdhive.Error{Code: "HDHIVE_SYMEDIA_RATE_LIMITED", Message: "rate limit exceeded"}
	if got := isolateHDHiveRateLimitError(rateLimit); got != nil {
		t.Fatalf("isolated rate-limit error = %v, want nil", got)
	}
	nonRateLimit := errors.New("telegram authentication failed")
	if got := isolateHDHiveRateLimitError(nonRateLimit); got != nonRateLimit {
		t.Fatalf("non-rate-limit error = %v, want original error", got)
	}
}

func TestHDHiveSubscriptionProcessesFreeHDHiveBeforeRegularSources(t *testing.T) {
	events := make([]string, 0, 4)
	client := &hdhiveSubscriptionFakeClient{
		resources: []hdhive.Resource{{
			Slug:    "ffffffffffffffffffffffffffffffff",
			PanType: "115",
		}},
		details: hdhive.ResourceDetails{UnlockPoints: intPointer(0)},
		events:  &events,
	}
	cleanup := installHDHiveRunFakes(t, client)
	defer cleanup()
	oldInspect := inspectShareLinkCandidatesFn
	inspectShareLinkCandidatesFn = func(ctx context.Context, sub *model.Subscription, cfg model.SubscriptionTelegramSourceConfig, rawShare string, now time.Time) (telegramPanSubscriptionSource, []shareTransferCandidate, bool, error) {
		if strings.Contains(rawShare, "/bound") {
			events = append(events, "bound")
		}
		return oldInspect(ctx, sub, cfg, rawShare, now)
	}
	runTelegramForHDHiveSubscription = func(context.Context, *model.Subscription, bool) ([]model.SubscriptionItem, string, int, int, int, error) {
		events = append(events, "telegram")
		return nil, "telegram-hash", 0, 0, 0, nil
	}
	runPanSouForHDHiveSubscription = func(context.Context, *model.Subscription, bool) ([]model.SubscriptionItem, string, int, int, int, error) {
		events = append(events, "pansou")
		return nil, "pansou-hash", 0, 0, 0, nil
	}
	unlockHDHiveResourceForSubscription = func(context.Context, string, model.SubscriptionTelegramHDHiveConfig) (model.SubscriptionResourceUnlockResp, error) {
		events = append(events, "hdhive-free")
		return model.SubscriptionResourceUnlockResp{URL: "https://115.com/s/free"}, nil
	}

	sub := &model.Subscription{ID: 6, Name: "Example", TMDBName: "Example", TMDBID: 1399, MediaType: "tv", SourceType: model.SubscriptionSourceHDHive, BoundShare: &model.SubscriptionBoundShare{ShareURL: "https://115.com/s/bound"}}
	_, _, _, _, _, err := runHDHiveFederated(context.Background(), sub, model.SubscriptionHDHiveSourceConfig{CloudType: "all", Limit: 10}, model.SubscriptionConfig{
		Telegram: model.SubscriptionTelegramSourceConfig{SearchCommand: []string{"telegram"}},
		PanSou:   model.SubscriptionPanSouSourceConfig{BaseURL: "https://pansou.example"},
	}, true)
	if err != nil {
		t.Fatalf("run HDHive subscription: %v", err)
	}
	if got, want := strings.Join(events, ","), "bound,hdhive-share,hdhive-free,telegram,pansou"; got != want {
		t.Fatalf("execution order = %q, want %q", got, want)
	}
}

func TestHDHiveSubscriptionNeverTreatsUnknownPointsAsFree(t *testing.T) {
	var unlockCalls int
	client := &hdhiveSubscriptionFakeClient{
		resources: []hdhive.Resource{{
			Slug:    "cccccccccccccccccccccccccccccccc",
			PanType: "115",
		}},
		details: hdhive.ResourceDetails{},
	}
	cleanup := installHDHiveRunFakes(t, client)
	defer cleanup()
	runTelegramForHDHiveSubscription = func(context.Context, *model.Subscription, bool) ([]model.SubscriptionItem, string, int, int, int, error) {
		return nil, "telegram-hash", 0, 0, 0, nil
	}
	runPanSouForHDHiveSubscription = func(context.Context, *model.Subscription, bool) ([]model.SubscriptionItem, string, int, int, int, error) {
		return nil, "pansou-hash", 0, 0, 0, nil
	}
	unlockHDHiveResourceForSubscription = func(context.Context, string, model.SubscriptionTelegramHDHiveConfig) (model.SubscriptionResourceUnlockResp, error) {
		unlockCalls++
		return model.SubscriptionResourceUnlockResp{URL: "https://115.com/s/unsafe"}, nil
	}

	sub := &model.Subscription{ID: 3, Name: "Example", TMDBName: "Example", TMDBID: 1399, MediaType: "tv", SourceType: model.SubscriptionSourceHDHive}
	_, _, _, _, _, err := runHDHiveFederated(context.Background(), sub, model.SubscriptionHDHiveSourceConfig{CloudType: "all", Limit: 10}, model.SubscriptionConfig{}, true)
	if err != nil {
		t.Fatalf("run HDHive subscription: %v", err)
	}
	if unlockCalls != 0 {
		t.Fatalf("unknown-point unlock calls = %d, want 0", unlockCalls)
	}
}

func TestHDHiveClusterDispatchesFreeShare(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &hdhiveRunTestDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })
	client := &hdhiveSubscriptionFakeClient{
		resources: []hdhive.Resource{{
			Slug:         "dddddddddddddddddddddddddddddddd",
			PanType:      "115",
			UnlockPoints: intPointer(0),
		}},
		details: hdhive.ResourceDetails{FullURL: "https://115.com/s/free"},
	}
	cleanup := installHDHiveRunFakes(t, client)
	defer cleanup()

	sub := &model.Subscription{ID: 4, Name: "Cluster HDHive", TMDBName: "Cluster HDHive", TMDBID: 1399, MediaType: "tv", SourceType: model.SubscriptionSourceHDHive}
	_, _, _, _, dispatched, err := runHDHiveCluster(context.Background(), sub)
	if err != nil {
		t.Fatalf("run HDHive cluster source: %v", err)
	}
	if dispatched != 1 || len(dispatcher.inspectTasks) != 1 {
		t.Fatalf("dispatched = %d tasks=%#v, want one inspect task", dispatched, dispatcher.inspectTasks)
	}
	if sub.BoundShare == nil || sub.BoundShare.ShareURL != "https://115.com/s/free" {
		t.Fatalf("cluster bound share = %#v", sub.BoundShare)
	}
}

type hdhiveSubscriptionFakeClient struct {
	resources []hdhive.Resource
	details   hdhive.ResourceDetails
	events    *[]string
	searchErr error
}

type hdhiveRunTestDispatcher struct {
	inspectTasks []ClusterInspectTask
}

func (d *hdhiveRunTestDispatcher) DispatchSubscriptionInspect(_ context.Context, task ClusterInspectTask) (string, error) {
	d.inspectTasks = append(d.inspectTasks, task)
	return "inspect-job", nil
}

func (d *hdhiveRunTestDispatcher) DispatchSubscriptionMedia(context.Context, []ClusterMediaTask) ([]ClusterDispatchResult, error) {
	return nil, nil
}

func (f *hdhiveSubscriptionFakeClient) Search(context.Context, string, int64) ([]hdhive.Resource, error) {
	if f.searchErr != nil {
		return nil, f.searchErr
	}
	return f.resources, nil
}

func (f *hdhiveSubscriptionFakeClient) Share(context.Context, string) (hdhive.ResourceDetails, error) {
	if f.events != nil {
		*f.events = append(*f.events, "hdhive-share")
	}
	return f.details, nil
}

func (f *hdhiveSubscriptionFakeClient) Unlock(context.Context, string) (hdhive.UnlockResult, error) {
	return hdhive.UnlockResult{}, nil
}

func intPointer(value int) *int {
	return &value
}

func installHDHiveRunFakes(t *testing.T, client hdhiveSubscriptionClient) func() {
	t.Helper()
	oldClient := newHDHiveSubscriptionClient
	oldInspect := inspectShareLinkCandidatesFn
	oldTransfer := transferSelectedShareCandidatesForSubscription
	newHDHiveSubscriptionClient = func(model.SubscriptionTelegramHDHiveConfig) (hdhiveSubscriptionClient, error) {
		return client, nil
	}
	inspectShareLinkCandidatesFn = func(context.Context, *model.Subscription, model.SubscriptionTelegramSourceConfig, string, time.Time) (telegramPanSubscriptionSource, []shareTransferCandidate, bool, error) {
		return telegramPanSubscriptionSource{}, []shareTransferCandidate{{Item: &model.SubscriptionItem{SourcePath: "/temp/selected.mkv", Status: model.SubscriptionItemStatusPending}}}, true, nil
	}
	transferSelectedShareCandidatesForSubscription = func(context.Context, *model.Subscription, []shareTransferCandidate, bool, time.Time, string) ([]model.SubscriptionItem, string, int, int, int, error) {
		return []model.SubscriptionItem{{SourcePath: "/temp/selected.mkv", Status: model.SubscriptionItemStatusPending}}, "transfer-hash", 1, 0, 0, nil
	}
	return func() {
		newHDHiveSubscriptionClient = oldClient
		inspectShareLinkCandidatesFn = oldInspect
		transferSelectedShareCandidatesForSubscription = oldTransfer
	}
}

package subscription

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestProjectExternalSubscriptionFallsBackWhenProgressDetailsFail(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	request := &model.ExternalSubscriptionRequest{
		IdempotencyKey:     t.Name(),
		LookupKey:          "movie:" + t.Name(),
		RequestFingerprint: "fingerprint:" + t.Name(),
		RequestJSON:        "{}",
		LastStatus:         "running",
		LastMessage:        "processing",
		ProgressJSON:       "{}",
		SeasonsJSON:        "[]",
	}
	subscription := &model.Subscription{
		Name:       t.Name(),
		MediaType:  "movie",
		TMDBID:     1399,
		LastStatus: model.SubscriptionStatusRunning,
	}
	if err := db.CreateExternalSubscriptionRequest(context.Background(), request, subscription); err != nil {
		t.Fatalf("create external subscription request: %v", err)
	}
	item := model.SubscriptionItem{
		SubscriptionID: subscription.ID,
		SourceKey:      "pending-item",
		Status:         model.SubscriptionItemStatusPending,
	}
	if _, _, err := db.UpsertSubscriptionItem(&item); err != nil {
		t.Fatalf("upsert subscription item: %v", err)
	}

	originalLoader := listSubscriptionEpisodeSourceDetails
	listSubscriptionEpisodeSourceDetails = func(uint) ([]model.SubscriptionEpisodeSourceDetail, error) {
		return nil, errors.New("progress details unavailable")
	}
	t.Cleanup(func() { listSubscriptionEpisodeSourceDetails = originalLoader })

	response, err := ProjectExternalSubscription(context.Background(), request.ID)
	if err != nil {
		t.Fatalf("project external subscription: %v", err)
	}
	if response.ProgressStatus != model.SubscriptionProgressStatusSearching {
		t.Fatalf("progress status = %q, want %q", response.ProgressStatus, model.SubscriptionProgressStatusSearching)
	}
}

func TestNormalizeExternalSubscriptionAcceptsHDHiveSource(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	_, sub, _, _, _, err := normalizeExternalSubscriptionCreateRequest(ExternalSubscriptionCreateRequest{
		Name:       "HDHive source",
		MediaType:  "tv",
		TMDBID:     1399,
		SourceType: model.SubscriptionSourceHDHive,
	})
	if err != nil {
		t.Fatalf("normalize HDHive subscription: %v", err)
	}
	if sub == nil || sub.SourceType != model.SubscriptionSourceHDHive {
		t.Fatalf("subscription = %#v", sub)
	}
	var source model.SubscriptionHDHiveSourceConfig
	if err := json.Unmarshal([]byte(sub.SourceConfig), &source); err != nil {
		t.Fatalf("decode source config: %v", err)
	}
	if source.CloudType != "all" || source.Limit <= 0 {
		t.Fatalf("source config = %#v", source)
	}
}

func TestNormalizeExternalSubscriptionKeepsSeasonEpisodeCounts(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	_, sub, _, _, _, err := normalizeExternalSubscriptionCreateRequest(ExternalSubscriptionCreateRequest{
		Name:                "Season counts",
		MediaType:           "tv",
		TMDBID:              1399,
		SeasonsSelected:     []int{1, 2},
		SeasonEpisodeCounts: map[int]int{1: 10, 2: 8, 3: 99, 0: 5},
	})
	if err != nil {
		t.Fatalf("normalize season counts: %v", err)
	}
	if got := sub.SeasonEpisodeCounts; !reflect.DeepEqual(got, map[int]int{1: 10, 2: 8}) {
		t.Fatalf("season episode counts = %#v, want %#v", got, map[int]int{1: 10, 2: 8})
	}
}

func TestProjectExternalSubscriptionStatus(t *testing.T) {
	tests := []struct {
		name               string
		items              []model.SubscriptionItem
		subscriptionStatus string
		subscriptionError  string
		requestStatus      string
		requestMessage     string
		requestError       string
		wantStatus         string
		wantMessage        string
		wantCompleted      bool
	}{
		{
			name: "failed and transferring items remain running while delivery is active",
			items: []model.SubscriptionItem{
				{SourceKey: "transferring-item", Status: model.SubscriptionItemStatusTransferring},
				{SourceKey: "failed-item", Status: model.SubscriptionItemStatusFailed, LastError: "upload failed"},
			},
			wantStatus:    "running",
			wantMessage:   model.SubscriptionItemStatusTransferring,
			wantCompleted: false,
		},
		{
			name: "failed and pending items remain running while delivery is active",
			items: []model.SubscriptionItem{
				{SourceKey: "pending-item", Status: model.SubscriptionItemStatusPending},
				{SourceKey: "failed-item", Status: model.SubscriptionItemStatusFailed, LastError: "upload failed"},
			},
			wantStatus:    "running",
			wantMessage:   model.SubscriptionItemStatusPending,
			wantCompleted: false,
		},
		{
			name: "failed and notifying items remain running while delivery is active",
			items: []model.SubscriptionItem{
				{SourceKey: "notifying-item", Status: model.SubscriptionItemStatusNotifying},
				{SourceKey: "failed-item", Status: model.SubscriptionItemStatusFailed, LastError: "upload failed"},
			},
			wantStatus:    "running",
			wantMessage:   model.SubscriptionItemStatusNotifying,
			wantCompleted: false,
		},
		{
			name: "active transfer keeps external status running",
			items: []model.SubscriptionItem{
				{SourceKey: "transferring-item", Status: model.SubscriptionItemStatusTransferring},
			},
			wantStatus:    "running",
			wantMessage:   model.SubscriptionItemStatusTransferring,
			wantCompleted: false,
		},
		{
			name: "pending delivery keeps external status running",
			items: []model.SubscriptionItem{
				{SourceKey: "pending-item", Status: model.SubscriptionItemStatusPending},
			},
			wantStatus:    "running",
			wantMessage:   model.SubscriptionItemStatusPending,
			wantCompleted: false,
		},
		{
			name: "notifying delivery keeps external status running",
			items: []model.SubscriptionItem{
				{SourceKey: "notifying-item", Status: model.SubscriptionItemStatusNotifying},
			},
			wantStatus:    "running",
			wantMessage:   model.SubscriptionItemStatusNotifying,
			wantCompleted: false,
		},
		{
			name: "success remains completed without failed or active items",
			items: []model.SubscriptionItem{
				{SourceKey: "transferred-item", Status: model.SubscriptionItemStatusTransferred},
			},
			wantStatus:    "completed",
			wantMessage:   "completed",
			wantCompleted: true,
		},
		{
			name: "failed item without error uses fallback",
			items: []model.SubscriptionItem{
				{SourceKey: "failed-item", Status: model.SubscriptionItemStatusFailed},
			},
			wantStatus:    "failed",
			wantMessage:   externalSubscriptionDeliveryFailedMessage,
			wantCompleted: false,
		},
		{
			name: "failed items prefer any specific error over fallback",
			items: []model.SubscriptionItem{
				{SourceKey: "failed-empty", Status: model.SubscriptionItemStatusFailed},
				{SourceKey: "failed-specific", Status: model.SubscriptionItemStatusFailed, LastError: "saving share failed"},
			},
			wantStatus:    "failed",
			wantMessage:   "saving share failed",
			wantCompleted: false,
		},
		{
			name:               "request completed cannot complete unknown discovery",
			subscriptionStatus: "unknown",
			requestStatus:      "completed",
			requestMessage:     "completed",
			wantStatus:         "pending",
			wantMessage:        "completed",
			wantCompleted:      false,
		},
		{
			name:               "subscription running fallback remains running",
			subscriptionStatus: model.SubscriptionStatusRunning,
			requestStatus:      "completed",
			requestMessage:     "completed",
			wantStatus:         "running",
			wantMessage:        "processing",
			wantCompleted:      false,
		},
		{
			name:               "subscription failed fallback remains failed",
			subscriptionStatus: model.SubscriptionStatusFailed,
			subscriptionError:  "discovery failed",
			requestStatus:      "completed",
			requestMessage:     "completed",
			wantStatus:         "failed",
			wantMessage:        "discovery failed",
			wantCompleted:      false,
		},
		{
			name:               "request failed fallback remains failed",
			subscriptionStatus: model.SubscriptionStatusIdle,
			requestStatus:      "failed",
			requestMessage:     "request failed",
			requestError:       "request failed error",
			wantStatus:         "failed",
			wantMessage:        "request failed",
			wantCompleted:      false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			setupSubscriptionRuntimeDB(t)
			subscriptionStatus := tc.subscriptionStatus
			if subscriptionStatus == "" {
				subscriptionStatus = model.SubscriptionStatusSuccess
			}
			requestStatus := tc.requestStatus
			if requestStatus == "" {
				requestStatus = "completed"
			}
			requestMessage := tc.requestMessage
			if requestMessage == "" {
				requestMessage = requestStatus
			}

			request := &model.ExternalSubscriptionRequest{
				IdempotencyKey:     t.Name(),
				LookupKey:          "tv:" + t.Name(),
				RequestFingerprint: "fingerprint:" + t.Name(),
				RequestJSON:        "{}",
				LastStatus:         requestStatus,
				LastMessage:        requestMessage,
				ProgressJSON:       "{}",
				SeasonsJSON:        "[]",
				LastError:          tc.requestError,
			}
			subscription := &model.Subscription{
				Name:       t.Name(),
				MediaType:  "tv",
				TMDBID:     1399,
				LastStatus: subscriptionStatus,
				LastError:  tc.subscriptionError,
			}
			if err := db.CreateExternalSubscriptionRequest(context.Background(), request, subscription); err != nil {
				t.Fatalf("create external subscription request: %v", err)
			}
			for _, item := range tc.items {
				item.SubscriptionID = subscription.ID
				if _, _, err := db.UpsertSubscriptionItem(&item); err != nil {
					t.Fatalf("upsert item %s: %v", item.SourceKey, err)
				}
			}

			response, err := ProjectExternalSubscription(context.Background(), request.ID)
			if err != nil {
				t.Fatalf("project external subscription: %v", err)
			}
			if response.Status != tc.wantStatus {
				t.Fatalf("status = %q, want %q", response.Status, tc.wantStatus)
			}
			if response.TaskStatus != tc.wantStatus {
				t.Fatalf("task status = %q, want %q", response.TaskStatus, tc.wantStatus)
			}
			if response.LastStatus != tc.wantStatus {
				t.Fatalf("last status = %q, want %q", response.LastStatus, tc.wantStatus)
			}
			if response.LastMessage != tc.wantMessage {
				t.Fatalf("last message = %q, want %q", response.LastMessage, tc.wantMessage)
			}
			if response.Completed != tc.wantCompleted {
				t.Fatalf("completed = %v, want %v", response.Completed, tc.wantCompleted)
			}
		})
	}
}

func TestProjectExternalSubscriptionProgressStatus(t *testing.T) {
	tests := []struct {
		name         string
		request      model.ExternalSubscriptionRequest
		subscription model.Subscription
		items        []model.SubscriptionItem
		details      []model.SubscriptionEpisodeSourceDetail
		want         string
	}{
		{
			name:         "active discovery stage is searching",
			request:      model.ExternalSubscriptionRequest{LastStatus: "running"},
			subscription: model.Subscription{LastStatus: model.SubscriptionStatusRunning},
			details: []model.SubscriptionEpisodeSourceDetail{{
				CurrentStage:       model.ClusterStageDiscoveringFiles,
				CurrentStageStatus: model.ClusterStageStatusRunning,
			}},
			want: model.SubscriptionProgressStatusSearching,
		},
		{
			name:         "active download stage is downloading",
			request:      model.ExternalSubscriptionRequest{LastStatus: "running"},
			subscription: model.Subscription{LastStatus: model.SubscriptionStatusRunning},
			details: []model.SubscriptionEpisodeSourceDetail{{
				CurrentStage:       model.ClusterStageDownloading,
				CurrentStageStatus: model.ClusterStageStatusRunning,
			}},
			want: model.SubscriptionProgressStatusDownloading,
		},
		{
			name:         "active upload stage is uploading",
			request:      model.ExternalSubscriptionRequest{LastStatus: "running"},
			subscription: model.Subscription{LastStatus: model.SubscriptionStatusRunning},
			details: []model.SubscriptionEpisodeSourceDetail{{
				CurrentStage:       model.ClusterStageUploadingMobile,
				CurrentStageStatus: model.ClusterStageStatusPermitted,
			}},
			want: model.SubscriptionProgressStatusUploading,
		},
		{
			name:         "advanced active stage wins when multiple items are running",
			request:      model.ExternalSubscriptionRequest{LastStatus: "running"},
			subscription: model.Subscription{LastStatus: model.SubscriptionStatusRunning},
			details: []model.SubscriptionEpisodeSourceDetail{
				{CurrentStage: model.ClusterStageDiscoveringFiles, CurrentStageStatus: model.ClusterStageStatusRunning},
				{CurrentStage: model.ClusterStageDownloading, CurrentStageStatus: model.ClusterStageStatusRunning},
				{CurrentStage: model.ClusterStageUploadingMobile, CurrentStageStatus: model.ClusterStageStatusRunning},
			},
			want: model.SubscriptionProgressStatusUploading,
		},
		{
			name:         "pending item is searching",
			request:      model.ExternalSubscriptionRequest{LastStatus: "running"},
			subscription: model.Subscription{LastStatus: model.SubscriptionStatusRunning},
			items:        []model.SubscriptionItem{{Status: model.SubscriptionItemStatusPending}},
			want:         model.SubscriptionProgressStatusSearching,
		},
		{
			name:         "transferred items are completed",
			request:      model.ExternalSubscriptionRequest{LastStatus: "completed"},
			subscription: model.Subscription{LastStatus: model.SubscriptionStatusSuccess},
			items:        []model.SubscriptionItem{{Status: model.SubscriptionItemStatusTransferred}},
			want:         model.SubscriptionProgressStatusCompleted,
		},
		{
			name:         "failed request remains failed",
			request:      model.ExternalSubscriptionRequest{LastStatus: "failed"},
			subscription: model.Subscription{LastStatus: model.SubscriptionStatusFailed},
			want:         model.SubscriptionProgressStatusFailed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := projectExternalSubscriptionProgressStatus(&tc.request, &tc.subscription, tc.items, tc.details, model.SubscriptionProgress{})
			if got != tc.want {
				t.Fatalf("progress status = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestProjectExternalSubscriptionProgressStatusKeepsPartialRangeInProgress(t *testing.T) {
	request := &model.ExternalSubscriptionRequest{LastStatus: "completed"}
	subscription := &model.Subscription{LastStatus: model.SubscriptionStatusSuccess}
	progress := model.SubscriptionProgress{ExpectedEpisodes: 10, CompletedEpisodes: 3}

	got := projectExternalSubscriptionProgressStatus(request, subscription, nil, nil, progress)
	if got != model.SubscriptionProgressStatusSearching {
		t.Fatalf("progress status = %q, want %q", got, model.SubscriptionProgressStatusSearching)
	}
}

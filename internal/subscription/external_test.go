package subscription

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

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

func TestProjectExternalSubscriptionStatus(t *testing.T) {
	tests := []struct {
		name          string
		items         []model.SubscriptionItem
		wantStatus    string
		wantMessage   string
		wantCompleted bool
	}{
		{
			name: "failed item overrides discovery success",
			items: []model.SubscriptionItem{
				{SourceKey: "transferring-item", Status: model.SubscriptionItemStatusTransferring},
				{SourceKey: "failed-item", Status: model.SubscriptionItemStatusFailed, LastError: "upload failed"},
			},
			wantStatus:    "failed",
			wantMessage:   "upload failed",
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
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			setupSubscriptionRuntimeDB(t)

			request := &model.ExternalSubscriptionRequest{
				IdempotencyKey:     t.Name(),
				LookupKey:          "tv:" + t.Name(),
				RequestFingerprint: "fingerprint:" + t.Name(),
				RequestJSON:        "{}",
				LastStatus:         "completed",
				LastMessage:        "completed",
				ProgressJSON:       "{}",
				SeasonsJSON:        "[]",
			}
			subscription := &model.Subscription{
				Name:       t.Name(),
				MediaType:  "tv",
				TMDBID:     1399,
				LastStatus: model.SubscriptionStatusSuccess,
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

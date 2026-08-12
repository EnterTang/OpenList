package subscription

import (
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
)

func ListSubscriptionsWithProgress(filter db.SubscriptionFilter, archiveStatus string, now time.Time) ([]model.Subscription, int64, error) {
	items, err := db.ListAllSubscriptions(filter)
	if err != nil {
		return nil, 0, err
	}
	subscriptionIDs := make([]uint, 0, len(items))
	for _, item := range items {
		subscriptionIDs = append(subscriptionIDs, item.ID)
	}
	subscriptionItems, err := db.ListSubscriptionProgressItemsBySubscriptionIDs(subscriptionIDs)
	if err != nil {
		return nil, 0, err
	}
	itemsBySubscription := make(map[uint][]model.SubscriptionItem, len(items))
	for _, item := range subscriptionItems {
		itemsBySubscription[item.SubscriptionID] = append(itemsBySubscription[item.SubscriptionID], item)
	}
	events, err := db.ListLatestSubscriptionTelegramEventsBySubscriptionIDs(subscriptionIDs)
	if err != nil {
		return nil, 0, err
	}
	latestEventBySubscription := make(map[uint]model.SubscriptionTelegramEvent, len(items))
	for _, event := range events {
		if _, exists := latestEventBySubscription[event.SubscriptionID]; !exists {
			latestEventBySubscription[event.SubscriptionID] = event
		}
	}

	archiveStatus = strings.ToLower(strings.TrimSpace(archiveStatus))
	filtered := make([]model.Subscription, 0, len(items))
	for _, item := range items {
		item.Progress = CalculateSubscriptionProgress(&item, itemsBySubscription[item.ID], now)
		var latestEvent *model.SubscriptionTelegramEvent
		if event, ok := latestEventBySubscription[item.ID]; ok {
			latestEvent = &event
		}
		hydrateSubscriptionRealtimeStatus(&item, itemsBySubscription[item.ID], latestEvent)
		if archiveStatus != "" && archiveStatus != "all" && item.Progress.ArchiveStatus != archiveStatus {
			continue
		}
		filtered = append(filtered, item)
	}

	total := int64(len(filtered))
	page := filter.Page
	if page < 1 {
		page = 1
	}
	perPage := filter.PerPage
	if perPage < 1 {
		perPage = 20
	}
	start := (page - 1) * perPage
	if start >= len(filtered) {
		return []model.Subscription{}, total, nil
	}
	end := start + perPage
	if end > len(filtered) {
		end = len(filtered)
	}
	return filtered[start:end], total, nil
}

func IsSubscriptionArchiveStatus(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "all", model.SubscriptionArchiveStatusOngoing, model.SubscriptionArchiveStatusCompleted, model.SubscriptionArchiveStatusStalled:
		return true
	default:
		return false
	}
}

func ValidateSubscriptionArchiveStatus(value string) error {
	if IsSubscriptionArchiveStatus(value) {
		return nil
	}
	return errors.Errorf("invalid subscription archive status %q", value)
}

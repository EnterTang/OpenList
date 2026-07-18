package subscription

import (
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func hydrateSubscriptionRealtimeStatus(sub *model.Subscription, items []model.SubscriptionItem, event *model.SubscriptionTelegramEvent) {
	if sub == nil {
		return
	}
	status := model.SubscriptionRealtimeStatus{DeliveryStatus: "idle", ListenerState: "disabled"}
	if strings.EqualFold(sub.SourceType, model.SubscriptionSourceTelegram) {
		if cfg, err := parseTelegramConfig(sub.SourceConfig); err == nil && cfg.RealtimeEnabled {
			status.Enabled = true
			status.ListenerState = TelegramRealtimeListenerState(sub.ID)
		}
	}
	if event != nil {
		occurred := event.CreatedAt
		status.LastEventAt = &occurred
		status.LastMessageChannel = event.Channel
		status.LastMessageID = event.MessageID
		status.LastError = event.LastError
		if event.Status == model.SubscriptionTelegramEventStatusRetryWait {
			retryAt := event.AvailableAt
			status.RetryAt = &retryAt
		}
	}
	for _, item := range items {
		switch item.Status {
		case model.SubscriptionItemStatusNotifying:
			status.DeliveryStatus = "notifying"
			status.ActiveJobCount++
		case model.SubscriptionItemStatusTransferring:
			if status.DeliveryStatus != "notifying" {
				status.DeliveryStatus = "transferring"
			}
			status.ActiveJobCount++
		case model.SubscriptionItemStatusPending:
			if status.DeliveryStatus == "idle" || status.DeliveryStatus == "succeeded" {
				status.DeliveryStatus = "inspecting"
			}
		case model.SubscriptionItemStatusFailed:
			if status.DeliveryStatus == "idle" || status.DeliveryStatus == "succeeded" {
				status.DeliveryStatus = "failed"
				if status.LastError == "" {
					status.LastError = item.LastError
				}
			}
		case model.SubscriptionItemStatusTransferred:
			if item.UpdatedAt.After(time.Time{}) && (status.LastCompletedAt == nil || item.UpdatedAt.After(*status.LastCompletedAt)) {
				completedAt := item.UpdatedAt
				status.LastCompletedAt = &completedAt
			}
			if status.DeliveryStatus == "idle" {
				status.DeliveryStatus = "succeeded"
			}
		}
	}
	if event != nil && event.Status == model.SubscriptionTelegramEventStatusDeadLetter && status.DeliveryStatus == "idle" {
		status.DeliveryStatus = "failed"
	}
	sub.RealtimeStatus = status
}

// HydrateRealtimeStatus enriches one subscription detail response. List
// handlers use the batched event lookup in ListSubscriptionsWithProgress.
func HydrateRealtimeStatus(sub *model.Subscription, items []model.SubscriptionItem) {
	if sub == nil {
		return
	}
	var latest *model.SubscriptionTelegramEvent
	if events, err := db.ListSubscriptionTelegramEventsBySubscriptionIDs([]uint{sub.ID}); err == nil && len(events) > 0 {
		latest = &events[0]
	}
	hydrateSubscriptionRealtimeStatus(sub, items, latest)
}

package subscription

import "github.com/OpenListTeam/OpenList/v4/internal/model"

// aggregateSubscriptionStatus describes durable transfer work, not merely the
// discovery or dispatch phase. Unknown states stay non-terminal so a new state
// cannot accidentally make the subscription look successful.
func aggregateSubscriptionStatus(items []model.SubscriptionItem) string {
	if len(items) == 0 {
		return model.SubscriptionStatusSuccess
	}
	failed := false
	for i := range items {
		switch items[i].Status {
		case model.SubscriptionItemStatusFailed:
			failed = true
		case model.SubscriptionItemStatusPending,
			model.SubscriptionItemStatusNotifying,
			model.SubscriptionItemStatusTransferring:
			return model.SubscriptionStatusRunning
		case model.SubscriptionItemStatusTransferred,
			model.SubscriptionItemStatusSkipped:
			continue
		default:
			return model.SubscriptionStatusRunning
		}
	}
	if failed {
		return model.SubscriptionStatusFailed
	}
	return model.SubscriptionStatusSuccess
}

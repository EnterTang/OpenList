package subscription

import "github.com/OpenListTeam/OpenList/v4/internal/model"

const (
	subscriptionStagePending = "pending"

	subscriptionCompletionScanning           = "scanning"
	subscriptionCompletionDispatching        = "dispatching"
	subscriptionCompletionTransferring       = "transferring"
	subscriptionCompletionBlocked            = "blocked"
	subscriptionCompletionCompleted          = "completed"
	subscriptionCompletionCompletedWithSkips = "completed_with_skips"
	subscriptionCompletionFailed             = "failed"
	subscriptionCompletionPartialFailed      = "partial_failed"
	subscriptionCompletionUnknown            = "unknown"
)

type subscriptionExecutionCounts struct {
	DiscoveredCount   int
	SucceededCount    int
	SkippedCount      int
	RetryableCount    int
	BlockedCount      int
	UnknownCount      int
	FailedCount       int
	PendingCount      int
	NotifyingCount    int
	TransferringCount int
}

type subscriptionRunProjectionInput struct {
	Items              []model.SubscriptionItem
	DiscoveredHint     int
	DispatchedHint     int
	HasDiscoveryStage  bool
	HasDispatchStage   bool
	DiscoverySucceeded bool
	DispatchSucceeded  bool
	TransferRequested  bool
	ClusterDispatch    bool
}

type subscriptionRunProjection struct {
	DiscoveredCount int
	DispatchedCount int
	SucceededCount  int
	SkippedCount    int
	RetryableCount  int
	BlockedCount    int
	UnknownCount    int
	FailedCount     int
	DiscoverStatus  string
	DispatchStatus  string
	TransferStatus  string
	CompletionState string
}

func summarizeSubscriptionItems(items []model.SubscriptionItem) subscriptionExecutionCounts {
	counts := subscriptionExecutionCounts{DiscoveredCount: len(items)}
	for i := range items {
		switch items[i].Status {
		case model.SubscriptionItemStatusTransferred:
			counts.SucceededCount++
		case model.SubscriptionItemStatusSkipped:
			counts.SkippedCount++
		case model.SubscriptionItemStatusRetryWait:
			counts.RetryableCount++
		case model.SubscriptionItemStatusBlocked:
			counts.BlockedCount++
		case model.SubscriptionItemStatusUnknown:
			counts.UnknownCount++
		case model.SubscriptionItemStatusFailed:
			counts.FailedCount++
		case model.SubscriptionItemStatusPending:
			counts.PendingCount++
		case model.SubscriptionItemStatusNotifying:
			counts.NotifyingCount++
		case model.SubscriptionItemStatusTransferring:
			counts.TransferringCount++
		default:
			counts.UnknownCount++
		}
	}
	return counts
}

func projectSubscriptionRun(input subscriptionRunProjectionInput) subscriptionRunProjection {
	counts := summarizeSubscriptionItems(input.Items)
	discoveredCount := counts.DiscoveredCount
	if input.DiscoveredHint > discoveredCount {
		discoveredCount = input.DiscoveredHint
	}
	dispatchedCount := input.DispatchedHint
	if dispatchedCount < 0 {
		dispatchedCount = 0
	}
	projection := subscriptionRunProjection{
		DiscoveredCount: discoveredCount,
		DispatchedCount: dispatchedCount,
		SucceededCount:  counts.SucceededCount,
		SkippedCount:    counts.SkippedCount,
		RetryableCount:  counts.RetryableCount,
		BlockedCount:    counts.BlockedCount,
		UnknownCount:    counts.UnknownCount,
		FailedCount:     counts.FailedCount,
		DiscoverStatus:  subscriptionStagePending,
		DispatchStatus:  subscriptionStagePending,
	}
	if input.HasDiscoveryStage {
		if input.DiscoverySucceeded {
			projection.DiscoverStatus = model.SubscriptionStatusSuccess
		} else {
			projection.DiscoverStatus = model.SubscriptionStatusFailed
		}
	}
	if input.HasDispatchStage {
		if input.DispatchSucceeded {
			projection.DispatchStatus = model.SubscriptionStatusSuccess
		} else {
			projection.DispatchStatus = model.SubscriptionStatusFailed
		}
	}

	pendingTransferCount := counts.PendingCount + counts.NotifyingCount + counts.TransferringCount
	switch {
	case counts.UnknownCount > 0:
		projection.TransferStatus = subscriptionCompletionUnknown
		projection.CompletionState = subscriptionCompletionUnknown
	case counts.BlockedCount > 0:
		projection.TransferStatus = subscriptionCompletionBlocked
		projection.CompletionState = subscriptionCompletionBlocked
	case counts.FailedCount > 0:
		projection.TransferStatus = model.SubscriptionStatusFailed
		if counts.SucceededCount > 0 || counts.SkippedCount > 0 || pendingTransferCount > 0 || counts.RetryableCount > 0 {
			projection.CompletionState = subscriptionCompletionPartialFailed
		} else {
			projection.CompletionState = subscriptionCompletionFailed
		}
	case pendingTransferCount > 0 || counts.RetryableCount > 0:
		if !input.TransferRequested && !input.ClusterDispatch {
			projection.TransferStatus = subscriptionStagePending
			projection.CompletionState = subscriptionCompletionScanning
			break
		}
		projection.TransferStatus = model.SubscriptionStatusRunning
		if input.ClusterDispatch && dispatchedCount > 0 && counts.PendingCount > 0 && counts.NotifyingCount == 0 && counts.TransferringCount == 0 && counts.RetryableCount == 0 {
			projection.CompletionState = subscriptionCompletionDispatching
		} else {
			projection.CompletionState = subscriptionCompletionTransferring
		}
	case discoveredCount == 0:
		if input.ClusterDispatch && dispatchedCount > 0 {
			projection.TransferStatus = subscriptionStagePending
			projection.CompletionState = subscriptionCompletionDispatching
		} else if input.HasDiscoveryStage && input.DiscoverySucceeded {
			projection.TransferStatus = model.SubscriptionStatusSuccess
			projection.CompletionState = subscriptionCompletionCompleted
		} else {
			projection.TransferStatus = subscriptionStagePending
			projection.CompletionState = subscriptionCompletionScanning
		}
	default:
		projection.TransferStatus = model.SubscriptionStatusSuccess
		if counts.SkippedCount > 0 {
			projection.CompletionState = subscriptionCompletionCompletedWithSkips
		} else {
			projection.CompletionState = subscriptionCompletionCompleted
		}
	}
	return projection
}

// aggregateSubscriptionStatus describes durable transfer work, not merely the
// discovery or dispatch phase. Unknown states stay non-terminal so a new state
// cannot accidentally make the subscription look successful.
func aggregateSubscriptionStatus(items []model.SubscriptionItem) string {
	counts := summarizeSubscriptionItems(items)
	if counts.DiscoveredCount == 0 {
		return model.SubscriptionStatusSuccess
	}
	if counts.PendingCount > 0 || counts.NotifyingCount > 0 || counts.TransferringCount > 0 ||
		counts.RetryableCount > 0 || counts.BlockedCount > 0 || counts.UnknownCount > 0 {
		return model.SubscriptionStatusRunning
	}
	if counts.FailedCount > 0 {
		return model.SubscriptionStatusFailed
	}
	return model.SubscriptionStatusSuccess
}

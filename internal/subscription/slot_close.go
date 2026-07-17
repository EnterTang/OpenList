package subscription

import "github.com/OpenListTeam/OpenList/v4/internal/model"

type slotCloseInput struct {
	MediaType        string
	Winner           *model.SubscriptionItem
	PendingProviders []string
	EpisodeMinBytes  int64
	MovieMinBytes    int64
	Priority         []string
}

type slotCloseDecision struct {
	Closed bool
	Reason string // "size_floor" | "priority_closed" | ""
}

func decideSlotClose(in slotCloseInput) slotCloseDecision {
	if in.Winner == nil {
		return slotCloseDecision{}
	}
	minBytes := in.EpisodeMinBytes
	if normalizeMediaType(in.MediaType) == "movie" {
		minBytes = in.MovieMinBytes
	}
	if minBytes > 0 && in.Winner.FileSize >= minBytes {
		return slotCloseDecision{Closed: true, Reason: "size_floor"}
	}
	priorityIndex := map[string]int{}
	for i, p := range normalizeTransferPriority(in.Priority) {
		priorityIndex[p] = i
	}
	winnerRank := providerPriorityRank(normalizeSubscriptionProvider(in.Winner.SourceProvider), priorityIndex)
	for _, provider := range in.PendingProviders {
		rank := providerPriorityRank(normalizeSubscriptionProvider(provider), priorityIndex)
		if rank <= winnerRank {
			return slotCloseDecision{}
		}
	}
	return slotCloseDecision{Closed: true, Reason: "priority_closed"}
}

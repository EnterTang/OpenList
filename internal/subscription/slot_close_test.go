package subscription

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestSlotClosePriorityClosedWhenRemainingWeaker(t *testing.T) {
	priority := []string{"pan123", "quark", "aliyun_drive"}
	decision := decideSlotClose(slotCloseInput{
		MediaType:        "tv",
		Winner:           &model.SubscriptionItem{SourceProvider: "pan123", FileSize: 100, Episode: 1, Season: 1},
		PendingProviders: []string{"quark"},
		EpisodeMinBytes:  1 << 30,
		MovieMinBytes:    20 << 30,
		Priority:         priority,
	})
	if !decision.Closed || decision.Reason != "priority_closed" {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestSlotCloseSizeFloorForSameProvider(t *testing.T) {
	decision := decideSlotClose(slotCloseInput{
		MediaType:        "tv",
		Winner:           &model.SubscriptionItem{SourceProvider: "pan123", FileSize: 1<<30 + 1, Episode: 1, Season: 1},
		PendingProviders: []string{"pan123"},
		EpisodeMinBytes:  1 << 30,
		MovieMinBytes:    20 << 30,
		Priority:         []string{"pan123", "quark"},
	})
	if !decision.Closed || decision.Reason != "size_floor" {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestSlotCloseWaitsSameProviderBelowFloor(t *testing.T) {
	decision := decideSlotClose(slotCloseInput{
		MediaType:        "tv",
		Winner:           &model.SubscriptionItem{SourceProvider: "pan123", FileSize: 500 << 20, Episode: 1, Season: 1},
		PendingProviders: []string{"pan123"},
		EpisodeMinBytes:  1 << 30,
		MovieMinBytes:    20 << 30,
		Priority:         []string{"pan123", "quark"},
	})
	if decision.Closed {
		t.Fatalf("should wait for same-tier inspect: %#v", decision)
	}
}

func TestSlotCloseSizeFloorDisabledWhenZero(t *testing.T) {
	decision := decideSlotClose(slotCloseInput{
		MediaType:        "tv",
		Winner:           &model.SubscriptionItem{SourceProvider: "pan123", FileSize: 2 << 30, Episode: 1, Season: 1},
		PendingProviders: []string{"pan123"},
		EpisodeMinBytes:  0,
		MovieMinBytes:    0,
		Priority:         []string{"pan123"},
	})
	if decision.Closed {
		t.Fatalf("size floor disabled: %#v", decision)
	}
}

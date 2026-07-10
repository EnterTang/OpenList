package handles

import (
	"reflect"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestNormalizeSubscriptionDefaultsTelegramAndSelectedSeasons(t *testing.T) {
	item := &model.Subscription{
		Name:      " Some Show ",
		TMDBName:  " Some Show ",
		MediaType: "tv",
		Season:    3,
		Seasons:   []int{3, 1, 3, 0, -1},
	}

	normalizeSubscription(item)

	if item.SourceType != model.SubscriptionSourceTelegram {
		t.Fatalf("source type = %q, want telegram", item.SourceType)
	}
	if item.Season != 1 {
		t.Fatalf("season = %d, want first selected season", item.Season)
	}
	if want := []int{1, 3}; !reflect.DeepEqual(item.Seasons, want) {
		t.Fatalf("seasons = %#v, want %#v", item.Seasons, want)
	}
	if item.Name != "Some Show" || item.TMDBName != "Some Show" {
		t.Fatalf("names were not trimmed: %#v", item)
	}
}

func TestNormalizeSubscriptionClearsMovieSeasons(t *testing.T) {
	item := &model.Subscription{
		SourceType: model.SubscriptionSourceManual,
		MediaType:  "movie",
		Season:     2,
		Seasons:    []int{1, 2},
	}

	normalizeSubscription(item)

	if item.Season != 0 || len(item.Seasons) != 0 {
		t.Fatalf("movie season fields = season %d seasons %#v, want cleared", item.Season, item.Seasons)
	}
}

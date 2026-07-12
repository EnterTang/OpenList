package handles

import (
	"reflect"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestFilterDisplayedSubscriptionItems(t *testing.T) {
	items := []model.SubscriptionItem{
		{Status: model.SubscriptionItemStatusSkipped, SourceProvider: "123"},
		{Status: model.SubscriptionItemStatusTransferred, SourceProvider: "123"},
		{Status: model.SubscriptionItemStatusTransferred, SourceProvider: "123", FileName: "Some.Show.S01E08.mkv", Episode: 8},
		{Status: model.SubscriptionItemStatusTransferred, SourceProvider: "123", TargetPath: "/shows/Some Show/Season 1/Some.Show.S01E09.mkv", Episode: 9},
	}

	filtered := filterDisplayedSubscriptionItems(items)

	if len(filtered) != 2 {
		t.Fatalf("len(filtered) = %d, want 2", len(filtered))
	}
	if filtered[0].Episode != 8 {
		t.Fatalf("filtered[0].Episode = %d, want 8", filtered[0].Episode)
	}
	if filtered[1].Episode != 9 {
		t.Fatalf("filtered[1].Episode = %d, want 9", filtered[1].Episode)
	}
}

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
		SourceType:               model.SubscriptionSourceManual,
		MediaType:                "movie",
		Season:                   2,
		Seasons:                  []int{1, 2},
		LatestSeasonEpisodeStart: 3,
		LatestSeasonEpisodeEnd:   8,
	}

	normalizeSubscription(item)

	if item.Season != 0 || len(item.Seasons) != 0 || item.LatestSeasonEpisodeStart != 0 || item.LatestSeasonEpisodeEnd != 0 {
		t.Fatalf("movie season fields = season %d seasons %#v range %d-%d, want cleared", item.Season, item.Seasons, item.LatestSeasonEpisodeStart, item.LatestSeasonEpisodeEnd)
	}
}

func TestValidateSubscriptionEpisodeRange(t *testing.T) {
	tests := []struct {
		name    string
		start   int
		end     int
		wantErr bool
	}{
		{name: "open range", start: 9},
		{name: "closed range", start: 9, end: 12},
		{name: "negative", start: -1, wantErr: true},
		{name: "reversed", start: 12, end: 9, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSubscriptionEpisodeRange(&model.Subscription{
				MediaType:                "tv",
				LatestSeasonEpisodeStart: tt.start,
				LatestSeasonEpisodeEnd:   tt.end,
			})
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

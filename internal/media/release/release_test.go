package release

import "testing"

func TestParseExtractsBilingualMovieFields(t *testing.T) {
	got := Parse("雨人 Rain Man (1988) 1080p BluRay REMUX 国英双语 简中")
	if got.Title != "雨人 Rain Man" || got.Year != 1988 || got.Quality != "1080p" {
		t.Fatalf("parsed movie = %#v", got)
	}
	if !contains(got.Audio, "国英双语") || !contains(got.Subtitles, "简中") {
		t.Fatalf("parsed language evidence = %#v", got)
	}
}

func TestParseExtractsSeasonAndEpisodeRange(t *testing.T) {
	got := Parse("权力的游戏.Game.of.Thrones.S02E01-E10.1080p.WEB-DL.中英字幕")
	if got.Season != 2 || got.EpisodeStart != 1 || got.EpisodeEnd != 10 {
		t.Fatalf("parsed range = %#v", got)
	}
	if got.Title != "权力的游戏 Game of Thrones" {
		t.Fatalf("parsed title = %q", got.Title)
	}
}

func TestParseExtractsRepeatedEpisodeRange(t *testing.T) {
	got := Parse("超感警探.2008.S03E23E24.1080p.WEB-DL.mkv")
	if got.Season != 3 || got.EpisodeStart != 23 || got.EpisodeEnd != 24 {
		t.Fatalf("parsed repeated episode range = %#v", got)
	}
}

func TestParseSupportsChineseSeasonAndCompleteMarkers(t *testing.T) {
	got := Parse("国产剧 斗破苍穹 第五季 104集全 4K HDR")
	if got.Season != 5 || got.EpisodeEnd != 104 || !got.Complete {
		t.Fatalf("parsed Chinese TV = %#v", got)
	}
}

func TestParseDoesNotTreatYearOnlyMovieAsReleaseNoise(t *testing.T) {
	got := Parse("1917")
	if got.Title != "1917" || got.Year != 0 {
		t.Fatalf("parsed year-only title = %#v", got)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

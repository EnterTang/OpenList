package recognize

import "testing"

func TestNormalizeTitleRemovesReleaseNoise(t *testing.T) {
	got := NormalizeTitle("Cars.3.2017.2160p.BluRay.REMUX.HEVC.DTS-HD.MA.TrueHD.7.1.Atmos.mkv")

	if got != "Cars 3" {
		t.Fatalf("NormalizeTitle = %q, want Cars 3", got)
	}
}

func TestRecognizeEpisodeFile(t *testing.T) {
	got := Recognize("The.Witcher.S03E04.2023.2160p.NF.WEB-DL.H265.DV.HDR.DDP5.1.Atmos-年糕.mkv", "/tv/欧美剧/The Witcher")

	if got.Title != "The Witcher" {
		t.Fatalf("Title = %q, want The Witcher", got.Title)
	}
	if got.Season != 3 || got.Episode != 4 {
		t.Fatalf("season/episode = %d/%d, want 3/4", got.Season, got.Episode)
	}
	if got.Year != 2023 {
		t.Fatalf("Year = %d, want 2023", got.Year)
	}
}

func TestRecognizeEnglishEpisodeReleaseName(t *testing.T) {
	got := Recognize("Agent.Kim.Reactivated.S01E03.1080p.NF.WEB-DL.AAC2.0.H.264-CXX.mkv", "/139_60t")

	if got.Title != "Agent Kim Reactivated" {
		t.Fatalf("Title = %q, want Agent Kim Reactivated", got.Title)
	}
	if got.Season != 1 || got.Episode != 3 {
		t.Fatalf("season/episode = %d/%d, want 1/3", got.Season, got.Episode)
	}
	if got.MediaTypeHint != "tv" {
		t.Fatalf("MediaTypeHint = %q, want tv", got.MediaTypeHint)
	}
}

func TestRecognizeChineseSeason(t *testing.T) {
	got := Recognize("嗜血法医 第8季 豆瓣8.8", "/tv")

	if got.Season != 8 {
		t.Fatalf("Season = %d, want 8", got.Season)
	}
	if got.Title != "嗜血法医" {
		t.Fatalf("Title = %q, want 嗜血法医", got.Title)
	}
}

func TestExtractChineseEpisode(t *testing.T) {
	season, episode := ExtractSeasonEpisode("凡人修仙传 第十二集.mp4")
	if season != 1 || episode != 12 {
		t.Fatalf("season/episode = %d/%d, want 1/12", season, episode)
	}
}

func TestPureYearTitleHasNoYearHint(t *testing.T) {
	if got := ExtractYearHint("1917"); got != 0 {
		t.Fatalf("ExtractYearHint = %d, want 0", got)
	}
}

func TestBuildQueryCandidatesStripLeadingIndex(t *testing.T) {
	got := BuildQueryCandidates("6.大黄蜂 4K原盘REMUX 杜比视界 国英双音")

	if len(got) == 0 || got[0] != "大黄蜂" {
		t.Fatalf("BuildQueryCandidates = %#v, want first candidate 大黄蜂", got)
	}
}

func TestPreferParentTitleForGenericEpisode(t *testing.T) {
	got := Recognize("S01E02.mkv", "/tv/欧美剧/Slow Horses")

	if got.Title != "Slow Horses" {
		t.Fatalf("Title = %q, want parent title Slow Horses", got.Title)
	}
	if got.Season != 1 || got.Episode != 2 {
		t.Fatalf("season/episode = %d/%d, want 1/2", got.Season, got.Episode)
	}
}

package subscription

import "testing"

func TestPlanTargetWeakEpisodeUsesSubscriptionTMDBName(t *testing.T) {
	got := PlanTarget(PlanInput{
		TargetRoot: "/media",
		TMDBID:     16997,
		TMDBName:   "太平洋战争",
		TMDBYear:   2010,
		MediaType:  "tv",
		Category:   "欧美剧",
		Season:     1,
	}, "01.iso", "/shares/The Pacific")

	if got.TargetDir != "/media/tv/欧美剧/太平洋战争 (2010) {tmdb-16997}/Season 1" {
		t.Fatalf("TargetDir = %q", got.TargetDir)
	}
	if got.TargetName != "太平洋战争.2010.S01E01.第1集.iso" {
		t.Fatalf("TargetName = %q", got.TargetName)
	}
	if got.Season != 1 || got.Episode != 1 {
		t.Fatalf("season/episode = %d/%d", got.Season, got.Episode)
	}
}

func TestPlanTargetInfersSeasonFromParentPath(t *testing.T) {
	got := PlanTarget(PlanInput{
		TargetRoot: "/media",
		TMDBID:     123,
		TMDBName:   "Some Show",
		TMDBYear:   2024,
		MediaType:  "tv",
		Category:   "欧美剧",
	}, "02.mkv", "/shares/Some Show/Season 2")

	if got.TargetName != "Some Show.2024.S02E02.第2集.mkv" {
		t.Fatalf("TargetName = %q", got.TargetName)
	}
	if got.TargetDir != "/media/tv/欧美剧/Some Show (2024) {tmdb-123}/Season 2" {
		t.Fatalf("TargetDir = %q", got.TargetDir)
	}
}

func TestPlanTargetInfersChineseSeasonFromParentPath(t *testing.T) {
	got := PlanTarget(PlanInput{
		TargetRoot: "/media",
		TMDBID:     999,
		TMDBName:   "海绵宝宝",
		TMDBYear:   1999,
		MediaType:  "tv",
		Category:   "动画",
	}, "02 4k.mp4", "/shares/海绵宝宝/第 2季")

	if got.TargetName != "海绵宝宝.1999.S02E02.第2集.mp4" {
		t.Fatalf("TargetName = %q", got.TargetName)
	}
	if got.TargetDir != "/media/tv/动画/海绵宝宝 (1999) {tmdb-999}/Season 2" {
		t.Fatalf("TargetDir = %q", got.TargetDir)
	}
}

func TestPlanTargetInfersSelectedSeasonFromTitleSuffix(t *testing.T) {
	got := PlanTarget(PlanInput{
		TargetRoot: "/media",
		TMDBID:     243236,
		TMDBName:   "飞常日志",
		TMDBYear:   2024,
		MediaType:  "tv",
		Category:   "港台剧",
		Season:     1,
		Seasons:    []int{1, 2},
	}, "08国语.mp4", "/ali/转存至移动/F飞常日志2（2026）［港剧］")

	if got.Season != 2 || got.Episode != 8 {
		t.Fatalf("season/episode = %d/%d, want 2/8", got.Season, got.Episode)
	}
	if got.TargetDir != "/media/tv/港台剧/飞常日志 (2024) {tmdb-243236}/Season 2" {
		t.Fatalf("TargetDir = %q", got.TargetDir)
	}
	if got.TargetName != "飞常日志.2024.S02E08.第8集.mp4" {
		t.Fatalf("TargetName = %q", got.TargetName)
	}
}

func TestPlanTargetMovie(t *testing.T) {
	got := PlanTarget(PlanInput{
		TargetRoot: "/media",
		TMDBID:     550,
		TMDBName:   "搏击俱乐部",
		TMDBYear:   1999,
		MediaType:  "movie",
		Category:   "剧情片",
	}, "movie-file.mkv", "/shares/Fight Club")

	if got.TargetPath != "/media/movie/剧情片/搏击俱乐部 (1999) {tmdb-550}/搏击俱乐部.1999.mkv" {
		t.Fatalf("TargetPath = %q", got.TargetPath)
	}
}

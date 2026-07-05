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

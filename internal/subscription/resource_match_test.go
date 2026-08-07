package subscription

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestDecideResourceMatchAcceptsNoisyBilingualTitle(t *testing.T) {
	target := buildResourceMatchTarget(&model.Subscription{
		TMDBName:  "雨人",
		TMDBYear:  1988,
		MediaType: "movie",
	}, "")
	decision := decideResourceMatch(target, resourceMatchCandidate{
		Title: "雨人 Rain Man 1988 1080p WEB-DL 国英双语",
	})
	if !decision.Accepted || decision.Score <= 0 {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestDecideResourceMatchRejectsExplicitWrongSeason(t *testing.T) {
	target := buildResourceMatchTarget(&model.Subscription{
		TMDBName:  "示例剧",
		MediaType: "tv",
		Seasons:   []int{2},
	}, "")
	decision := decideResourceMatch(target, resourceMatchCandidate{
		Title: "示例剧 S01E01 1080p",
	})
	if decision.Accepted {
		t.Fatalf("decision = %#v, want rejection", decision)
	}
}

func TestRankResourceCandidatesPrefersCompleteCoverageBeforeQuality(t *testing.T) {
	target := buildResourceMatchTarget(&model.Subscription{
		TMDBName:  "示例剧",
		MediaType: "tv",
		Seasons:   []int{1},
	}, "")
	candidates := []resourceMatchCandidate{
		{ID: "partial-4k", Title: "示例剧 S01E01-E04 4K"},
		{ID: "complete-1080p", Title: "示例剧 S01 1080p 12集全"},
	}
	ranked := rankResourceCandidates(target, candidates)
	if len(ranked) != 2 || ranked[0].ID != "complete-1080p" {
		t.Fatalf("ranked = %#v, want complete pack first", ranked)
	}
}

func TestDecideResourceMatchRejectsGenericAndConflictingYearTitles(t *testing.T) {
	target := buildResourceMatchTarget(&model.Subscription{
		TMDBName:  "雨人",
		TMDBYear:  1988,
		MediaType: "movie",
	}, "")
	for _, title := range []string{"合集", "雨人 2019 1080p"} {
		decision := decideResourceMatch(target, resourceMatchCandidate{Title: title})
		if decision.Accepted {
			t.Fatalf("title %q decision = %#v, want rejection", title, decision)
		}
	}
}

func TestRankResourceCandidatesPreservesStableOrderOnEqualEvidence(t *testing.T) {
	target := buildResourceMatchTarget(&model.Subscription{TMDBName: "示例电影", MediaType: "movie"}, "")
	candidates := []resourceMatchCandidate{
		{ID: "first", Title: "示例电影 1080p"},
		{ID: "second", Title: "示例电影 1080p"},
	}
	ranked := rankResourceCandidates(target, candidates)
	if len(ranked) != 2 || ranked[0].ID != "first" || ranked[1].ID != "second" {
		t.Fatalf("ranked = %#v, want stable input order", ranked)
	}
}

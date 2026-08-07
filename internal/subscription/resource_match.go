package subscription

import (
	"sort"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/media/release"
	"github.com/OpenListTeam/OpenList/v4/internal/media/titlematch"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type resourceMatchTarget struct {
	Titles       []string
	MediaType    string
	Year         int
	Seasons      map[int]struct{}
	Season       int
	EpisodeStart int
	EpisodeEnd   int
}

type resourceMatchCandidate struct {
	ID       string
	Title    string
	Path     string
	Provider string
	Size     int64
	Result   *model.SubscriptionResourceSearchResult
	Parsed   release.ParsedName
}

type resourceMatchDecision struct {
	Accepted        bool
	Score           int
	TitleScore      int
	EpisodeCoverage int
	Reasons         []string
	Parsed          release.ParsedName
}

func buildResourceMatchTarget(sub *model.Subscription, query string) resourceMatchTarget {
	target := resourceMatchTarget{
		MediaType: strings.ToLower(strings.TrimSpace(query)),
		Seasons:   map[int]struct{}{},
	}
	if sub != nil {
		target.MediaType = strings.ToLower(strings.TrimSpace(sub.MediaType))
		target.Year = sub.TMDBYear
		for _, season := range sub.Seasons {
			if season > 0 {
				target.Seasons[season] = struct{}{}
			}
		}
		if len(target.Seasons) == 0 && sub.Season > 0 {
			target.Seasons[sub.Season] = struct{}{}
		}
		if len(target.Seasons) == 1 {
			for season := range target.Seasons {
				target.Season = season
			}
		}
		target.EpisodeStart = sub.LatestSeasonEpisodeStart
		target.EpisodeEnd = sub.LatestSeasonEpisodeEnd
	}
	seen := map[string]struct{}{}
	for _, value := range []string{query, targetTitle(sub), targetName(sub)} {
		for _, candidate := range titlematch.BuildMediaQueryCandidates(value) {
			candidate = strings.TrimSpace(candidate)
			if candidate == "" {
				continue
			}
			key := strings.ToLower(candidate)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			target.Titles = append(target.Titles, candidate)
		}
	}
	return target
}

func decideResourceMatch(target resourceMatchTarget, candidate resourceMatchCandidate) resourceMatchDecision {
	candidate = enrichResourceMatchCandidate(candidate)
	decision := resourceMatchDecision{Parsed: candidate.Parsed}
	if len(target.Titles) == 0 || candidateTitle(candidate) == "" {
		decision.Reasons = append(decision.Reasons, "title_missing")
		return decision
	}
	for _, targetTitle := range target.Titles {
		for _, candidateTitleValue := range candidateTitles(candidate) {
			if !titlematch.TitlesCompatible(targetTitle, candidateTitleValue) {
				continue
			}
			score := titlematch.ScoreTitleMatch(targetTitle, candidateTitleValue)
			if score > decision.TitleScore {
				decision.TitleScore = score
			}
		}
	}
	if decision.TitleScore == 0 {
		decision.Reasons = append(decision.Reasons, "title_mismatch")
		return decision
	}
	if target.MediaType == "movie" && candidate.Parsed.Season > 0 {
		decision.Reasons = append(decision.Reasons, "movie_has_season")
		return decision
	}
	if target.Year > 0 && candidate.Parsed.Year > 0 {
		if target.Year != candidate.Parsed.Year {
			decision.Reasons = append(decision.Reasons, "year_conflict")
			return decision
		}
		decision.Score += 20
		decision.Reasons = append(decision.Reasons, "year_match")
	}
	if len(target.Seasons) > 0 && candidate.Parsed.Season > 0 {
		if _, ok := target.Seasons[candidate.Parsed.Season]; !ok {
			decision.Reasons = append(decision.Reasons, "season_conflict")
			return decision
		}
		decision.Score += 30
		decision.Reasons = append(decision.Reasons, "season_match")
	}
	decision.Score += decision.TitleScore
	if candidate.Parsed.Complete {
		decision.Score += 30
		decision.Reasons = append(decision.Reasons, "complete_pack")
	}
	if target.EpisodeStart > 0 && candidate.Parsed.EpisodeStart > 0 {
		coverage := episodeCoverage(target.EpisodeStart, target.EpisodeEnd, candidate.Parsed.EpisodeStart, candidate.Parsed.EpisodeEnd)
		decision.EpisodeCoverage = coverage
		if coverage == 0 {
			decision.Reasons = append(decision.Reasons, "episode_conflict")
			return decision
		}
		decision.Score += coverage * 5
		decision.Reasons = append(decision.Reasons, "episode_overlap")
	}
	if len(candidate.Parsed.Subtitles) > 0 {
		decision.Score += 3
		decision.Reasons = append(decision.Reasons, "subtitle_evidence")
	}
	decision.Accepted = true
	return decision
}

func rankResourceCandidates(target resourceMatchTarget, candidates []resourceMatchCandidate) []resourceMatchCandidate {
	accepted := make([]resourceMatchCandidate, 0, len(candidates))
	decisions := make(map[string]resourceMatchDecision, len(candidates))
	for index, candidate := range candidates {
		if candidate.ID == "" {
			candidate.ID = string(rune(index + 1))
		}
		candidate = enrichResourceMatchCandidate(candidate)
		decision := decideResourceMatch(target, candidate)
		if !decision.Accepted {
			continue
		}
		accepted = append(accepted, candidate)
		decisions[candidate.ID] = decision
	}
	sort.SliceStable(accepted, func(i, j int) bool {
		left := decisions[accepted[i].ID]
		right := decisions[accepted[j].ID]
		if left.Score != right.Score {
			return left.Score > right.Score
		}
		if accepted[i].Size != accepted[j].Size {
			return accepted[i].Size > accepted[j].Size
		}
		return false
	})
	return accepted
}

func enrichResourceMatchCandidate(candidate resourceMatchCandidate) resourceMatchCandidate {
	if candidate.Title == "" && candidate.Result != nil {
		candidate.Title = candidate.Result.Title
	}
	if candidate.Parsed.Raw == "" {
		candidate.Parsed = release.Parse(candidateTitle(candidate))
	}
	return candidate
}

func candidateTitle(candidate resourceMatchCandidate) string {
	if strings.TrimSpace(candidate.Title) != "" {
		return strings.TrimSpace(candidate.Title)
	}
	return strings.TrimSpace(candidate.Path)
}

func candidateTitles(candidate resourceMatchCandidate) []string {
	values := make([]string, 0, 1+len(candidate.Parsed.TitleCandidates))
	if title := strings.TrimSpace(candidate.Parsed.Title); title != "" {
		values = append(values, title)
	}
	values = append(values, candidate.Parsed.TitleCandidates...)
	if len(values) == 0 && candidateTitle(candidate) != "" {
		values = append(values, candidateTitle(candidate))
	}
	return values
}

func episodeCoverage(targetStart, targetEnd, candidateStart, candidateEnd int) int {
	if targetEnd <= 0 {
		targetEnd = targetStart
	}
	if candidateEnd <= 0 {
		candidateEnd = candidateStart
	}
	start := targetStart
	if candidateStart > start {
		start = candidateStart
	}
	end := targetEnd
	if candidateEnd < end {
		end = candidateEnd
	}
	if start > end {
		return 0
	}
	return end - start + 1
}

func targetTitle(sub *model.Subscription) string {
	if sub == nil {
		return ""
	}
	return strings.TrimSpace(sub.TMDBName)
}

func targetName(sub *model.Subscription) string {
	if sub == nil {
		return ""
	}
	return strings.TrimSpace(sub.Name)
}

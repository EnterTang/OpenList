package subscription

import (
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

const subscriptionStalledAfter = 30 * 24 * time.Hour

// CalculateSubscriptionProgress derives a card-ready progress summary from
// persisted subscription items. Discovery progress intentionally includes
// non-transferred entries, while completion only counts transferred entries.
func CalculateSubscriptionProgress(sub *model.Subscription, items []model.SubscriptionItem, now time.Time) model.SubscriptionProgress {
	progress := model.SubscriptionProgress{
		ArchiveStatus:   model.SubscriptionArchiveStatusOngoing,
		MissingEpisodes: []int{},
	}
	if sub == nil {
		return progress
	}
	if sub.MediaType == "movie" {
		return calculateMovieSubscriptionProgress(sub, items, now, progress)
	}
	if hasSeasonEpisodeCounts(sub) {
		return calculateSeasonEpisodeCountProgress(sub, items, now, progress)
	}

	latestSeason := latestSubscriptionSeason(sub)
	if latestSeason <= 0 {
		return progress
	}
	progress.LatestSeason = latestSeason
	start := sub.LatestSeasonEpisodeStart
	if start <= 0 {
		start = 1
	}
	end := sub.LatestSeasonEpisodeEnd
	if end >= start {
		progress.ExpectedEpisodes = end - start + 1
	}

	discovered := make(map[int]struct{})
	transferred := make(map[int]struct{})
	var latestAddedAt time.Time
	for _, item := range items {
		if item.Season != latestSeason || item.Episode < start || item.Status == model.SubscriptionItemStatusSkipped {
			continue
		}
		if end > 0 && item.Episode > end {
			continue
		}
		discovered[item.Episode] = struct{}{}
		if item.Episode > progress.LatestEpisode {
			progress.LatestEpisode = item.Episode
			latestAddedAt = item.CreatedAt
		} else if item.Episode == progress.LatestEpisode && item.CreatedAt.After(latestAddedAt) {
			latestAddedAt = item.CreatedAt
		}
		if item.Status == model.SubscriptionItemStatusTransferred {
			transferred[item.Episode] = struct{}{}
		} else if item.Status == model.SubscriptionItemStatusFailed {
			progress.FailedEpisodes++
		}
	}
	if !latestAddedAt.IsZero() {
		progress.LastEpisodeAddedAt = &latestAddedAt
	}

	if progress.LatestEpisode >= start {
		for episode := start; episode <= progress.LatestEpisode; episode++ {
			if _, ok := discovered[episode]; !ok {
				progress.MissingEpisodes = append(progress.MissingEpisodes, episode)
			}
		}
	}
	if progress.ExpectedEpisodes > 0 {
		for episode := start; episode <= end; episode++ {
			if _, ok := transferred[episode]; ok {
				progress.CompletedEpisodes++
			}
		}
		if progress.CompletedEpisodes == progress.ExpectedEpisodes {
			progress.ArchiveStatus = model.SubscriptionArchiveStatusCompleted
			return progress
		}
	}
	return archiveStalledProgress(sub, now, progress)
}

func hasSeasonEpisodeCounts(sub *model.Subscription) bool {
	if sub == nil {
		return false
	}
	for _, count := range sub.SeasonEpisodeCounts {
		if count > 0 {
			return true
		}
	}
	return false
}

func calculateSeasonEpisodeCountProgress(sub *model.Subscription, items []model.SubscriptionItem, now time.Time, progress model.SubscriptionProgress) model.SubscriptionProgress {
	latestSeason := latestSubscriptionSeason(sub)
	progress.LatestSeason = latestSeason
	seasons := make(map[int]struct{}, len(sub.SeasonEpisodeCounts))
	for _, season := range sub.Seasons {
		if sub.SeasonEpisodeCounts[season] > 0 {
			seasons[season] = struct{}{}
		}
	}
	if len(seasons) == 0 {
		for season, count := range sub.SeasonEpisodeCounts {
			if season > 0 && count > 0 {
				seasons[season] = struct{}{}
			}
		}
	}

	startBySeason := make(map[int]int, len(seasons))
	endBySeason := make(map[int]int, len(seasons))
	for season := range seasons {
		start := 1
		end := sub.SeasonEpisodeCounts[season]
		if season == latestSeason {
			if sub.LatestSeasonEpisodeStart > 0 {
				start = sub.LatestSeasonEpisodeStart
			}
			if sub.LatestSeasonEpisodeEnd > 0 && sub.LatestSeasonEpisodeEnd < end {
				end = sub.LatestSeasonEpisodeEnd
			}
		}
		if end < start {
			continue
		}
		startBySeason[season] = start
		endBySeason[season] = end
		progress.ExpectedEpisodes += end - start + 1
	}

	type episodeKey struct {
		season  int
		episode int
	}
	discovered := make(map[episodeKey]struct{})
	transferred := make(map[episodeKey]struct{})
	failed := make(map[episodeKey]struct{})
	var latestAddedAt time.Time
	for _, item := range items {
		start, ok := startBySeason[item.Season]
		if !ok || item.Episode < start || item.Episode > endBySeason[item.Season] || item.Status == model.SubscriptionItemStatusSkipped {
			continue
		}
		key := episodeKey{season: item.Season, episode: item.Episode}
		discovered[key] = struct{}{}
		if item.Season == latestSeason && item.Episode > progress.LatestEpisode {
			progress.LatestEpisode = item.Episode
		}
		if item.CreatedAt.After(latestAddedAt) {
			latestAddedAt = item.CreatedAt
		}
		switch item.Status {
		case model.SubscriptionItemStatusTransferred:
			transferred[key] = struct{}{}
		case model.SubscriptionItemStatusFailed:
			failed[key] = struct{}{}
		}
	}

	progress.CompletedEpisodes = len(transferred)
	progress.FailedEpisodes = len(failed)
	if !latestAddedAt.IsZero() {
		progress.LastEpisodeAddedAt = &latestAddedAt
	}
	if latestStart, ok := startBySeason[latestSeason]; ok && progress.LatestEpisode >= latestStart {
		for episode := latestStart; episode <= progress.LatestEpisode; episode++ {
			if _, ok := discovered[episodeKey{season: latestSeason, episode: episode}]; !ok {
				progress.MissingEpisodes = append(progress.MissingEpisodes, episode)
			}
		}
	}
	if progress.ExpectedEpisodes > 0 && progress.CompletedEpisodes == progress.ExpectedEpisodes {
		progress.ArchiveStatus = model.SubscriptionArchiveStatusCompleted
		return progress
	}
	return archiveStalledProgress(sub, now, progress)
}

func calculateMovieSubscriptionProgress(sub *model.Subscription, items []model.SubscriptionItem, now time.Time, progress model.SubscriptionProgress) model.SubscriptionProgress {
	var lastAddedAt time.Time
	for _, item := range items {
		if item.Status == model.SubscriptionItemStatusSkipped {
			continue
		}
		if item.CreatedAt.After(lastAddedAt) {
			lastAddedAt = item.CreatedAt
		}
		if item.Status == model.SubscriptionItemStatusTransferred {
			progress.CompletedEpisodes = 1
			progress.ExpectedEpisodes = 1
			progress.ArchiveStatus = model.SubscriptionArchiveStatusCompleted
		} else if item.Status == model.SubscriptionItemStatusFailed {
			progress.FailedEpisodes = 1
		}
	}
	if !lastAddedAt.IsZero() {
		progress.LastEpisodeAddedAt = &lastAddedAt
	}
	if progress.ArchiveStatus == model.SubscriptionArchiveStatusCompleted {
		return progress
	}
	return archiveStalledProgress(sub, now, progress)
}

func archiveStalledProgress(sub *model.Subscription, now time.Time, progress model.SubscriptionProgress) model.SubscriptionProgress {
	lastAddedAt := sub.CreatedAt
	if progress.LastEpisodeAddedAt != nil {
		lastAddedAt = *progress.LastEpisodeAddedAt
	}
	if !lastAddedAt.IsZero() && now.Sub(lastAddedAt) > subscriptionStalledAfter {
		progress.ArchiveStatus = model.SubscriptionArchiveStatusStalled
	}
	return progress
}

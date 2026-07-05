package subscription

import (
	"fmt"
	stdpath "path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/media/recognize"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

var (
	leadingEpisodePattern = regexp.MustCompile(`^\s*0*([1-9]\d{0,3})(?:[\s._\-、]|$)`)
	seasonDirPattern      = regexp.MustCompile(`(?i)(?:^|[\s._\-/])(?:season|s)\s*0*([1-9]\d?)(?:$|[\s._\-/])`)
)

type PlanInput struct {
	TargetRoot string
	TMDBID     int64
	TMDBName   string
	TMDBYear   int
	MediaType  string
	Category   string
	Season     int
}

type PlannedName struct {
	Season     int    `json:"season"`
	Episode    int    `json:"episode"`
	TargetDir  string `json:"target_dir"`
	TargetName string `json:"target_name"`
	TargetPath string `json:"target_path"`
}

func PlanTarget(input PlanInput, fileName, parentPath string) PlannedName {
	mediaType := normalizeMediaType(input.MediaType)
	recognized := recognize.Recognize(fileName, parentPath)
	season := recognized.Season
	if season <= 0 {
		season = inferSeason(parentPath)
	}
	if season <= 0 {
		season = input.Season
	}
	if season <= 0 {
		season = 1
	}
	episode := recognized.Episode
	if episode <= 0 && mediaType == "tv" {
		episode = inferLeadingEpisode(fileName)
	}

	targetRoot := utils.FixAndCleanPath(input.TargetRoot)
	if targetRoot == "/" {
		targetRoot = ""
	}
	category := sanitizeComponent(input.Category)
	if category == "" {
		category = "未分类"
	}
	titleFolder := buildTitleFolder(input.TMDBName, input.TMDBYear, input.TMDBID)
	ext := filepath.Ext(fileName)

	var targetDir, targetName string
	if mediaType == "tv" {
		targetDir = stdpath.Join(targetRoot, "tv", category, titleFolder, fmt.Sprintf("Season %d", season))
		targetName = buildEpisodeName(input.TMDBName, input.TMDBYear, season, episode, ext)
	} else {
		targetDir = stdpath.Join(targetRoot, "movie", category, titleFolder)
		targetName = buildMovieName(input.TMDBName, input.TMDBYear, ext)
	}
	return PlannedName{
		Season:     season,
		Episode:    episode,
		TargetDir:  utils.FixAndCleanPath(targetDir),
		TargetName: targetName,
		TargetPath: utils.FixAndCleanPath(stdpath.Join(targetDir, targetName)),
	}
}

func normalizeMediaType(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "movie") {
		return "movie"
	}
	return "tv"
}

func inferSeason(parentPath string) int {
	if matches := seasonDirPattern.FindStringSubmatch(" " + parentPath + " "); len(matches) == 2 {
		season, _ := strconv.Atoi(matches[1])
		return season
	}
	if season, _ := recognize.ExtractSeasonEpisode(parentPath); season > 0 {
		return season
	}
	return 0
}

func inferLeadingEpisode(fileName string) int {
	stem := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	if len(stem) == 4 {
		if year, err := strconv.Atoi(stem); err == nil && year >= 1900 && year <= 2099 {
			return 0
		}
	}
	matches := leadingEpisodePattern.FindStringSubmatch(stem)
	if len(matches) != 2 {
		return 0
	}
	episode, _ := strconv.Atoi(matches[1])
	return episode
}

func buildTitleFolder(name string, year int, tmdbID int64) string {
	name = sanitizeComponent(name)
	if name == "" {
		name = "未命名"
	}
	if year > 0 && tmdbID > 0 {
		return fmt.Sprintf("%s (%d) {tmdb-%d}", name, year, tmdbID)
	}
	if year > 0 {
		return fmt.Sprintf("%s (%d)", name, year)
	}
	if tmdbID > 0 {
		return fmt.Sprintf("%s {tmdb-%d}", name, tmdbID)
	}
	return name
}

func buildEpisodeName(name string, year, season, episode int, ext string) string {
	name = sanitizeComponent(name)
	if name == "" {
		name = "未命名"
	}
	if episode <= 0 {
		return fmt.Sprintf("%s.%d.S%02d%s", name, year, season, ext)
	}
	if year > 0 {
		return fmt.Sprintf("%s.%d.S%02dE%02d.第%d集%s", name, year, season, episode, episode, ext)
	}
	return fmt.Sprintf("%s.S%02dE%02d.第%d集%s", name, season, episode, episode, ext)
}

func buildMovieName(name string, year int, ext string) string {
	name = sanitizeComponent(name)
	if name == "" {
		name = "未命名"
	}
	if year > 0 {
		return fmt.Sprintf("%s.%d%s", name, year, ext)
	}
	return name + ext
}

func sanitizeComponent(value string) string {
	value = strings.TrimSpace(value)
	value = strings.NewReplacer("\\", " ", "/", " ", ":", " ", "*", " ", "?", " ", "\"", " ", "<", " ", ">", " ", "|", " ").Replace(value)
	value = strings.Join(strings.Fields(value), " ")
	return strings.Trim(value, ". ")
}

func planInputFromSubscription(sub *model.Subscription) PlanInput {
	if sub == nil {
		return PlanInput{}
	}
	return PlanInput{
		TargetRoot: sub.TargetRoot,
		TMDBID:     sub.TMDBID,
		TMDBName:   sub.TMDBName,
		TMDBYear:   sub.TMDBYear,
		MediaType:  sub.MediaType,
		Category:   sub.Category,
		Season:     sub.Season,
	}
}

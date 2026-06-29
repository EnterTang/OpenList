package recognize

import (
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type Result struct {
	Title         string
	QueryList     []string
	Year          int
	Season        int
	Episode       int
	TMDBID        int64
	MediaTypeHint string
}

var (
	tmdbIDPattern         = regexp.MustCompile(`(?i)(?:tmdb(?:id)?[-_:\s]*|[{\[]tmdb[-_:]?)(\d+)`)
	seasonEpisodePattern  = regexp.MustCompile(`(?i)\bS(\d{1,2})E(\d{1,3})\b`)
	chineseSeasonPattern  = regexp.MustCompile(`第\s*(\d{1,2})\s*季`)
	yearPattern           = regexp.MustCompile(`(?:^|[^0-9])((?:19|20)\d{2})(?:[^0-9]|$)`)
	leadingIndexPattern   = regexp.MustCompile(`^\s*\d+\s*[\.\-、_ ]\s*`)
	doubanPattern         = regexp.MustCompile(`(?i)\s*豆瓣\s*\d+(?:\.\d+)?`)
	releaseNoisePattern   = regexp.MustCompile(`(?i)(?:^|[\s._\-\[])(?:19|20)\d{2}\b|(?:^|[\s._\-\[])(?:4320p|2160p|1080p|720p|480p|4k|8k|uhd|bluray|blu-ray|remux|web-dl|webrip|hdtv|hdrip|dvdrip|x264|x265|h264|h265|hevc|avc|hdr|dv|dolby|atmos|truehd|dts|ddp|nf|amzn|ma)\b`)
	genericEpisodePattern = regexp.MustCompile(`(?i)^S\d{1,2}E\d{1,3}$`)
	spacePattern          = regexp.MustCompile(`\s+`)
)

func Recognize(fileName, parentPath string) Result {
	result := Result{
		Year:          ExtractYearHint(fileName),
		MediaTypeHint: mediaTypeHint(fileName, parentPath),
	}
	result.Season, result.Episode = ExtractSeasonEpisode(fileName)
	result.TMDBID = extractTMDBID(fileName)
	if result.TMDBID == 0 {
		result.TMDBID = extractTMDBID(parentPath)
	}

	fileTitle := NormalizeTitle(fileName)
	parentTitle := NormalizeTitle(parentCandidate(parentPath))
	if shouldPreferParent(fileTitle, parentTitle) {
		result.Title = parentTitle
	} else {
		result.Title = fileTitle
	}
	if result.Title == "" {
		result.Title = parentTitle
	}
	if result.Title != "" {
		result.QueryList = appendUnique(result.QueryList, result.Title)
	}
	for _, candidate := range BuildQueryCandidates(fileName) {
		result.QueryList = appendUnique(result.QueryList, candidate)
	}
	for _, candidate := range BuildQueryCandidates(parentCandidate(parentPath)) {
		result.QueryList = appendUnique(result.QueryList, candidate)
	}
	return result
}

func NormalizeTitle(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = trimMediaExt(value)
	value = doubanPattern.ReplaceAllString(value, "")
	value = strings.ReplaceAll(value, "_", " ")
	value = strings.ReplaceAll(value, ".", " ")
	value = strings.ReplaceAll(value, "-", " ")
	value = leadingIndexPattern.ReplaceAllString(value, "")
	value = tmdbIDPattern.ReplaceAllString(value, "")
	value = seasonEpisodePattern.ReplaceAllString(value, "")
	value = chineseSeasonPattern.ReplaceAllString(value, "")
	if loc := releaseNoisePattern.FindStringIndex(value); loc != nil {
		value = value[:loc[0]]
	}
	value = strings.Trim(value, " ._-[]()【】")
	value = spacePattern.ReplaceAllString(value, " ")
	return strings.TrimSpace(value)
}

func BuildQueryCandidates(value string) []string {
	title := NormalizeTitle(value)
	if title == "" {
		return nil
	}
	return []string{title}
}

func ExtractYearHint(value string) int {
	value = trimMediaExt(strings.TrimSpace(value))
	if regexp.MustCompile(`^\d{4}$`).MatchString(value) {
		return 0
	}
	matches := yearPattern.FindStringSubmatch(value)
	if len(matches) < 2 {
		return 0
	}
	year, _ := strconv.Atoi(matches[1])
	return year
}

func trimMediaExt(value string) string {
	ext := strings.ToLower(filepath.Ext(value))
	switch ext {
	case ".mkv", ".mp4", ".avi", ".mov", ".rmvb", ".webm", ".flv", ".m2ts", ".ts", ".iso", ".strm", ".etf":
		return strings.TrimSuffix(value, filepath.Ext(value))
	default:
		return value
	}
}

func ExtractSeasonEpisode(value string) (season int, episode int) {
	if matches := seasonEpisodePattern.FindStringSubmatch(value); len(matches) == 3 {
		season, _ = strconv.Atoi(matches[1])
		episode, _ = strconv.Atoi(matches[2])
		return season, episode
	}
	if matches := chineseSeasonPattern.FindStringSubmatch(value); len(matches) == 2 {
		season, _ = strconv.Atoi(matches[1])
	}
	return season, 0
}

func parentCandidate(parentPath string) string {
	clean := strings.TrimSpace(parentPath)
	if clean == "" || clean == "/" || clean == "." {
		return ""
	}
	return path.Base(clean)
}

func shouldPreferParent(fileTitle, parentTitle string) bool {
	if parentTitle == "" {
		return false
	}
	if fileTitle == "" {
		return true
	}
	if genericEpisodePattern.MatchString(fileTitle) {
		return true
	}
	return len([]rune(parentTitle)) > len([]rune(fileTitle))*2
}

func mediaTypeHint(fileName, parentPath string) string {
	value := strings.ToLower(fileName + " " + parentPath)
	if season, episode := ExtractSeasonEpisode(value); season > 0 || episode > 0 {
		return "tv"
	}
	if strings.Contains(value, "/tv/") || strings.Contains(value, "剧") || strings.Contains(value, "season") {
		return "tv"
	}
	return "movie"
}

func extractTMDBID(value string) int64 {
	matches := tmdbIDPattern.FindStringSubmatch(value)
	if len(matches) < 2 {
		return 0
	}
	id, _ := strconv.ParseInt(matches[1], 10, 64)
	return id
}

func appendUnique(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if strings.EqualFold(existing, value) {
			return values
		}
	}
	return append(values, value)
}

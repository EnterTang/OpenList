package recognize

import (
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/media/titlematch"
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
	seasonEpisodePattern  = regexp.MustCompile(`(?i)\bS0*(\d{1,2})\s*E0*(\d{1,4})\b`)
	seasonPattern         = regexp.MustCompile(`(?i)(?:\bSeason\s*0*([1-9]\d?)\b|(?:^|[\s/._-])S0*([1-9]\d?)(?:$|[\s/._-]))`)
	episodePattern        = regexp.MustCompile(`(?i)(?:第\s*([一二三四五六七八九十百零〇两\d]{1,4})\s*[集话章回]|\b(?:EP|Episode)\s*0*([1-9]\d{0,3})\b|(?:^|[\s._/-])E0*([1-9]\d{0,3})(?:$|[\s._/-]))`)
	chineseSeasonPattern  = regexp.MustCompile(`第\s*([一二三四五六七八九十百零〇两\d]{1,4})\s*季`)
	yearPattern           = regexp.MustCompile(`(?:^|[^0-9])((?:19|20)\d{2})(?:[^0-9]|$)`)
	leadingIndexPattern   = regexp.MustCompile(`^\s*\d+\s*[\.\-、_ ]\s*`)
	doubanPattern         = regexp.MustCompile(`(?i)\s*豆瓣\s*\d+(?:\.\d+)?`)
	releaseNoisePattern   = regexp.MustCompile(`(?i)(?:^|[\s._\-\[])(?:19|20)\d{2}\b|(?:^|[\s._\-\[])(?:4320p|2160p|1440p|1080p|720p|576p|540p|480p|4k|8k|uhd|bluray|blu-ray|bdrip|remux|web-dl|webdl|webrip|hdtv|hdrip|dvdrip|hybrid|x264|x265|h\.?264|h\.?265|hevc|avc|av1|hdr10\+?|hdr|dv|dovi|sdr|dolby|vision|atmos|truehd|dts(?:-?hd)?|eac3|ac3|aac|ddp?|flac|mp3|pcm|nf|netflix|amzn|hmax|hulu|dsnp|ma|proper|repack|imax|10bit|60fps|5\.1|7\.1|2\.0)\b`)
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
	value = leadingIndexPattern.ReplaceAllString(value, "")
	value = doubanPattern.ReplaceAllString(value, "")
	return titlematch.NormalizeMediaTitle(value)
}

func BuildQueryCandidates(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	trimmed := trimMediaExt(value)
	title := NormalizeTitle(trimmed)
	candidates := []string{}
	for _, candidate := range titlematch.BuildMediaQueryCandidates(trimmed) {
		candidates = appendUnique(candidates, candidate)
	}
	candidates = appendUnique(candidates, title)
	candidates = appendUnique(candidates, stripCollectionSuffix(title))
	if prefix := titlePrefixBeforeEpisode(value); prefix != "" {
		for _, candidate := range titlematch.BuildMediaQueryCandidates(prefix) {
			candidates = appendUnique(candidates, candidate)
		}
		prefix = NormalizeTitle(prefix)
		candidates = appendUnique(candidates, prefix)
		candidates = appendUnique(candidates, stripCollectionSuffix(prefix))
	}
	return candidates
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
		season = parseNumberToken(matches[1])
	}
	if matches := episodePattern.FindStringSubmatch(value); len(matches) == 4 {
		for _, token := range matches[1:] {
			if n := parseNumberToken(token); n > 0 {
				episode = n
				break
			}
		}
	}
	if season <= 0 && episode > 0 {
		season = 1
	}
	return season, episode
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

func titlePrefixBeforeEpisode(value string) string {
	value = trimMediaExt(strings.TrimSpace(value))
	index := -1
	for _, pattern := range []*regexp.Regexp{seasonEpisodePattern, episodePattern, chineseSeasonPattern} {
		if loc := pattern.FindStringIndex(value); loc != nil && (index < 0 || loc[0] < index) {
			index = loc[0]
		}
	}
	if index <= 0 {
		return ""
	}
	return NormalizeTitle(value[:index])
}

func stripCollectionSuffix(value string) string {
	value = strings.TrimSpace(value)
	for _, suffix := range []string{"系列", "合集", "全集", "全季", "collection", "complete"} {
		value = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(value), suffix))
	}
	return value
}

func parseNumberToken(value string) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if n, err := strconv.Atoi(value); err == nil {
		return n
	}
	digits := map[rune]int{'零': 0, '〇': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9}
	units := map[rune]int{'十': 10, '百': 100}
	total := 0
	current := 0
	for _, r := range value {
		if n, ok := digits[r]; ok {
			current = n
			continue
		}
		if unit, ok := units[r]; ok {
			if current == 0 {
				current = 1
			}
			total += current * unit
			current = 0
			continue
		}
		return 0
	}
	total += current
	return total
}

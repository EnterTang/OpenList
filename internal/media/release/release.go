package release

import (
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/OpenListTeam/OpenList/v4/internal/media/titlematch"
)

var (
	seasonEpisodeRangePattern  = regexp.MustCompile(`(?i)\bS0*(\d{1,2})E0*(\d{1,4})\s*-\s*E?0*(\d{1,4})\b`)
	seasonEpisodePattern       = regexp.MustCompile(`(?i)\bS0*(\d{1,2})E0*(\d{1,4})\b`)
	seasonPattern              = regexp.MustCompile(`(?i)\bSeason\s*0*(\d{1,2})\b|\bS0*(\d{1,2})\b`)
	chineseSeasonPattern       = regexp.MustCompile(`第\s*([一二三四五六七八九十百零〇两\d]{1,4})\s*季`)
	episodeRangePattern        = regexp.MustCompile(`(?i)\bE0*(\d{1,4})\s*-\s*E?0*(\d{1,4})\b`)
	chineseEpisodePattern      = regexp.MustCompile(`(?:更新至|更新到|更至)?\s*([一二三四五六七八九十百零〇两\d]{1,4})\s*集`)
	yearPattern                = regexp.MustCompile(`(?:^|[^0-9])((?:19|20)\d{2})(?:[^0-9]|$)`)
	qualityPattern             = regexp.MustCompile(`(?i)(4320p|2160p|1440p|1080p|720p|576p|540p|480p|8k|4k|uhd)`)
	sourcePattern              = regexp.MustCompile(`(?i)(web[- .]?dl|web[- .]?rip|blu[- .]?ray|bdrip|remux|hdtv|dvdrip|原盘|蓝光)`)
	completePattern            = regexp.MustCompile(`(?i)(全集|集全|完结|complete|complete series|完结篇)`)
	removeSeasonEpisodePattern = regexp.MustCompile(`(?i)\bS0*\d{1,2}E0*\d{1,4}(?:\s*-\s*E?0*\d{1,4})?\b|\bSeason\s*0*\d{1,2}\b|\bS0*\d{1,2}\b|第\s*[一二三四五六七八九十百零〇两\d]{1,4}\s*[季集]|(?:更新至|更新到|更至)?\s*[一二三四五六七八九十百零〇两\d]{1,4}\s*集|\bE0*\d{1,4}(?:\s*-\s*E?0*\d{1,4})?\b`)
	removeCompletePattern      = regexp.MustCompile(`(?i)(全集|集全|完结|complete(?:\s+series)?|完结篇)`)
	removeLanguagePattern      = regexp.MustCompile(`(?i)(国英双语|中英字幕|中文字幕|内封字幕|特效字幕|简中|繁中|中字|国配|国语|粤语|英语|日语|韩语|双语|字幕)`)
	removeYearPattern          = regexp.MustCompile(`(?:^|[\s._\-\[(（])(?:19|20)\d{2}(?:$|[\s._\-\])）])`)
	separatorPattern           = strings.NewReplacer("丨", " ", "·", " ", "•", " ", "_", " ", "/", " ", "\\", " ")
)

type ParsedName struct {
	Raw             string
	Title           string
	TitleCandidates []string
	Year            int
	Season          int
	EpisodeStart    int
	EpisodeEnd      int
	Complete        bool
	Quality         string
	Source          string
	Audio           []string
	Subtitles       []string
}

func Parse(raw string) ParsedName {
	info := ParsedName{Raw: strings.TrimSpace(raw)}
	if info.Raw == "" {
		return info
	}
	value := trimMediaExtension(info.Raw)
	info.Year = extractYear(value)
	info.Season, info.EpisodeStart, info.EpisodeEnd = extractEpisodeEvidence(value)
	info.Complete = completePattern.MatchString(value)
	info.Quality = extractQuality(value)
	info.Source = extractSource(value)
	info.Audio = extractAudio(value)
	info.Subtitles = extractSubtitles(value)

	titleValue := titleValue(value)
	info.Title = titlematch.NormalizeMediaTitle(titleValue)
	info.TitleCandidates = titlematch.BuildMediaQueryCandidates(titleValue)
	if info.Title == "" && len(info.TitleCandidates) > 0 {
		info.Title = info.TitleCandidates[0]
	}
	return info
}

func trimMediaExtension(value string) string {
	ext := strings.ToLower(filepath.Ext(value))
	switch ext {
	case ".mkv", ".mp4", ".avi", ".mov", ".rmvb", ".webm", ".flv", ".m2ts", ".ts", ".iso", ".strm", ".etf":
		return strings.TrimSuffix(value, filepath.Ext(value))
	default:
		return value
	}
}

func titleValue(value string) string {
	cleaned := removeCompletePattern.ReplaceAllString(value, " ")
	cleaned = removeSeasonEpisodePattern.ReplaceAllString(cleaned, " ")
	cleaned = removeLanguagePattern.ReplaceAllString(cleaned, " ")
	if !isPureYear(strings.TrimSpace(cleaned)) {
		cleaned = removeYearPattern.ReplaceAllString(cleaned, " ")
	}
	return separatorPattern.Replace(cleaned)
}

func extractYear(value string) int {
	trimmed := strings.TrimSpace(value)
	if isPureYear(trimmed) {
		return 0
	}
	match := yearPattern.FindStringSubmatch(trimmed)
	if len(match) != 2 {
		return 0
	}
	year, _ := strconv.Atoi(match[1])
	return year
}

func extractEpisodeEvidence(value string) (season, episodeStart, episodeEnd int) {
	if match := seasonEpisodeRangePattern.FindStringSubmatch(value); len(match) == 4 {
		season = atoi(match[1])
		episodeStart = atoi(match[2])
		episodeEnd = atoi(match[3])
		return season, episodeStart, episodeEnd
	}
	if match := seasonEpisodePattern.FindStringSubmatch(value); len(match) == 3 {
		season = atoi(match[1])
		episodeStart = atoi(match[2])
		episodeEnd = episodeStart
		return season, episodeStart, episodeEnd
	}
	if match := episodeRangePattern.FindStringSubmatch(value); len(match) == 3 {
		episodeStart = atoi(match[1])
		episodeEnd = atoi(match[2])
	}
	if match := chineseEpisodePattern.FindStringSubmatch(value); len(match) == 2 {
		episodeStart = parseNumberToken(match[1])
		episodeEnd = episodeStart
	}
	if match := chineseSeasonPattern.FindStringSubmatch(value); len(match) == 2 {
		season = parseNumberToken(match[1])
	}
	if season == 0 {
		matches := seasonPattern.FindStringSubmatch(value)
		if len(matches) == 3 {
			season = atoi(firstNonEmpty(matches[1], matches[2]))
		}
	}
	if season == 0 && episodeStart > 0 {
		season = 1
	}
	return season, episodeStart, episodeEnd
}

func extractQuality(value string) string {
	match := qualityPattern.FindStringSubmatch(value)
	if len(match) != 2 {
		return ""
	}
	return strings.ToLower(strings.ReplaceAll(match[1], " ", ""))
}

func extractSource(value string) string {
	match := sourcePattern.FindStringSubmatch(value)
	if len(match) != 2 {
		return ""
	}
	return strings.ToLower(strings.ReplaceAll(match[1], " ", ""))
}

func extractAudio(value string) []string {
	return collectMarkers(value, []string{"国英双语", "国配", "国语", "粤语", "英语", "日语", "韩语", "双语"})
}

func extractSubtitles(value string) []string {
	return collectMarkers(value, []string{"中英字幕", "中文字幕", "简中", "繁中", "中字", "内封字幕", "特效字幕", "字幕"})
}

func collectMarkers(value string, markers []string) []string {
	result := make([]string, 0, len(markers))
	seen := map[string]struct{}{}
	for _, marker := range markers {
		if !strings.Contains(strings.ToLower(value), strings.ToLower(marker)) {
			continue
		}
		if _, ok := seen[marker]; ok {
			continue
		}
		seen[marker] = struct{}{}
		result = append(result, marker)
	}
	return result
}

func hasHan(value string) bool {
	for _, r := range value {
		if unicode.Is(unicode.Han, r) {
			return true
		}
	}
	return false
}

func parseNumberToken(value string) int {
	if n := atoi(value); n > 0 {
		return n
	}
	if !hasHan(value) {
		return 0
	}
	values := map[rune]int{'零': 0, '〇': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '十': 10, '百': 100}
	total, current := 0, 0
	for _, r := range value {
		n, ok := values[r]
		if !ok {
			return 0
		}
		if n == 10 || n == 100 {
			if current == 0 {
				current = 1
			}
			total += current * n
			current = 0
			continue
		}
		current = current*10 + n
	}
	return total + current
}

func atoi(value string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(value))
	return n
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func isPureYear(value string) bool {
	if len(value) != 4 {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

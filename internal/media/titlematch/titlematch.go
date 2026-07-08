package titlematch

import (
	"regexp"
	"sort"
	"strings"
	"unicode"
)

var (
	mediaTMDBTagPattern         = regexp.MustCompile(`(?i)\\{tmdbid-\d+\\}|\[tmdb[-_:]?\d+\]`)
	mediaBracketContentPattern  = regexp.MustCompile(`[\(（\[【\{][^\)）\]】\}]{1,80}[\)）\]】\}]`)
	mediaLeadingIndexPattern    = regexp.MustCompile(`^\s*\d{1,3}\s*[\.\-、_ ]\s*`)
	mediaReleaseGroupSuffixPattern = regexp.MustCompile(`(?i)[-–—]\s*(?:[\p{Han}]{1,6}[A-Za-z]{0,2}|[A-Z][A-Za-z0-9]{1,7})\s*$`)
	mediaSeasonEpisodePattern   = regexp.MustCompile(`(?i)(?:\bS\d{1,2}E\d{1,3}(?:E\d{1,3})*\b|\bE\d{1,3}\b|\bSeason\s*\d+\b|第\s*[一二三四五六七八九十百零〇两\d]+\s*[季集])`)
	mediaEnglishNoisePattern    = regexp.MustCompile(`(?i)(?:^|[\s._\-\[])(?:4320p|2160p|1440p|1080p|720p|576p|540p|480p|4k|8k|uhd|bluray|blu-ray|bdrip|remux|web-dl|webdl|webrip|hdtv|hdrip|hybrid|x264|x265|h\.?264|h\.?265|hevc|avc|av1|hdr10\+?|hdr|dv|dovi|sdr|dolby|vision|atmos|truehd|dts(?:-?hd)?|eac3|ac3|aac(?:\d(?:\.\d)?)?|ddp?(?:\d(?:\.\d)?)?|flac|mp3|pcm|nf|netflix|amzn|hmax|hulu|dsnp|imax|ma|10bit|60fps|5\.1|7\.1|2\.0)\b`)
	mediaEnglishNoiseLoosePattern = regexp.MustCompile(`(?i)\b(?:web\s*dl|web\s*rip|blu\s*ray|aac\s*\d\s+\d|ddp\s*\d\s+\d|ddp\s*\d(?:\s*\.\s*\d)?|dts\s*hd|true\s*hd|h\s*265|h\s*264|x\s*265|x\s*264)\b`)
	mediaChannelLayoutPattern   = regexp.MustCompile(`\b(?:5\s+1|7\s+1|2\s+0)\b`)
	mediaChineseNoisePattern    = regexp.MustCompile(`(?i)(蓝光|原盘|国配|中字|双语|内封字幕|特效字幕|修复版|杜比视界|国英双音|粤语|国语|衍生剧)`)
	mediaCatalogPrefixPattern   = regexp.MustCompile(`^(?i)(美剧|韩剧|日剧|电影|电视剧|动漫|番剧|BBC)\s*`)
	mediaCollectionSuffixPattern = regexp.MustCompile(`(?i)(系列|合集|三部曲)\s*$`)
	mediaYearPattern            = regexp.MustCompile(`\b(?:19|20)\d{2}\b`)
	mediaExportStampPattern     = regexp.MustCompile(`(?:^|[_\s])\d{8}[_\s]\d{6}(?:$|[_\s])`)
	mediaSizeTailPattern        = regexp.MustCompile(`\b\d+(?:\.\d+)?\s*[GM]B?\b`)
	mediaSpacePattern           = regexp.MustCompile(`\s+`)
	mediaTokenPattern           = regexp.MustCompile(`[\p{Han}]+|[A-Za-z0-9]+`)
	mediaASCIIBoundaryPattern1  = regexp.MustCompile(`([A-Za-z0-9])([\p{Han}])`)
	mediaASCIIBoundaryPattern2  = regexp.MustCompile(`([\p{Han}])([A-Za-z0-9])`)
)

var mediaGenericTerms = map[string]struct{}{
	"系列":  {},
	"合集":  {},
	"电影":  {},
	"宇宙":  {},
	"三部曲": {},
}

var mediaStopWords = map[string]struct{}{
	"a":   {},
	"an":  {},
	"and": {},
	"of":  {},
	"the": {},
}

var mediaDocumentaryKeywords = []string{
	"纪录片",
	"幕后",
	"花絮",
	"访谈",
	"特辑",
	"秘话",
	"诞生",
	"making of",
	"documentary",
	"behind the scenes",
	"interview",
}

func NormalizeMediaTitle(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return ""
	}
	if mediaSeasonEpisodePattern.MatchString(value) || mediaEnglishNoisePattern.MatchString(value) || mediaChineseNoisePattern.MatchString(value) {
		value = mediaReleaseGroupSuffixPattern.ReplaceAllString(value, "")
	}
	value = mediaTMDBTagPattern.ReplaceAllString(value, " ")
	value = mediaBracketContentPattern.ReplaceAllString(value, " ")
	value = strings.NewReplacer("丨", " ", "·", " ", "•", " ", "《", " ", "》", " ", "“", " ", "”", " ", "‘", " ", "’", " ", "_", " ", ".", " ", "-", " ", ":", " ", "：", " ", ",", " ", "，", " ", "/", " ", "\\", " ", "&", " ", "!", " ", "！", " ", "?", " ", "？", " ", ";", " ", "；", " ", "'", " ", `"`, " ").Replace(value)
	value = mediaLeadingIndexPattern.ReplaceAllString(value, "")
	value = mediaASCIIBoundaryPattern1.ReplaceAllString(value, `${1} ${2}`)
	value = mediaASCIIBoundaryPattern2.ReplaceAllString(value, `${1} ${2}`)
	value = mediaCatalogPrefixPattern.ReplaceAllString(value, "")
	value = mediaExportStampPattern.ReplaceAllString(value, " ")
	value = mediaSizeTailPattern.ReplaceAllString(value, " ")
	value = mediaSeasonEpisodePattern.ReplaceAllString(value, " ")
	value = mediaEnglishNoiseLoosePattern.ReplaceAllString(value, " ")
	value = mediaEnglishNoisePattern.ReplaceAllString(value, " ")
	value = mediaChannelLayoutPattern.ReplaceAllString(value, " ")
	value = mediaChineseNoisePattern.ReplaceAllString(value, " ")
	value = mediaEnglishNoisePattern.ReplaceAllString(value, " ")
	value = mediaSpacePattern.ReplaceAllString(value, " ")
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if !isPureYearTitle(value) {
		value = mediaYearPattern.ReplaceAllString(value, " ")
		value = mediaSpacePattern.ReplaceAllString(value, " ")
		value = strings.TrimSpace(value)
	}
	value = strings.Trim(value, " [](){}【】")
	value = mediaSpacePattern.ReplaceAllString(value, " ")
	return strings.TrimSpace(value)
}

func BuildMediaQueryCandidates(raw string) []string {
	candidates := make([]string, 0, 6)
	seen := map[string]struct{}{}
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		candidates = append(candidates, value)
	}
	base := NormalizeMediaTitle(raw)
	add(base)
	add(stripCollectionSuffix(base))
	if bracket := bracketAliasCandidate(raw); bracket != "" {
		add(bracket)
	}
	if cjk := cjkCandidate(base); cjk != "" {
		add(cjk)
	}
	if ascii := asciiCandidate(base); ascii != "" {
		add(ascii)
	}
	return candidates
}

func TokenizeMediaMatchTerms(raw string) []string {
	normalized := NormalizeMediaTitle(raw)
	if normalized == "" {
		return nil
	}
	seen := map[string]struct{}{}
	terms := make([]string, 0, 8)
	for _, token := range mediaTokenPattern.FindAllString(normalized, -1) {
		term := strings.ToLower(strings.TrimSpace(token))
		if term == "" {
			continue
		}
		if _, ok := mediaStopWords[term]; ok {
			continue
		}
		if _, ok := mediaGenericTerms[term]; ok {
			continue
		}
		if allDigits(term) {
			if len(term) != 4 {
				continue
			}
		} else if !containsCJK(term) && len([]rune(term)) < 2 {
			continue
		}
		if _, ok := seen[term]; ok {
			continue
		}
		seen[term] = struct{}{}
		terms = append(terms, term)
	}
	sort.Strings(terms)
	return terms
}

func TitlesCompatible(query, candidate string) bool {
	queryNormalized := NormalizeMediaTitle(query)
	candidateNormalized := NormalizeMediaTitle(candidate)
	if isGenericOnlyTitle(queryNormalized) || isGenericOnlyTitle(candidateNormalized) {
		return false
	}
	if queryNormalized == candidateNormalized {
		return true
	}
	if containsDocumentaryKeyword(candidateNormalized) {
		return false
	}
	if containsCJK(queryNormalized) && strings.Contains(candidateNormalized, queryNormalized) {
		return true
	}
	queryTerms := TokenizeMediaMatchTerms(queryNormalized)
	candidateTerms := TokenizeMediaMatchTerms(candidateNormalized)
	overlap := termOverlap(queryTerms, candidateTerms)
	if overlap == 0 {
		return false
	}
	queryCoverage := float64(overlap) / float64(len(queryTerms))
	candidateCoverage := float64(overlap) / float64(len(candidateTerms))
	if queryCoverage == 1 && candidateCoverage >= 0.5 {
		return true
	}
	return queryCoverage >= 0.75 && candidateCoverage >= 0.6
}

func ScoreTitleMatch(query, candidate string) int {
	queryNormalized := NormalizeMediaTitle(query)
	candidateNormalized := NormalizeMediaTitle(candidate)
	if isGenericOnlyTitle(queryNormalized) || isGenericOnlyTitle(candidateNormalized) {
		return 0
	}
	if queryNormalized == candidateNormalized {
		return 200
	}
	queryTerms := TokenizeMediaMatchTerms(queryNormalized)
	candidateTerms := TokenizeMediaMatchTerms(candidateNormalized)
	overlap := termOverlap(queryTerms, candidateTerms)
	if overlap == 0 {
		return 0
	}
	queryCoverage := float64(overlap) / float64(len(queryTerms))
	candidateCoverage := float64(overlap) / float64(len(candidateTerms))
	score := int(queryCoverage*70 + candidateCoverage*30)
	if containsDocumentaryKeyword(candidateNormalized) {
		score -= 60
	}
	if score < 0 {
		return 0
	}
	return score
}

func isPureYearTitle(value string) bool {
	return len(value) == 4 && allDigits(value)
}

func allDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func containsCJK(value string) bool {
	for _, r := range value {
		if unicode.Is(unicode.Han, r) {
			return true
		}
	}
	return false
}

func stripCollectionSuffix(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = mediaCollectionSuffixPattern.ReplaceAllString(value, "")
	value = mediaSpacePattern.ReplaceAllString(value, " ")
	return strings.TrimSpace(value)
}

func bracketAliasCandidate(raw string) string {
	matches := mediaBracketContentPattern.FindAllString(raw, -1)
	for _, match := range matches {
		trimmed := strings.Trim(match, "()（）[]【】{}")
		candidate := NormalizeMediaTitle(trimmed)
		if candidate != "" && !isPureYearTitle(candidate) {
			return candidate
		}
	}
	return ""
}

func cjkCandidate(base string) string {
	if base == "" {
		return ""
	}
	parts := strings.Fields(base)
	candidate := make([]string, 0, len(parts))
	seenCJK := false
	for _, part := range parts {
		if containsCJK(part) {
			candidate = append(candidate, part)
			seenCJK = true
			continue
		}
		if seenCJK && len([]rune(part)) == 1 {
			candidate = append(candidate, part)
			continue
		}
		if seenCJK {
			break
		}
	}
	return strings.TrimSpace(strings.Join(candidate, " "))
}

func asciiCandidate(base string) string {
	if base == "" {
		return ""
	}
	parts := strings.Fields(base)
	candidate := make([]string, 0, len(parts))
	for _, part := range parts {
		if containsCJK(part) {
			continue
		}
		if len([]rune(part)) < 2 && !allDigits(part) {
			if len(candidate) == 0 {
				continue
			}
		}
		candidate = append(candidate, part)
	}
	return strings.TrimSpace(strings.Join(candidate, " "))
}

func isGenericOnlyTitle(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return true
	}
	terms := TokenizeMediaMatchTerms(value)
	if len(terms) == 0 {
		return true
	}
	if len(terms) == 1 {
		_, ok := mediaGenericTerms[terms[0]]
		return ok
	}
	return false
}

func containsDocumentaryKeyword(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return false
	}
	for _, keyword := range mediaDocumentaryKeywords {
		if strings.Contains(value, strings.ToLower(keyword)) {
			return true
		}
	}
	return false
}

func termOverlap(left, right []string) int {
	if len(left) == 0 || len(right) == 0 {
		return 0
	}
	set := map[string]struct{}{}
	for _, term := range left {
		set[term] = struct{}{}
	}
	overlap := 0
	for _, term := range right {
		if _, ok := set[term]; ok {
			overlap++
		}
	}
	return overlap
}

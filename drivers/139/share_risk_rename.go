package _139

import (
	"context"
	"path"
	"regexp"
	"strings"
	stdunicode "unicode"

	"github.com/mozillazg/go-pinyin"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/media/recognize"
	"github.com/OpenListTeam/OpenList/v4/internal/media/tmdb"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

var shareRiskSeasonPattern = regexp.MustCompile(`(?i)^season\s+\d+$`)

var shareRiskSettingValue = getSettingValue
var shareRiskTMDBResolve = tmdb.Resolve
var shareRiskPinyin = defaultShareRiskPinyin

func isShareRiskStructuralDir(name string) bool {
	name = strings.TrimSpace(name)
	if shareRiskSeasonPattern.MatchString(name) {
		return true
	}
	switch strings.ToLower(name) {
	case "specials", "extras":
		return true
	default:
		return false
	}
}

func replaceShareRiskTitle(name, oldTitle, newTitle string) string {
	name = strings.TrimSpace(name)
	oldTitle = strings.TrimSpace(oldTitle)
	newTitle = strings.TrimSpace(newTitle)
	if name == "" || oldTitle == "" || newTitle == "" {
		return name
	}
	replaced := strings.ReplaceAll(name, oldTitle, newTitle)
	replaced = strings.Join(strings.Fields(replaced), " ")
	return strings.TrimSpace(replaced)
}

func shareRiskActualPath(obj model.Obj) string {
	if obj == nil {
		return "/"
	}
	joined := path.Join(obj.GetPath(), obj.GetName())
	if joined == "." || joined == "" {
		return "/"
	}
	return joined
}

func (d *Yun139) resolveShareRiskCanonicalTitle(ctx context.Context, result recognize.Result, fallbackTitle string) (string, error) {
	apiKey := strings.TrimSpace(shareRiskSettingValue(conf.TMDBApiKey))
	if apiKey != "" {
		meta, err := shareRiskTMDBResolve(ctx, tmdb.Config{
			APIKey:        apiKey,
			BaseURL:       shareRiskSettingValue(conf.TMDBApiBaseURL),
			Language:      shareRiskSettingValue(conf.TMDBLanguage),
			CategoryRules: shareRiskSettingValue(conf.MediaCategoryRules),
		}, result)
		if err != nil {
			return "", err
		}
		if meta != nil {
			if original := sanitizeETFPathSegment(strings.TrimSpace(meta.OriginalName)); original != "" && !containsHan(original) {
				return original, nil
			}
			if name := sanitizeETFPathSegment(strings.TrimSpace(meta.Name)); name != "" && !containsHan(name) {
				return name, nil
			}
		}
	}
	return sanitizeETFPathSegment(shareRiskPinyin(fallbackTitle)), nil
}

func defaultShareRiskPinyin(title string) string {
	title = strings.TrimSpace(title)
	if title == "" {
		return ""
	}
	if !containsHan(title) {
		return sanitizeETFPathSegment(title)
	}
	args := pinyin.NewArgs()
	parts := make([]string, 0)
	currentASCII := strings.Builder{}
	flushASCII := func() {
		if currentASCII.Len() == 0 {
			return
		}
		parts = append(parts, currentASCII.String())
		currentASCII.Reset()
	}
	for _, r := range title {
		if stdunicode.Is(stdunicode.Han, r) {
			flushASCII()
			py := pinyin.Pinyin(string(r), args)
			if len(py) == 0 || len(py[0]) == 0 || strings.TrimSpace(py[0][0]) == "" {
				continue
			}
			token := py[0][0]
			parts = append(parts, strings.ToUpper(token[:1])+token[1:])
			continue
		}
		if stdunicode.IsLetter(r) || stdunicode.IsDigit(r) {
			currentASCII.WriteRune(r)
			continue
		}
		flushASCII()
	}
	flushASCII()
	return sanitizeETFPathSegment(strings.Join(parts, " "))
}

func containsHan(value string) bool {
	for _, r := range value {
		if stdunicode.Is(stdunicode.Han, r) {
			return true
		}
	}
	return false
}

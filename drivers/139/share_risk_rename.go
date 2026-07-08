package _139

import (
	"context"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	stdunicode "unicode"

	"github.com/mozillazg/go-pinyin"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/media/recognize"
	"github.com/OpenListTeam/OpenList/v4/internal/media/tmdb"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type shareRiskRenameNode struct {
	Obj        model.Obj
	ParentPath string
	Depth      int
	OldName    string
	NewName    string
}

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

func shareRiskPathDepth(actualPath string) int {
	trimmed := strings.Trim(strings.TrimSpace(actualPath), "/")
	if trimmed == "" {
		return 0
	}
	return len(strings.Split(trimmed, "/")) - 1
}

func (d *Yun139) buildShareRiskRenamePlan(ctx context.Context, root model.Obj, actualPath string) ([]shareRiskRenameNode, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if root == nil {
		return nil, "", fmt.Errorf("share risk rename target is nil")
	}
	actualPath = strings.TrimSpace(actualPath)
	if actualPath == "" {
		actualPath = shareRiskActualPath(root)
	}
	result := recognize.Recognize(root.GetName(), path.Dir(actualPath))
	oldTitle := strings.TrimSpace(result.Title)
	if oldTitle == "" {
		oldTitle = recognize.NormalizeTitle(root.GetName())
	}
	if oldTitle == "" || !containsHan(oldTitle) {
		return nil, "", nil
	}
	canonicalTitle, err := d.resolveShareRiskCanonicalTitle(ctx, result, oldTitle)
	if err != nil {
		return nil, "", err
	}
	if canonicalTitle == "" {
		return nil, "", nil
	}
	plan := make([]shareRiskRenameNode, 0)
	if newName := replaceShareRiskTitle(root.GetName(), oldTitle, canonicalTitle); newName != "" && newName != root.GetName() {
		plan = append(plan, shareRiskRenameNode{
			Obj:        root,
			ParentPath: path.Dir(actualPath),
			Depth:      shareRiskPathDepth(actualPath),
			OldName:    root.GetName(),
			NewName:    newName,
		})
	}
	if !root.IsDir() {
		return plan, canonicalTitle, nil
	}
	if err := d.collectShareRiskRenameNodes(ctx, root, actualPath, oldTitle, canonicalTitle, &plan); err != nil {
		return nil, "", err
	}
	return plan, canonicalTitle, nil
}

func (d *Yun139) collectShareRiskRenameNodes(ctx context.Context, dir model.Obj, actualPath, oldTitle, canonicalTitle string, plan *[]shareRiskRenameNode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	children, err := d.List(ctx, dir, model.ListArgs{})
	if err != nil {
		return err
	}
	for _, child := range children {
		childPath := path.Join(actualPath, child.GetName())
		if child.IsDir() {
			if !isShareRiskStructuralDir(child.GetName()) {
				if newName := replaceShareRiskTitle(child.GetName(), oldTitle, canonicalTitle); newName != "" && newName != child.GetName() {
					*plan = append(*plan, shareRiskRenameNode{
						Obj:        child,
						ParentPath: path.Dir(childPath),
						Depth:      shareRiskPathDepth(childPath),
						OldName:    child.GetName(),
						NewName:    newName,
					})
				}
			}
			if err := d.collectShareRiskRenameNodes(ctx, child, childPath, oldTitle, canonicalTitle, plan); err != nil {
				return err
			}
			continue
		}
		if newName := replaceShareRiskTitle(child.GetName(), oldTitle, canonicalTitle); newName != "" && newName != child.GetName() {
			*plan = append(*plan, shareRiskRenameNode{
				Obj:        child,
				ParentPath: path.Dir(childPath),
				Depth:      shareRiskPathDepth(childPath),
				OldName:    child.GetName(),
				NewName:    newName,
			})
		}
	}
	return nil
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

func (d *Yun139) applyShareRiskRenamePlan(ctx context.Context, plan []shareRiskRenameNode) error {
	sort.SliceStable(plan, func(i, j int) bool {
		if plan[i].Depth != plan[j].Depth {
			return plan[i].Depth > plan[j].Depth
		}
		return strings.ToLower(plan[i].OldName) < strings.ToLower(plan[j].OldName)
	})
	for _, item := range plan {
		if err := ctx.Err(); err != nil {
			return err
		}
		if strings.TrimSpace(item.NewName) == "" || item.NewName == item.OldName {
			continue
		}
		if err := d.Rename(ctx, item.Obj, item.NewName); err != nil {
			return fmt.Errorf("rename %s -> %s: %w", item.OldName, item.NewName, err)
		}
	}
	return nil
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

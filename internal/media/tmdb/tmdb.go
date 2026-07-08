package tmdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"unicode"

	"github.com/OpenListTeam/OpenList/v4/internal/media/category"
	"github.com/OpenListTeam/OpenList/v4/internal/media/recognize"
	"github.com/OpenListTeam/OpenList/v4/internal/media/titlematch"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

const defaultBaseURL = "https://api.themoviedb.org/3"

type Config struct {
	APIKey        string
	BaseURL       string
	Language      string
	CategoryRules string
}

type Metadata struct {
	MediaType        string
	TMDBID           int64
	Name             string
	OriginalName     string
	Year             int
	GenreIDs         []int
	OriginCountry    []string
	OriginalLanguage string
	Category         string
}

type searchResp struct {
	Results []tmdbItem `json:"results"`
}

type tmdbItem struct {
	ID               int64    `json:"id"`
	MediaType        string   `json:"media_type"`
	Title            string   `json:"title"`
	Name             string   `json:"name"`
	OriginalTitle    string   `json:"original_title"`
	OriginalName     string   `json:"original_name"`
	ReleaseDate      string   `json:"release_date"`
	FirstAirDate     string   `json:"first_air_date"`
	PosterPath       string   `json:"poster_path"`
	GenreIDs         []int    `json:"genre_ids"`
	Genres           []genre  `json:"genres"`
	OriginCountry    []string `json:"origin_country"`
	OriginalLanguage string   `json:"original_language"`
	SearchLanguage   string   `json:"-"`
}

type genre struct {
	ID int `json:"id"`
}

func Resolve(ctx context.Context, cfg Config, recognized recognize.Result) (*Metadata, error) {
	cfg = normalizeConfig(cfg)
	if cfg.APIKey == "" {
		return nil, nil
	}
	if recognized.TMDBID > 0 {
		mediaType := recognized.MediaTypeHint
		if mediaType == "" {
			mediaType = "movie"
		}
		item, err := requestItem(ctx, cfg, "/"+mediaType+"/"+strconv.FormatInt(recognized.TMDBID, 10), nil)
		if err != nil {
			return nil, err
		}
		item.MediaType = mediaType
		meta := item.metadata()
		applyCategory(meta, cfg.CategoryRules)
		return meta, nil
	}

	queries := recognized.QueryList
	if len(queries) == 0 && recognized.Title != "" {
		queries = []string{recognized.Title}
	}
	bestScore := 0
	var best *Metadata
	for _, query := range queries {
		items, err := searchAllLanguages(ctx, cfg, query)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			if item.MediaType != "movie" && item.MediaType != "tv" {
				continue
			}
			score := scoreCandidate(item, recognized)
			if score > bestScore {
				meta := localizeItem(ctx, cfg, item).metadata()
				best = meta
				bestScore = score
			}
		}
		if bestScore >= 95 {
			break
		}
	}
	if bestScore < 80 || best == nil {
		return nil, nil
	}
	applyCategory(best, cfg.CategoryRules)
	return best, nil
}

func SearchCandidates(ctx context.Context, cfg Config, query string) ([]model.ETFArchiveTMDBCandidate, error) {
	cfg = normalizeConfig(cfg)
	queries := recognize.BuildQueryCandidates(query)
	if len(queries) == 0 {
		queries = []string{strings.TrimSpace(query)}
	}
	items := make([]tmdbItem, 0)
	seen := map[string]struct{}{}
	if id, ok := parseNumericID(query); ok {
		for _, mediaType := range []string{"tv", "movie"} {
			item, err := requestItem(ctx, cfg, "/"+mediaType+"/"+strconv.FormatInt(id, 10), nil)
			if err != nil {
				continue
			}
			item.MediaType = mediaType
			key := mediaType + ":" + strconv.FormatInt(item.ID, 10)
			seen[key] = struct{}{}
			items = append(items, *item)
		}
	}
	for _, q := range queries {
		found, err := searchAllLanguages(ctx, cfg, q)
		if err != nil {
			return nil, err
		}
		for _, item := range found {
			if item.MediaType != "movie" && item.MediaType != "tv" {
				continue
			}
			key := item.MediaType + ":" + strconv.FormatInt(item.ID, 10)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			items = append(items, item)
		}
	}
	candidates := make([]model.ETFArchiveTMDBCandidate, 0, len(items))
	for _, item := range items {
		item = localizeItem(ctx, cfg, item)
		meta := item.metadata()
		applyCategory(meta, cfg.CategoryRules)
		candidates = append(candidates, model.ETFArchiveTMDBCandidate{
			TMDBID:           meta.TMDBID,
			Name:             meta.Name,
			OriginalName:     item.originalDisplayName(),
			Year:             meta.Year,
			MediaType:        meta.MediaType,
			Category:         meta.Category,
			PosterPath:       item.PosterPath,
			PosterURL:        posterURL(item.PosterPath),
			GenreIDs:         meta.GenreIDs,
			OriginCountry:    meta.OriginCountry,
			OriginalLanguage: meta.OriginalLanguage,
		})
	}
	return candidates, nil
}

func normalizeConfig(cfg Config) Config {
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	cfg.BaseURL = strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultBaseURL
	}
	cfg.Language = strings.TrimSpace(cfg.Language)
	if cfg.Language == "" {
		cfg.Language = "zh-CN"
	}
	return cfg
}

func search(ctx context.Context, cfg Config, query string) (*searchResp, error) {
	var resp searchResp
	params := url.Values{"query": []string{query}}
	if err := request(ctx, cfg, "/search/multi", params, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func searchAllLanguages(ctx context.Context, cfg Config, query string) ([]tmdbItem, error) {
	languages := []string{cfg.Language}
	if isASCII(query) && !strings.EqualFold(cfg.Language, "en-US") {
		languages = append(languages, "en-US")
	}
	seen := map[string]struct{}{}
	var items []tmdbItem
	for _, language := range languages {
		searchCfg := cfg
		searchCfg.Language = language
		resp, err := search(ctx, searchCfg, query)
		if err != nil {
			return nil, err
		}
		for _, item := range resp.Results {
			key := item.MediaType + ":" + strconv.FormatInt(item.ID, 10)
			if _, ok := seen[key]; ok {
				continue
			}
			item.SearchLanguage = language
			seen[key] = struct{}{}
			items = append(items, item)
		}
	}
	return items, nil
}

func localizeItem(ctx context.Context, cfg Config, item tmdbItem) tmdbItem {
	if item.SearchLanguage == "" || strings.EqualFold(item.SearchLanguage, cfg.Language) {
		return item
	}
	detail, err := requestItem(ctx, cfg, "/"+item.MediaType+"/"+strconv.FormatInt(item.ID, 10), nil)
	if err != nil {
		return item
	}
	detail.MediaType = item.MediaType
	detail.SearchLanguage = cfg.Language
	return *detail
}

func requestItem(ctx context.Context, cfg Config, endpoint string, params url.Values) (*tmdbItem, error) {
	var item tmdbItem
	if err := request(ctx, cfg, endpoint, params, &item); err != nil {
		return nil, err
	}
	return &item, nil
}

func request(ctx context.Context, cfg Config, endpoint string, params url.Values, out any) error {
	u, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return fmt.Errorf("tmdb base url: %w", err)
	}
	u.Path = path.Join(u.Path, endpoint)
	query := u.Query()
	query.Set("api_key", cfg.APIKey)
	query.Set("language", cfg.Language)
	for key, values := range params {
		for _, value := range values {
			query.Add(key, value)
		}
	}
	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return redactKey(err, cfg.APIKey)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("tmdb status %d", res.StatusCode)
	}
	if err := json.NewDecoder(res.Body).Decode(out); err != nil {
		return redactKey(err, cfg.APIKey)
	}
	return nil
}

func (i tmdbItem) metadata() *Metadata {
	genreIDs := i.GenreIDs
	if len(genreIDs) == 0 && len(i.Genres) > 0 {
		genreIDs = make([]int, 0, len(i.Genres))
		for _, item := range i.Genres {
			genreIDs = append(genreIDs, item.ID)
		}
	}
	return &Metadata{
		MediaType:        i.MediaType,
		TMDBID:           i.ID,
		Name:             i.displayName(),
		OriginalName:     i.originalDisplayName(),
		Year:             yearFromDate(i.date()),
		GenreIDs:         genreIDs,
		OriginCountry:    i.OriginCountry,
		OriginalLanguage: i.OriginalLanguage,
	}
}

func (i tmdbItem) displayName() string {
	if i.Title != "" {
		return i.Title
	}
	return i.Name
}

func (i tmdbItem) originalDisplayName() string {
	if i.OriginalTitle != "" {
		return i.OriginalTitle
	}
	return i.OriginalName
}

func (i tmdbItem) date() string {
	if i.ReleaseDate != "" {
		return i.ReleaseDate
	}
	return i.FirstAirDate
}

func scoreCandidate(item tmdbItem, recognized recognize.Result) int {
	titles := []string{item.displayName(), item.originalDisplayName()}
	queryCandidates := make([]string, 0, len(recognized.QueryList)+1)
	if strings.TrimSpace(recognized.Title) != "" {
		queryCandidates = append(queryCandidates, recognized.Title)
	}
	for _, candidate := range recognized.QueryList {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" || candidate == recognized.Title {
			continue
		}
		queryCandidates = append(queryCandidates, candidate)
	}
	score := 0
	if len(queryCandidates) > 0 {
		bestTitleScore := 0
		for _, query := range queryCandidates {
			for _, title := range titles {
				if strings.TrimSpace(title) == "" {
					continue
				}
				titleScore := titlematch.ScoreTitleMatch(query, title)
				if titlematch.TitlesCompatible(query, title) {
					titleScore += 60
				}
				bestTitleScore = max(bestTitleScore, titleScore)
			}
		}
		score += bestTitleScore
	}
	if recognized.Year > 0 && yearFromDate(item.date()) == recognized.Year {
		score += 20
	}
	if recognized.MediaTypeHint != "" && item.MediaType == recognized.MediaTypeHint {
		score += 5
	}
	return score
}

func normalizeTitle(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	replacer := strings.NewReplacer(".", " ", "_", " ", "-", " ", ":", " ", "：", " ", "'", " ", "\"", " ", "·", " ")
	value = replacer.Replace(value)
	return strings.Join(strings.Fields(value), " ")
}

func titleSimilarityScore(target, title string) int {
	targetTerms := termSet(target)
	titleTerms := termSet(title)
	if len(targetTerms) == 0 || len(titleTerms) == 0 {
		return 0
	}
	overlap := 0
	for term := range targetTerms {
		if _, ok := titleTerms[term]; ok {
			overlap++
		}
	}
	overlapScore := int(math.Round(float64(overlap) / float64(len(targetTerms)) * 70))
	lcsScore := 0
	targetCompact := strings.ReplaceAll(target, " ", "")
	titleCompact := strings.ReplaceAll(title, " ", "")
	if len([]rune(targetCompact)) > 0 {
		lcsScore = int(math.Round(float64(longestCommonSubstringLen(targetCompact, titleCompact)) / float64(len([]rune(targetCompact))) * 65))
	}
	return max(overlapScore, lcsScore)
}

func termSet(value string) map[string]struct{} {
	terms := map[string]struct{}{}
	for _, term := range strings.Fields(value) {
		if len([]rune(term)) < 2 {
			continue
		}
		terms[term] = struct{}{}
	}
	return terms
}

func longestCommonSubstringLen(left, right string) int {
	a := []rune(left)
	b := []rune(right)
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	dp := make([]int, len(b)+1)
	best := 0
	for i := 1; i <= len(a); i++ {
		prev := 0
		for j := 1; j <= len(b); j++ {
			tmp := dp[j]
			if a[i-1] == b[j-1] {
				dp[j] = prev + 1
				if dp[j] > best {
					best = dp[j]
				}
			} else {
				dp[j] = 0
			}
			prev = tmp
		}
	}
	return best
}

func parseNumericID(query string) (int64, bool) {
	query = strings.TrimSpace(query)
	if query == "" {
		return 0, false
	}
	for _, r := range query {
		if !unicode.IsDigit(r) {
			return 0, false
		}
	}
	id, err := strconv.ParseInt(query, 10, 64)
	return id, err == nil && id > 0
}

func isASCII(value string) bool {
	for _, r := range value {
		if r > unicode.MaxASCII {
			return false
		}
	}
	return true
}

func posterURL(path string) string {
	if strings.TrimSpace(path) == "" {
		return ""
	}
	return "https://image.tmdb.org/t/p/w185" + path
}

func yearFromDate(value string) int {
	if len(value) < 4 {
		return 0
	}
	year, _ := strconv.Atoi(value[:4])
	return year
}

func applyCategory(meta *Metadata, rules string) {
	if meta == nil || strings.TrimSpace(rules) == "" {
		return
	}
	meta.Category = category.Match(rules, category.Metadata{
		MediaType:        meta.MediaType,
		GenreIDs:         meta.GenreIDs,
		OriginCountry:    meta.OriginCountry,
		OriginalLanguage: meta.OriginalLanguage,
	})
}

func redactKey(err error, key string) error {
	if err == nil || key == "" {
		return err
	}
	return errors.New(strings.ReplaceAll(err.Error(), key, "***"))
}

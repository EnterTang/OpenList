package tmdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/media/category"
	"github.com/OpenListTeam/OpenList/v4/internal/media/recognize"
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
	GenreIDs         []int    `json:"genre_ids"`
	Genres           []genre  `json:"genres"`
	OriginCountry    []string `json:"origin_country"`
	OriginalLanguage string   `json:"original_language"`
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
		resp, err := search(ctx, cfg, query)
		if err != nil {
			return nil, err
		}
		for _, item := range resp.Results {
			if item.MediaType != "movie" && item.MediaType != "tv" {
				continue
			}
			score := scoreCandidate(item, recognized)
			if score > bestScore {
				meta := item.metadata()
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
	resp, err := search(ctx, cfg, query)
	if err != nil {
		return nil, err
	}
	candidates := make([]model.ETFArchiveTMDBCandidate, 0, len(resp.Results))
	for _, item := range resp.Results {
		if item.MediaType != "movie" && item.MediaType != "tv" {
			continue
		}
		meta := item.metadata()
		applyCategory(meta, cfg.CategoryRules)
		candidates = append(candidates, model.ETFArchiveTMDBCandidate{
			TMDBID:           meta.TMDBID,
			Name:             meta.Name,
			OriginalName:     item.originalDisplayName(),
			Year:             meta.Year,
			MediaType:        meta.MediaType,
			Category:         meta.Category,
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
	titles := []string{normalizeTitle(item.displayName()), normalizeTitle(item.originalDisplayName())}
	target := normalizeTitle(recognized.Title)
	score := 0
	if target != "" {
		for _, title := range titles {
			if title == "" {
				continue
			}
			if title == target {
				score += 80
				break
			}
			if strings.Contains(title, target) || strings.Contains(target, title) {
				score += 55
				break
			}
		}
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
	replacer := strings.NewReplacer(".", " ", "_", " ", "-", " ")
	value = replacer.Replace(value)
	return strings.Join(strings.Fields(value), " ")
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

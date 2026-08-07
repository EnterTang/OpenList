package subscription

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/hdhive"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
)

const defaultResourceSearchLimit = 40

func SearchResources(ctx context.Context, req model.SubscriptionResourceSearchReq) (*model.SubscriptionResourceSearchResp, error) {
	query := strings.TrimSpace(req.Query)
	if query == "" {
		return nil, errors.New("query is required")
	}
	limit := req.Limit
	if limit <= 0 {
		limit = defaultResourceSearchLimit
	}
	sources := normalizeResourceSearchSources(req.Sources)
	cfg, err := GetConfig()
	if err != nil {
		return nil, err
	}
	resp := &model.SubscriptionResourceSearchResp{
		Query:              query,
		Sources:            sources,
		SourceErrors:       map[string]string{},
		SourceCapabilities: ResourceSearchSourceCapabilities(cfg),
	}
	for _, source := range sources {
		var results []model.SubscriptionResourceSearchResult
		var searchErr error
		switch source {
		case model.SubscriptionSourceTelegram:
			results, searchErr = searchTelegramResources(ctx, query, limit, cfg.Telegram)
		case model.SubscriptionSourcePanSou:
			results, searchErr = searchPanSouResources(ctx, query, limit, cfg.PanSou)
		case model.SubscriptionSourceHDHive:
			results, searchErr = searchHDHiveResources(ctx, req, query, limit, cfg.Telegram.HDHive)
		default:
			searchErr = fmt.Errorf("unsupported resource search source: %s", source)
		}
		if searchErr != nil {
			resp.SourceErrors[source] = searchErr.Error()
			continue
		}
		resp.Results = append(resp.Results, results...)
	}
	if len(resp.SourceErrors) == 0 {
		resp.SourceErrors = nil
	}
	return resp, nil
}

// ResourceSearchSourceCapabilities reports configuration-derived availability
// without performing external calls. The frontend can use this before a search
// to avoid offering sources that are guaranteed to fail.
func ResourceSearchSourceCapabilities(cfg model.SubscriptionConfig) map[string]model.SubscriptionSearchSourceCapability {
	telegramConfigured := hasTelegramSearchCommand(cfg.Telegram) || hasBuiltinTelegramConfig(cfg.Telegram)
	telegramAvailable := hasTelegramSearchCommand(cfg.Telegram) ||
		(hasBuiltinTelegramConfig(cfg.Telegram) &&
			(len(telegramChannelGroups(cfg.Telegram)) > 0 || len(cfg.Telegram.Channels) > 0))
	telegramReason := ""
	if !telegramConfigured {
		telegramReason = "not_configured"
	} else if !telegramAvailable {
		telegramReason = "channels_not_configured"
	}

	panSouConfigured := (len(cfg.PanSou.SearchCommand) > 0 && strings.TrimSpace(cfg.PanSou.SearchCommand[0]) != "") ||
		strings.TrimSpace(cfg.PanSou.BaseURL) != ""
	panSouReason := ""
	if !panSouConfigured {
		panSouReason = "not_configured"
	}

	hdhive := cfg.Telegram.HDHive
	hdhiveConfigured := hdhive.Enabled && strings.TrimSpace(hdhive.BaseURL) != "" &&
		strings.TrimSpace(hdhive.UserID) != "" && strings.TrimSpace(hdhive.ProxyUserKey) != "" &&
		strings.TrimSpace(hdhive.ProxySecret) != ""
	hdhiveReason := ""
	if !hdhiveConfigured {
		hdhiveReason = "not_configured"
	}

	return map[string]model.SubscriptionSearchSourceCapability{
		model.SubscriptionSourceTelegram: {
			Configured:        telegramConfigured,
			Available:         telegramAvailable,
			UnavailableReason: telegramReason,
		},
		model.SubscriptionSourcePanSou: {
			Configured:        panSouConfigured,
			Available:         panSouConfigured,
			UnavailableReason: panSouReason,
		},
		model.SubscriptionSourceHDHive: {
			Configured:        hdhiveConfigured,
			Available:         hdhiveConfigured,
			UnavailableReason: hdhiveReason,
		},
	}
}

func normalizeResourceSearchSources(values []string) []string {
	if len(values) == 0 {
		return []string{model.SubscriptionSourceTelegram, model.SubscriptionSourcePanSou}
	}
	seen := map[string]struct{}{}
	var sources []string
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" || value == "all" {
			return []string{model.SubscriptionSourceTelegram, model.SubscriptionSourcePanSou}
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		sources = append(sources, value)
	}
	if len(sources) == 0 {
		return []string{model.SubscriptionSourceTelegram, model.SubscriptionSourcePanSou}
	}
	return sources
}

func searchTelegramResources(ctx context.Context, query string, limit int, cfg model.SubscriptionTelegramSourceConfig) ([]model.SubscriptionResourceSearchResult, error) {
	cfg = normalizeTelegramSourceConfig(cfg)
	cfg.Limit = limit
	sub := &model.Subscription{
		Name:       query,
		TMDBName:   query,
		SourceType: model.SubscriptionSourceTelegram,
	}
	rows, err := runTelegramSearch(ctx, sub, cfg)
	if err != nil {
		return nil, err
	}
	results := make([]model.SubscriptionResourceSearchResult, 0, min(len(rows), limit))
	for _, row := range rows {
		links := rowLinks(row)
		hdhiveLinks, err := resolveTelegramHDHiveLinks(ctx, row, cfg)
		if err != nil {
			return nil, err
		}
		for _, link := range hdhiveLinks {
			links = append(links, normalizeTelegramLinkWithAccessCode(link.URL, link.AccessCode))
		}
		result := model.SubscriptionResourceSearchResult{
			SourceType: model.SubscriptionSourceTelegram,
			Title:      resourceTitle(rowText(row)),
			Content:    resourceContent(rowText(row)),
			Channel:    normalizeTelegramChannel(row.Channel),
			MessageURL: telegramMessageURL(row),
			Date:       strings.TrimSpace(row.Date),
			Links:      resourceLinksFromURLs(links, rowAccessCode(row)),
		}
		result.Provider = firstResultProvider(result.Links)
		if result.Title == "" && len(result.Links) == 0 {
			continue
		}
		results = append(results, result)
		if len(results) >= limit {
			break
		}
	}
	return filterResourceSearchResults(results, query, limit), nil
}

func telegramMessageURL(row telegramCommandRow) string {
	if strings.TrimSpace(row.MessageURL) != "" {
		return strings.TrimSpace(row.MessageURL)
	}
	channel := normalizeTelegramChannel(row.Channel)
	msgID := rowMessageID(row)
	if channel == "" || msgID <= 0 {
		return ""
	}
	return fmt.Sprintf("https://t.me/%s/%d", channel, msgID)
}

func searchPanSouResources(ctx context.Context, query string, limit int, cfg model.SubscriptionPanSouSourceConfig) ([]model.SubscriptionResourceSearchResult, error) {
	return searchPanSouResourcesWithTarget(ctx, query, limit, cfg, nil)
}

func searchPanSouResourcesForSubscription(ctx context.Context, sub *model.Subscription, cfg model.SubscriptionPanSouSourceConfig) ([]model.SubscriptionResourceSearchResult, error) {
	if sub == nil {
		return nil, errors.New("subscription is required")
	}
	query := telegramSearchQuery(sub)
	if query == "" {
		return nil, errors.New("pansou search query is required")
	}
	target := buildResourceMatchTarget(sub, query)
	return searchPanSouResourcesWithTarget(ctx, query, cfg.Limit, cfg, &target)
}

func searchPanSouResourcesWithTarget(ctx context.Context, query string, limit int, cfg model.SubscriptionPanSouSourceConfig, target *resourceMatchTarget) ([]model.SubscriptionResourceSearchResult, error) {
	cfg = normalizePanSouSourceConfig(cfg)
	if len(cfg.SearchCommand) > 0 && strings.TrimSpace(cfg.SearchCommand[0]) != "" {
		stdout, err := runPanSouSearchCommand(ctx, query, limit, cfg)
		if err != nil {
			return nil, err
		}
		results, err := parseResourceSearchOutput(model.SubscriptionSourcePanSou, stdout, limit)
		if err != nil {
			return nil, err
		}
		return filterResourceSearchResultsForTarget(results, buildQueryResourceTarget(query, target), limit), nil
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, errors.New("pansou base_url or search_command is required")
	}
	body, err := requestPanSouSearch(ctx, query, limit, cfg)
	if err != nil {
		return nil, err
	}
	results, err := parseResourceSearchOutput(model.SubscriptionSourcePanSou, body, limit)
	if err != nil {
		return nil, err
	}
	return filterResourceSearchResultsForTarget(results, buildQueryResourceTarget(query, target), limit), nil
}

func buildQueryResourceTarget(query string, target *resourceMatchTarget) resourceMatchTarget {
	if target != nil {
		return *target
	}
	return buildResourceMatchTarget(nil, query)
}

func searchHDHiveResources(ctx context.Context, req model.SubscriptionResourceSearchReq, query string, limit int, cfg model.SubscriptionTelegramHDHiveConfig) ([]model.SubscriptionResourceSearchResult, error) {
	if !cfg.Enabled || strings.TrimSpace(cfg.BaseURL) == "" || strings.TrimSpace(cfg.UserID) == "" || strings.TrimSpace(cfg.ProxyUserKey) == "" || strings.TrimSpace(cfg.ProxySecret) == "" {
		return nil, errors.New("hdhive is not configured")
	}
	mediaType := strings.ToLower(strings.TrimSpace(req.MediaType))
	if mediaType != "tv" && mediaType != "movie" {
		return nil, errors.New("hdhive search requires media_type tv or movie")
	}
	if req.TMDBID <= 0 {
		return nil, errors.New("hdhive search requires a positive tmdb_id")
	}
	if !isHDHiveCloudType(req.CloudType) {
		return nil, fmt.Errorf("unsupported hdhive cloud_type: %s", strings.TrimSpace(req.CloudType))
	}
	client, err := telegramHDHiveClientForConfig(cfg)
	if err != nil {
		return nil, err
	}
	resources, err := client.Search(ctx, mediaType, req.TMDBID)
	if err != nil {
		return nil, err
	}
	resources = filterHDHiveResources(resources, req.CloudType)
	results := make([]model.SubscriptionResourceSearchResult, 0, min(len(resources), limit))
	for _, resource := range resources {
		if len(results) >= limit {
			break
		}
		resourceURL := strings.TrimSpace(resource.ResourceURL)
		if resourceURL == "" {
			resourceURL = defaultHDHiveResourceURL(resource)
		}
		if _, ok := hdhive.ResourceRefFromURL(resourceURL, resource.PanType); !ok {
			resourceURL = defaultHDHiveResourceURL(resource)
		}
		if resourceURL == "" || strings.TrimSpace(resource.Slug) == "" {
			continue
		}
		results = append(results, model.SubscriptionResourceSearchResult{
			SourceType: model.SubscriptionSourceHDHive,
			Provider:   strings.TrimSpace(resource.PanType),
			Title:      firstNonEmpty(resource.Title, query),
			Content:    hdhiveResourceContent(resource),
			Links: []model.SubscriptionResourceSearchLink{{
				URL:        resourceURL,
				Provider:   strings.TrimSpace(resource.PanType),
				Unlockable: true,
				// Search only returns metadata. Even an already-owned item still
				// needs the share endpoint to provide its current share URL.
				Unlocked:     false,
				UnlockPoints: resource.UnlockPoints,
			}},
		})
	}
	return results, nil
}

func isHDHiveCloudType(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "all", "channel_115", "channel_123", "channel_189", "channel_quark", "channel_alipan":
		return true
	default:
		return false
	}
}

func filterHDHiveResources(resources []hdhive.Resource, cloudType string) []hdhive.Resource {
	cloudType = strings.ToLower(strings.TrimSpace(cloudType))
	if cloudType == "" || cloudType == "all" {
		return resources
	}
	allowed := map[string]struct{}{}
	switch cloudType {
	case "channel_115":
		allowed["115"] = struct{}{}
		allowed["ed2k"] = struct{}{}
	case "channel_123":
		allowed["123"] = struct{}{}
	case "channel_189":
		allowed["189"] = struct{}{}
	case "channel_quark":
		allowed["quark"] = struct{}{}
	case "channel_alipan":
		allowed["alipan"] = struct{}{}
	default:
		return nil
	}
	filtered := make([]hdhive.Resource, 0, len(resources))
	for _, resource := range resources {
		if _, ok := allowed[strings.ToLower(strings.TrimSpace(resource.PanType))]; ok {
			filtered = append(filtered, resource)
		}
	}
	return filtered
}

func defaultHDHiveResourceURL(resource hdhive.Resource) string {
	slug := strings.TrimSpace(resource.Slug)
	if slug == "" {
		return ""
	}
	if strings.EqualFold(strings.TrimSpace(resource.PanType), "ed2k") {
		return "https://hdhive.com/resource/" + slug
	}
	panType := strings.TrimSpace(resource.PanType)
	if panType == "" {
		panType = "189"
	}
	return "https://hdhive.com/resource/" + panType + "/" + slug
}

func hdhiveResourceContent(resource hdhive.Resource) string {
	parts := make([]string, 0, 4)
	if resource.PanType != "" {
		parts = append(parts, "网盘: "+resource.PanType)
	}
	if len(resource.VideoResolution) > 0 {
		parts = append(parts, "分辨率: "+strings.Join(resource.VideoResolution, ", "))
	}
	if resource.UnlockPoints != nil {
		parts = append(parts, fmt.Sprintf("解锁积分: %d", *resource.UnlockPoints))
	}
	if resource.Remark != "" {
		parts = append(parts, resource.Remark)
	}
	return strings.Join(parts, " · ")
}

func runPanSouSearchCommand(ctx context.Context, query string, limit int, cfg model.SubscriptionPanSouSourceConfig) ([]byte, error) {
	timeout := time.Duration(cfg.CommandTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	command := cfg.SearchCommand
	cmd := exec.CommandContext(cmdCtx, command[0], command[1:]...)
	cmd.Env = panSouCommandEnv(cfg, limit)
	payload := map[string]any{
		"query": query,
		"kw":    query,
		"limit": limit,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	cmd.Stdin = bytes.NewReader(body)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if errors.Is(cmdCtx.Err(), context.DeadlineExceeded) {
			return nil, errors.New("pansou command timed out")
		}
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return nil, fmt.Errorf("pansou command failed: %w: %s", err, msg)
		}
		return nil, fmt.Errorf("pansou command failed: %w", err)
	}
	return stdout.Bytes(), nil
}

func panSouCommandEnv(cfg model.SubscriptionPanSouSourceConfig, limit int) []string {
	env := append([]string{}, os.Environ()...)
	env = append(env, cfg.CommandEnv...)
	if cfg.BaseURL != "" {
		env = append(env, "PANSOU_BASE_URL="+cfg.BaseURL)
	}
	if limit > 0 {
		env = append(env, "PANSOU_SEARCH_LIMIT="+strconv.Itoa(limit))
	}
	return env
}

func requestPanSouSearch(ctx context.Context, query string, limit int, cfg model.SubscriptionPanSouSourceConfig) ([]byte, error) {
	endpoint, err := panSouSearchEndpoint(cfg.BaseURL)
	if err != nil {
		return nil, err
	}
	timeout := time.Duration(cfg.CommandTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	client := &http.Client{Timeout: timeout}
	body, err := doPanSouSearchRequest(ctx, client, http.MethodGet, endpoint, query, limit)
	if err == nil {
		return body, nil
	}
	postBody, postErr := doPanSouSearchRequest(ctx, client, http.MethodPost, endpoint, query, limit)
	if postErr == nil {
		return postBody, nil
	}
	return nil, err
}

func doPanSouSearchRequest(ctx context.Context, client *http.Client, method, endpoint, query string, limit int) ([]byte, error) {
	var body io.Reader
	target := endpoint
	if method == http.MethodGet {
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return nil, err
		}
		values := parsed.Query()
		values.Set("kw", query)
		values.Set("src", "all")
		if limit > 0 {
			values.Set("limit", strconv.Itoa(limit))
		}
		parsed.RawQuery = values.Encode()
		target = parsed.String()
	} else {
		payload := map[string]any{
			"kw":    query,
			"query": query,
			"src":   "all",
			"limit": limit,
		}
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(data)
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, err
	}
	if method == http.MethodPost {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	httpResp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(httpResp.Body, 8<<20))
	if readErr != nil {
		return nil, readErr
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return nil, fmt.Errorf("pansou search returned HTTP %d: %s", httpResp.StatusCode, trimForDisplay(string(data), 200))
	}
	return data, nil
}

func panSouSearchEndpoint(baseURL string) (string, error) {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return "", errors.New("pansou base_url is required")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	path := strings.TrimRight(parsed.Path, "/")
	switch {
	case strings.HasSuffix(path, "/api/search"):
		parsed.Path = path
	case strings.HasSuffix(path, "/api"):
		parsed.Path = path + "/search"
	default:
		parsed.Path = path + "/api/search"
	}
	return parsed.String(), nil
}

func parseResourceSearchOutput(source string, data []byte, limit int) ([]model.SubscriptionResourceSearchResult, error) {
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		text := strings.TrimSpace(string(data))
		links := resourceLinksFromText(text, "")
		if len(links) == 0 {
			return nil, errors.WithMessage(err, "decode resource search output")
		}
		return []model.SubscriptionResourceSearchResult{{
			SourceType: source,
			Provider:   firstResultProvider(links),
			Title:      resourceTitle(text),
			Content:    resourceContent(text),
			Links:      links,
		}}, nil
	}
	return dedupeResourceSearchResults(resourceResultsFromAny(source, value, limit), limit), nil
}

func resourceResultsFromAny(source string, value any, limit int) []model.SubscriptionResourceSearchResult {
	var results []model.SubscriptionResourceSearchResult
	var walk func(any)
	walk = func(current any) {
		if limit > 0 && len(results) >= limit {
			return
		}
		switch typed := current.(type) {
		case []any:
			for _, item := range typed {
				walk(item)
			}
		case map[string]any:
			if collection, ok := firstResourceCollection(typed); ok {
				walk(collection)
				return
			}
			result := resourceResultFromMap(source, typed)
			if result.Title != "" || result.Content != "" || len(result.Links) > 0 {
				results = append(results, result)
			}
		case string:
			links := resourceLinksFromText(typed, "")
			if len(links) > 0 {
				results = append(results, model.SubscriptionResourceSearchResult{
					SourceType: source,
					Provider:   firstResultProvider(links),
					Title:      resourceTitle(typed),
					Content:    resourceContent(typed),
					Links:      links,
				})
			}
		}
	}
	walk(value)
	return results
}

func firstResourceCollection(item map[string]any) (any, bool) {
	for _, key := range []string{"data", "results", "rows", "list", "items", "messages", "records"} {
		value, ok := item[key]
		if !ok {
			continue
		}
		switch value.(type) {
		case []any:
			return value, true
		}
	}
	return nil, false
}

func resourceResultFromMap(source string, item map[string]any) model.SubscriptionResourceSearchResult {
	content := firstStringValue(item, "content", "text", "message", "raw_text", "description", "desc", "summary")
	title := firstStringValue(item, "title", "name", "filename", "file_name", "share_name", "subject")
	if title == "" {
		title = resourceTitle(content)
	}
	if content == "" {
		content = title
	}
	links := collectResourceLinks(item)
	return model.SubscriptionResourceSearchResult{
		SourceType: source,
		Provider:   firstNonEmpty(firstStringValue(item, "provider", "type", "cloud_type", "cloudType"), firstResultProvider(links)),
		Title:      trimForDisplay(title, 120),
		Content:    resourceContent(content),
		Channel:    firstStringValue(item, "channel", "source", "source_name"),
		MessageURL: firstStringValue(item, "message_url", "messageUrl", "page_url", "pageUrl"),
		Date:       firstStringValue(item, "date", "datetime", "time", "created_at"),
		Links:      links,
	}
}

func firstStringValue(item map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := item[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case string:
			if strings.TrimSpace(typed) != "" {
				return strings.TrimSpace(typed)
			}
		case float64:
			return strconv.FormatFloat(typed, 'f', -1, 64)
		case bool:
			return strconv.FormatBool(typed)
		}
	}
	return ""
}

func collectResourceLinks(value any) []model.SubscriptionResourceSearchLink {
	links := map[string]model.SubscriptionResourceSearchLink{}
	collectResourceLinksInto(value, "", links)
	values := make([]string, 0, len(links))
	for url := range links {
		values = append(values, url)
	}
	sort.Strings(values)
	results := make([]model.SubscriptionResourceSearchLink, 0, len(values))
	for _, url := range values {
		results = append(results, links[url])
	}
	return results
}

func collectResourceLinksInto(value any, passcode string, out map[string]model.SubscriptionResourceSearchLink) {
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			collectResourceLinksInto(item, passcode, out)
		}
	case map[string]any:
		localPasscode := firstNonEmpty(
			firstStringValue(typed, "password", "pwd", "passcode", "access_code", "accessCode", "code"),
			passcode,
		)
		for _, item := range typed {
			collectResourceLinksInto(item, localPasscode, out)
		}
	case string:
		for _, link := range resourceLinksFromText(typed, passcode) {
			out[link.URL] = link
		}
	}
}

func resourceLinksFromURLs(values []string, passcode string) []model.SubscriptionResourceSearchLink {
	out := map[string]model.SubscriptionResourceSearchLink{}
	for _, value := range values {
		for _, link := range resourceLinksFromText(value, passcode) {
			out[link.URL] = link
		}
	}
	keys := make([]string, 0, len(out))
	for key := range out {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	links := make([]model.SubscriptionResourceSearchLink, 0, len(keys))
	for _, key := range keys {
		links = append(links, out[key])
	}
	return links
}

func resourceLinksFromText(value, passcode string) []model.SubscriptionResourceSearchLink {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if passcode == "" {
		passcode = rowAccessCode(telegramCommandRow{Text: value})
	}
	matches := telegramCloudLinkPattern.FindAllString(value, -1)
	matches = append(matches, extractPan123FastLinks(value)...)
	seen := map[string]struct{}{}
	links := make([]model.SubscriptionResourceSearchLink, 0, len(matches))
	for _, match := range matches {
		match = strings.TrimRight(strings.TrimSpace(match), "，,;；)]）>")
		if match == "" {
			continue
		}
		link := normalizeTelegramLinkWithAccessCode(match, passcode)
		if _, ok := seen[link]; ok {
			continue
		}
		provider, ok := DetectShareProvider(link)
		if !ok {
			continue
		}
		seen[link] = struct{}{}
		links = append(links, model.SubscriptionResourceSearchLink{
			URL:      link,
			Provider: string(provider),
		})
	}
	return links
}

func firstResultProvider(links []model.SubscriptionResourceSearchLink) string {
	for _, link := range links {
		if strings.TrimSpace(link.Provider) != "" {
			return strings.TrimSpace(link.Provider)
		}
	}
	return ""
}

func filterResourceSearchResults(results []model.SubscriptionResourceSearchResult, query string, limit int) []model.SubscriptionResourceSearchResult {
	return filterResourceSearchResultsForTarget(results, buildResourceMatchTarget(nil, query), limit)
}

func filterResourceSearchResultsForTarget(results []model.SubscriptionResourceSearchResult, target resourceMatchTarget, limit int) []model.SubscriptionResourceSearchResult {
	candidates := make([]resourceMatchCandidate, 0, len(results))
	for index := range results {
		result := results[index]
		if len(result.Links) == 0 {
			continue
		}
		result.Provider = firstResultProvider(result.Links)
		resultCopy := result
		candidates = append(candidates, resourceMatchCandidate{
			ID:       strconv.Itoa(index),
			Title:    result.Title,
			Provider: result.Provider,
			Result:   &resultCopy,
		})
	}
	ranked := rankResourceCandidates(target, candidates)
	filtered := make([]model.SubscriptionResourceSearchResult, 0, len(ranked))
	seenLinks := map[string]struct{}{}
	for _, candidate := range ranked {
		if candidate.Result == nil {
			continue
		}
		duplicate := false
		for _, link := range candidate.Result.Links {
			key := strings.ToLower(strings.TrimSpace(link.URL))
			if key == "" {
				continue
			}
			if _, ok := seenLinks[key]; ok {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		for _, link := range candidate.Result.Links {
			key := strings.ToLower(strings.TrimSpace(link.URL))
			if key != "" {
				seenLinks[key] = struct{}{}
			}
		}
		filtered = append(filtered, *candidate.Result)
		if limit > 0 && len(filtered) >= limit {
			break
		}
	}
	return filtered
}

func resourceTitle(value string) string {
	value = strings.TrimSpace(value)
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			return trimForDisplay(line, 120)
		}
	}
	return ""
}

func resourceContent(value string) string {
	return trimForDisplay(strings.TrimSpace(value), 1200)
}

func trimForDisplay(value string, limit int) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if limit <= 0 || len(runes) <= limit {
		return value
	}
	return string(runes[:limit]) + "..."
}

func dedupeResourceSearchResults(results []model.SubscriptionResourceSearchResult, limit int) []model.SubscriptionResourceSearchResult {
	seen := map[string]struct{}{}
	deduped := make([]model.SubscriptionResourceSearchResult, 0, len(results))
	for _, result := range results {
		key := result.SourceType + "\x00" + result.Title + "\x00" + result.Content
		for _, link := range result.Links {
			key += "\x00" + link.URL
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		deduped = append(deduped, result)
		if limit > 0 && len(deduped) >= limit {
			break
		}
	}
	return deduped
}

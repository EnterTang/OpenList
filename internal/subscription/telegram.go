package subscription

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/media/recognize"
	"github.com/OpenListTeam/OpenList/v4/internal/media/titlematch"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
)

var (
	telegramANSIEscapePattern = regexp.MustCompile(`\x1b\[[0-9;]*m`)
	telegramCloudLinkPattern  = regexp.MustCompile(`https?://[^\s'"<>，,;\]）]+`)
	telegramAccessCodePattern = regexp.MustCompile(`(?i)(?:访问码|提取码|密码|access(?:\s*code)?|pwd|pass(?:word)?|passcode)[？?:：\s]*([A-Za-z0-9]{4,8})`)
)

type telegramCommandEnvelope struct {
	Rows     []telegramCommandRow `json:"rows"`
	Results  []telegramCommandRow `json:"results"`
	Messages []telegramCommandRow `json:"messages"`
}

type telegramCommandRow struct {
	ID              any      `json:"id"`
	MsgID           any      `json:"msgId"`
	MessageID       any      `json:"message_id"`
	Date            string   `json:"date"`
	Text            string   `json:"text"`
	RawText         string   `json:"raw_text"`
	Caption         string   `json:"caption"`
	MessageURL      string   `json:"message_url"`
	Channel         string   `json:"channel"`
	Links           []string `json:"links"`
	AccessCode      string   `json:"accessCode"`
	AccessCodeSnake string   `json:"access_code"`
	Entities        []struct {
		URL string `json:"url"`
	} `json:"entities"`
	Buttons []struct {
		Text string `json:"text"`
		URL  string `json:"url"`
	} `json:"buttons"`
}

type telegramAuthPayload struct {
	Phone              string `json:"phone,omitempty"`
	Code               string `json:"code,omitempty"`
	PhoneCodeHash      string `json:"phone_code_hash,omitempty"`
	PhoneCodeHashCamel string `json:"phoneCodeHash,omitempty"`
}

type telegramAuthCommandResp struct {
	OK                 bool           `json:"ok,omitempty"`
	Authorized         bool           `json:"authorized"`
	User               map[string]any `json:"user,omitempty"`
	PhoneCodeHash      string         `json:"phone_code_hash,omitempty"`
	PhoneCodeHashCamel string         `json:"phoneCodeHash,omitempty"`
	Error              string         `json:"error,omitempty"`
}

type telegramPanSubscriptionSource struct {
	Name            string
	Config          model.SubscriptionTelegramPanConfig
	BoundShareNames map[string]struct{}
	BoundSharePaths map[string]struct{}
}

type telegramTempCandidate struct {
	Source telegramPanSubscriptionSource
	Entry  TreeEntry
	Item   *model.SubscriptionItem
}

func selectTelegramTempTransferCandidates(sub *model.Subscription, candidates []telegramTempCandidate, priority []string) []telegramTempCandidate {
	priority = normalizeTransferPriority(priority)
	priorityIndex := make(map[string]int, len(priority))
	for index, name := range priority {
		priorityIndex[name] = index
	}
	bestBySlot := make(map[string]telegramTempCandidate, len(candidates))
	for _, candidate := range candidates {
		if candidate.Item == nil || !subscriptionEpisodeMatches(sub, candidate.Item.Season, candidate.Item.Episode) {
			continue
		}
		candidate.Item.SourceProvider = normalizeSubscriptionProvider(candidate.Source.Name)
		slot := mediaSlotKey(sub, candidate.Item)
		if slot == "" {
			continue
		}
		existing, ok := bestBySlot[slot]
		if !ok {
			bestBySlot[slot] = candidate
			continue
		}
		candidateRank := providerPriorityRank(candidate.Source.Name, priorityIndex)
		existingRank := providerPriorityRank(existing.Source.Name, priorityIndex)
		switch {
		case candidateRank < existingRank:
			bestBySlot[slot] = candidate
		case candidateRank == existingRank && candidate.Entry.Size > existing.Entry.Size:
			bestBySlot[slot] = candidate
		}
	}
	selected := make([]telegramTempCandidate, 0, len(bestBySlot))
	for _, candidate := range bestBySlot {
		selected = append(selected, candidate)
	}
	sort.Slice(selected, func(i, j int) bool {
		left, right := selected[i].Item, selected[j].Item
		if left.Season != right.Season {
			return left.Season < right.Season
		}
		if left.Episode != right.Episode {
			return left.Episode < right.Episode
		}
		return providerPriorityRank(selected[i].Source.Name, priorityIndex) < providerPriorityRank(selected[j].Source.Name, priorityIndex)
	})
	return selected
}

func mediaSlotKey(sub *model.Subscription, item *model.SubscriptionItem) string {
	if item == nil {
		return ""
	}
	if normalizeMediaType(sub.MediaType) == "movie" {
		return "movie:" + item.TargetPath
	}
	if item.Episode > 0 {
		return fmt.Sprintf("tv:%d:%d", item.Season, item.Episode)
	}
	return "path:" + item.TargetPath
}

func providerPriorityRank(provider string, priorityIndex map[string]int) int {
	if rank, ok := priorityIndex[strings.TrimSpace(provider)]; ok {
		return rank
	}
	return len(priorityIndex)
}

func telegramSourceNamesByPriority(sources map[string]telegramPanSubscriptionSource, priority []string) []string {
	priority = normalizeTransferPriority(priority)
	seen := map[string]struct{}{}
	names := make([]string, 0, len(sources))
	for _, name := range priority {
		if _, ok := sources[name]; !ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	remaining := make([]string, 0, len(sources))
	for name := range sources {
		if _, ok := seen[name]; ok {
			continue
		}
		remaining = append(remaining, name)
	}
	sort.Strings(remaining)
	return append(names, remaining...)
}

var telegramPanSourceHosts = map[string][]string{
	"quark":        {"pan.quark.cn"},
	"aliyun_drive": {"alipan.com", "aliyundrive.com"},
	"pan123":       {"123pan.com"},
	"pan115":       {"115cdn.com", "115.com"},
}

func runTelegram(ctx context.Context, sub *model.Subscription, transfer bool) ([]model.SubscriptionItem, string, int, int, int, error) {
	cfg, err := parseTelegramConfig(sub.SourceConfig)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	rows, err := runTelegramSearch(ctx, sub, cfg)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	cursor := parseTelegramCursor(sub.LastCursor)
	nextCursor := cursor.clone()
	now := time.Now()
	added := 0
	changed := 0
	transferred := 0
	var saved []model.SubscriptionItem
	triggeredSources := map[string]telegramPanSubscriptionSource{}
	for _, row := range rows {
		msgID := rowMessageID(row)
		if msgID > 0 {
			if telegramCursorHasSeen(cursor, row) {
				continue
			}
			nextCursor.advance(row)
		}
		links, sources := rowLinksForTelegramPanSources(row, cfg)
		for _, source := range sources {
			triggeredSources[source.Name] = mergeBoundShareSource(triggeredSources[source.Name], source)
		}
		accessCode := rowAccessCode(row)
		for _, link := range links {
			var saveErr error
			if len(sources) > 0 {
				var source telegramPanSubscriptionSource
				var handled bool
				source, handled, saveErr = trySaveShareLinkToTemp(ctx, sub, cfg, normalizeTelegramLinkWithAccessCode(link, accessCode))
				if source.Name != "" {
					triggeredSources[source.Name] = mergeBoundShareSource(triggeredSources[source.Name], source)
				}
				if handled {
					continue
				}
			}
			item := telegramLinkItem(sub, row, link, now)
			if saveErr != nil {
				item.LastError = "telegram share URL transfer failed: " + saveErr.Error()
			}
			stored, isNew, err := db.UpsertSubscriptionItem(item)
			if err != nil {
				return saved, sub.LastTreeHash, added, changed, transferred, err
			}
			if isNew {
				added++
			}
			saved = append(saved, *stored)
		}
	}
	tempItems, tempHash, tempAdded, tempChanged, tempTransferred, err := runTelegramTempTransfers(ctx, sub, telegramPanSourcesForTransfer(cfg, triggeredSources), cfg, transfer, now)
	if err != nil {
		return saved, sub.LastTreeHash, added, changed, transferred, err
	}
	saved = append(saved, tempItems...)
	added += tempAdded
	changed += tempChanged
	transferred += tempTransferred
	if formatted := formatTelegramCursor(nextCursor); formatted != strings.TrimSpace(sub.LastCursor) {
		sub.LastCursor = formatted
	}
	hash := telegramRowsHash(rows)
	if tempHash != "" {
		hash = combinedHash(hash, []string{tempHash})
	}
	return saved, hash, added, changed, transferred, nil
}

func TelegramAuth(ctx context.Context, subscriptionID uint, action string, req model.SubscriptionTelegramAuthReq) (model.SubscriptionTelegramAuthResp, error) {
	cfg, err := telegramAuthConfig(subscriptionID)
	if err != nil {
		return model.SubscriptionTelegramAuthResp{}, err
	}
	payload := telegramAuthPayload{
		Phone:              strings.TrimSpace(req.Phone),
		Code:               strings.TrimSpace(req.Code),
		PhoneCodeHash:      strings.TrimSpace(req.PhoneCodeHash),
		PhoneCodeHashCamel: strings.TrimSpace(req.PhoneCodeHash),
	}
	result, err := runTelegramAuth(ctx, cfg, action, payload)
	if err != nil {
		return model.SubscriptionTelegramAuthResp{}, err
	}
	return model.SubscriptionTelegramAuthResp{
		OK:            result.OK,
		Authorized:    result.Authorized,
		User:          result.User,
		PhoneCodeHash: firstNonEmpty(result.PhoneCodeHash, result.PhoneCodeHashCamel),
		Error:         result.Error,
	}, nil
}

func telegramAuthConfig(subscriptionID uint) (model.SubscriptionTelegramSourceConfig, error) {
	if subscriptionID == 0 {
		cfg, err := GetConfig()
		if err != nil {
			return model.SubscriptionTelegramSourceConfig{}, err
		}
		return cfg.Telegram, nil
	}
	sub, err := db.GetSubscriptionByID(subscriptionID)
	if err != nil {
		return model.SubscriptionTelegramSourceConfig{}, err
	}
	if err := ApplyDefaults(sub); err != nil {
		return model.SubscriptionTelegramSourceConfig{}, err
	}
	return parseTelegramConfig(sub.SourceConfig)
}

func parseTelegramConfig(raw string) (model.SubscriptionTelegramSourceConfig, error) {
	var cfg model.SubscriptionTelegramSourceConfig
	if strings.TrimSpace(raw) == "" {
		return cfg, errors.New("telegram source_config is required")
	}
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return cfg, errors.WithMessage(err, "invalid telegram source config")
	}
	return normalizeTelegramSourceConfig(cfg), nil
}

func runTelegramSearchCommand(ctx context.Context, sub *model.Subscription, cfg model.SubscriptionTelegramSourceConfig) ([]telegramCommandRow, error) {
	if len(cfg.SearchCommand) == 0 || strings.TrimSpace(cfg.SearchCommand[0]) == "" {
		return nil, errors.New("telegram search_command is not configured")
	}
	query := telegramSearchQuery(sub)
	if query == "" {
		return nil, errors.New("telegram search query is required")
	}
	payload := map[string]any{
		"channels": cfg.Channels,
		"query":    query,
		"limit":    cfg.Limit,
	}
	stdout, err := runTelegramCommand(ctx, cfg, cfg.SearchCommand, nil, payload)
	if err != nil {
		return nil, err
	}
	return parseTelegramRows(stdout)
}

func telegramPanSourcesForTransfer(cfg model.SubscriptionTelegramSourceConfig, triggered map[string]telegramPanSubscriptionSource) map[string]telegramPanSubscriptionSource {
	merged := make(map[string]telegramPanSubscriptionSource, len(triggered)+4)
	for _, source := range telegramPanSources(cfg) {
		if strings.TrimSpace(source.Config.TempTransferRoot) == "" {
			continue
		}
		merged[source.Name] = source
	}
	for name, source := range triggered {
		merged[name] = source
	}
	return merged
}

func runTelegramTempTransfers(ctx context.Context, sub *model.Subscription, sources map[string]telegramPanSubscriptionSource, cfg model.SubscriptionTelegramSourceConfig, transfer bool, seenAt time.Time) ([]model.SubscriptionItem, string, int, int, int, error) {
	if len(sources) == 0 {
		return nil, "", 0, 0, 0, nil
	}
	var saved []model.SubscriptionItem
	var snapshotHashes []string
	added := 0
	changed := 0
	transferred := 0
	priority := cfg.TransferPriority
	var candidates []telegramTempCandidate
	for _, name := range telegramSourceNamesByPriority(sources, priority) {
		source := sources[name]
		root := strings.TrimSpace(source.Config.TempTransferRoot)
		if root == "" {
			continue
		}
		snapshot, err := snapshotPaths(ctx, []string{root})
		if err != nil {
			if errs.IsObjectNotFound(err) {
				continue
			}
			return saved, "", added, changed, transferred, err
		}
		snapshotHashes = append(snapshotHashes, source.Name+":"+snapshot.Hash)
		for _, entry := range MediaFiles(snapshot.Entries) {
			if !entryMatchesSubscriptionOrBoundShare(sub, entry, source.BoundShareNames, source.BoundSharePaths) {
				continue
			}
			item := itemFromEntry(sub, entry, seenAt)
			candidates = append(candidates, telegramTempCandidate{
				Source: source,
				Entry:  entry,
				Item:   item,
			})
		}
	}
	resultHash := ""
	if len(snapshotHashes) > 0 {
		resultHash = combinedHash("telegram-temp", snapshotHashes)
	}
	return finalizeTempTransferCandidates(ctx, sub, candidates, priority, transfer, seenAt, resultHash)
}

func applyTelegramTempTransferCandidates(ctx context.Context, sub *model.Subscription, selected []telegramTempCandidate, transfer bool, seenAt time.Time, resultHash string) ([]model.SubscriptionItem, string, int, int, int, error) {
	var saved []model.SubscriptionItem
	added := 0
	changed := 0
	transferred := 0
	seenKeys := map[string]struct{}{}
	for _, candidate := range selected {
		item := candidate.Item
		if _, ok := seenKeys[item.SourceKey]; ok {
			continue
		}
		seenKeys[item.SourceKey] = struct{}{}
		stored, isNew, err := db.UpsertSubscriptionItem(item)
		if err != nil {
			return saved, resultHash, added, changed, transferred, err
		}
		if isNew {
			added++
		}
		beforePath := stored.TargetPath
		beforeStatus := stored.Status
		stored = syncSubscriptionItemPaths(stored, sub, candidate.Entry, seenAt)
		if !isNew {
			switch {
			case beforeStatus == model.SubscriptionItemStatusFailed:
				stored.Status = model.SubscriptionItemStatusPending
				stored.LastError = ""
				changed++
			case beforeStatus == model.SubscriptionItemStatusPending && stored.TargetPath != beforePath:
				changed++
			}
		}
		if transfer && sub.TransferEnabled && stored.SourcePath != "" && stored.TargetPath != "" && stored.Status == model.SubscriptionItemStatusPending {
			var delta int
			stored, delta, err = applyItemTransfer(ctx, stored, candidate.Source.Config.DeleteSourceAfter)
			if err != nil {
				return saved, resultHash, added, changed, transferred, err
			}
			transferred += delta
		}
		saved = append(saved, *stored)
	}
	if resultHash == "" {
		return saved, "", added, changed, transferred, nil
	}
	return saved, resultHash, added, changed, transferred, nil
}

func telegramSearchQuery(sub *model.Subscription) string {
	if sub == nil {
		return ""
	}
	if query := strings.TrimSpace(sub.TMDBName); query != "" {
		return query
	}
	return strings.TrimSpace(sub.Name)
}

func telegramPanSources(cfg model.SubscriptionTelegramSourceConfig) []telegramPanSubscriptionSource {
	candidates := []telegramPanSubscriptionSource{
		{Name: "quark", Config: cfg.Quark},
		{Name: "aliyun_drive", Config: cfg.AliyunDrive},
		{Name: "pan123", Config: cfg.Pan123},
		{Name: "pan115", Config: cfg.Pan115},
	}
	sources := make([]telegramPanSubscriptionSource, 0, len(candidates))
	for _, candidate := range candidates {
		candidate.Config = normalizeTelegramPanConfig(candidate.Config)
		if len(candidate.Config.Channels) == 0 && candidate.Config.TempTransferRoot == "" {
			continue
		}
		sources = append(sources, candidate)
	}
	return sources
}

func telegramPanSourceForRow(row telegramCommandRow, cfg model.SubscriptionTelegramSourceConfig) (telegramPanSubscriptionSource, bool) {
	sources := telegramPanSourcesForRow(row, cfg)
	if len(sources) == 0 {
		return telegramPanSubscriptionSource{}, false
	}
	return sources[0], true
}

func telegramPanSourcesForRow(row telegramCommandRow, cfg model.SubscriptionTelegramSourceConfig) []telegramPanSubscriptionSource {
	channel := normalizeTelegramChannel(row.Channel)
	if channel == "" {
		return nil
	}
	var matched []telegramPanSubscriptionSource
	for _, source := range telegramPanSources(cfg) {
		if channelInList(channel, source.Config.Channels) {
			matched = append(matched, source)
		}
	}
	return matched
}

func channelInList(channel string, channels []string) bool {
	channel = normalizeTelegramChannel(channel)
	if channel == "" {
		return false
	}
	for _, candidate := range channels {
		if strings.EqualFold(channel, normalizeTelegramChannel(candidate)) {
			return true
		}
	}
	return false
}

func subscriptionEntryMatches(sub *model.Subscription, entry TreeEntry) bool {
	needles := subscriptionMatchNeedles(sub)
	if len(needles) == 0 {
		return false
	}
	if !subscriptionSeasonMatches(sub, entry) {
		return false
	}
	haystacks := []string{
		strings.TrimSpace(entry.Name),
		strings.TrimSpace(entry.Path),
		strings.TrimSpace(fullPath(entry)),
	}
	for _, needle := range needles {
		for _, haystack := range haystacks {
			if haystack != "" && titlematch.TitlesCompatible(needle, haystack) {
				return true
			}
		}
	}
	return false
}

func boundShareEntryMatches(sub *model.Subscription, entry TreeEntry) bool {
	return isMediaEntry(entry) && subscriptionSeasonMatches(sub, entry)
}

func subscriptionSeasonMatches(sub *model.Subscription, entry TreeEntry) bool {
	seasons := selectedSubscriptionSeasons(sub)
	season, episode := entrySeasonEpisode(entry)
	if (season <= 0 || episode <= 0) && hasSubscriptionEpisodeRange(sub) {
		planned := PlanTarget(planInputFromSubscription(sub), entry.Name, parentPath(entry))
		if season <= 0 {
			season = planned.Season
		}
		if episode <= 0 {
			episode = planned.Episode
		}
	}
	if len(seasons) > 0 && season > 0 {
		if _, ok := seasons[season]; !ok {
			return false
		}
	}
	return subscriptionEpisodeMatches(sub, season, episode)
}

func selectedSubscriptionSeasons(sub *model.Subscription) map[int]struct{} {
	if sub == nil || strings.EqualFold(strings.TrimSpace(sub.MediaType), "movie") {
		return nil
	}
	values := append([]int(nil), sub.Seasons...)
	if len(values) == 0 && sub.Season > 0 {
		values = append(values, sub.Season)
	}
	if len(values) == 0 {
		return nil
	}
	seasons := make(map[int]struct{}, len(values))
	for _, season := range values {
		if season > 0 {
			seasons[season] = struct{}{}
		}
	}
	return seasons
}

func entrySeason(entry TreeEntry) int {
	season, _ := entrySeasonEpisode(entry)
	return season
}

func entrySeasonEpisode(entry TreeEntry) (int, int) {
	recognized := recognize.Recognize(entry.Name, parentPath(entry))
	season, episode := recognized.Season, recognized.Episode
	if season <= 0 {
		season = inferSeason(parentPath(entry))
	}
	if season <= 0 || episode <= 0 {
		pathSeason, pathEpisode := recognize.ExtractSeasonEpisode(entry.Path)
		if season <= 0 {
			season = pathSeason
		}
		if episode <= 0 {
			episode = pathEpisode
		}
	}
	if episode <= 0 {
		episode = inferLeadingEpisode(entry.Name)
	}
	return season, episode
}

func subscriptionEpisodeMatches(sub *model.Subscription, season, episode int) bool {
	if sub == nil || strings.EqualFold(strings.TrimSpace(sub.MediaType), "movie") {
		return true
	}
	start, end := sub.LatestSeasonEpisodeStart, sub.LatestSeasonEpisodeEnd
	if !hasSubscriptionEpisodeRange(sub) {
		return true
	}
	latestSeason := latestSubscriptionSeason(sub)
	if latestSeason <= 0 || season <= 0 || season != latestSeason {
		return true
	}
	if episode <= 0 {
		return false
	}
	if start > 0 && episode < start {
		return false
	}
	return end <= 0 || episode <= end
}

func hasSubscriptionEpisodeRange(sub *model.Subscription) bool {
	return sub != nil && (sub.LatestSeasonEpisodeStart > 0 || sub.LatestSeasonEpisodeEnd > 0)
}

func latestSubscriptionSeason(sub *model.Subscription) int {
	if sub == nil {
		return 0
	}
	latest := sub.Season
	for _, season := range sub.Seasons {
		if season > latest {
			latest = season
		}
	}
	return latest
}

func subscriptionMatchNeedles(sub *model.Subscription) []string {
	if sub == nil {
		return nil
	}
	candidates := []string{sub.TMDBName, sub.Name}
	if sub.TMDBYear > 0 {
		for _, title := range []string{sub.TMDBName, sub.Name} {
			if strings.TrimSpace(title) != "" {
				candidates = append(candidates, fmt.Sprintf("%s %d", title, sub.TMDBYear))
			}
		}
	}
	seen := map[string]struct{}{}
	needles := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		for _, needle := range titlematch.BuildMediaQueryCandidates(candidate) {
			if len([]rune(needle)) < 2 {
				continue
			}
			if _, ok := seen[needle]; ok {
				continue
			}
			seen[needle] = struct{}{}
			needles = append(needles, needle)
		}
	}
	return needles
}

func normalizeMediaMatchText(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var builder strings.Builder
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

func runTelegramSearch(ctx context.Context, sub *model.Subscription, cfg model.SubscriptionTelegramSourceConfig) ([]telegramCommandRow, error) {
	if hasTelegramSearchCommand(cfg) {
		return runTelegramSearchCommand(ctx, sub, cfg)
	}
	if hasBuiltinTelegramConfig(cfg) {
		return builtinTelegramSearch(ctx, sub, cfg)
	}
	return nil, errors.New("telegram search backend is not configured")
}

func runTelegramAuth(ctx context.Context, cfg model.SubscriptionTelegramSourceConfig, action string, payload telegramAuthPayload) (telegramAuthCommandResp, error) {
	if hasTelegramAuthCommand(cfg) {
		return runTelegramAuthCommand(ctx, cfg, action, payload)
	}
	if hasBuiltinTelegramConfig(cfg) {
		return builtinTelegramAuth(ctx, cfg, action, payload)
	}
	if action == "status" {
		return telegramAuthCommandResp{Authorized: false}, nil
	}
	return telegramAuthCommandResp{}, errors.New("telegram login backend is not configured")
}

func runTelegramAuthCommand(ctx context.Context, cfg model.SubscriptionTelegramSourceConfig, action string, payload telegramAuthPayload) (telegramAuthCommandResp, error) {
	command := cfg.AuthCommand
	if len(command) == 0 {
		command = telegramAuthCommandFromSearch(cfg.SearchCommand)
	}
	if len(command) == 0 || strings.TrimSpace(command[0]) == "" {
		if action == "status" {
			return telegramAuthCommandResp{Authorized: false}, nil
		}
		return telegramAuthCommandResp{}, errors.New("telegram login backend is not configured")
	}
	stdout, err := runTelegramCommand(ctx, cfg, command, []string{action}, payload)
	if err != nil {
		return telegramAuthCommandResp{}, err
	}
	var result telegramAuthCommandResp
	if err := json.Unmarshal(stdout, &result); err != nil {
		return telegramAuthCommandResp{}, errors.WithMessage(err, "decode telegram auth command output")
	}
	if result.Error != "" {
		return telegramAuthCommandResp{}, errors.New(result.Error)
	}
	return result, nil
}

func hasTelegramSearchCommand(cfg model.SubscriptionTelegramSourceConfig) bool {
	return len(cfg.SearchCommand) > 0 && strings.TrimSpace(cfg.SearchCommand[0]) != ""
}

func hasTelegramAuthCommand(cfg model.SubscriptionTelegramSourceConfig) bool {
	command := cfg.AuthCommand
	if len(command) == 0 {
		command = telegramAuthCommandFromSearch(cfg.SearchCommand)
	}
	return len(command) > 0 && strings.TrimSpace(command[0]) != ""
}

func hasBuiltinTelegramConfig(cfg model.SubscriptionTelegramSourceConfig) bool {
	return cfg.APIID > 0 && strings.TrimSpace(cfg.APIHash) != ""
}

func runTelegramCommand(ctx context.Context, cfg model.SubscriptionTelegramSourceConfig, command []string, extraArgs []string, payload any) ([]byte, error) {
	timeout := time.Duration(cfg.CommandTimeoutSeconds) * time.Second
	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	args := append([]string{}, command[1:]...)
	args = append(args, extraArgs...)
	cmd := exec.CommandContext(cmdCtx, command[0], args...)
	cmd.Env = telegramCommandEnv(cfg)
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
			return nil, errors.New("telegram command timed out")
		}
		if msg := cleanTelegramCommandError(stderr.String()); msg != "" {
			return nil, fmt.Errorf("telegram command failed: %w: %s", err, msg)
		}
		return nil, fmt.Errorf("telegram command failed: %w", err)
	}
	return stdout.Bytes(), nil
}

func telegramCommandEnv(cfg model.SubscriptionTelegramSourceConfig) []string {
	env := append([]string{}, os.Environ()...)
	env = append(env, cfg.CommandEnv...)
	if cfg.APIID > 0 {
		env = append(env, "TELEGRAM_API_ID="+strconv.Itoa(cfg.APIID))
	}
	if cfg.APIHash != "" {
		env = append(env, "TELEGRAM_API_HASH="+cfg.APIHash)
	}
	if cfg.SessionFile != "" {
		env = append(env, "TELEGRAM_SESSION_FILE="+cfg.SessionFile)
	}
	if cfg.Limit > 0 {
		env = append(env, "TELEGRAM_SEARCH_LIMIT="+strconv.Itoa(cfg.Limit))
	}
	return env
}

func telegramAuthCommandFromSearch(search []string) []string {
	if len(search) == 0 {
		return nil
	}
	command := append([]string(nil), search...)
	last := filepath.Base(command[len(command)-1])
	if strings.HasPrefix(last, "telegram_search.") {
		command[len(command)-1] = strings.TrimSuffix(command[len(command)-1], last) + "telegram_auth.mjs"
		return command
	}
	return nil
}

func parseTelegramRows(data []byte) ([]telegramCommandRow, error) {
	var rows []telegramCommandRow
	if err := json.Unmarshal(data, &rows); err == nil {
		return rows, nil
	}
	var envelope telegramCommandEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, errors.WithMessage(err, "decode telegram command output")
	}
	for _, candidate := range [][]telegramCommandRow{envelope.Rows, envelope.Results, envelope.Messages} {
		if len(candidate) > 0 {
			return candidate, nil
		}
	}
	return nil, nil
}

func telegramLinkItem(sub *model.Subscription, row telegramCommandRow, link string, seenAt time.Time) *model.SubscriptionItem {
	msgID := rowMessageID(row)
	channel := normalizeTelegramChannel(row.Channel)
	sourceURL := normalizeTelegramLinkWithAccessCode(link, rowAccessCode(row))
	keyMaterial := fmt.Sprintf("%d:%s:%d:%s", sub.ID, channel, msgID, sourceURL)
	sum := sha256.Sum256([]byte(keyMaterial))
	sourceKey := hex.EncodeToString(sum[:])
	if msgID > 0 && channel != "" {
		sourceKey = fmt.Sprintf("telegram:%s:%d:%s", channel, msgID, shortHash(sourceURL))
	}
	return &model.SubscriptionItem{
		SubscriptionID: sub.ID,
		SourceKey:      sourceKey,
		SourceProvider: sourceProviderFromURL(sourceURL),
		SourceURL:      sourceURL,
		FileHash:       shortHash(sourceURL),
		Status:         model.SubscriptionItemStatusSkipped,
		LastSeenAt:     seenAt,
		LastError:      "telegram share URL is discovered; mount or provider transfer is required before file-tree checks",
	}
}

func rowLinks(row telegramCommandRow) []string {
	seen := map[string]struct{}{}
	var links []string
	appendLink := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		value = strings.TrimRight(value, "，,;；？?)]）>")
		if !isPan123FastLink(value) {
			if _, err := url.ParseRequestURI(value); err != nil {
				return
			}
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		links = append(links, value)
	}
	for _, link := range row.Links {
		appendLink(link)
	}
	for _, entity := range row.Entities {
		appendLink(entity.URL)
	}
	for _, button := range row.Buttons {
		appendLink(button.URL)
	}
	for _, match := range telegramCloudLinkPattern.FindAllString(rowText(row), -1) {
		appendLink(match)
	}
	for _, match := range extractPan123FastLinks(rowText(row)) {
		appendLink(match)
	}
	return links
}

func rowLinksForTelegramPanSources(row telegramCommandRow, cfg model.SubscriptionTelegramSourceConfig) ([]string, []telegramPanSubscriptionSource) {
	links := rowLinks(row)
	sources := telegramPanSourcesForRow(row, cfg)
	if len(sources) == 0 {
		return links, nil
	}
	var filtered []string
	triggered := make([]telegramPanSubscriptionSource, 0, len(sources))
	triggeredNames := map[string]struct{}{}
	for _, link := range links {
		for _, source := range sources {
			if telegramPanSourceAcceptsLink(source.Name, link) {
				filtered = append(filtered, link)
				if _, ok := triggeredNames[source.Name]; !ok {
					triggeredNames[source.Name] = struct{}{}
					triggered = append(triggered, source)
				}
				break
			}
		}
	}
	return filtered, triggered
}

func telegramPanSourceAcceptsLink(sourceName, link string) bool {
	if isPan123FastLink(link) {
		return sourceName == string(ShareProviderPan123)
	}
	hosts := telegramPanSourceHosts[sourceName]
	if len(hosts) == 0 {
		return true
	}
	parsed, err := url.Parse(link)
	if err != nil {
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	for _, allowed := range hosts {
		if hostMatchesDomain(host, allowed) {
			return true
		}
	}
	return false
}

func hostMatchesDomain(host, domain string) bool {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	domain = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(domain)), ".")
	if host == "" || domain == "" {
		return false
	}
	return host == domain || strings.HasSuffix(host, "."+domain)
}

func rowText(row telegramCommandRow) string {
	return strings.Join([]string{row.Text, row.RawText, row.Caption}, "\n")
}

func rowAccessCode(row telegramCommandRow) string {
	if code := firstNonEmpty(row.AccessCode, row.AccessCodeSnake); code != "" {
		return code
	}
	matches := telegramAccessCodePattern.FindStringSubmatch(rowText(row))
	if len(matches) == 2 {
		return strings.TrimSpace(matches[1])
	}
	return ""
}

func normalizeTelegramLinkWithAccessCode(link, accessCode string) string {
	link = strings.TrimSpace(link)
	accessCode = strings.TrimSpace(accessCode)
	if link == "" || accessCode == "" || strings.Contains(link, ",") || isPan123FastLink(link) {
		return link
	}
	return link + "," + accessCode
}

func rowMessageID(row telegramCommandRow) int64 {
	for _, value := range []any{row.MsgID, row.MessageID, row.ID} {
		switch v := value.(type) {
		case float64:
			return int64(v)
		case int:
			return int64(v)
		case int64:
			return v
		case string:
			if id, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
				return id
			}
		}
	}
	return 0
}

type telegramCursor struct {
	Legacy   int64
	Channels map[string]int64
}

func parseTelegramCursor(value string) telegramCursor {
	value = strings.TrimSpace(value)
	if value == "" {
		return telegramCursor{}
	}
	if strings.HasPrefix(value, "{") {
		var channels map[string]int64
		if err := json.Unmarshal([]byte(value), &channels); err == nil {
			normalized := make(map[string]int64, len(channels))
			for channel, cursor := range channels {
				key := telegramCursorChannelKey(channel)
				if key == "" || cursor <= 0 {
					continue
				}
				if cursor > normalized[key] {
					normalized[key] = cursor
				}
			}
			return telegramCursor{Channels: normalized}
		}
	}
	cursor, _ := strconv.ParseInt(value, 10, 64)
	return telegramCursor{Legacy: cursor}
}

func telegramCursorHasSeen(cursor telegramCursor, row telegramCommandRow) bool {
	msgID := rowMessageID(row)
	if msgID <= 0 {
		return false
	}
	channel := telegramCursorChannelKey(row.Channel)
	if channel != "" {
		return cursor.Channels != nil && msgID <= cursor.Channels[channel]
	}
	return cursor.Legacy > 0 && msgID <= cursor.Legacy
}

func (cursor telegramCursor) clone() telegramCursor {
	next := telegramCursor{Legacy: cursor.Legacy}
	if len(cursor.Channels) > 0 {
		next.Channels = make(map[string]int64, len(cursor.Channels))
		for channel, value := range cursor.Channels {
			next.Channels[channel] = value
		}
	}
	return next
}

func (cursor *telegramCursor) advance(row telegramCommandRow) {
	msgID := rowMessageID(row)
	if msgID <= 0 {
		return
	}
	channel := telegramCursorChannelKey(row.Channel)
	if channel == "" {
		if msgID > cursor.Legacy {
			cursor.Legacy = msgID
		}
		return
	}
	if cursor.Channels == nil {
		cursor.Channels = map[string]int64{}
	}
	if msgID > cursor.Channels[channel] {
		cursor.Channels[channel] = msgID
	}
}

func formatTelegramCursor(cursor telegramCursor) string {
	if len(cursor.Channels) > 0 {
		body, _ := json.Marshal(cursor.Channels)
		return string(body)
	}
	if cursor.Legacy > 0 {
		return strconv.FormatInt(cursor.Legacy, 10)
	}
	return ""
}

func telegramCursorChannelKey(channel string) string {
	return strings.ToLower(normalizeTelegramChannel(channel))
}

func normalizeTelegramChannel(channel string) string {
	channel = strings.TrimSpace(channel)
	channel = strings.TrimPrefix(channel, "https://t.me/")
	channel = strings.TrimPrefix(channel, "http://t.me/")
	channel = strings.TrimPrefix(channel, "t.me/")
	channel = strings.TrimPrefix(channel, "@")
	return strings.Trim(channel, "/")
}

func telegramRowsHash(rows []telegramCommandRow) string {
	body, _ := json.Marshal(rows)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:16]
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func cleanTelegramCommandError(raw string) string {
	raw = telegramANSIEscapePattern.ReplaceAllString(raw, "")
	lines := strings.Split(raw, "\n")
	var cleaned []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" ||
			strings.HasPrefix(line, "at ") ||
			strings.Contains(line, "/node_modules/telegram/") ||
			strings.Contains(line, "Running gramJS version") {
			continue
		}
		cleaned = append(cleaned, line)
	}
	return strings.Join(cleaned, "\n")
}

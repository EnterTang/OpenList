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
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
)

var (
	telegramANSIEscapePattern = regexp.MustCompile(`\x1b\[[0-9;]*m`)
	telegramCloudLinkPattern  = regexp.MustCompile(`https?://[^\s'"<>，,;；)\]）]+`)
	telegramAccessCodePattern = regexp.MustCompile(`(?i)(?:访问码|提取码|密码|access(?:\s*code)?|pwd|pass(?:word)?|passcode)[：:\s]*([A-Za-z0-9]{4,8})`)
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

func runTelegram(ctx context.Context, sub *model.Subscription, transfer bool) ([]model.SubscriptionItem, string, int, int, int, error) {
	cfg, err := parseTelegramConfig(sub.SourceConfig)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	rows, err := runTelegramSearchCommand(ctx, sub, cfg)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	lastSeenMsgID := parseCursor(sub.LastCursor)
	nextCursor := lastSeenMsgID
	now := time.Now()
	added := 0
	var saved []model.SubscriptionItem
	for _, row := range rows {
		msgID := rowMessageID(row)
		if msgID > 0 {
			if msgID <= lastSeenMsgID {
				continue
			}
			if msgID > nextCursor {
				nextCursor = msgID
			}
		}
		for _, link := range rowLinks(row) {
			item := telegramLinkItem(sub, row, link, now)
			stored, isNew, err := db.UpsertSubscriptionItem(item)
			if err != nil {
				return saved, sub.LastTreeHash, added, 0, 0, err
			}
			if isNew {
				added++
			}
			saved = append(saved, *stored)
		}
	}
	if nextCursor > lastSeenMsgID {
		sub.LastCursor = strconv.FormatInt(nextCursor, 10)
	}
	hash := telegramRowsHash(rows)
	return saved, hash, added, 0, 0, nil
}

func TelegramAuth(ctx context.Context, subscriptionID uint, action string, req model.SubscriptionTelegramAuthReq) (model.SubscriptionTelegramAuthResp, error) {
	sub, err := db.GetSubscriptionByID(subscriptionID)
	if err != nil {
		return model.SubscriptionTelegramAuthResp{}, err
	}
	if err := ApplyDefaults(sub); err != nil {
		return model.SubscriptionTelegramAuthResp{}, err
	}
	cfg, err := parseTelegramConfig(sub.SourceConfig)
	if err != nil {
		return model.SubscriptionTelegramAuthResp{}, err
	}
	payload := telegramAuthPayload{
		Phone:              strings.TrimSpace(req.Phone),
		Code:               strings.TrimSpace(req.Code),
		PhoneCodeHash:      strings.TrimSpace(req.PhoneCodeHash),
		PhoneCodeHashCamel: strings.TrimSpace(req.PhoneCodeHash),
	}
	result, err := runTelegramAuthCommand(ctx, cfg, action, payload)
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

func parseTelegramConfig(raw string) (model.SubscriptionTelegramSourceConfig, error) {
	var cfg model.SubscriptionTelegramSourceConfig
	if strings.TrimSpace(raw) == "" {
		return cfg, errors.New("telegram source_config is required")
	}
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return cfg, errors.WithMessage(err, "invalid telegram source config")
	}
	cfg.Channels = cleanStringList(cfg.Channels, false)
	cfg.CommandEnv = cleanStringList(cfg.CommandEnv, false)
	if cfg.Limit <= 0 {
		cfg.Limit = 40
	}
	if cfg.CommandTimeoutSeconds <= 0 {
		cfg.CommandTimeoutSeconds = 30
	}
	cfg.SessionFile = strings.TrimSpace(cfg.SessionFile)
	cfg.Query = strings.TrimSpace(cfg.Query)
	return cfg, nil
}

func runTelegramSearchCommand(ctx context.Context, sub *model.Subscription, cfg model.SubscriptionTelegramSourceConfig) ([]telegramCommandRow, error) {
	if len(cfg.SearchCommand) == 0 || strings.TrimSpace(cfg.SearchCommand[0]) == "" {
		return nil, errors.New("telegram search_command is not configured")
	}
	query := cfg.Query
	if query == "" {
		query = sub.TMDBName
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

func runTelegramAuthCommand(ctx context.Context, cfg model.SubscriptionTelegramSourceConfig, action string, payload telegramAuthPayload) (telegramAuthCommandResp, error) {
	command := cfg.AuthCommand
	if len(command) == 0 {
		command = telegramAuthCommandFromSearch(cfg.SearchCommand)
	}
	if len(command) == 0 || strings.TrimSpace(command[0]) == "" {
		return telegramAuthCommandResp{}, errors.New("telegram auth_command is not configured")
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
		value = strings.TrimRight(value, "，,;；)]）>")
		if _, err := url.ParseRequestURI(value); err != nil {
			return
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
	return links
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
	if link == "" || accessCode == "" || strings.Contains(link, ",") {
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

func parseCursor(value string) int64 {
	cursor, _ := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	return cursor
}

func normalizeTelegramChannel(channel string) string {
	channel = strings.TrimSpace(channel)
	return strings.TrimPrefix(channel, "@")
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

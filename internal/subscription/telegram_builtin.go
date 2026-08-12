package subscription

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/cmd/flags"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
	"github.com/pkg/errors"
)

var (
	builtinTelegramAuth   = runBuiltinTelegramAuth
	builtinTelegramSearch = runBuiltinTelegramSearch
)

const (
	builtinTelegramRetryMaxAttempts = 3
	builtinTelegramRetryInitialWait = 100 * time.Millisecond
	builtinTelegramRetryMaxWait     = 200 * time.Millisecond
	builtinTelegramTimeoutMessage   = "telegram request timed out"
)

func runBuiltinTelegramAuth(ctx context.Context, cfg model.SubscriptionTelegramSourceConfig, action string, payload telegramAuthPayload) (telegramAuthCommandResp, error) {
	var result telegramAuthCommandResp
	err := runBuiltinTelegramClient(ctx, cfg, false, func(ctx context.Context, client *telegram.Client) error {
		switch action {
		case "status":
			status, err := client.Auth().Status(ctx)
			if err != nil {
				return formatBuiltinTelegramError(err)
			}
			result = telegramAuthCommandResp{
				OK:         true,
				Authorized: status.Authorized,
				User:       telegramUserMap(status.User),
			}
		case "send-code":
			phone := strings.TrimSpace(payload.Phone)
			if phone == "" {
				return errors.New("phone is required")
			}
			sent, err := client.API().AuthSendCode(ctx, &tg.AuthSendCodeRequest{
				PhoneNumber: phone,
				APIID:       cfg.APIID,
				APIHash:     cfg.APIHash,
				Settings:    tg.CodeSettings{},
			})
			if err != nil {
				return formatBuiltinTelegramError(err)
			}
			hash, authorized, user := telegramSentCodeResult(sent)
			result = telegramAuthCommandResp{
				OK:            true,
				Authorized:    authorized,
				User:          user,
				PhoneCodeHash: hash,
			}
		case "signin":
			phone := strings.TrimSpace(payload.Phone)
			code := strings.TrimSpace(payload.Code)
			hash := firstNonEmpty(payload.PhoneCodeHash, payload.PhoneCodeHashCamel)
			if phone == "" || code == "" || hash == "" {
				return errors.New("phone, code and phone_code_hash are required")
			}
			req := &tg.AuthSignInRequest{
				PhoneNumber:   phone,
				PhoneCodeHash: hash,
			}
			req.SetPhoneCode(code)
			authorization, err := client.API().AuthSignIn(ctx, req)
			if err != nil {
				return formatBuiltinTelegramError(err)
			}
			result = telegramAuthorizationResult(authorization)
		case "logout":
			if _, err := client.API().AuthLogOut(ctx); err != nil && !auth.IsUnauthorized(err) {
				return formatBuiltinTelegramError(err)
			}
			_ = os.Remove(defaultTelegramSessionFile(cfg))
			result = telegramAuthCommandResp{OK: true, Authorized: false}
		default:
			return fmt.Errorf("unsupported telegram auth action: %s", action)
		}
		return nil
	})
	if err != nil {
		return telegramAuthCommandResp{}, err
	}
	return result, nil
}

func runBuiltinTelegramSearch(ctx context.Context, sub *model.Subscription, cfg model.SubscriptionTelegramSourceConfig) ([]telegramCommandRow, error) {
	channels := telegramChannelGroups(cfg)
	if len(channels) == 0 {
		channels = cfg.Channels
	}
	channels = cleanStringList(channels, false)
	if len(channels) == 0 {
		return nil, errors.New("telegram channels are not configured")
	}
	query := telegramSearchQuery(sub)
	if query == "" {
		return nil, errors.New("telegram search query is required")
	}
	limit := cfg.Limit
	if limit <= 0 {
		limit = 40
	}
	perChannelLimit := telegramBuiltinPerChannelLimit(limit, len(channels))
	var rows []telegramCommandRow
	err := runBuiltinTelegramClient(ctx, cfg, true, func(ctx context.Context, client *telegram.Client) error {
		status, err := client.Auth().Status(ctx)
		if err != nil {
			return formatBuiltinTelegramError(err)
		}
		if !status.Authorized {
			return errors.New("telegram is not logged in")
		}
		for _, channel := range channels {
			peer, normalized, err := resolveTelegramChannel(ctx, client.API(), channel)
			if err != nil {
				return err
			}
			resp, err := client.API().MessagesSearch(ctx, &tg.MessagesSearchRequest{
				Peer:   peer,
				Q:      query,
				Filter: &tg.InputMessagesFilterEmpty{},
				Limit:  perChannelLimit,
			})
			if err != nil {
				return formatBuiltinTelegramError(err)
			}
			modified, ok := resp.AsModified()
			if !ok {
				continue
			}
			rows = append(rows, telegramRowsFromMessages(modified.GetMessages(), normalized)...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func telegramBuiltinPerChannelLimit(limit, channelCount int) int {
	if limit <= 0 {
		limit = 40
	}
	if channelCount <= 0 {
		return limit
	}
	return limit
}

func runBuiltinTelegramClient(ctx context.Context, cfg model.SubscriptionTelegramSourceConfig, allowRetry bool, fn func(context.Context, *telegram.Client) error) error {
	if cfg.APIID <= 0 || strings.TrimSpace(cfg.APIHash) == "" {
		return errors.New("telegram API ID and API hash are required")
	}
	sessionFile := defaultTelegramSessionFile(cfg)
	if err := os.MkdirAll(filepath.Dir(sessionFile), 0700); err != nil {
		return err
	}
	unlock, err := lockTelegramSession(ctx, sessionFile)
	if err != nil {
		return err
	}
	defer unlock()

	timeout := time.Duration(cfg.CommandTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return runBuiltinTelegramRetryLoop(ctx, allowRetry, func(ctx context.Context) error {
		client := telegram.NewClient(cfg.APIID, cfg.APIHash, telegram.Options{
			SessionStorage: &session.FileStorage{Path: sessionFile},
			NoUpdates:      true,
			Device:         telegram.DeviceTDesktopWindows(),
		})
		return client.Run(ctx, func(ctx context.Context) error {
			return fn(ctx, client)
		})
	}, sleepBuiltinTelegramRetry)
}

func runBuiltinTelegramRetryLoop(ctx context.Context, allowRetry bool, run func(context.Context) error, sleep func(context.Context, time.Duration) error) error {
	var lastErr error
	for attempt := 1; attempt <= builtinTelegramRetryMaxAttempts; attempt++ {
		err := run(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
		if !allowRetry || attempt >= builtinTelegramRetryMaxAttempts || !shouldRetryBuiltinTelegramError(err) {
			return finalizeBuiltinTelegramClientError(ctx, err)
		}
		if err := sleep(ctx, builtinTelegramRetryDelay(attempt+1)); err != nil {
			return finalizeBuiltinTelegramClientError(ctx, err)
		}
	}
	return finalizeBuiltinTelegramClientError(ctx, lastErr)
}

func builtinTelegramRetryDelay(attempt int) time.Duration {
	if attempt <= 1 {
		return 0
	}
	delay := builtinTelegramRetryInitialWait
	for current := 2; current < attempt; current++ {
		if delay >= builtinTelegramRetryMaxWait {
			return builtinTelegramRetryMaxWait
		}
		delay *= 2
	}
	if delay > builtinTelegramRetryMaxWait {
		return builtinTelegramRetryMaxWait
	}
	return delay
}

func sleepBuiltinTelegramRetry(ctx context.Context, wait time.Duration) error {
	if wait <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func shouldRetryBuiltinTelegramError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || auth.IsUnauthorized(err) || isBuiltinTelegramPermanentError(err) {
		return false
	}
	if isBuiltinTelegramTimeoutError(err) {
		return true
	}
	var temporary interface{ Temporary() bool }
	return errors.As(err, &temporary) && temporary.Temporary()
}

func isBuiltinTelegramTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func isBuiltinTelegramPermanentError(err error) bool {
	switch err.Error() {
	case "telegram API ID and API hash are required",
		"telegram session is busy",
		"Telegram API ID/API hash is invalid",
		"phone number is invalid",
		"phone number is banned by Telegram",
		"verification code requests are too frequent",
		"verification code expired",
		"verification code is invalid",
		"telegram account has two-step verification enabled; password login is not supported yet",
		"telegram channel username is invalid or not found",
		"telegram channel is private or not joined",
		"telegram is not logged in":
		return true
	default:
		return false
	}
}

func finalizeBuiltinTelegramClientError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	if isBuiltinTelegramTimeoutError(err) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return errors.New(builtinTelegramTimeoutMessage)
	}
	return formatBuiltinTelegramError(err)
}

func defaultTelegramSessionFile(cfg model.SubscriptionTelegramSourceConfig) string {
	if strings.TrimSpace(cfg.SessionFile) != "" {
		return strings.TrimSpace(cfg.SessionFile)
	}
	base := flags.DataDir
	if base == "" {
		base = "data"
	}
	return filepath.Join(base, "telegram.session")
}

func resolveTelegramChannel(ctx context.Context, api *tg.Client, channel string) (tg.InputPeerClass, string, error) {
	channel = strings.TrimSpace(channel)
	channel = strings.TrimPrefix(channel, "https://t.me/")
	channel = strings.TrimPrefix(channel, "http://t.me/")
	channel = strings.TrimPrefix(channel, "t.me/")
	channel = strings.TrimPrefix(channel, "@")
	channel = strings.Trim(channel, "/")
	if channel == "" {
		return nil, "", errors.New("telegram channel is empty")
	}
	resolved, err := api.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{Username: channel})
	if err != nil {
		return nil, "", formatBuiltinTelegramError(err)
	}
	for _, chat := range resolved.Chats {
		if c, ok := chat.(*tg.Channel); ok {
			return c.AsInputPeer(), c.Username, nil
		}
	}
	return nil, "", fmt.Errorf("telegram channel %s was not found", channel)
}

func telegramRowsFromMessages(messages []tg.MessageClass, channel string) []telegramCommandRow {
	rows := make([]telegramCommandRow, 0, len(messages))
	for _, item := range messages {
		msg, ok := item.(*tg.Message)
		if !ok {
			continue
		}
		row := telegramCommandRow{
			ID:      int64(msg.ID),
			MsgID:   int64(msg.ID),
			Date:    time.Unix(int64(msg.Date), 0).Format(time.RFC3339),
			Text:    msg.Message,
			RawText: msg.Message,
			Channel: channel,
		}
		row.Entities = telegramEntityLinks(msg.Entities)
		row.Buttons = telegramButtonLinks(msg.ReplyMarkup)
		rows = append(rows, row)
	}
	return rows
}

func telegramEntityLinks(entities []tg.MessageEntityClass) []struct {
	URL string `json:"url"`
} {
	links := make([]struct {
		URL string `json:"url"`
	}, 0, len(entities))
	for _, entity := range entities {
		switch e := entity.(type) {
		case *tg.MessageEntityTextURL:
			links = append(links, struct {
				URL string `json:"url"`
			}{URL: e.URL})
		}
	}
	return links
}

func telegramButtonLinks(markup tg.ReplyMarkupClass) []struct {
	Text string `json:"text"`
	URL  string `json:"url"`
} {
	inline, ok := markup.(*tg.ReplyInlineMarkup)
	if !ok {
		return nil
	}
	var buttons []struct {
		Text string `json:"text"`
		URL  string `json:"url"`
	}
	for _, row := range inline.Rows {
		for _, button := range row.Buttons {
			if b, ok := button.(*tg.KeyboardButtonURL); ok {
				buttons = append(buttons, struct {
					Text string `json:"text"`
					URL  string `json:"url"`
				}{Text: b.Text, URL: b.URL})
			}
		}
	}
	return buttons
}

func telegramSentCodeResult(sent tg.AuthSentCodeClass) (string, bool, map[string]any) {
	switch v := sent.(type) {
	case *tg.AuthSentCode:
		return v.PhoneCodeHash, false, nil
	case *tg.AuthSentCodePaymentRequired:
		return v.PhoneCodeHash, false, nil
	case *tg.AuthSentCodeSuccess:
		authResult := telegramAuthorizationResult(v.Authorization)
		return "", authResult.Authorized, authResult.User
	default:
		return "", false, nil
	}
}

func telegramAuthorizationResult(authorization tg.AuthAuthorizationClass) telegramAuthCommandResp {
	switch v := authorization.(type) {
	case *tg.AuthAuthorization:
		if user, ok := v.User.(*tg.User); ok {
			return telegramAuthCommandResp{
				OK:         true,
				Authorized: true,
				User:       telegramUserMap(user),
			}
		}
	}
	return telegramAuthCommandResp{OK: true, Authorized: true}
}

func telegramUserMap(user *tg.User) map[string]any {
	if user == nil {
		return nil
	}
	result := map[string]any{
		"id": user.ID,
	}
	if user.Username != "" {
		result["username"] = user.Username
	}
	if user.Phone != "" {
		result["phone"] = user.Phone
	}
	if user.FirstName != "" {
		result["first_name"] = user.FirstName
		result["firstName"] = user.FirstName
	}
	if user.LastName != "" {
		result["last_name"] = user.LastName
		result["lastName"] = user.LastName
	}
	return result
}

func formatBuiltinTelegramError(err error) error {
	if err == nil {
		return nil
	}
	if auth.IsUnauthorized(err) {
		return err
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "API_ID_INVALID"), strings.Contains(msg, "API_HASH_INVALID"):
		return errors.New("Telegram API ID/API hash is invalid")
	case strings.Contains(msg, "PHONE_NUMBER_INVALID"):
		return errors.New("phone number is invalid")
	case strings.Contains(msg, "PHONE_NUMBER_BANNED"):
		return errors.New("phone number is banned by Telegram")
	case strings.Contains(msg, "PHONE_NUMBER_FLOOD"):
		return errors.New("verification code requests are too frequent")
	case strings.Contains(msg, "PHONE_CODE_EXPIRED"):
		return errors.New("verification code expired")
	case strings.Contains(msg, "PHONE_CODE_INVALID"):
		return errors.New("verification code is invalid")
	case strings.Contains(msg, "SESSION_PASSWORD_NEEDED"), strings.Contains(msg, "2FA"):
		return errors.New("telegram account has two-step verification enabled; password login is not supported yet")
	case strings.Contains(msg, "USERNAME_INVALID"), strings.Contains(msg, "USERNAME_NOT_OCCUPIED"):
		return errors.New("telegram channel username is invalid or not found")
	case strings.Contains(msg, "CHANNEL_PRIVATE"):
		return errors.New("telegram channel is private or not joined")
	default:
		return err
	}
}

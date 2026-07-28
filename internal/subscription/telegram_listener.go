package subscription

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	log "github.com/sirupsen/logrus"
)

const realtimeMaintenanceInterval = time.Second

type realtimeTelegramProfile struct {
	key                  string
	config               model.SubscriptionTelegramSourceConfig
	subscriptionsByGroup map[string][]uint
}

type telegramRealtimeListener struct {
	mu            sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	sessionCancel context.CancelFunc
	started       bool
	states        map[uint]string
}

var defaultTelegramRealtimeListener = &telegramRealtimeListener{states: map[uint]string{}}

// StartTelegramRealtimeListener starts the Coordinator-owned realtime inbox
// and shared Telegram sessions. Calling it more than once is safe.
func StartTelegramRealtimeListener() {
	defaultTelegramRealtimeListener.Start()
}

func StopTelegramRealtimeListener() {
	defaultTelegramRealtimeListener.Stop()
}

// RefreshTelegramRealtimeListener restarts session groups after subscription
// configuration changes. It never starts a listener on its own; bootstrap owns
// the role gate.
func RefreshTelegramRealtimeListener() {
	defaultTelegramRealtimeListener.Refresh()
}

func TelegramRealtimeListenerState(subscriptionID uint) string {
	return defaultTelegramRealtimeListener.State(subscriptionID)
}

func (l *telegramRealtimeListener) Start() {
	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		return
	}
	l.ctx, l.cancel = context.WithCancel(context.Background())
	l.started = true
	l.mu.Unlock()
	l.Refresh()
	go l.maintenanceLoop(l.ctx)
}

func (l *telegramRealtimeListener) Stop() {
	l.mu.Lock()
	if !l.started {
		l.mu.Unlock()
		return
	}
	cancel := l.cancel
	sessionCancel := l.sessionCancel
	l.started = false
	l.cancel = nil
	l.sessionCancel = nil
	l.ctx = nil
	l.states = map[uint]string{}
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if sessionCancel != nil {
		sessionCancel()
	}
}

func (l *telegramRealtimeListener) State(subscriptionID uint) string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if state := l.states[subscriptionID]; state != "" {
		return state
	}
	return "disabled"
}

func (l *telegramRealtimeListener) Refresh() {
	l.mu.Lock()
	if !l.started {
		l.mu.Unlock()
		return
	}
	oldCancel := l.sessionCancel
	ctx, cancel := context.WithCancel(l.ctx)
	l.sessionCancel = cancel
	l.states = map[uint]string{}
	l.mu.Unlock()
	if oldCancel != nil {
		oldCancel()
	}
	profiles, err := loadRealtimeTelegramProfiles()
	if err != nil {
		log.Errorf("load realtime Telegram listener profiles: %v", err)
		return
	}
	for _, profile := range profiles {
		l.setProfileState(profile, "starting")
		go l.runProfile(ctx, profile)
	}
}

func (l *telegramRealtimeListener) setProfileState(profile realtimeTelegramProfile, state string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ids := range profile.subscriptionsByGroup {
		for _, id := range ids {
			l.states[id] = state
		}
	}
}

func (l *telegramRealtimeListener) maintenanceLoop(ctx context.Context) {
	ticker := time.NewTicker(realtimeMaintenanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := ProcessPendingRealtimeTelegramEvents(ctx, 20); err != nil {
				log.Errorf("process realtime Telegram events: %v", err)
			}
			if _, err := ProcessReadyRealtimeCandidates(ctx, 100); err != nil {
				log.Errorf("process realtime Telegram candidates: %v", err)
			}
		}
	}
}

func loadRealtimeTelegramProfiles() ([]realtimeTelegramProfile, error) {
	subscriptions, err := db.ListAllSubscriptions(db.SubscriptionFilter{Active: boolPointer(true)})
	if err != nil {
		return nil, err
	}
	profiles := map[string]*realtimeTelegramProfile{}
	for i := range subscriptions {
		sub := &subscriptions[i]
		if !strings.EqualFold(sub.SourceType, model.SubscriptionSourceTelegram) {
			continue
		}
		if err := ApplyDefaults(sub); err != nil {
			return nil, err
		}
		cfg, err := parseTelegramConfig(sub.SourceConfig)
		if err != nil || !cfg.RealtimeEnabled || cfg.APIID <= 0 || strings.TrimSpace(cfg.APIHash) == "" {
			continue
		}
		groups := RealtimeTelegramGroups(cfg)
		if len(groups) == 0 {
			continue
		}
		key := shortHash(fmt.Sprintf("%d\x00%s\x00%s", cfg.APIID, cfg.APIHash, defaultTelegramSessionFile(cfg)))
		profile := profiles[key]
		if profile == nil {
			profile = &realtimeTelegramProfile{key: key, config: cfg, subscriptionsByGroup: map[string][]uint{}}
			profiles[key] = profile
		}
		for _, group := range groups {
			group = normalizeTelegramChannel(group)
			if group != "" {
				profile.subscriptionsByGroup[group] = append(profile.subscriptionsByGroup[group], sub.ID)
			}
		}
	}
	keys := make([]string, 0, len(profiles))
	for key := range profiles {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]realtimeTelegramProfile, 0, len(keys))
	for _, key := range keys {
		profile := profiles[key]
		for group := range profile.subscriptionsByGroup {
			sort.Slice(profile.subscriptionsByGroup[group], func(i, j int) bool {
				return profile.subscriptionsByGroup[group][i] < profile.subscriptionsByGroup[group][j]
			})
		}
		result = append(result, *profile)
	}
	return result, nil
}

func (l *telegramRealtimeListener) runProfile(ctx context.Context, profile realtimeTelegramProfile) {
	for ctx.Err() == nil {
		if err := l.runProfileOnce(ctx, profile); err != nil && ctx.Err() == nil {
			l.setProfileState(profile, "degraded")
			log.Errorf("realtime Telegram listener %s stopped: %v", profile.key, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
			continue
		}
		return
	}
}

func (l *telegramRealtimeListener) runProfileOnce(ctx context.Context, profile realtimeTelegramProfile) error {
	dispatcher := tg.NewUpdateDispatcher()
	handle := func(ctx context.Context, entities tg.Entities, messageClass tg.MessageClass) error {
		message, ok := messageClass.(*tg.Message)
		if !ok || message.Out {
			return nil
		}
		channel := telegramUpdateChannel(entities, message)
		if channel == "" {
			return nil
		}
		ids := profile.subscriptionsByGroup[channel]
		if len(ids) == 0 {
			return nil
		}
		row := telegramCommandRow{ID: int64(message.ID), MsgID: int64(message.ID), Date: time.Unix(int64(message.Date), 0).UTC().Format(time.RFC3339), Text: message.Message, RawText: message.Message, Channel: channel}
		row.Entities = telegramEntityLinks(message.Entities)
		row.Buttons = telegramButtonLinks(message.ReplyMarkup)
		for _, subscriptionID := range ids {
			if _, _, err := EnqueueRealtimeTelegramRow(subscriptionID, row); err != nil {
				return err
			}
		}
		return nil
	}
	dispatcher.OnNewChannelMessage(func(ctx context.Context, entities tg.Entities, update *tg.UpdateNewChannelMessage) error {
		return handle(ctx, entities, update.Message)
	})
	dispatcher.OnNewMessage(func(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage) error {
		return handle(ctx, entities, update.Message)
	})
	client := telegram.NewClient(profile.config.APIID, profile.config.APIHash, telegram.Options{
		SessionStorage: &session.FileStorage{Path: defaultTelegramSessionFile(profile.config)},
		UpdateHandler:  dispatcher,
		Device:         telegram.DeviceTDesktopWindows(),
		OnConnectionState: func(state telegram.ConnectionState) {
			if state == telegram.ConnectionStateReady {
				l.setProfileState(profile, "connected")
			} else {
				l.setProfileState(profile, "backing_off")
			}
		},
	})
	return client.Run(ctx, func(ctx context.Context) error {
		status, err := client.Auth().Status(ctx)
		if err != nil {
			return err
		}
		if !status.Authorized {
			return fmt.Errorf("Telegram session %s is not authorized", defaultTelegramSessionFile(profile.config))
		}
		l.setProfileState(profile, "connected")
		<-ctx.Done()
		return ctx.Err()
	})
}

func telegramUpdateChannel(entities tg.Entities, message *tg.Message) string {
	if message == nil {
		return ""
	}
	switch peer := message.PeerID.(type) {
	case *tg.PeerChannel:
		channel := entities.Channels[peer.ChannelID]
		if channel != nil {
			if username := normalizeTelegramChannel(channel.Username); username != "" {
				return username
			}
		}
		return fmt.Sprintf("-100%d", peer.ChannelID)
	case *tg.PeerChat:
		return fmt.Sprintf("-%d", peer.ChatID)
	default:
		return ""
	}
}

func boolPointer(value bool) *bool { return &value }

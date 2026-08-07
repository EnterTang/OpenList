package subscription

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/hdhive"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	log "github.com/sirupsen/logrus"
)

type telegramHDHiveLink struct {
	URL        string
	AccessCode string
}

var (
	telegramHDHiveServiceMu  sync.Mutex
	telegramHDHiveServiceKey string
	telegramHDHiveService    *hdhive.Service
	telegramHDHiveClientKey  string
	telegramHDHiveClient     *hdhive.SymediaClient
	newTelegramHDHiveClient  = func(cfg model.SubscriptionTelegramHDHiveConfig) (*hdhive.SymediaClient, error) {
		return hdhive.NewSymediaClient(hdhive.SymediaConfig{
			BaseURL:      cfg.BaseURL,
			UserID:       cfg.UserID,
			ProxyUserKey: cfg.ProxyUserKey,
			ProxySecret:  cfg.ProxySecret,
			Timeout:      time.Duration(cfg.TimeoutSeconds) * time.Second,
		}, &http.Client{Timeout: time.Duration(cfg.TimeoutSeconds) * time.Second})
	}
	newTelegramHDHiveService = func(cfg model.SubscriptionTelegramHDHiveConfig) (*hdhive.Service, error) {
		client, err := newTelegramHDHiveClient(cfg)
		if err != nil {
			return nil, err
		}
		return hdhive.NewService(client), nil
	}
)

func resolveTelegramHDHiveLinks(ctx context.Context, row telegramCommandRow, cfg model.SubscriptionTelegramSourceConfig) ([]telegramHDHiveLink, error) {
	refs := hdhive.ExtractResourceRefs(rowText(row), telegramRowURLFields(row), "189")
	if len(refs) == 0 || !cfg.HDHive.Enabled {
		return nil, nil
	}
	if strings.TrimSpace(cfg.HDHive.BaseURL) == "" || strings.TrimSpace(cfg.HDHive.UserID) == "" || strings.TrimSpace(cfg.HDHive.ProxyUserKey) == "" || strings.TrimSpace(cfg.HDHive.ProxySecret) == "" {
		return nil, nil
	}
	service, err := telegramHDHiveServiceForConfig(cfg.HDHive)
	if err != nil {
		return nil, err
	}
	result, err := service.Resolve(ctx, refs, hdhive.Config{
		Enabled:         true,
		MaxUnlockPoints: cfg.HDHive.MaxUnlockPoints,
	})
	if err != nil {
		return nil, err
	}
	if len(result.Failures) > 0 || len(result.Skipped) > 0 {
		log.WithFields(log.Fields{
			"failed":  len(result.Failures),
			"skipped": len(result.Skipped),
			"source":  "telegram",
		}).Warn("subscription: HDHive resources were not all unlocked")
	}
	links := make([]telegramHDHiveLink, 0, len(result.Items))
	for _, item := range result.Items {
		if !item.Success || strings.TrimSpace(item.FullURL) == "" {
			continue
		}
		links = append(links, telegramHDHiveLink{URL: item.FullURL, AccessCode: item.AccessCode})
	}
	return links, nil
}

func UnlockHDHiveResource(ctx context.Context, rawURL string, cfg model.SubscriptionTelegramHDHiveConfig) (model.SubscriptionResourceUnlockResp, error) {
	ref, ok := hdhive.ResourceRefFromURL(rawURL, "189")
	if !ok {
		return model.SubscriptionResourceUnlockResp{}, fmt.Errorf("invalid HDHive resource URL")
	}
	if !cfg.Enabled || strings.TrimSpace(cfg.BaseURL) == "" || strings.TrimSpace(cfg.UserID) == "" || strings.TrimSpace(cfg.ProxyUserKey) == "" || strings.TrimSpace(cfg.ProxySecret) == "" {
		return model.SubscriptionResourceUnlockResp{}, fmt.Errorf("hdhive is not configured")
	}
	service, err := telegramHDHiveServiceForConfig(cfg)
	if err != nil {
		return model.SubscriptionResourceUnlockResp{}, err
	}
	result, err := service.Resolve(ctx, []hdhive.ResourceRef{ref}, hdhive.Config{
		Enabled:         true,
		MaxUnlockPoints: cfg.MaxUnlockPoints,
	})
	if err != nil {
		return model.SubscriptionResourceUnlockResp{}, err
	}
	if len(result.Items) > 0 {
		item := result.Items[0]
		return model.SubscriptionResourceUnlockResp{
			URL:         item.FullURL,
			AccessCode:  item.AccessCode,
			FromCache:   item.FromCache,
			PointsSpent: item.PointsSpent,
		}, nil
	}
	if len(result.Skipped) > 0 {
		item := result.Skipped[0]
		return model.SubscriptionResourceUnlockResp{}, fmt.Errorf("HDHive resource skipped: %s (unlock points: %d)", item.Reason, item.UnlockPoints)
	}
	if len(result.Failures) > 0 {
		failure := result.Failures[0]
		if failure.Message != "" {
			return model.SubscriptionResourceUnlockResp{}, fmt.Errorf("%s: %s", failure.ErrorCode, failure.Message)
		}
		return model.SubscriptionResourceUnlockResp{}, fmt.Errorf("%s", failure.ErrorCode)
	}
	return model.SubscriptionResourceUnlockResp{}, fmt.Errorf("HDHive did not return an unlocked share")
}

func telegramHDHiveServiceForConfig(cfg model.SubscriptionTelegramHDHiveConfig) (*hdhive.Service, error) {
	key := telegramHDHiveConfigKey(cfg)
	telegramHDHiveServiceMu.Lock()
	defer telegramHDHiveServiceMu.Unlock()
	if telegramHDHiveService != nil && telegramHDHiveServiceKey == key {
		return telegramHDHiveService, nil
	}
	service, err := newTelegramHDHiveService(cfg)
	if err != nil {
		return nil, err
	}
	telegramHDHiveServiceKey = key
	telegramHDHiveService = service
	return service, nil
}

func telegramHDHiveClientForConfig(cfg model.SubscriptionTelegramHDHiveConfig) (*hdhive.SymediaClient, error) {
	key := telegramHDHiveConfigKey(cfg)
	telegramHDHiveServiceMu.Lock()
	defer telegramHDHiveServiceMu.Unlock()
	if telegramHDHiveClient != nil && telegramHDHiveClientKey == key {
		return telegramHDHiveClient, nil
	}
	client, err := newTelegramHDHiveClient(cfg)
	if err != nil {
		return nil, err
	}
	telegramHDHiveClientKey = key
	telegramHDHiveClient = client
	return client, nil
}

func telegramHDHiveConfigKey(cfg model.SubscriptionTelegramHDHiveConfig) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		cfg.BaseURL,
		cfg.UserID,
		cfg.ProxyUserKey,
		cfg.ProxySecret,
		fmt.Sprint(cfg.TimeoutSeconds),
		fmt.Sprint(cfg.MaxUnlockPoints),
	}, "\x00")))
	return hex.EncodeToString(sum[:])
}

func telegramRowURLFields(row telegramCommandRow) []string {
	urls := make([]string, 0, len(row.Links)+len(row.Entities)+len(row.Buttons))
	urls = append(urls, row.Links...)
	for _, entity := range row.Entities {
		urls = append(urls, entity.URL)
	}
	for _, button := range row.Buttons {
		urls = append(urls, button.URL)
	}
	return urls
}

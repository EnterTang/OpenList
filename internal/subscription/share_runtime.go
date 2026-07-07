package subscription

import (
	"context"
	"fmt"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

var (
	newShareSaverForProvider = defaultNewShareSaverForProvider
	saveShareToTemp          = SaveShareToTemp
)

func trySaveShareLinkToTemp(ctx context.Context, sub *model.Subscription, cfg model.SubscriptionTelegramSourceConfig, rawLink string) (telegramPanSubscriptionSource, bool, error) {
	ref, err := ParseShareURL(rawLink)
	if err != nil {
		if provider, ok := DetectShareProvider(rawLink); ok {
			source, _ := telegramPanSourceForProvider(cfg, provider)
			return source, false, err
		}
		return telegramPanSubscriptionSource{}, false, nil
	}
	source, ok := telegramPanSourceForProvider(cfg, ref.Provider)
	if !ok {
		return telegramPanSubscriptionSource{}, false, nil
	}
	source.Config = telegramPanSourceConfigWithStorageFallback(ref.Provider, source.Config)
	if !telegramPanSourceCanSave(ref.Provider, source.Config) {
		return source, false, nil
	}
	provider, err := newShareSaverForProvider(ref.Provider, source.Config)
	if err != nil {
		return source, false, err
	}
	_, err = saveShareToTemp(ctx, provider, ref, SaveShareOptions{
		TempRoot: source.Config.TempTransferRoot,
		Match: func(entry TreeEntry) bool {
			return !entry.IsDir && subscriptionEntryMatches(sub, entry)
		},
	})
	if err != nil {
		return source, false, err
	}
	return source, true, nil
}

func telegramPanSourceConfigWithStorageFallback(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) model.SubscriptionTelegramPanConfig {
	cfg = normalizeTelegramPanConfig(cfg)
	if provider == ShareProviderAliyunDrive {
		return aliyunDriveConfigWithStorageFallback(cfg)
	}
	return cfg
}

func telegramPanSourceForProvider(cfg model.SubscriptionTelegramSourceConfig, provider ShareProviderName) (telegramPanSubscriptionSource, bool) {
	var source telegramPanSubscriptionSource
	switch provider {
	case ShareProviderQuark:
		source = telegramPanSubscriptionSource{Name: string(ShareProviderQuark), Config: cfg.Quark}
	case ShareProviderAliyunDrive:
		source = telegramPanSubscriptionSource{Name: string(ShareProviderAliyunDrive), Config: cfg.AliyunDrive}
	case ShareProviderPan123:
		source = telegramPanSubscriptionSource{Name: string(ShareProviderPan123), Config: cfg.Pan123}
	case ShareProviderPan115:
		source = telegramPanSubscriptionSource{Name: string(ShareProviderPan115), Config: cfg.Pan115}
	default:
		return telegramPanSubscriptionSource{}, false
	}
	source.Config = normalizeTelegramPanConfig(source.Config)
	if isZeroTelegramPanConfig(source.Config) {
		return telegramPanSubscriptionSource{}, false
	}
	return source, true
}

func telegramPanSourceCanSave(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) bool {
	cfg = normalizeTelegramPanConfig(cfg)
	if cfg.TempTransferRoot == "" {
		return false
	}
	switch provider {
	case ShareProviderQuark, ShareProviderPan115:
		return strings.TrimSpace(cfg.Cookie) != ""
	case ShareProviderAliyunDrive:
		return strings.TrimSpace(cfg.RefreshToken) != "" ||
			(strings.TrimSpace(cfg.AccessToken) != "" && strings.TrimSpace(cfg.DriveID) != "")
	case ShareProviderPan123:
		return strings.TrimSpace(cfg.AccessToken) != ""
	default:
		return false
	}
}

func defaultNewShareSaverForProvider(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) (ShareSaver, error) {
	switch provider {
	case ShareProviderQuark:
		return NewQuarkShareProvider(cfg), nil
	case ShareProviderAliyunDrive:
		return NewAliyunDriveShareProvider(cfg), nil
	case ShareProviderPan123:
		return NewPan123ShareProvider(cfg), nil
	case ShareProviderPan115:
		return NewPan115ShareProvider(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported share provider: %s", provider)
	}
}

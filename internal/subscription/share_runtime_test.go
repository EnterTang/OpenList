package subscription

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestTrySaveShareLinkToTempCallsProviderWhenConfigured(t *testing.T) {
	oldFactory := newShareSaverForProvider
	oldSave := saveShareToTemp
	defer func() {
		newShareSaverForProvider = oldFactory
		saveShareToTemp = oldSave
	}()

	var factoryProvider ShareProviderName
	newShareSaverForProvider = func(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) (ShareSaver, error) {
		factoryProvider = provider
		return &fakeShareSaver{}, nil
	}
	var savedRef ShareRef
	var savedTempRoot string
	var matchAccepted bool
	saveShareToTemp = func(ctx context.Context, provider ShareSaver, ref ShareRef, opts SaveShareOptions) ([]TreeEntry, error) {
		savedRef = ref
		savedTempRoot = opts.TempRoot
		matchAccepted = opts.Match(TreeEntry{
			RootPath: ref.RawURL,
			Path:     "/Some.Show.S01E01.mkv",
			Name:     "Some.Show.S01E01.mkv",
		})
		return nil, nil
	}
	cfg := normalizeTelegramSourceConfig(model.SubscriptionTelegramSourceConfig{
		Quark: model.SubscriptionTelegramPanConfig{
			Channels:         []string{"@quark"},
			TempTransferRoot: "/tmp/quark",
			Cookie:           "cookie",
		},
	})
	sub := &model.Subscription{TMDBName: "Some Show"}

	source, handled, err := trySaveShareLinkToTemp(context.Background(), sub, cfg, "https://pan.quark.cn/s/bc18e4ea5fb8")
	if err != nil {
		t.Fatalf("save share link: %v", err)
	}
	if !handled {
		t.Fatal("handled = false, want true")
	}
	if source.Name != "quark" || factoryProvider != ShareProviderQuark {
		t.Fatalf("source=%#v factoryProvider=%s, want quark", source, factoryProvider)
	}
	if savedRef.ShareID != "bc18e4ea5fb8" || savedTempRoot != "/tmp/quark" {
		t.Fatalf("saved ref/root = %#v %q", savedRef, savedTempRoot)
	}
	if !matchAccepted {
		t.Fatal("expected subscription media match to be accepted")
	}
}

func TestTrySaveShareLinkToTempSkipsIncompleteConfig(t *testing.T) {
	oldFactory := newShareSaverForProvider
	defer func() { newShareSaverForProvider = oldFactory }()
	newShareSaverForProvider = func(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) (ShareSaver, error) {
		t.Fatal("factory should not be called without provider credentials")
		return nil, nil
	}
	cfg := normalizeTelegramSourceConfig(model.SubscriptionTelegramSourceConfig{
		Quark: model.SubscriptionTelegramPanConfig{
			Channels:         []string{"@quark"},
			TempTransferRoot: "/tmp/quark",
		},
	})

	source, handled, err := trySaveShareLinkToTemp(context.Background(), &model.Subscription{TMDBName: "Some Show"}, cfg, "https://pan.quark.cn/s/bc18e4ea5fb8")
	if err != nil {
		t.Fatalf("save share link: %v", err)
	}
	if handled {
		t.Fatal("handled = true, want false")
	}
	if source.Name != "quark" {
		t.Fatalf("source = %#v, want quark fallback source", source)
	}
}

func TestTrySaveShareLinkToTempRequiresAliyunDriveIDWithAccessToken(t *testing.T) {
	oldFactory := newShareSaverForProvider
	defer func() { newShareSaverForProvider = oldFactory }()
	newShareSaverForProvider = func(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) (ShareSaver, error) {
		t.Fatal("factory should not be called without aliyun drive_id")
		return nil, nil
	}
	cfg := normalizeTelegramSourceConfig(model.SubscriptionTelegramSourceConfig{
		AliyunDrive: model.SubscriptionTelegramPanConfig{
			Channels:         []string{"@aliyun"},
			TempTransferRoot: "/tmp/aliyun",
			AccessToken:      "access-1",
		},
	})

	source, handled, err := trySaveShareLinkToTemp(context.Background(), &model.Subscription{TMDBName: "Some Show"}, cfg, "https://www.alipan.com/s/odeXVKsEKxr")
	if err != nil {
		t.Fatalf("save share link: %v", err)
	}
	if handled {
		t.Fatal("handled = true, want false")
	}
	if source.Name != "aliyun_drive" {
		t.Fatalf("source = %#v, want aliyun fallback source", source)
	}
}

func TestTrySaveShareLinkToTempAllowsAliyunAccessTokenWithDriveID(t *testing.T) {
	oldFactory := newShareSaverForProvider
	oldSave := saveShareToTemp
	defer func() {
		newShareSaverForProvider = oldFactory
		saveShareToTemp = oldSave
	}()

	var factoryProvider ShareProviderName
	var factoryConfig model.SubscriptionTelegramPanConfig
	newShareSaverForProvider = func(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) (ShareSaver, error) {
		factoryProvider = provider
		factoryConfig = cfg
		return &fakeShareSaver{}, nil
	}
	saveShareToTemp = func(ctx context.Context, provider ShareSaver, ref ShareRef, opts SaveShareOptions) ([]TreeEntry, error) {
		if ref.ShareID != "odeXVKsEKxr" || opts.TempRoot != "/tmp/aliyun" {
			t.Fatalf("save ref/root = %#v %q", ref, opts.TempRoot)
		}
		return nil, nil
	}
	cfg := normalizeTelegramSourceConfig(model.SubscriptionTelegramSourceConfig{
		AliyunDrive: model.SubscriptionTelegramPanConfig{
			Channels:         []string{"@aliyun"},
			TempTransferRoot: "/tmp/aliyun",
			AccessToken:      "access-1",
			DriveID:          "drive-1",
		},
	})

	source, handled, err := trySaveShareLinkToTemp(context.Background(), &model.Subscription{TMDBName: "Some Show"}, cfg, "https://www.alipan.com/s/odeXVKsEKxr")
	if err != nil {
		t.Fatalf("save share link: %v", err)
	}
	if !handled {
		t.Fatal("handled = false, want true")
	}
	if source.Name != "aliyun_drive" || factoryProvider != ShareProviderAliyunDrive {
		t.Fatalf("source=%#v factoryProvider=%s, want aliyun", source, factoryProvider)
	}
	if factoryConfig.AccessToken != "access-1" || factoryConfig.DriveID != "drive-1" {
		t.Fatalf("factory config = %#v, want access token and drive id", factoryConfig)
	}
}

func TestTrySaveShareLinkToTempUsesAliyunOpenWebRefreshTokenFallback(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	if err := db.CreateStorage(&model.Storage{
		MountPath: "/ali",
		Driver:    "AliyundriveOpen",
		Addition:  `{"web_refresh_token":" web-refresh-1 ","drive_type":"resource"}`,
	}); err != nil {
		t.Fatalf("create aliyun open storage: %v", err)
	}

	oldFactory := newShareSaverForProvider
	oldSave := saveShareToTemp
	defer func() {
		newShareSaverForProvider = oldFactory
		saveShareToTemp = oldSave
	}()

	var factoryConfig model.SubscriptionTelegramPanConfig
	newShareSaverForProvider = func(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) (ShareSaver, error) {
		if provider != ShareProviderAliyunDrive {
			t.Fatalf("provider = %s, want aliyun", provider)
		}
		factoryConfig = cfg
		return &fakeShareSaver{}, nil
	}
	saveShareToTemp = func(ctx context.Context, provider ShareSaver, ref ShareRef, opts SaveShareOptions) ([]TreeEntry, error) {
		return nil, nil
	}
	cfg := normalizeTelegramSourceConfig(model.SubscriptionTelegramSourceConfig{
		AliyunDrive: model.SubscriptionTelegramPanConfig{
			Channels:         []string{"@aliyun"},
			TempTransferRoot: "/ali/.tmp-share",
		},
	})

	source, handled, err := trySaveShareLinkToTemp(context.Background(), &model.Subscription{TMDBName: "Some Show"}, cfg, "https://www.alipan.com/s/odeXVKsEKxr")
	if err != nil {
		t.Fatalf("save share link: %v", err)
	}
	if !handled || source.Name != "aliyun_drive" {
		t.Fatalf("handled/source = %v/%#v, want aliyun handled", handled, source)
	}
	if factoryConfig.RefreshToken != "web-refresh-1" {
		t.Fatalf("factory refresh token = %q, want web-refresh-1", factoryConfig.RefreshToken)
	}
	if factoryConfig.DriveType != "resource" {
		t.Fatalf("factory drive type = %q, want resource", factoryConfig.DriveType)
	}
}

func TestRunManualShareProviderSavesTempRoot(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	if _, err := SaveConfig(model.SubscriptionConfig{
		Telegram: model.SubscriptionTelegramSourceConfig{
			Quark: model.SubscriptionTelegramPanConfig{
				TempTransferRoot: "/tmp/quark",
				Cookie:           "cookie",
			},
		},
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	oldFactory := newShareSaverForProvider
	oldSave := saveShareToTemp
	oldSnapshot := snapshotPaths
	defer func() {
		newShareSaverForProvider = oldFactory
		saveShareToTemp = oldSave
		snapshotPaths = oldSnapshot
	}()

	newShareSaverForProvider = func(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) (ShareSaver, error) {
		if provider != ShareProviderQuark {
			t.Fatalf("provider = %s, want quark", provider)
		}
		if cfg.Cookie != "cookie" || cfg.TempTransferRoot != "/tmp/quark" {
			t.Fatalf("cfg = %#v, want quark credentials and temp root", cfg)
		}
		return &fakeShareSaver{}, nil
	}
	saveShareToTemp = func(ctx context.Context, provider ShareSaver, ref ShareRef, opts SaveShareOptions) ([]TreeEntry, error) {
		if ref.ShareID != "bc18e4ea5fb8" || opts.TempRoot != "/tmp/quark" {
			t.Fatalf("save ref/root = %#v %q", ref, opts.TempRoot)
		}
		if !opts.Match(TreeEntry{Name: "Some.Show.S01E01.mkv", Path: "/Some.Show.S01E01.mkv"}) {
			t.Fatal("expected manual provider save match to accept subscription media")
		}
		return nil, nil
	}
	snapshotPaths = func(ctx context.Context, roots []string) (*TreeSnapshot, error) {
		if got, want := roots, []string{"/tmp/quark"}; !stringSlicesEqual(got, want) {
			t.Fatalf("snapshot roots = %#v, want %#v", got, want)
		}
		return &TreeSnapshot{
			Hash: "temp-hash",
			Entries: []TreeEntry{{
				RootPath: "/tmp/quark",
				Path:     "/Some.Show.S01E01.mkv",
				Name:     "Some.Show.S01E01.mkv",
				ID:       "file-1",
				Size:     1024,
				Modified: time.Unix(1700000000, 0),
			}},
		}, nil
	}

	items, _, added, _, _, err := runManual(context.Background(), &model.Subscription{
		ID:           1,
		SourceConfig: `{"links":["https://pan.quark.cn/s/bc18e4ea5fb8"]}`,
		TMDBName:     "Some Show",
		TargetRoot:   "/target",
		MediaType:    "tv",
		Category:     "test",
	}, false)
	if err != nil {
		t.Fatalf("run manual: %v", err)
	}
	if added != 1 || len(items) != 1 {
		t.Fatalf("added/items = %d/%d, want 1/1", added, len(items))
	}
	if items[0].Status != model.SubscriptionItemStatusPending || items[0].SourcePath != "/tmp/quark/Some.Show.S01E01.mkv" {
		t.Fatalf("item = %#v, want pending temp file item", items[0])
	}
	if items[0].SourceURL != "" {
		t.Fatalf("source URL = %q, want provider-handled link not recorded as skipped", items[0].SourceURL)
	}
}

func setupSubscriptionRuntimeDB(t *testing.T) {
	t.Helper()
	dsn := "file:" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()) + "?mode=memory&cache=shared"
	database, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	conf.Conf = conf.DefaultConfig("data")
	db.Init(database)
	op.SettingCacheUpdate()
	t.Cleanup(func() {
		op.SettingCacheUpdate()
		sqlDB, err := database.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})
}

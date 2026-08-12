package worker

import (
	"context"
	"errors"
	"strings"
	"testing"

	internaldriver "github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

func TestBuildInventoryIncludesProviderAccounts(t *testing.T) {
	oldList := listInventoryStorages
	oldHydrate := hydrateInventoryStorage
	oldConfig := getInventorySubscriptionConfig
	defer func() {
		listInventoryStorages = oldList
		hydrateInventoryStorage = oldHydrate
		getInventorySubscriptionConfig = oldConfig
	}()

	listInventoryStorages = func() ([]model.Storage, error) {
		return []model.Storage{{
			ID:        7,
			MountPath: "/139-a",
			Driver:    "139Yun",
			Remark:    "mobile-a",
			Status:    "work",
			Addition:  `{"type":"personal_new","cluster_dedicated_account":true,"membership_tier":"diamond","user_domain_id":"account-a"}`,
		}}, nil
	}
	hydrateInventoryStorage = func(ctx context.Context, nodeID string, storage model.Storage) (inventoryStorageSnapshot, error) {
		return inventoryStorageSnapshot{
			Mount:   providerMountInventory(nodeID, storage),
			Account: providerAccountInventory(nodeID, storage, 1024, 2048),
		}, nil
	}
	getInventorySubscriptionConfig = func() (model.SubscriptionConfig, error) {
		return model.SubscriptionConfig{DefaultTarget: model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "worker-delivery"}}, nil
	}

	report, err := BuildInventory(context.Background(), "node-1", true)
	if err != nil {
		t.Fatalf("build inventory: %v", err)
	}
	if len(report.ProviderAccounts) != 1 {
		t.Fatalf("provider accounts = %d, want 1", len(report.ProviderAccounts))
	}
	if report.ProviderAccounts[0].Provider != "yidong139" {
		t.Fatalf("provider = %q, want yidong139", report.ProviderAccounts[0].Provider)
	}
	account := report.ProviderAccounts[0]
	if account.StorageID != 7 || account.AccountAlias != "mobile-a" || account.AccountFingerprint == "" {
		t.Fatalf("provider account identity = %#v", account)
	}
	if account.MembershipTier != "diamond" || account.MembershipWeight != 400 || account.MaxSingleUploadBytes != 500<<30 {
		t.Fatalf("provider account membership = %#v", account)
	}
}

func TestBuildInventoryRequiresWorkerLocalDeliveryRoutingFor139Upload(t *testing.T) {
	oldList := listInventoryStorages
	oldHydrate := hydrateInventoryStorage
	oldConfig := getInventorySubscriptionConfig
	defer func() {
		listInventoryStorages = oldList
		hydrateInventoryStorage = oldHydrate
		getInventorySubscriptionConfig = oldConfig
	}()

	storage := model.Storage{
		ID: 7, MountPath: "/139", Driver: "139Yun", Status: "work",
		Addition: `{"type":"personal_new","cluster_dedicated_account":true,"membership_tier":"diamond"}`,
	}
	listInventoryStorages = func() ([]model.Storage, error) { return []model.Storage{storage}, nil }
	hydrateInventoryStorage = func(_ context.Context, nodeID string, storage model.Storage) (inventoryStorageSnapshot, error) {
		return inventoryStorageSnapshot{
			Mount:   providerMountInventory(nodeID, storage),
			Account: providerAccountInventory(nodeID, storage, 1<<40, 2<<40),
		}, nil
	}

	for _, tc := range []struct {
		name   string
		config model.SubscriptionConfig
		want   bool
	}{
		{name: "missing", want: false},
		{
			name:   "configured",
			config: model.SubscriptionConfig{DefaultTarget: model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "worker-delivery"}},
			want:   true,
		},
		{
			name:   "invalid folder",
			config: model.SubscriptionConfig{DefaultTarget: model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "../unsafe"}},
			want:   false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			getInventorySubscriptionConfig = func() (model.SubscriptionConfig, error) { return tc.config, nil }
			report, err := BuildInventory(context.Background(), "node-1", true)
			if err != nil {
				t.Fatalf("build inventory: %v", err)
			}
			if got, want := report.ProviderAccounts[0].SupportsUpload, tc.want; got != want {
				t.Fatalf("SupportsUpload = %v, want %v for worker config %#v", got, want, tc.config)
			}
		})
	}
}

func TestBuildInventoryRequiresWorkerLocalStagingRoutingForPanShareSave(t *testing.T) {
	oldList := listInventoryStorages
	oldHydrate := hydrateInventoryStorage
	oldConfig := getInventorySubscriptionConfig
	defer func() {
		listInventoryStorages = oldList
		hydrateInventoryStorage = oldHydrate
		getInventorySubscriptionConfig = oldConfig
	}()

	for _, tc := range []struct {
		provider string
		driver   string
		config   model.SubscriptionConfig
		want     bool
	}{
		{provider: "pan123", driver: "123Pan", want: false},
		{provider: "pan115", driver: "115 Cloud", want: false},
		{
			provider: "pan123",
			driver:   "123Pan",
			config: model.SubscriptionConfig{Telegram: model.SubscriptionTelegramSourceConfig{
				Pan123: model.SubscriptionTelegramPanConfig{
					TempTransferTarget: model.SubscriptionStorageTarget{Provider: "pan123", Folder: "worker-staging"},
				},
			}},
			want: true,
		},
		{
			provider: "pan115",
			driver:   "115 Cloud",
			config: model.SubscriptionConfig{Telegram: model.SubscriptionTelegramSourceConfig{
				Pan115: model.SubscriptionTelegramPanConfig{
					TempTransferTarget: model.SubscriptionStorageTarget{Provider: "pan115", Folder: "worker-staging"},
				},
			}},
			want: true,
		},
		{
			provider: "pan123",
			driver:   "123Pan",
			config: model.SubscriptionConfig{Telegram: model.SubscriptionTelegramSourceConfig{
				Pan123: model.SubscriptionTelegramPanConfig{
					TempTransferTarget: model.SubscriptionStorageTarget{Provider: "pan123", Folder: "../unsafe"},
				},
			}},
			want: false,
		},
		{provider: "quark", driver: "Quark", want: false},
		{provider: "aliyun_drive", driver: "AliyundriveOpen", want: false},
		{
			provider: "quark",
			driver:   "Quark",
			config: model.SubscriptionConfig{Telegram: model.SubscriptionTelegramSourceConfig{
				Quark: model.SubscriptionTelegramPanConfig{
					TempTransferTarget: model.SubscriptionStorageTarget{Provider: "quark", Folder: "\u8f6c\u5b58\u5230\u79fb\u52a8"},
				},
			}},
			want: true,
		},
		{
			provider: "aliyun_drive",
			driver:   "AliyundriveOpen",
			config: model.SubscriptionConfig{Telegram: model.SubscriptionTelegramSourceConfig{
				AliyunDrive: model.SubscriptionTelegramPanConfig{
					TempTransferTarget: model.SubscriptionStorageTarget{Provider: "aliyun_drive", Folder: "\u8f6c\u5b58\u81f3\u79fb\u52a8"},
				},
			}},
			want: true,
		},
	} {
		t.Run(tc.provider+"/"+tc.driver, func(t *testing.T) {
			storage := model.Storage{ID: 7, MountPath: "/" + tc.provider, Driver: tc.driver, Status: "work"}
			listInventoryStorages = func() ([]model.Storage, error) { return []model.Storage{storage}, nil }
			hydrateInventoryStorage = func(_ context.Context, nodeID string, storage model.Storage) (inventoryStorageSnapshot, error) {
				return inventoryStorageSnapshot{
					Mount:   providerMountInventory(nodeID, storage),
					Account: providerAccountInventory(nodeID, storage, 1<<40, 2<<40),
				}, nil
			}
			getInventorySubscriptionConfig = func() (model.SubscriptionConfig, error) { return tc.config, nil }

			report, err := BuildInventory(context.Background(), "node-1", true)
			if err != nil {
				t.Fatalf("build inventory: %v", err)
			}
			if got, want := report.ProviderAccounts[0].SupportsShareSave, tc.want; got != want {
				t.Fatalf("SupportsShareSave = %v, want %v for worker config %#v", got, want, tc.config)
			}
		})
	}
}

func TestProviderSupportedOperationsKeepsPan123DirectDownloadWithoutShareSave(t *testing.T) {
	operations := providerSupportedOperations("pan123", true, false)
	if !containsInventoryOperation(operations, "share.download") {
		t.Fatalf("operations = %#v, want share.download", operations)
	}
	if containsInventoryOperation(operations, "share.save") {
		t.Fatalf("operations = %#v, did not expect share.save", operations)
	}
}

func TestProviderSupportedOperationsAdvertisesPan115DirectDownload(t *testing.T) {
	operations := providerSupportedOperations("pan115", true, true)
	if !containsInventoryOperation(operations, "share.download") {
		t.Fatalf("operations = %#v, want share.download", operations)
	}
}

func TestProviderAccountInventoryKeepsUnknown139MembershipWithoutUploadLimit(t *testing.T) {
	storage := model.Storage{ID: 9, MountPath: "/139-b", Driver: "139Yun", Status: "work"}
	account := providerAccountInventory("node-1", storage, 10<<30, 20<<30)
	if account.MembershipTier != "unknown" || account.MembershipWeight != 0 || account.MaxSingleUploadBytes != 0 {
		t.Fatalf("provider account = %#v", account)
	}
}

func TestProviderAccountInventoryDefaults139PersonalNewToETFSupport(t *testing.T) {
	storage := model.Storage{ID: 10, MountPath: "/139-c", Driver: "139Yun", Status: "work", Addition: `{"type":"personal_new"}`}
	account := providerAccountInventory("node-1", storage, 10<<30, 20<<30)
	mount := providerMountInventory("node-1", storage)

	if !account.SupportsETF || !mount.SupportsETF {
		t.Fatalf("supports ETF account=%v mount=%v, want true by default for personal_new 139", account.SupportsETF, mount.SupportsETF)
	}
}

func TestProviderAccountInventoryAllowsDisabling139ETFSupport(t *testing.T) {
	storage := model.Storage{ID: 11, MountPath: "/139-d", Driver: "139Yun", Status: "work", Addition: `{"type":"personal_new","cluster_dedicated_account":false}`}
	account := providerAccountInventory("node-1", storage, 10<<30, 20<<30)

	if account.SupportsETF {
		t.Fatalf("supports ETF = true, want false when cluster_dedicated_account is explicitly disabled")
	}
}

func TestProviderAccountInventoryMapsConfigured123And115Membership(t *testing.T) {
	for _, storage := range []model.Storage{
		{ID: 1, MountPath: "/123", Driver: "123Pan", Status: "work", Addition: `{"membership_tier":"svip"}`},
		{ID: 2, MountPath: "/115", Driver: "115 Cloud", Status: "work", Addition: `{"membership_tier":"vip"}`},
	} {
		account := providerAccountInventory("node-1", storage, 1<<40, 2<<40)
		if storage.ID == 1 && (account.MembershipTier != "svip" || account.MembershipWeight != 300) {
			t.Fatalf("123 account membership = %#v", account)
		}
		if storage.ID == 2 && (account.MembershipTier != "vip" || account.MembershipWeight != 200) {
			t.Fatalf("115 account membership = %#v", account)
		}
	}
}

func TestInventoryProbeErrorCodeClassifiesCredentialFailures(t *testing.T) {
	if got := inventoryProbeErrorCode(errors.New("refresh_token 无效")); got != inventoryStatusReauthorizationRequired {
		t.Fatalf("credential probe code = %q, want %q", got, inventoryStatusReauthorizationRequired)
	}
	if got := inventoryProbeErrorCode(errors.New("connection reset by peer")); got != "provider_health_probe_failed" {
		t.Fatalf("transient probe code = %q, want provider_health_probe_failed", got)
	}
}

func TestProviderAccountInventoryDoesNotAdvertiseUnsupported115OpenShareSave(t *testing.T) {
	storage := model.Storage{ID: 3, MountPath: "/115-open", Driver: "115 Open", Status: "work"}
	account := providerAccountInventory("node-1", storage, 1<<40, 2<<40)
	if account.SupportsShareSave {
		t.Fatalf("115 Open account unexpectedly advertises share-save support: %#v", account)
	}
}

func TestProviderAccountInventoryAdvertisesGuangYaPanShareSave(t *testing.T) {
	storage := model.Storage{ID: 4, MountPath: "/guangya", Driver: "GuangYaPan", Status: "work"}
	account := providerAccountInventory("node-1", storage, 1<<40, 2<<40)
	if !account.SupportsShareSave {
		t.Fatalf("GuangYaPan account should advertise share-save support: %#v", account)
	}
}

func TestDefaultHydrateInventoryStorageClassifiesDriverInitializationFailures(t *testing.T) {
	previousLookup := getInventoryStorageByMountPath
	defer func() {
		getInventoryStorageByMountPath = previousLookup
	}()

	baseStorage := model.Storage{
		ID:        115,
		MountPath: "/115-cd2",
		Driver:    "115 CD2",
		Status:    op.WORK,
	}

	for _, tc := range []struct {
		name       string
		lookupErr  error
		driver     *inventoryStatusDriver
		wantStatus string
	}{
		{
			name:       "lookup refresh token invalid",
			lookupErr:  errors.New("refresh_token invalid: token=secret-cookie"),
			wantStatus: "reauthorization_required",
		},
		{
			name:       "lookup generic initialization failure",
			lookupErr:  errors.New("failed init storage: upstream timeout cookie=secret-cookie"),
			wantStatus: "storage_unavailable",
		},
		{
			name: "driver status refresh token invalid",
			driver: &inventoryStatusDriver{
				storage: func() model.Storage {
					storage := baseStorage
					storage.Status = "authorization expired for refresh token secret-cookie"
					return storage
				}(),
			},
			wantStatus: "reauthorization_required",
		},
		{
			name: "driver status disabled remains stable",
			driver: &inventoryStatusDriver{
				storage: func() model.Storage {
					storage := baseStorage
					storage.Status = op.DISABLED
					return storage
				}(),
			},
			wantStatus: op.DISABLED,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			getInventoryStorageByMountPath = func(mountPath string) (internaldriver.Driver, error) {
				if mountPath != baseStorage.MountPath {
					t.Fatalf("lookup mount path = %q, want %q", mountPath, baseStorage.MountPath)
				}
				if tc.lookupErr != nil {
					return nil, tc.lookupErr
				}
				return tc.driver, nil
			}

			snapshot, err := defaultHydrateInventoryStorage(context.Background(), "node-1", baseStorage)
			if err != nil {
				t.Fatalf("hydrate inventory storage: %v", err)
			}
			if snapshot.Account.Provider != "pan115" {
				t.Fatalf("provider = %q, want pan115", snapshot.Account.Provider)
			}
			if snapshot.Account.Status != tc.wantStatus {
				t.Fatalf("account status = %q, want %q", snapshot.Account.Status, tc.wantStatus)
			}
			if snapshot.Mount.Status != tc.wantStatus {
				t.Fatalf("mount status = %q, want %q", snapshot.Mount.Status, tc.wantStatus)
			}
			if snapshot.Account.SupportsDownload || snapshot.Account.SupportsUpload || snapshot.Account.SupportsShareSave || snapshot.Account.SupportsETF {
				t.Fatalf("account capabilities should all be false for unhealthy storage: %#v", snapshot.Account)
			}
			if !snapshot.Mount.ReadOnly || snapshot.Mount.CanUpload || snapshot.Mount.CanShare || snapshot.Mount.SupportsETF {
				t.Fatalf("mount capabilities should all be unhealthy: %#v", snapshot.Mount)
			}
			for _, value := range []string{snapshot.Account.Status, snapshot.Mount.Status} {
				if strings.Contains(value, "secret-cookie") || strings.Contains(strings.ToLower(value), "token=") {
					t.Fatalf("status leaked sensitive detail: %q", value)
				}
			}
		})
	}
}

func TestBuildInventoryMarks115CD2ReauthorizationFailuresUnavailable(t *testing.T) {
	oldList := listInventoryStorages
	oldConfig := getInventorySubscriptionConfig
	oldHydrate := hydrateInventoryStorage
	previousLookup := getInventoryStorageByMountPath
	defer func() {
		listInventoryStorages = oldList
		getInventorySubscriptionConfig = oldConfig
		hydrateInventoryStorage = oldHydrate
		getInventoryStorageByMountPath = previousLookup
	}()

	storage := model.Storage{ID: 115, MountPath: "/115-cd2", Driver: "115 CD2", Status: op.WORK}
	listInventoryStorages = func() ([]model.Storage, error) { return []model.Storage{storage}, nil }
	hydrateInventoryStorage = defaultHydrateInventoryStorage
	getInventorySubscriptionConfig = func() (model.SubscriptionConfig, error) {
		return model.SubscriptionConfig{Telegram: model.SubscriptionTelegramSourceConfig{
			Pan115: model.SubscriptionTelegramPanConfig{
				TempTransferTarget: model.SubscriptionStorageTarget{Provider: "pan115", Folder: "worker-staging"},
			},
		}}, nil
	}
	getInventoryStorageByMountPath = func(mountPath string) (internaldriver.Driver, error) {
		if mountPath != storage.MountPath {
			t.Fatalf("lookup mount path = %q, want %q", mountPath, storage.MountPath)
		}
		return nil, errors.New("refresh token unauthorized: cookie=secret-cookie")
	}

	report, err := BuildInventory(context.Background(), "node-1", true)
	if err != nil {
		t.Fatalf("build inventory: %v", err)
	}
	if len(report.ProviderAccounts) != 1 || len(report.Mounts) != 1 {
		t.Fatalf("inventory sizes accounts=%d mounts=%d, want 1/1", len(report.ProviderAccounts), len(report.Mounts))
	}
	account := report.ProviderAccounts[0]
	if account.Provider != "pan115" {
		t.Fatalf("provider = %q, want pan115", account.Provider)
	}
	if account.Status != "reauthorization_required" {
		t.Fatalf("account status = %q, want reauthorization_required", account.Status)
	}
	if account.SupportsShareSave || account.SupportsDownload || account.SupportsUpload || account.SupportsETF {
		t.Fatalf("unhealthy CD2 account should not advertise capabilities: %#v", account)
	}
	for _, provider := range report.Capabilities.SupportedProviders {
		if provider == "pan115" {
			t.Fatalf("unhealthy CD2 provider should not be advertised as supported: %#v", report.Capabilities)
		}
	}
	mount := report.Mounts[0]
	if mount.Status != "reauthorization_required" || !mount.ReadOnly || mount.CanShare || mount.CanUpload || mount.SupportsETF {
		t.Fatalf("unhealthy CD2 mount = %#v", mount)
	}
}

func TestBuildInventoryKeepsUnregisteredDriverUnavailable(t *testing.T) {
	oldList := listInventoryStorages
	oldConfig := getInventorySubscriptionConfig
	oldHydrate := hydrateInventoryStorage
	oldLookup := getInventoryStorageByMountPath
	defer func() {
		listInventoryStorages = oldList
		getInventorySubscriptionConfig = oldConfig
		hydrateInventoryStorage = oldHydrate
		getInventoryStorageByMountPath = oldLookup
	}()

	storage := model.Storage{ID: 123, MountPath: "/123-static", Driver: "123pan", Status: op.WORK}
	listInventoryStorages = func() ([]model.Storage, error) { return []model.Storage{storage}, nil }
	getInventorySubscriptionConfig = func() (model.SubscriptionConfig, error) {
		return model.SubscriptionConfig{}, nil
	}
	hydrateInventoryStorage = defaultHydrateInventoryStorage
	getInventoryStorageByMountPath = func(mountPath string) (internaldriver.Driver, error) {
		if mountPath != storage.MountPath {
			t.Fatalf("lookup mount path = %q, want %q", mountPath, storage.MountPath)
		}
		return nil, errors.New("no mount path for an storage is: /123-static")
	}

	report, err := BuildInventory(context.Background(), "node-1", true)
	if err != nil {
		t.Fatalf("build inventory: %v", err)
	}
	if len(report.ProviderAccounts) != 1 || len(report.Mounts) != 1 {
		t.Fatalf("inventory sizes accounts=%d mounts=%d, want 1/1", len(report.ProviderAccounts), len(report.Mounts))
	}
	account := report.ProviderAccounts[0]
	if account.Status != inventoryStatusStorageUnavailable || account.SupportsDownload || account.SupportsUpload || account.SupportsShareSave || account.SupportsETF {
		t.Fatalf("unregistered driver account must be unavailable: %#v", account)
	}
	mount := report.Mounts[0]
	if mount.Status != inventoryStatusStorageUnavailable || !mount.ReadOnly || mount.CanUpload || mount.CanShare || mount.SupportsETF {
		t.Fatalf("unregistered driver mount must be unavailable: %#v", mount)
	}
}

type inventoryStatusDriver struct {
	storage model.Storage
	config  internaldriver.Config
}

func (d *inventoryStatusDriver) Config() internaldriver.Config {
	return d.config
}

func (d *inventoryStatusDriver) GetStorage() *model.Storage {
	return &d.storage
}

func (d *inventoryStatusDriver) SetStorage(storage model.Storage) {
	d.storage = storage
}

func (d *inventoryStatusDriver) GetAddition() internaldriver.Additional {
	return nil
}

func (d *inventoryStatusDriver) Init(context.Context) error {
	return nil
}

func (d *inventoryStatusDriver) Drop(context.Context) error {
	return nil
}

func (d *inventoryStatusDriver) List(context.Context, model.Obj, model.ListArgs) ([]model.Obj, error) {
	return nil, nil
}

func (d *inventoryStatusDriver) Link(context.Context, model.Obj, model.LinkArgs) (*model.Link, error) {
	return nil, nil
}

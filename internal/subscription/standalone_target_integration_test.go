package subscription

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type resolvedTargetObservation struct {
	Request ResolveProviderTargetRequest
	Target  ResolvedProviderTarget
}

func TestStandalonePan123To139ResolvesAccountsEnsuresDeliveryAndTransfers(t *testing.T) {
	const fileSize = int64(1024)
	observations, ensured, transfers := runStandaloneTargetWorkflow(t, fileSize)

	temp := findResolvedTargetObservation(t, observations, "pan123", true, 0)
	if temp.Target.StorageID != 12 || temp.Target.FullPath != "/123-high/staging" {
		t.Fatalf("temp target = %#v, want storage 12 /123-high/staging", temp.Target)
	}
	delivery := findResolvedTargetObservation(t, observations, "yidong139", false, fileSize)
	if delivery.Target.StorageID != 21 || delivery.Target.FullPath != "/139-main/delivery" {
		t.Fatalf("delivery target = %#v, want storage 21 /139-main/delivery", delivery.Target)
	}
	if got, want := *ensured, []string{"/139-main/delivery"}; !stringSlicesEqual(got, want) {
		t.Fatalf("ensured folders = %#v, want %#v", got, want)
	}
	if len(*transfers) != 1 || !strings.HasPrefix((*transfers)[0], "/139-main/delivery/") {
		t.Fatalf("transfer targets = %#v", *transfers)
	}
}

func TestStandalone139OversizeRejectsBeforeEnsureOrTransfer(t *testing.T) {
	const fileSize = int64(6 << 30)
	_, ensured, transfers, result, err := runStandaloneTargetWorkflowResult(t, fileSize)
	if err == nil || !strings.Contains(err.Error(), "no compatible provider account") {
		t.Fatalf("error = %v, want 139 upload limit rejection", err)
	}
	if result == nil || result.Run.Status != model.SubscriptionStatusFailed {
		t.Fatalf("result = %#v, want failed run", result)
	}
	if len(*ensured) != 0 {
		t.Fatalf("ensure side effects = %#v, want none", *ensured)
	}
	if len(*transfers) != 0 {
		t.Fatalf("transfer side effects = %#v, want none", *transfers)
	}
}

func TestRunForRolePreservesStandaloneTransferFlag(t *testing.T) {
	const fileSize = int64(1024)
	_, ensured, transfers, result, err := runStandaloneTargetWorkflowResultForRole(
		t,
		fileSize,
		false,
		model.ClusterRoleStandalone,
	)
	if err != nil {
		t.Fatalf("run standalone discovery: %v", err)
	}
	if result == nil || result.Run.TransferredCount != 0 {
		t.Fatalf("result = %#v, want discovery without transfer", result)
	}
	if len(*ensured) != 0 {
		t.Fatalf("ensure side effects = %#v, want none", *ensured)
	}
	if len(*transfers) != 0 {
		t.Fatalf("transfer side effects = %#v, want none", *transfers)
	}
}

func TestTempTargetRuntimePrefersHigherMembershipPan123Account(t *testing.T) {
	oldList := listProviderTargetStorages
	oldFree := storageFreeBytesForMountPath
	defer func() {
		listProviderTargetStorages = oldList
		storageFreeBytesForMountPath = oldFree
	}()
	listProviderTargetStorages = func() ([]model.Storage, error) {
		return standaloneTargetStorages(), nil
	}
	storageFreeBytesForMountPath = func(context.Context, string) (int64, bool) { return 100 << 30, true }

	cfg, err := telegramPanSourceConfigWithStorageFallback(ShareProviderPan123, model.SubscriptionTelegramPanConfig{
		TempTransferTarget: model.SubscriptionStorageTarget{Provider: "pan123", Folder: "staging"},
	})
	if err != nil {
		t.Fatalf("resolve temp target: %v", err)
	}
	if cfg.TempTransferRoot != "/123-high/staging" {
		t.Fatalf("temp root = %q, want high-membership account path", cfg.TempTransferRoot)
	}
}

func runStandaloneTargetWorkflow(t *testing.T, fileSize int64) ([]resolvedTargetObservation, *[]string, *[]string) {
	observations, ensured, transfers, result, err := runStandaloneTargetWorkflowResult(t, fileSize)
	if err != nil {
		t.Fatalf("run standalone workflow: %v", err)
	}
	if result == nil || result.Run.TransferredCount != 1 {
		t.Fatalf("result = %#v, want one transfer", result)
	}
	return observations, ensured, transfers
}

func runStandaloneTargetWorkflowResult(t *testing.T, fileSize int64) ([]resolvedTargetObservation, *[]string, *[]string, *model.SubscriptionRunResult, error) {
	return runStandaloneTargetWorkflowResultForRole(t, fileSize, true, "")
}

func runStandaloneTargetWorkflowResultForRole(t *testing.T, fileSize int64, transfer bool, role string) ([]resolvedTargetObservation, *[]string, *[]string, *model.SubscriptionRunResult, error) {
	t.Helper()
	setupSubscriptionRuntimeDB(t)

	oldList := listProviderTargetStorages
	oldFree := storageFreeBytesForMountPath
	oldEnsure := ensureProviderTargetFolder
	oldObserve := observeResolvedProviderTarget
	oldFactory := newShareSaverForProvider
	oldSaveImported := saveImportedFilesToTemp
	oldSnapshot := snapshotPaths
	oldTransfer := applySubscriptionItemTransfer
	t.Cleanup(func() {
		listProviderTargetStorages = oldList
		storageFreeBytesForMountPath = oldFree
		ensureProviderTargetFolder = oldEnsure
		observeResolvedProviderTarget = oldObserve
		newShareSaverForProvider = oldFactory
		saveImportedFilesToTemp = oldSaveImported
		snapshotPaths = oldSnapshot
		applySubscriptionItemTransfer = oldTransfer
	})

	listProviderTargetStorages = func() ([]model.Storage, error) { return standaloneTargetStorages(), nil }
	storageFreeBytesForMountPath = func(context.Context, string) (int64, bool) { return 100 << 30, true }
	observations := make([]resolvedTargetObservation, 0, 4)
	observeResolvedProviderTarget = func(req ResolveProviderTargetRequest, target ResolvedProviderTarget) {
		observations = append(observations, resolvedTargetObservation{Request: req, Target: target})
	}
	ensured := []string{}
	ensureProviderTargetFolder = func(_ context.Context, path string) error {
		ensured = append(ensured, path)
		return nil
	}
	newShareSaverForProvider = func(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) (ShareSaver, error) {
		if provider != ShareProviderPan123 || cfg.TempTransferRoot != "/123-high/staging" {
			t.Fatalf("provider/config = %s/%#v", provider, cfg)
		}
		return &fakeShareSaver{}, nil
	}
	entry := TreeEntry{
		RootPath: "/123-high/staging", Path: "/Movie.mkv", Name: "Movie.mkv", ID: "file-1",
		Size: fileSize, Modified: time.Unix(1700000000, 0),
	}
	saveImportedFilesToTemp = func(_ context.Context, _ ShareSaver, _ string, _ []pan123ImportedFile, opts SaveShareOptions) ([]TreeEntry, error) {
		if opts.TempRoot != entry.RootPath {
			t.Fatalf("temp root = %q, want %q", opts.TempRoot, entry.RootPath)
		}
		return []TreeEntry{entry}, nil
	}
	snapshotPaths = func(_ context.Context, roots []string) (*TreeSnapshot, error) {
		if len(roots) != 1 || roots[0] != entry.RootPath {
			t.Fatalf("snapshot roots = %#v", roots)
		}
		return &TreeSnapshot{Hash: "standalone-target", Entries: []TreeEntry{entry}}, nil
	}
	var expectedSubscriptionID uint
	transfers := []string{}
	applySubscriptionItemTransfer = func(_ context.Context, sourceSub *model.Subscription, item *model.SubscriptionItem, _ bool) (*model.SubscriptionItem, int, error) {
		if sourceSub == nil || sourceSub.ID != expectedSubscriptionID {
			t.Fatalf("source subscription = %#v, want subscription %d", sourceSub, expectedSubscriptionID)
		}
		transfers = append(transfers, item.TargetPath)
		return item, 1, nil
	}

	if _, err := SaveConfig(model.SubscriptionConfig{
		DefaultTarget: model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "delivery"},
		Telegram: model.SubscriptionTelegramSourceConfig{
			Pan123: model.SubscriptionTelegramPanConfig{
				TempTransferTarget: model.SubscriptionStorageTarget{Provider: "pan123", Folder: "staging"},
				AccessToken:        "token-1",
			},
		},
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}
	sub := &model.Subscription{
		Name: "Standalone target", SourceType: model.SubscriptionSourceManual,
		SourceConfig:    fmt.Sprintf(`{"imports_text":"123FSLinkV2$bc18e4ea5fb89ec5778d1f38c9772f5f#%d#Movie.mkv"}`, fileSize),
		TransferEnabled: true, TMDBName: "Movie", MediaType: "movie",
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	expectedSubscriptionID = sub.ID
	result, err := RunForRole(context.Background(), sub.ID, transfer, role)
	return observations, &ensured, &transfers, result, err
}

func standaloneTargetStorages() []model.Storage {
	return []model.Storage{
		{ID: 11, MountPath: "/123-low", Driver: "123Pan", Status: "work", Addition: `{"membership_weight":10}`},
		{ID: 12, MountPath: "/123-high", Driver: "123Pan", Status: "work", Addition: `{"membership_weight":20}`},
		{ID: 21, MountPath: "/139-main", Driver: "139Yun", Status: "work", Addition: `{"membership_tier":"ordinary"}`},
	}
}

func findResolvedTargetObservation(t *testing.T, observations []resolvedTargetObservation, provider string, shareSave bool, fileSize int64) resolvedTargetObservation {
	t.Helper()
	for _, observation := range observations {
		if observation.Request.Provider == provider && observation.Request.NeedShareSave == shareSave && observation.Request.FileSize == fileSize {
			return observation
		}
	}
	t.Fatalf("missing resolved target observation provider=%s share_save=%v size=%d: %#v", provider, shareSave, fileSize, observations)
	return resolvedTargetObservation{}
}

package subscription

import (
	"context"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestResolveProviderTargetPrefersHigherFreeSpace(t *testing.T) {
	oldList := listProviderTargetStorages
	oldFree := storageFreeBytesForMountPath
	defer func() {
		listProviderTargetStorages = oldList
		storageFreeBytesForMountPath = oldFree
	}()

	listProviderTargetStorages = func() ([]model.Storage, error) {
		return []model.Storage{
			{ID: 1, MountPath: "/123-a", Driver: "123Pan", Status: "work"},
			{ID: 2, MountPath: "/123-b", Driver: "123Pan", Status: "work"},
		}, nil
	}
	storageFreeBytesForMountPath = func(ctx context.Context, mountPath string) (int64, bool) {
		switch mountPath {
		case "/123-a":
			return 100, true
		case "/123-b":
			return 200, true
		default:
			return 0, false
		}
	}

	resolved, err := ResolveProviderTarget(context.Background(), ResolveProviderTargetRequest{
		Provider: "pan123",
		Folder:   "转存至移动",
	})
	if err != nil {
		t.Fatalf("resolve provider target: %v", err)
	}
	if resolved.StorageID != 2 {
		t.Fatalf("storage id = %d, want 2", resolved.StorageID)
	}
	if resolved.MountPath != "/123-b" {
		t.Fatalf("mount path = %q, want /123-b", resolved.MountPath)
	}
	if resolved.FullPath != "/123-b/转存至移动" {
		t.Fatalf("full path = %q, want /123-b/转存至移动", resolved.FullPath)
	}
}

func TestResolveProviderTargetFromCandidatesFiltersCapabilitiesAndSortsAccounts(t *testing.T) {
	candidates := []ProviderAccountCandidate{
		{Provider: "pan123", StorageID: 1, MountPath: "/123-a", Status: "work", SupportsShareSave: true, SupportsDownload: true, FreeBytes: 500, HasFreeBytes: true, MembershipWeight: 10, ActiveJobs: 0},
		{Provider: "pan123", StorageID: 2, MountPath: "/123-b", Status: "work", SupportsShareSave: true, SupportsDownload: true, FreeBytes: 400, HasFreeBytes: true, MembershipWeight: 20, ActiveJobs: 2},
		{Provider: "pan123", StorageID: 3, MountPath: "/123-c", Status: "work", SupportsShareSave: false, SupportsDownload: true, FreeBytes: 900, HasFreeBytes: true, MembershipWeight: 100},
	}
	resolved, err := ResolveProviderTargetFromCandidates(context.Background(), ResolveProviderTargetRequest{
		Provider: "pan123", Folder: "转存至移动", NeedShareSave: true, FileSize: 100,
	}, candidates)
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	if resolved.StorageID != 2 {
		t.Fatalf("storage id = %d, want membership-preferred account 2", resolved.StorageID)
	}
	if resolved.ActiveJobs != 2 || resolved.MembershipWeight != 20 {
		t.Fatalf("resolved metadata = %#v", resolved)
	}
}

func TestResolveProviderTargetFromCandidatesRejects139OversizeUpload(t *testing.T) {
	_, err := ResolveProviderTargetFromCandidates(context.Background(), ResolveProviderTargetRequest{
		Provider: "yidong139", Folder: "剧集", NeedUpload: true, FileSize: 9 << 30,
	}, []ProviderAccountCandidate{{
		Provider: "yidong139", StorageID: 1, MountPath: "/139", Status: "work", SupportsUpload: true,
		FreeBytes: 100 << 30, HasFreeBytes: true, MaxSingleUploadBytes: 8 << 30,
	}})
	if err == nil {
		t.Fatal("expected upload limit rejection")
	}
}

func TestResolveProviderTargetFromCandidatesPrefersLowerLoadAfterCapacityTie(t *testing.T) {
	resolved, err := ResolveProviderTargetFromCandidates(context.Background(), ResolveProviderTargetRequest{
		Provider: "pan115", Folder: "转存", NeedShareSave: true,
	}, []ProviderAccountCandidate{
		{Provider: "pan115", StorageID: 1, MountPath: "/115-a", Status: "work", SupportsShareSave: true, SupportsDownload: true, HasFreeBytes: true, FreeBytes: 100, MembershipWeight: 10, ActiveJobs: 3},
		{Provider: "pan115", StorageID: 2, MountPath: "/115-b", Status: "work", SupportsShareSave: true, SupportsDownload: true, HasFreeBytes: true, FreeBytes: 100, MembershipWeight: 10, ActiveJobs: 1},
	})
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	if resolved.StorageID != 2 {
		t.Fatalf("storage id = %d, want lower-load account 2", resolved.StorageID)
	}
}

func TestResolveProviderTargetRejectsAbnormalOrNonDownloadableShareAccounts(t *testing.T) {
	_, err := ResolveProviderTargetFromCandidates(context.Background(), ResolveProviderTargetRequest{
		Provider: "pan123", Folder: "转存", NeedShareSave: true,
	}, []ProviderAccountCandidate{
		{Provider: "pan123", StorageID: 1, MountPath: "/failed", Status: "error", SupportsShareSave: true, SupportsDownload: true},
		{Provider: "pan123", StorageID: 2, MountPath: "/disabled", Status: "work", Disabled: true, SupportsShareSave: true, SupportsDownload: true},
		{Provider: "pan123", StorageID: 3, MountPath: "/no-download", Status: "work", SupportsShareSave: true, SupportsDownload: false},
	})
	if err == nil {
		t.Fatal("expected all abnormal or non-downloadable accounts to be rejected")
	}
}

func TestResolveProviderTargetRejectsUnknown139Membership(t *testing.T) {
	oldList := listProviderTargetStorages
	oldFree := storageFreeBytesForMountPath
	defer func() {
		listProviderTargetStorages = oldList
		storageFreeBytesForMountPath = oldFree
	}()
	listProviderTargetStorages = func() ([]model.Storage, error) {
		return []model.Storage{{ID: 1, MountPath: "/139", Driver: "139Yun", Status: "work"}}, nil
	}
	storageFreeBytesForMountPath = func(context.Context, string) (int64, bool) { return 100 << 30, true }

	_, err := ResolveProviderTarget(context.Background(), ResolveProviderTargetRequest{
		Provider: "yidong139", Folder: "剧集", NeedUpload: true, FileSize: 1 << 30,
	})
	if err == nil {
		t.Fatal("expected unknown 139 membership to be ineligible for upload")
	}
}

func TestResolveProviderTargetEnforcesConfiguredOrdinary139UploadLimit(t *testing.T) {
	oldList := listProviderTargetStorages
	oldFree := storageFreeBytesForMountPath
	defer func() {
		listProviderTargetStorages = oldList
		storageFreeBytesForMountPath = oldFree
	}()
	listProviderTargetStorages = func() ([]model.Storage, error) {
		return []model.Storage{{ID: 1, MountPath: "/139", Driver: "139Yun", Status: "work", Addition: `{"membership_tier":"ordinary"}`}}, nil
	}
	storageFreeBytesForMountPath = func(context.Context, string) (int64, bool) { return 100 << 30, true }

	_, err := ResolveProviderTarget(context.Background(), ResolveProviderTargetRequest{
		Provider: "yidong139", Folder: "剧集", NeedUpload: true, FileSize: 6 << 30,
	})
	if err == nil {
		t.Fatal("expected configured ordinary 139 account 5 GiB upload limit rejection")
	}
}

func TestResolveProviderTargetRejectsUnsafeFolderBeforeListingStorages(t *testing.T) {
	oldList := listProviderTargetStorages
	defer func() { listProviderTargetStorages = oldList }()
	called := false
	listProviderTargetStorages = func() ([]model.Storage, error) {
		called = true
		return nil, nil
	}
	_, err := ResolveProviderTarget(context.Background(), ResolveProviderTargetRequest{Provider: "pan123", Folder: "../escape"})
	if err == nil {
		t.Fatal("expected unsafe folder rejection")
	}
	if called {
		t.Fatal("storage listing should not run for an invalid target")
	}
}

func TestEnsureResolvedProviderFolder(t *testing.T) {
	oldEnsure := ensureProviderTargetFolder
	defer func() { ensureProviderTargetFolder = oldEnsure }()
	var ensured string
	ensureProviderTargetFolder = func(ctx context.Context, fullPath string) error {
		ensured = fullPath
		return nil
	}
	target := ResolvedProviderTarget{Provider: "pan123", MountPath: "/123", Folder: "转存", FullPath: "/123/转存"}
	got, err := EnsureResolvedProviderFolder(context.Background(), target)
	if err != nil {
		t.Fatalf("ensure folder: %v", err)
	}
	if ensured != target.FullPath || got.FullPath != target.FullPath {
		t.Fatalf("ensured = %q target = %#v", ensured, got)
	}
}

func TestTelegramPanSourceConfigWithStorageFallbackReturnsResolverError(t *testing.T) {
	oldList := listProviderTargetStorages
	defer func() { listProviderTargetStorages = oldList }()
	listProviderTargetStorages = func() ([]model.Storage, error) { return nil, nil }

	_, err := telegramPanSourceConfigWithStorageFallback(ShareProviderPan123, model.SubscriptionTelegramPanConfig{
		TempTransferTarget: model.SubscriptionStorageTarget{Provider: "pan123", Folder: "转存"},
	})
	if err == nil {
		t.Fatal("expected resolver error to be returned")
	}
}

func TestResolveSubscriptionDeliveryTargetUsesRuntimePathWithoutMutatingStoredLegacyPath(t *testing.T) {
	oldList := listProviderTargetStorages
	oldFree := storageFreeBytesForMountPath
	oldEnsure := ensureProviderTargetFolder
	defer func() {
		listProviderTargetStorages = oldList
		storageFreeBytesForMountPath = oldFree
		ensureProviderTargetFolder = oldEnsure
	}()
	listProviderTargetStorages = func() ([]model.Storage, error) {
		return []model.Storage{{ID: 7, MountPath: "/139-account", Driver: "139Yun", Status: "work", Addition: `{"membership_tier":"diamond"}`}}, nil
	}
	storageFreeBytesForMountPath = func(context.Context, string) (int64, bool) { return 100 << 30, true }
	var ensured string
	ensureProviderTargetFolder = func(_ context.Context, path string) error { ensured = path; return nil }

	sub := &model.Subscription{
		TargetRoot:     "/139_60t/legacy",
		DeliveryTarget: model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "港台剧"},
	}
	runtimeSub, err := resolveSubscriptionDeliveryTarget(context.Background(), sub, true)
	if err != nil {
		t.Fatalf("resolve subscription target: %v", err)
	}
	if runtimeSub.TargetRoot != "/139-account/港台剧" || ensured != runtimeSub.TargetRoot {
		t.Fatalf("runtime target = %q ensured = %q", runtimeSub.TargetRoot, ensured)
	}
	if sub.TargetRoot != "/139_60t/legacy" {
		t.Fatalf("stored legacy path mutated to %q", sub.TargetRoot)
	}
}

func TestTelegramPanSourceConfigWithStorageFallbackResolvesTempTransferTarget(t *testing.T) {
	oldList := listProviderTargetStorages
	oldFree := storageFreeBytesForMountPath
	defer func() {
		listProviderTargetStorages = oldList
		storageFreeBytesForMountPath = oldFree
	}()

	listProviderTargetStorages = func() ([]model.Storage, error) {
		return []model.Storage{{ID: 3, MountPath: "/123-main", Driver: "123Pan", Status: "work"}}, nil
	}
	storageFreeBytesForMountPath = func(ctx context.Context, mountPath string) (int64, bool) {
		return 300, true
	}

	cfg, err := telegramPanSourceConfigWithStorageFallback(ShareProviderPan123, model.SubscriptionTelegramPanConfig{
		TempTransferTarget: model.SubscriptionStorageTarget{
			Provider: "pan123",
			Folder:   "转存至移动",
		},
	})
	if err != nil {
		t.Fatalf("resolve temp config: %v", err)
	}
	if cfg.TempTransferRoot != "/123-main/转存至移动" {
		t.Fatalf("temp root = %q, want /123-main/转存至移动", cfg.TempTransferRoot)
	}
	if cfg.TempTransferTarget.Provider != "pan123" || cfg.TempTransferTarget.Folder != "转存至移动" {
		t.Fatalf("temp transfer target = %#v", cfg.TempTransferTarget)
	}
}

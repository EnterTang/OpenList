package subscription

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	stdpath "path"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/pkg/errors"
)

var subscriptionRunLocks sync.Map // subscriptionID -> *sync.Mutex

func lockSubscriptionRun(subscriptionID uint) func() {
	value, _ := subscriptionRunLocks.LoadOrStore(subscriptionID, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func Run(ctx context.Context, subscriptionID uint, transfer bool) (*model.SubscriptionRunResult, error) {
	return run(ctx, subscriptionID, transfer, false)
}

// RunForRole selects the same subscription execution path for manual and
// scheduled runs. Only standalone deployments may enqueue local transfers.
func RunForRole(ctx context.Context, subscriptionID uint, transfer bool, role string) (*model.SubscriptionRunResult, error) {
	if subscriptionTransfersLocally(role) {
		return Run(ctx, subscriptionID, transfer)
	}
	return RunCluster(ctx, subscriptionID)
}

// RetryFailedForRole replays durable cluster media children in coordinator
// roles. A normal cluster run performs source discovery and is not a retry:
// using it here would create a new inspect observation while leaving the
// existing failed media children untouched.
func RetryFailedForRole(ctx context.Context, subscriptionID uint, role string) (*model.SubscriptionRunResult, error) {
	if subscriptionTransfersLocally(role) {
		if _, err := db.ResetFailedSubscriptionItems(ctx, subscriptionID); err != nil {
			return nil, err
		}
		return Run(ctx, subscriptionID, true)
	}
	dispatcher := currentClusterDispatcher()
	retrier, ok := dispatcher.(ClusterFailedSubscriptionRetrier)
	if !ok {
		return nil, errors.New("cluster subscription retry is unavailable")
	}
	unlock := lockSubscriptionRun(subscriptionID)
	defer unlock()
	if _, err := ReconcileSubscriptionExecution(ctx, subscriptionID); err != nil {
		return nil, err
	}
	retry, err := retrier.RetryFailedSubscriptionItems(ctx, subscriptionID)
	if err != nil {
		return nil, err
	}
	orphanRetry, orphanErr := RetryOrphanedClusterSubscriptionItems(ctx, subscriptionID)
	if orphanErr != nil {
		return nil, orphanErr
	}
	requeued := retry.Requeued + orphanRetry.Requeued
	unmatched := orphanRetry.Unmatched
	result, finishErr := finishClusterRetry(subscriptionID, requeued)
	if finishErr != nil {
		return nil, finishErr
	}
	if unmatched > 0 {
		return result, fmt.Errorf("%d failed subscription items have no replayable source or cluster job", unmatched)
	}
	return result, nil
}

func finishClusterRetry(subscriptionID uint, requeued int) (*model.SubscriptionRunResult, error) {
	sub, err := db.GetSubscriptionByID(subscriptionID)
	if err != nil {
		return nil, err
	}
	items, err := db.ListSubscriptionItems(subscriptionID)
	if err != nil {
		return nil, err
	}
	if requeued == 0 {
		return &model.SubscriptionRunResult{Subscription: sub, Items: items}, nil
	}
	now := time.Now()
	status := aggregateSubscriptionStatus(items)
	if requeued > 0 {
		status = model.SubscriptionStatusRunning
	}
	run := &model.SubscriptionRun{
		SubscriptionID:   subscriptionID,
		StartedAt:        now,
		FinishedAt:       &now,
		Status:           status,
		PreviousTreeHash: sub.LastTreeHash,
		CurrentTreeHash:  sub.LastTreeHash,
		QueuedCount:      requeued,
	}
	projection := projectSubscriptionRun(subscriptionRunProjectionInput{
		Items:             items,
		DiscoveredHint:    len(items),
		DispatchedHint:    requeued,
		HasDispatchStage:  true,
		DispatchSucceeded: true,
		TransferRequested: true,
		ClusterDispatch:   true,
	})
	run.DiscoveredCount = projection.DiscoveredCount
	run.DispatchedCount = projection.DispatchedCount
	run.SucceededCount = projection.SucceededCount
	run.SkippedCount = projection.SkippedCount
	run.RetryableCount = projection.RetryableCount
	run.BlockedCount = projection.BlockedCount
	run.UnknownCount = projection.UnknownCount
	run.FailedCount = projection.FailedCount
	run.DiscoverStatus = projection.DiscoverStatus
	run.DispatchStatus = projection.DispatchStatus
	run.TransferStatus = projection.TransferStatus
	run.CompletionState = projection.CompletionState
	if err := db.CreateSubscriptionRun(run); err != nil {
		return nil, err
	}
	sub.LastStatus = run.Status
	sub.LastError = ""
	sub.LastCheckedAt = &now
	if err := db.UpdateSubscription(sub); err != nil {
		return nil, err
	}
	return &model.SubscriptionRunResult{Subscription: sub, Run: run, Items: items}, nil
}

func subscriptionTransfersLocally(role string) bool {
	role = strings.ToLower(strings.TrimSpace(role))
	return role == "" || role == model.ClusterRoleStandalone
}

// RunCluster performs discovery and planning on a Coordinator, then hands
// media children to the registered cluster dispatcher instead of invoking the
// local OpenList copy/move pipeline.
func RunCluster(ctx context.Context, subscriptionID uint) (*model.SubscriptionRunResult, error) {
	return run(ctx, subscriptionID, false, true)
}

func run(ctx context.Context, subscriptionID uint, transfer, clusterDispatch bool) (*model.SubscriptionRunResult, error) {
	unlock := lockSubscriptionRun(subscriptionID)
	defer unlock()
	sub, err := db.GetSubscriptionByID(subscriptionID)
	if err != nil {
		return nil, err
	}
	if err := ApplyDefaults(sub); err != nil {
		return nil, err
	}
	started := time.Now()
	run := &model.SubscriptionRun{
		SubscriptionID:   sub.ID,
		StartedAt:        started,
		Status:           model.SubscriptionStatusRunning,
		PreviousTreeHash: sub.LastTreeHash,
	}
	sub.LastStatus = model.SubscriptionStatusRunning
	sub.LastError = ""
	_ = db.UpdateSubscription(sub)

	var items []model.SubscriptionItem
	var currentHash string
	var added, changed, transferred int
	var runErr error
	runtimeSub := sub
	if !clusterDispatch {
		runtimeSub, runErr = resolveSubscriptionDeliveryTarget(ctx, sub, false)
	}
	if runErr != nil {
		// Keep the normal run finalization path so resolution failures are
		// persisted and surfaced like discovery or transfer failures.
	} else if clusterDispatch {
		items, currentHash, added, changed, transferred, runErr = runClusterBySource(ctx, sub)
	} else {
		items, currentHash, added, changed, transferred, runErr = runBySource(ctx, runtimeSub, transfer)
	}
	finished := time.Now()
	run.FinishedAt = &finished
	run.CurrentTreeHash = currentHash
	run.AddedCount = added
	run.ChangedCount = changed
	durableItems, durableItemsErr := db.ListSubscriptionItems(sub.ID)
	if durableItemsErr != nil && runErr == nil {
		runErr = durableItemsErr
	}
	if clusterDispatch {
		// Cluster dispatch counts are recorded separately. The legacy field
		// must only describe items whose durable state is actually transferred,
		// never items that were merely submitted to a worker.
		run.TransferredCount = summarizeSubscriptionItems(durableItems).SucceededCount
	} else {
		run.TransferredCount = transferred
	}
	durableStatus := aggregateSubscriptionStatus(durableItems)
	activeClusterJobs := false
	if clusterDispatch && runErr == nil {
		activeClusterJobs, durableItemsErr = subscriptionHasActiveClusterJobs(ctx, sub.ID)
		if durableItemsErr != nil {
			runErr = durableItemsErr
		}
	}
	if runErr != nil && clusterDispatch && durableStatus == model.SubscriptionStatusRunning && errors.Is(runErr, ErrClusterWorkerUnavailable) {
		// No compatible worker is a recoverable scheduling condition. Keep the
		// item pending and let the scheduler retry when worker capability returns.
		run.Status = model.SubscriptionStatusRunning
		sub.LastStatus = model.SubscriptionStatusRunning
		sub.LastError = runErr.Error()
	} else if runErr != nil {
		run.Status = model.SubscriptionStatusFailed
		run.Error = runErr.Error()
		sub.LastStatus = model.SubscriptionStatusFailed
		sub.LastError = runErr.Error()
	} else {
		run.Status = durableStatus
		if clusterDispatch && activeClusterJobs && run.Status == model.SubscriptionStatusSuccess {
			run.Status = model.SubscriptionStatusRunning
		}
		sub.LastStatus = run.Status
		sub.LastError = ""
		if run.Status == model.SubscriptionStatusSuccess {
			sub.LastTreeHash = currentHash
		}
	}
	discoveredHint := len(durableItems)
	if clusterDispatch && transferred > discoveredHint {
		discoveredHint = transferred
	}
	discoverySucceeded := runErr == nil ||
		discoveredHint > 0 ||
		added > 0 ||
		changed > 0 ||
		transferred > 0 ||
		(strings.TrimSpace(currentHash) != "" && currentHash != run.PreviousTreeHash)
	projection := projectSubscriptionRun(subscriptionRunProjectionInput{
		Items:              durableItems,
		DiscoveredHint:     discoveredHint,
		DispatchedHint:     map[bool]int{true: transferred, false: 0}[clusterDispatch],
		HasDiscoveryStage:  true,
		HasDispatchStage:   clusterDispatch,
		DiscoverySucceeded: discoverySucceeded,
		DispatchSucceeded:  clusterDispatch && runErr == nil,
		TransferRequested:  transfer,
		ClusterDispatch:    clusterDispatch,
	})
	run.DiscoveredCount = projection.DiscoveredCount
	run.DispatchedCount = projection.DispatchedCount
	run.SucceededCount = projection.SucceededCount
	run.SkippedCount = projection.SkippedCount
	run.RetryableCount = projection.RetryableCount
	run.BlockedCount = projection.BlockedCount
	run.UnknownCount = projection.UnknownCount
	run.FailedCount = projection.FailedCount
	run.DiscoverStatus = projection.DiscoverStatus
	run.DispatchStatus = projection.DispatchStatus
	run.TransferStatus = projection.TransferStatus
	run.CompletionState = projection.CompletionState
	sub.LastCheckedAt = &finished
	if shouldPersistSubscriptionRun(run) {
		_ = db.CreateSubscriptionRun(run)
	}
	_ = db.UpdateSubscription(sub)
	return &model.SubscriptionRunResult{
		Subscription: sub,
		Run:          run,
		Items:        items,
	}, runErr
}

func subscriptionHasActiveClusterJobs(ctx context.Context, subscriptionID uint) (bool, error) {
	if subscriptionID == 0 {
		return false, nil
	}
	var count int64
	err := db.GetDb().WithContext(ctx).Model(&model.ClusterJob{}).
		Where("subscription_id = ? AND status IN ?", subscriptionID, subscriptionActiveJobStatuses()).Count(&count).Error
	return count > 0, err
}

func resolveSubscriptionDeliveryTarget(ctx context.Context, sub *model.Subscription, ensure bool) (*model.Subscription, error) {
	return resolveSubscriptionDeliveryTargetForFile(ctx, sub, 0, ensure)
}

func resolveSubscriptionDeliveryTargetForFile(ctx context.Context, sub *model.Subscription, fileSize int64, ensure bool) (*model.Subscription, error) {
	if sub == nil {
		return nil, errors.New("subscription is nil")
	}
	runtimeSub := *sub
	target := NormalizeSubscriptionStorageTarget(sub.DeliveryTarget)
	if target.Provider == "" {
		cfg, err := GetConfig()
		if err != nil {
			return nil, fmt.Errorf("get standalone subscription target defaults: %w", err)
		}
		target = NormalizeSubscriptionStorageTarget(cfg.DefaultTarget)
	}
	if target.Provider == "" {
		if sub.TransferEnabled {
			return nil, errors.New("delivery target is required for standalone subscriptions; configure subscription_config.default_target or an explicit delivery target")
		}
		return &runtimeSub, nil
	}
	resolved, err := ResolveProviderTarget(ctx, ResolveProviderTargetRequest{
		Provider:   target.Provider,
		Folder:     target.Folder,
		NeedUpload: true,
		FileSize:   fileSize,
	})
	if err != nil {
		return nil, fmt.Errorf("resolve subscription delivery target: %w", err)
	}
	if ensure {
		resolved, err = EnsureResolvedProviderFolder(ctx, resolved)
		if err != nil {
			return nil, err
		}
	}
	runtimeSub.TargetRoot = resolved.FullPath
	return &runtimeSub, nil
}

func shouldPersistSubscriptionRun(run *model.SubscriptionRun) bool {
	if run == nil {
		return false
	}
	if run.Status != model.SubscriptionStatusSuccess {
		return true
	}
	if strings.TrimSpace(run.Error) != "" {
		return true
	}
	return run.AddedCount > 0 || run.ChangedCount > 0 || run.TransferredCount > 0
}

func Preview(ctx context.Context, subscriptionID uint) ([]model.SubscriptionItem, error) {
	result, err := Run(ctx, subscriptionID, false)
	if result != nil {
		return result.Items, err
	}
	return nil, err
}

func runBySource(ctx context.Context, sub *model.Subscription, transfer bool) ([]model.SubscriptionItem, string, int, int, int, error) {
	switch strings.ToLower(strings.TrimSpace(sub.SourceType)) {
	case model.SubscriptionSourceManual, "":
		return runManual(ctx, sub, transfer)
	case model.SubscriptionSourceTelegram:
		return runTelegram(ctx, sub, transfer)
	case model.SubscriptionSourcePanSou:
		return runPanSou(ctx, sub, transfer)
	case model.SubscriptionSourceHDHive, model.SubscriptionSourceAuto:
		return runHDHive(ctx, sub, transfer)
	default:
		return nil, sub.LastTreeHash, 0, 0, 0, fmt.Errorf("unsupported subscription source type: %s", sub.SourceType)
	}
}

func runManual(ctx context.Context, sub *model.Subscription, transfer bool) ([]model.SubscriptionItem, string, int, int, int, error) {
	cfg, err := parseManualConfig(sub.SourceConfig)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	now := time.Now()
	var saved []model.SubscriptionItem
	added := 0
	changed := 0
	transferred := 0
	var shareCfg model.SubscriptionTelegramSourceConfig
	var inspected []shareTransferCandidate
	if len(cfg.Links) > 0 || strings.TrimSpace(cfg.ImportsText) != "" {
		globalCfg, err := GetConfig()
		if err != nil {
			return saved, sub.LastTreeHash, added, changed, transferred, err
		}
		shareCfg = globalCfg.Telegram
	}

	for _, link := range cfg.Links {
		source, candidates, handled, inspectErr := inspectShareLinkCandidatesFn(ctx, sub, shareCfg, link, now)
		if handled {
			inspected = append(inspected, candidates...)
			continue
		}
		item := manualLinkItem(sub, link, now)
		if inspectErr != nil {
			item.LastError = "share URL inspect failed: " + inspectErr.Error()
		} else if source.Name != "" {
			item.LastError = "share URL provider is not ready for temp transfer"
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

	if strings.TrimSpace(cfg.ImportsText) != "" {
		files, _, err := parseManualImportText(cfg.ImportsText)
		if err != nil {
			return saved, sub.LastTreeHash, added, changed, transferred, err
		}
		panCfg, err := telegramPanSourceConfigWithStorageFallback(
			ShareProviderPan123,
			normalizeTelegramPanConfig(shareCfg.Pan123),
		)
		if err != nil {
			return saved, sub.LastTreeHash, added, changed, transferred, err
		}
		if strings.TrimSpace(panCfg.TempTransferRoot) == "" {
			return saved, sub.LastTreeHash, added, changed, transferred, fmt.Errorf("pan123 temp_transfer_root is required for manual imports")
		}
		if strings.TrimSpace(panCfg.AccessToken) == "" {
			return saved, sub.LastTreeHash, added, changed, transferred, fmt.Errorf("pan123 access_token is required for manual imports; configure a 123Pan storage so the token can be loaded automatically")
		}
		source := telegramPanSubscriptionSource{Name: string(ShareProviderPan123), Config: panCfg}
		inspected = append(inspected, importFilesToShareTransferCandidates(sub, source, "manual_import://pan123", files, now)...)
	}

	selected := selectShareTransferCandidates(sub, inspected, shareCfg.TransferPriority)
	hashParts := make([]string, 0, 2)
	if len(selected) > 0 {
		tempItems, _, tempAdded, tempChanged, tempTransferred, err := transferSelectedShareCandidates(ctx, sub, selected, transfer, now, "")
		if err != nil {
			return saved, sub.LastTreeHash, added, changed, transferred, err
		}
		saved = append(saved, tempItems...)
		added += tempAdded
		changed += tempChanged
		transferred += tempTransferred
	}

	if len(cfg.Paths) > 0 {
		snapshot, err := snapshotPaths(ctx, cfg.Paths)
		if err != nil {
			return saved, sub.LastTreeHash, added, changed, transferred, err
		}
		var pathCandidates []telegramTempCandidate
		for _, entry := range MediaFiles(snapshot.Entries) {
			if !subscriptionEntryMatches(sub, entry) {
				continue
			}
			pathCandidates = append(pathCandidates, telegramTempCandidate{
				Entry: entry,
				Item:  itemFromEntry(sub, entry, now),
			})
		}
		tempItems, _, tempAdded, tempChanged, tempTransferred, err := finalizeTempTransferCandidates(ctx, sub, pathCandidates, shareCfg.TransferPriority, transfer, now, snapshot.Hash)
		if err != nil {
			return saved, snapshot.Hash, added, changed, transferred, err
		}
		saved = append(saved, tempItems...)
		added += tempAdded
		changed += tempChanged
		transferred += tempTransferred
		hashParts = append(hashParts, snapshot.Hash)
	}
	hash := ""
	if len(hashParts) > 0 {
		hash = combinedHash("manual", hashParts)
	}
	if len(cfg.Links) > 0 {
		hash = combinedHash(hash, cfg.Links)
	}
	if cfg.ImportsText != "" {
		hash = combinedHash(hash, []string{cfg.ImportsText})
	}
	if hash == "" {
		hash = sub.LastTreeHash
	}
	return saved, hash, added, changed, transferred, nil
}

func parseManualConfig(raw string) (model.SubscriptionManualSourceConfig, error) {
	var cfg model.SubscriptionManualSourceConfig
	if strings.TrimSpace(raw) == "" {
		return cfg, nil
	}
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return cfg, errors.WithMessage(err, "invalid manual source config")
	}
	cfg.Paths = cleanStringList(cfg.Paths, true)
	cfg.Links = cleanStringList(cfg.Links, false)
	cfg.ImportsText = strings.TrimSpace(cfg.ImportsText)
	return cfg, nil
}

func runPanSou(ctx context.Context, sub *model.Subscription, transfer bool) ([]model.SubscriptionItem, string, int, int, int, error) {
	cfg, err := parsePanSouConfig(sub.SourceConfig)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	results, err := searchPanSouResourcesForSubscription(ctx, sub, cfg)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	now := time.Now()
	var saved []model.SubscriptionItem
	added := 0
	changed := 0
	transferred := 0
	globalCfg, err := GetConfig()
	if err != nil {
		return saved, sub.LastTreeHash, added, changed, transferred, err
	}
	var inspected []shareTransferCandidate
	for _, result := range results {
		for _, link := range result.Links {
			source, candidates, handled, inspectErr := inspectShareLinkCandidatesFn(ctx, sub, globalCfg.Telegram, link.URL, now)
			if handled {
				inspected = append(inspected, candidates...)
				continue
			}
			item := panSouLinkItem(sub, result, link, now)
			if inspectErr != nil {
				item.LastError = "pansou share URL inspect failed: " + inspectErr.Error()
			} else if source.Name != "" {
				item.LastError = "pansou share URL provider is not ready for temp transfer"
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
	selected := selectShareTransferCandidates(sub, inspected, globalCfg.Telegram.TransferPriority)
	links := panSouResultLinks(results)
	hash := combinedHash("", links)
	if len(selected) > 0 {
		tempItems, tempHash, tempAdded, tempChanged, tempTransferred, err := transferSelectedShareCandidates(ctx, sub, selected, transfer, now, hash)
		if err != nil {
			return saved, sub.LastTreeHash, added, changed, transferred, err
		}
		saved = append(saved, tempItems...)
		added += tempAdded
		changed += tempChanged
		transferred += tempTransferred
		if tempHash != "" {
			hash = tempHash
		}
	}
	return saved, hash, added, changed, transferred, nil
}

func parsePanSouConfig(raw string) (model.SubscriptionPanSouSourceConfig, error) {
	var cfg model.SubscriptionPanSouSourceConfig
	if strings.TrimSpace(raw) == "" {
		return normalizePanSouSourceConfig(cfg), nil
	}
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return cfg, errors.WithMessage(err, "invalid pansou source config")
	}
	return normalizePanSouSourceConfig(cfg), nil
}

func panSouLinkItem(sub *model.Subscription, result model.SubscriptionResourceSearchResult, link model.SubscriptionResourceSearchLink, seenAt time.Time) *model.SubscriptionItem {
	keyMaterial := fmt.Sprintf("%d:%s:%s", sub.ID, result.Title, link.URL)
	provider := normalizeSubscriptionProvider(link.Provider)
	if provider == "" {
		provider = sourceProviderFromURL(link.URL)
	}
	return &model.SubscriptionItem{
		SubscriptionID: sub.ID,
		SourceKey:      "pansou:" + shortHash(keyMaterial),
		SourceProvider: provider,
		SourceURL:      link.URL,
		FileHash:       shortHash(link.URL),
		Status:         model.SubscriptionItemStatusSkipped,
		LastSeenAt:     seenAt,
		LastError:      "pansou share URL is discovered; mount or provider transfer is required before file-tree checks",
	}
}

func panSouResultLinks(results []model.SubscriptionResourceSearchResult) []string {
	seen := map[string]struct{}{}
	var links []string
	for _, result := range results {
		for _, link := range result.Links {
			value := strings.TrimSpace(link.URL)
			if value == "" {
				continue
			}
			if _, ok := seen[value]; ok {
				continue
			}
			seen[value] = struct{}{}
			links = append(links, value)
		}
	}
	return links
}

func cleanStringList(values []string, fixPath bool) []string {
	seen := map[string]struct{}{}
	cleaned := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if fixPath {
			value = utils.FixAndCleanPath(value)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		cleaned = append(cleaned, value)
	}
	return cleaned
}

func appendPathOnce(paths []string, path string) []string {
	path = cleanConfigPath(path)
	if path == "" {
		return paths
	}
	for _, existing := range paths {
		if cleanConfigPath(existing) == path {
			return paths
		}
	}
	return append(paths, path)
}

func manualLinkItem(sub *model.Subscription, link string, seenAt time.Time) *model.SubscriptionItem {
	sum := sha256.Sum256([]byte(link))
	key := hex.EncodeToString(sum[:])
	return &model.SubscriptionItem{
		SubscriptionID: sub.ID,
		SourceKey:      key,
		SourceProvider: sourceProviderFromURL(link),
		SourceURL:      link,
		FileHash:       key,
		Status:         model.SubscriptionItemStatusSkipped,
		LastSeenAt:     seenAt,
		LastError:      "share URL is recorded but not mounted as an OpenList path yet",
	}
}

func sourceProviderFromURL(raw string) string {
	provider, ok := DetectShareProvider(strings.TrimSpace(raw))
	if !ok {
		return ""
	}
	return string(provider)
}

func normalizeSubscriptionProvider(value string) string {
	return normalizeTransferPriorityName(value)
}

func itemFromEntry(sub *model.Subscription, entry TreeEntry, seenAt time.Time) *model.SubscriptionItem {
	item := &model.SubscriptionItem{
		SubscriptionID: sub.ID,
		SourceKey:      SourceKey(entry),
		SourcePath:     fullPath(entry),
		FileID:         entry.ID,
		FilePath:       entry.Path,
		FileName:       entry.Name,
		FileSize:       entry.Size,
		FileHash:       FileHash(entry),
		ProviderData:   cloneShareItemProviderData(entry.ProviderData),
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     seenAt,
	}
	return syncSubscriptionItemPaths(item, sub, entry, seenAt)
}

func cloneShareItemProviderData(value ShareItemProviderData) map[string]string {
	if len(value) == 0 {
		return nil
	}
	result := make(map[string]string, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}

func syncSubscriptionItemPaths(item *model.SubscriptionItem, sub *model.Subscription, entry TreeEntry, seenAt time.Time) *model.SubscriptionItem {
	if item == nil {
		return nil
	}
	planned := PlanTarget(planInputFromSubscription(sub), entry.Name, parentPath(entry))
	item.Season = planned.Season
	item.Episode = planned.Episode
	item.TargetDir = planned.TargetDir
	item.TargetName = planned.TargetName
	item.TargetPath = planned.TargetPath
	if !seenAt.IsZero() {
		item.LastSeenAt = seenAt
	}
	return item
}

func ensureDir(ctx context.Context, path string) error {
	path = utils.FixAndCleanPath(path)
	if path == "" || path == "/" {
		return nil
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	current := ""
	for _, part := range parts {
		current = utils.FixAndCleanPath(stdpath.Join(current, part))
		if obj, err := fs.Get(ctx, current, &fs.GetArgs{NoLog: true}); err == nil && obj != nil {
			continue
		}
		if err := fs.MakeDir(ctx, current); err != nil && !errors.Is(errors.Cause(err), errs.ObjectAlreadyExists) {
			return err
		}
	}
	return nil
}

func combinedHash(treeHash string, links []string) string {
	payload, _ := json.Marshal(struct {
		TreeHash string   `json:"tree_hash"`
		Links    []string `json:"links"`
	}{TreeHash: treeHash, Links: links})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

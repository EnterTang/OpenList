package subscription

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	stdpath "path"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/pkg/errors"
)

func Run(ctx context.Context, subscriptionID uint, transfer bool) (*model.SubscriptionRunResult, error) {
	return run(ctx, subscriptionID, transfer, false)
}

// RunCluster performs discovery and planning on a Coordinator, then hands
// media children to the registered cluster dispatcher instead of invoking the
// local OpenList copy/move pipeline.
func RunCluster(ctx context.Context, subscriptionID uint) (*model.SubscriptionRunResult, error) {
	return run(ctx, subscriptionID, false, true)
}

func run(ctx context.Context, subscriptionID uint, transfer, clusterDispatch bool) (*model.SubscriptionRunResult, error) {
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
	if clusterDispatch {
		items, currentHash, added, changed, transferred, runErr = runClusterBySource(ctx, sub)
	} else {
		items, currentHash, added, changed, transferred, runErr = runBySource(ctx, sub, transfer)
	}
	finished := time.Now()
	run.FinishedAt = &finished
	run.CurrentTreeHash = currentHash
	run.AddedCount = added
	run.ChangedCount = changed
	run.TransferredCount = transferred
	if runErr != nil {
		run.Status = model.SubscriptionStatusFailed
		run.Error = runErr.Error()
		sub.LastStatus = model.SubscriptionStatusFailed
		sub.LastError = runErr.Error()
	} else {
		run.Status = model.SubscriptionStatusSuccess
		sub.LastStatus = model.SubscriptionStatusSuccess
		sub.LastError = ""
		sub.LastTreeHash = currentHash
	}
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
	snapshotRoots := append([]string(nil), cfg.Paths...)
	tempRootSources := map[string]telegramPanSubscriptionSource{}
	tempRootBoundNames := map[string]map[string]struct{}{}
	tempRootBoundPaths := map[string]map[string]struct{}{}
	var shareCfg model.SubscriptionTelegramSourceConfig
	if len(cfg.Links) > 0 {
		globalCfg, err := GetConfig()
		if err != nil {
			return saved, sub.LastTreeHash, added, changed, transferred, err
		}
		shareCfg = globalCfg.Telegram
	}

	for _, link := range cfg.Links {
		source, handled, saveErr := trySaveShareLinkToTemp(ctx, sub, shareCfg, link)
		if source.Name != "" && handled {
			root := strings.TrimSpace(source.Config.TempTransferRoot)
			if root != "" {
				snapshotRoots = appendPathOnce(snapshotRoots, root)
				tempRootSources[root] = source
				tempRootBoundNames[root] = mergeStringSet(tempRootBoundNames[root], source.BoundShareNames)
				tempRootBoundPaths[root] = mergeStringSet(tempRootBoundPaths[root], source.BoundSharePaths)
			}
			continue
		}
		item := manualLinkItem(sub, link, now)
		if saveErr != nil {
			item.LastError = "share URL transfer failed: " + saveErr.Error()
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
		globalCfg, err := GetConfig()
		if err != nil {
			return saved, sub.LastTreeHash, added, changed, transferred, err
		}
		panCfg := telegramPanSourceConfigWithStorageFallback(
			ShareProviderPan123,
			normalizeTelegramPanConfig(globalCfg.Telegram.Pan123),
		)
		if strings.TrimSpace(panCfg.TempTransferRoot) == "" {
			return saved, sub.LastTreeHash, added, changed, transferred, fmt.Errorf("pan123 temp_transfer_root is required for manual imports")
		}
		if strings.TrimSpace(panCfg.AccessToken) == "" {
			return saved, sub.LastTreeHash, added, changed, transferred, fmt.Errorf("pan123 access_token is required for manual imports; configure a 123Pan storage so the token can be loaded automatically")
		}
		provider, err := newShareSaverForProvider(ShareProviderPan123, panCfg)
		if err != nil {
			return saved, sub.LastTreeHash, added, changed, transferred, err
		}
		selected, err := saveImportedFilesToTemp(ctx, provider, "manual_import://pan123", files, SaveShareOptions{
			TempRoot:     panCfg.TempTransferRoot,
			Subscription: sub,
			Match: func(entry TreeEntry) bool {
				return boundShareEntryMatches(sub, entry)
			},
		})
		if err != nil {
			return saved, sub.LastTreeHash, added, changed, transferred, err
		}
		snapshotRoots = appendPathOnce(snapshotRoots, panCfg.TempTransferRoot)
		tempRootSources[panCfg.TempTransferRoot] = telegramPanSubscriptionSource{
			Name:   string(ShareProviderPan123),
			Config: panCfg,
		}
		tempRootBoundNames[panCfg.TempTransferRoot], tempRootBoundPaths[panCfg.TempTransferRoot] = mergeBoundShareMarkers(
			tempRootBoundNames[panCfg.TempTransferRoot],
			tempRootBoundPaths[panCfg.TempTransferRoot],
			selected,
		)
	}

	snapshot, err := snapshotPaths(ctx, snapshotRoots)
	if err != nil {
		return saved, sub.LastTreeHash, added, changed, transferred, err
	}
	var candidates []telegramTempCandidate
	for _, entry := range MediaFiles(snapshot.Entries) {
		root := cleanConfigPath(entry.RootPath)
		if names, ok := tempRootBoundNames[root]; ok {
			if !entryMatchesSubscriptionOrBoundShare(sub, entry, names, tempRootBoundPaths[root]) {
				continue
			}
		}
		source := tempRootSources[root]
		candidates = append(candidates, telegramTempCandidate{
			Source: source,
			Entry:  entry,
			Item:   itemFromEntry(sub, entry, now),
		})
	}
	tempItems, _, tempAdded, tempChanged, tempTransferred, err := finalizeTempTransferCandidates(ctx, sub, candidates, shareCfg.TransferPriority, transfer, now, snapshot.Hash)
	if err != nil {
		return saved, snapshot.Hash, added, changed, transferred, err
	}
	saved = append(saved, tempItems...)
	added += tempAdded
	changed += tempChanged
	transferred += tempTransferred
	hash := snapshot.Hash
	if len(cfg.Links) > 0 {
		hash = combinedHash(hash, cfg.Links)
	}
	if cfg.ImportsText != "" {
		hash = combinedHash(hash, []string{cfg.ImportsText})
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
	query := telegramSearchQuery(sub)
	if query == "" {
		return nil, sub.LastTreeHash, 0, 0, 0, errors.New("pansou search query is required")
	}
	results, err := searchPanSouResources(ctx, query, cfg.Limit, cfg)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	now := time.Now()
	var saved []model.SubscriptionItem
	added := 0
	changed := 0
	transferred := 0
	snapshotRoots := []string{}
	tempRootSources := map[string]telegramPanSubscriptionSource{}
	tempRootBoundNames := map[string]map[string]struct{}{}
	tempRootBoundPaths := map[string]map[string]struct{}{}
	globalCfg, err := GetConfig()
	if err != nil {
		return saved, sub.LastTreeHash, added, changed, transferred, err
	}
	for _, result := range results {
		for _, link := range result.Links {
			source, handled, saveErr := trySaveShareLinkToTemp(ctx, sub, globalCfg.Telegram, link.URL)
			if source.Name != "" && handled {
				root := strings.TrimSpace(source.Config.TempTransferRoot)
				if root != "" {
					snapshotRoots = appendPathOnce(snapshotRoots, root)
					tempRootSources[root] = source
					tempRootBoundNames[root] = mergeStringSet(tempRootBoundNames[root], source.BoundShareNames)
					tempRootBoundPaths[root] = mergeStringSet(tempRootBoundPaths[root], source.BoundSharePaths)
				}
				continue
			}
			item := panSouLinkItem(sub, result, link, now)
			if saveErr != nil {
				item.LastError = "pansou share URL transfer failed: " + saveErr.Error()
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
	snapshot, err := snapshotPaths(ctx, snapshotRoots)
	if err != nil {
		return saved, sub.LastTreeHash, added, changed, transferred, err
	}
	var candidates []telegramTempCandidate
	for _, entry := range MediaFiles(snapshot.Entries) {
		root := cleanConfigPath(entry.RootPath)
		if !entryMatchesSubscriptionOrBoundShare(sub, entry, tempRootBoundNames[root], tempRootBoundPaths[root]) {
			continue
		}
		candidates = append(candidates, telegramTempCandidate{
			Source: tempRootSources[root],
			Entry:  entry,
			Item:   itemFromEntry(sub, entry, now),
		})
	}
	tempItems, _, tempAdded, tempChanged, tempTransferred, err := finalizeTempTransferCandidates(ctx, sub, candidates, globalCfg.Telegram.TransferPriority, transfer, now, snapshot.Hash)
	if err != nil {
		return saved, snapshot.Hash, added, changed, transferred, err
	}
	saved = append(saved, tempItems...)
	added += tempAdded
	changed += tempChanged
	transferred += tempTransferred
	links := panSouResultLinks(results)
	return saved, combinedHash(snapshot.Hash, links), added, changed, transferred, nil
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
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     seenAt,
	}
	return syncSubscriptionItemPaths(item, sub, entry, seenAt)
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

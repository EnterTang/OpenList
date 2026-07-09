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

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/pkg/errors"
)

func Run(ctx context.Context, subscriptionID uint, transfer bool) (*model.SubscriptionRunResult, error) {
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
	_ = db.CreateSubscriptionRun(run)
	sub.LastStatus = model.SubscriptionStatusRunning
	sub.LastError = ""
	_ = db.UpdateSubscription(sub)

	items, currentHash, added, changed, transferred, runErr := runBySource(ctx, sub, transfer)
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
	_ = db.UpdateSubscriptionRun(run)
	_ = db.UpdateSubscription(sub)
	return &model.SubscriptionRunResult{
		Subscription: sub,
		Run:          run,
		Items:        items,
	}, runErr
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
	tempRootConfigs := map[string]model.SubscriptionTelegramPanConfig{}
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
				tempRootConfigs[root] = source.Config
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
		_, err = saveImportedFilesToTemp(ctx, provider, "manual_import://pan123", files, SaveShareOptions{
			TempRoot: panCfg.TempTransferRoot,
			Match: func(entry TreeEntry) bool {
				return !entry.IsDir && subscriptionEntryMatches(sub, entry)
			},
		})
		if err != nil {
			return saved, sub.LastTreeHash, added, changed, transferred, err
		}
		snapshotRoots = appendPathOnce(snapshotRoots, panCfg.TempTransferRoot)
		tempRootConfigs[panCfg.TempTransferRoot] = panCfg
	}

	snapshot, err := snapshotPaths(ctx, snapshotRoots)
	if err != nil {
		return saved, sub.LastTreeHash, added, changed, transferred, err
	}
	for _, entry := range MediaFiles(snapshot.Entries) {
		item := itemFromEntry(sub, entry, now)
		stored, isNew, err := db.UpsertSubscriptionItem(item)
		if err != nil {
			return saved, snapshot.Hash, added, changed, transferred, err
		}
		if isNew {
			added++
		} else if stored.Status == model.SubscriptionItemStatusPending {
			changed++
		}
		if transfer && sub.TransferEnabled && stored.SourcePath != "" && stored.TargetPath != "" && stored.Status == model.SubscriptionItemStatusPending {
			if err := transferItem(ctx, stored); err != nil {
				stored.Status = model.SubscriptionItemStatusFailed
				stored.LastError = err.Error()
			} else {
				stored.Status = model.SubscriptionItemStatusTransferred
				stored.LastError = ""
				transferred++
				if sourceCfg, ok := tempRootConfigs[entry.RootPath]; ok && sourceCfg.DeleteSourceAfter {
					if err := cleanupSourceAfterTransfer(ctx, stored.SourcePath); err != nil {
						stored.LastError = "source cleanup failed after transfer: " + err.Error()
					}
				}
			}
			_, _, err = db.UpsertSubscriptionItem(stored)
			if err != nil {
				return saved, snapshot.Hash, added, changed, transferred, err
			}
		}
		saved = append(saved, *stored)
	}
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
	tempRootConfigs := map[string]model.SubscriptionTelegramPanConfig{}
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
					tempRootConfigs[root] = source.Config
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
	for _, entry := range MediaFiles(snapshot.Entries) {
		if !subscriptionEntryMatches(sub, entry) {
			continue
		}
		item := itemFromEntry(sub, entry, now)
		stored, isNew, err := db.UpsertSubscriptionItem(item)
		if err != nil {
			return saved, snapshot.Hash, added, changed, transferred, err
		}
		if isNew {
			added++
		} else if stored.Status == model.SubscriptionItemStatusPending {
			changed++
		}
		if transfer && sub.TransferEnabled && stored.SourcePath != "" && stored.TargetPath != "" && stored.Status == model.SubscriptionItemStatusPending {
			if err := transferItem(ctx, stored); err != nil {
				stored.Status = model.SubscriptionItemStatusFailed
				stored.LastError = err.Error()
			} else {
				stored.Status = model.SubscriptionItemStatusTransferred
				stored.LastError = ""
				transferred++
				if sourceCfg, ok := tempRootConfigs[entry.RootPath]; ok && sourceCfg.DeleteSourceAfter {
					if err := cleanupSourceAfterTransfer(ctx, stored.SourcePath); err != nil {
						stored.LastError = "source cleanup failed after transfer: " + err.Error()
					}
				}
			}
			_, _, err = db.UpsertSubscriptionItem(stored)
			if err != nil {
				return saved, snapshot.Hash, added, changed, transferred, err
			}
		}
		saved = append(saved, *stored)
	}
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
	return &model.SubscriptionItem{
		SubscriptionID: sub.ID,
		SourceKey:      "pansou:" + shortHash(keyMaterial),
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
		SourceURL:      link,
		FileHash:       key,
		Status:         model.SubscriptionItemStatusSkipped,
		LastSeenAt:     seenAt,
		LastError:      "share URL is recorded but not mounted as an OpenList path yet",
	}
}

func itemFromEntry(sub *model.Subscription, entry TreeEntry, seenAt time.Time) *model.SubscriptionItem {
	planned := PlanTarget(planInputFromSubscription(sub), entry.Name, parentPath(entry))
	sourcePath := fullPath(entry)
	return &model.SubscriptionItem{
		SubscriptionID: sub.ID,
		SourceKey:      SourceKey(entry),
		SourcePath:     sourcePath,
		FileID:         entry.ID,
		FilePath:       entry.Path,
		FileName:       entry.Name,
		FileSize:       entry.Size,
		FileHash:       FileHash(entry),
		Season:         planned.Season,
		Episode:        planned.Episode,
		TargetDir:      planned.TargetDir,
		TargetName:     planned.TargetName,
		TargetPath:     planned.TargetPath,
		Status:         model.SubscriptionItemStatusPending,
		LastSeenAt:     seenAt,
	}
}

func transferItem(ctx context.Context, item *model.SubscriptionItem) error {
	if item == nil {
		return errors.New("subscription item is nil")
	}
	targetDir := utils.FixAndCleanPath(item.TargetDir)
	if targetDir == "" || targetDir == "/" {
		return errors.New("target dir is empty")
	}
	if err := ensureDir(ctx, targetDir); err != nil {
		return err
	}
	syncCtx := context.WithValue(ctx, conf.NoTaskKey, struct{}{})
	if _, err := fs.Copy(syncCtx, item.SourcePath, targetDir, true); err != nil {
		return err
	}
	copiedPath := utils.FixAndCleanPath(stdpath.Join(targetDir, item.FileName))
	if item.TargetName != "" && item.TargetName != item.FileName {
		if err := fs.Rename(syncCtx, copiedPath, item.TargetName, true); err != nil {
			return err
		}
	}
	return nil
}

func cleanupSourceAfterTransfer(ctx context.Context, sourcePath string) error {
	storage, actualPath, err := op.GetStorageAndActualPath(sourcePath)
	if err != nil {
		return errors.WithMessage(err, "failed get source storage")
	}
	sourceObj, err := op.Get(ctx, storage, actualPath, true)
	if err != nil {
		if errs.IsObjectNotFound(err) {
			return nil
		}
		return errors.WithMessage(err, "failed get source object")
	}
	sourceObj = model.UnwrapObjName(sourceObj)
	if err := op.Remove(ctx, storage, actualPath); err != nil {
		return err
	}
	if cleaner, ok := storage.(driver.RecycleEntryCleaner); ok {
		return cleaner.ClearRecycleEntry(ctx, sourceObj)
	}
	return nil
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

package subscription

import (
	"context"
	"fmt"
	stdpath "path"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var removeTempFile = func(ctx context.Context, path string) error {
	return fs.Remove(ctx, path)
}

func filterLargestSharePairsPerSlot(sub *model.Subscription, pairs []shareTreePair) []shareTreePair {
	if sub == nil || len(pairs) <= 1 {
		return pairs
	}
	bestBySlot := make(map[string]shareTreePair, len(pairs))
	slotOrder := make([]string, 0, len(pairs))
	filtered := make([]shareTreePair, 0, len(pairs))
	for _, pair := range pairs {
		item := itemFromEntry(sub, pair.entry, time.Time{})
		if !subscriptionEpisodeMatches(sub, item.Season, item.Episode) {
			filtered = append(filtered, pair)
			continue
		}
		slot := mediaSlotKey(sub, item)
		if slot == "" {
			filtered = append(filtered, pair)
			continue
		}
		existing, ok := bestBySlot[slot]
		if !ok {
			slotOrder = append(slotOrder, slot)
		}
		if !ok || pair.entry.Size > existing.entry.Size {
			bestBySlot[slot] = pair
		}
	}
	for _, slot := range slotOrder {
		filtered = append(filtered, bestBySlot[slot])
	}
	return filtered
}

func filterLargestImportedFilesPerSlot(sub *model.Subscription, rootPath string, files []pan123ImportedFile) []pan123ImportedFile {
	if sub == nil || len(files) <= 1 {
		return files
	}
	bestBySlot := make(map[string]pan123ImportedFile, len(files))
	slotOrder := make([]string, 0, len(files))
	filtered := make([]pan123ImportedFile, 0, len(files))
	for _, file := range files {
		entry := TreeEntry{
			RootPath: rootPath,
			Path:     utils.FixAndCleanPath(stdpath.Join("/", file.Path)),
			Name:     file.Name,
			Size:     file.Size,
		}
		item := itemFromEntry(sub, entry, time.Time{})
		if !subscriptionEpisodeMatches(sub, item.Season, item.Episode) {
			filtered = append(filtered, file)
			continue
		}
		slot := mediaSlotKey(sub, item)
		if slot == "" {
			filtered = append(filtered, file)
			continue
		}
		existing, ok := bestBySlot[slot]
		if !ok {
			slotOrder = append(slotOrder, slot)
		}
		if !ok || file.Size > existing.Size {
			bestBySlot[slot] = file
		}
	}
	for _, slot := range slotOrder {
		filtered = append(filtered, bestBySlot[slot])
	}
	return filtered
}

func removeUnselectedTempCandidates(ctx context.Context, sub *model.Subscription, candidates, selected []telegramTempCandidate) error {
	if sub == nil || len(candidates) == 0 || len(selected) == 0 {
		return nil
	}
	selectedPaths := make(map[string]struct{}, len(selected))
	selectedSlots := make(map[string]struct{}, len(selected))
	for _, candidate := range selected {
		selectedPaths[fullPath(candidate.Entry)] = struct{}{}
		if candidate.Item != nil {
			if slot := mediaSlotKey(sub, candidate.Item); slot != "" {
				selectedSlots[slot] = struct{}{}
			}
		}
	}
	var firstErr error
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := fullPath(candidate.Entry)
		if _, ok := selectedPaths[path]; ok {
			continue
		}
		if candidate.Item == nil {
			continue
		}
		slot := mediaSlotKey(sub, candidate.Item)
		if slot == "" {
			continue
		}
		if _, ok := selectedSlots[slot]; !ok {
			continue
		}
		if err := removeTempFile(ctx, path); err != nil {
			if errs.IsObjectNotFound(err) {
				continue
			}
			log.Warnf("subscription %d failed to remove duplicate temp file %s: %v", sub.ID, path, err)
			if firstErr == nil {
				firstErr = fmt.Errorf("remove duplicate temp file %s: %w", path, err)
			}
		}
	}
	return firstErr
}

func finalizeTempTransferCandidates(ctx context.Context, sub *model.Subscription, candidates []telegramTempCandidate, priority []string, transfer bool, seenAt time.Time, resultHash string) ([]model.SubscriptionItem, string, int, int, int, error) {
	selected := selectTelegramTempTransferCandidates(sub, candidates, priority)
	_ = removeUnselectedTempCandidates(ctx, sub, candidates, selected)
	return applyTelegramTempTransferCandidates(ctx, sub, selected, transfer, seenAt, resultHash)
}

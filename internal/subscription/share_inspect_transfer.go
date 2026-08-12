package subscription

import (
	"context"
	"fmt"
	stdpath "path"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

// shareTransferCandidate is a share/import media file discovered via metadata
// inspect (size/path) before any 123/115/quark temp save.
type shareTransferCandidate struct {
	Source     telegramPanSubscriptionSource
	Ref        ShareRef
	Pair       shareTreePair
	ImportFile *pan123ImportedFile
	ImportRoot string
	Entry      TreeEntry
	Item       *model.SubscriptionItem
}

var (
	inspectShareLinkCandidatesFn  = inspectShareLinkCandidates
	saveShareTransferCandidatesFn = saveShareTransferCandidates
	saveSharePairsToTempFn        = saveSharePairsToTemp
)

// resolveShareLinkSource prepares provider credentials and temp root without
// listing or saving share contents.
func resolveShareLinkSource(sub *model.Subscription, cfg model.SubscriptionTelegramSourceConfig, rawLink string) (telegramPanSubscriptionSource, ShareRef, bool, error) {
	ref, err := ParseShareURL(rawLink)
	if err != nil {
		if provider, ok := DetectShareProvider(rawLink); ok {
			source, _ := telegramPanSourceForProvider(cfg, provider)
			return source, ShareRef{}, false, err
		}
		return telegramPanSubscriptionSource{}, ShareRef{}, false, nil
	}
	source, ok := telegramPanSourceForProvider(cfg, ref.Provider)
	if !ok {
		return telegramPanSubscriptionSource{}, ref, false, nil
	}
	if sub != nil && sub.TempTarget.Provider != "" {
		tempTarget := NormalizeSubscriptionStorageTarget(sub.TempTarget)
		if tempTarget.Provider != providerTargetNameForShareProvider(ref.Provider) {
			return source, ref, false, nil
		}
		source.Config.TempTransferTarget = tempTarget
	}
	source.Config, err = telegramPanSourceConfigWithStorageFallback(ref.Provider, source.Config)
	if err != nil {
		return source, ref, false, err
	}
	source.runtimeConfigResolved = true
	if !telegramPanSourceCanSave(ref.Provider, source.Config) {
		return source, ref, false, nil
	}
	return source, ref, true, nil
}

func inspectShareLinkCandidates(ctx context.Context, sub *model.Subscription, cfg model.SubscriptionTelegramSourceConfig, rawLink string, seenAt time.Time) (telegramPanSubscriptionSource, []shareTransferCandidate, bool, error) {
	source, ref, ok, err := resolveShareLinkSource(sub, cfg, rawLink)
	if err != nil || !ok {
		return source, nil, false, err
	}
	provider, err := newShareSaverForProvider(ref.Provider, source.Config)
	if err != nil {
		return source, nil, false, err
	}
	pairs, err := collectShareTreePairs(ctx, provider, ref)
	if err != nil {
		return source, nil, false, err
	}
	candidates := make([]shareTransferCandidate, 0, len(pairs))
	matchedEntries := make([]TreeEntry, 0, len(pairs))
	for _, pair := range pairs {
		if pair.entry.IsDir {
			continue
		}
		if !boundShareEntryMatches(sub, pair.entry) {
			continue
		}
		entry := pair.entry
		item := itemFromEntry(sub, entry, seenAt)
		item.SourceProvider = string(ref.Provider)
		item.SourceURL = ref.RawURL
		candidates = append(candidates, shareTransferCandidate{
			Source: source,
			Ref:    ref,
			Pair:   pair,
			Entry:  entry,
			Item:   item,
		})
		matchedEntries = append(matchedEntries, entry)
	}
	source.BoundShareNames, source.BoundSharePaths = boundShareMarkers(matchedEntries)
	return source, candidates, true, nil
}

func importFilesToShareTransferCandidates(sub *model.Subscription, source telegramPanSubscriptionSource, rootPath string, files []pan123ImportedFile, seenAt time.Time) []shareTransferCandidate {
	candidates := make([]shareTransferCandidate, 0, len(files))
	for _, file := range files {
		entry := TreeEntry{
			RootPath: rootPath,
			Path:     utils.FixAndCleanPath(stdpath.Join("/", file.Path)),
			Name:     file.Name,
			Size:     file.Size,
		}
		if !boundShareEntryMatches(sub, entry) {
			continue
		}
		fileCopy := file
		item := itemFromEntry(sub, entry, seenAt)
		item.SourceProvider = source.Name
		candidates = append(candidates, shareTransferCandidate{
			Source:     source,
			ImportFile: &fileCopy,
			ImportRoot: rootPath,
			Entry:      entry,
			Item:       item,
		})
	}
	return candidates
}

func selectShareTransferCandidates(sub *model.Subscription, candidates []shareTransferCandidate, priority []string) []shareTransferCandidate {
	if len(candidates) <= 1 {
		return candidates
	}
	temp := make([]telegramTempCandidate, 0, len(candidates))
	byKey := make(map[string]shareTransferCandidate, len(candidates))
	for _, candidate := range candidates {
		if candidate.Item == nil {
			continue
		}
		temp = append(temp, telegramTempCandidate{
			Source: candidate.Source,
			Entry:  candidate.Entry,
			Item:   candidate.Item,
		})
		byKey[candidate.Item.SourceKey] = candidate
	}
	selectedTemp := selectTelegramTempTransferCandidates(sub, temp, priority)
	selected := make([]shareTransferCandidate, 0, len(selectedTemp))
	for _, candidate := range selectedTemp {
		if candidate.Item == nil {
			continue
		}
		if original, ok := byKey[candidate.Item.SourceKey]; ok {
			selected = append(selected, original)
		}
	}
	return selected
}

func saveShareTransferCandidates(ctx context.Context, selected []shareTransferCandidate) ([]shareTransferCandidate, error) {
	if len(selected) == 0 {
		return nil, nil
	}
	type shareGroup struct {
		source telegramPanSubscriptionSource
		ref    ShareRef
		pairs  []shareTreePair
		index  []int
	}
	shareGroups := make(map[string]*shareGroup)
	shareOrder := make([]string, 0)
	type importGroup struct {
		source telegramPanSubscriptionSource
		root   string
		files  []pan123ImportedFile
		index  []int
	}
	importGroups := make(map[string]*importGroup)
	importOrder := make([]string, 0)

	for i, candidate := range selected {
		if candidate.ImportFile != nil {
			key := candidate.Source.Name + "\x00" + candidate.ImportRoot + "\x00" + candidate.Source.Config.TempTransferRoot
			group := importGroups[key]
			if group == nil {
				group = &importGroup{source: candidate.Source, root: candidate.ImportRoot}
				importGroups[key] = group
				importOrder = append(importOrder, key)
			}
			group.files = append(group.files, *candidate.ImportFile)
			group.index = append(group.index, i)
			continue
		}
		key := strings.Join([]string{
			string(candidate.Ref.Provider),
			candidate.Ref.RawURL,
			candidate.Ref.Passcode,
			candidate.Source.Config.TempTransferRoot,
		}, "\x00")
		group := shareGroups[key]
		if group == nil {
			group = &shareGroup{source: candidate.Source, ref: candidate.Ref}
			shareGroups[key] = group
			shareOrder = append(shareOrder, key)
		}
		group.pairs = append(group.pairs, candidate.Pair)
		group.index = append(group.index, i)
	}

	saved := append([]shareTransferCandidate(nil), selected...)
	for _, key := range shareOrder {
		group := shareGroups[key]
		provider, err := newShareSaverForProvider(group.ref.Provider, group.source.Config)
		if err != nil {
			return nil, err
		}
		if _, err := saveSharePairsToTempFn(ctx, provider, group.ref, group.pairs, SaveShareOptions{
			TempRoot: group.source.Config.TempTransferRoot,
		}); err != nil {
			return nil, err
		}
		tempRoot := cleanConfigPath(group.source.Config.TempTransferRoot)
		for _, candidateIndex := range group.index {
			entry := saved[candidateIndex].Pair.entry
			entry.RootPath = tempRoot
			saved[candidateIndex].Entry = entry
		}
	}
	for _, key := range importOrder {
		group := importGroups[key]
		provider, err := newShareSaverForProvider(ShareProviderPan123, group.source.Config)
		if err != nil {
			return nil, err
		}
		// Skip per-slot filtering here: winners were already chosen via
		// selectShareTransferCandidates using inspect metadata.
		entries, err := saveImportedFilesToTemp(ctx, provider, group.root, group.files, SaveShareOptions{
			TempRoot: group.source.Config.TempTransferRoot,
		})
		if err != nil {
			return nil, err
		}
		if len(entries) != len(group.index) {
			return nil, fmt.Errorf("saved %d imported files, want %d", len(entries), len(group.index))
		}
		for offset, candidateIndex := range group.index {
			saved[candidateIndex].Entry = entries[offset]
		}
	}
	return saved, nil
}

func rebuildShareTransferItems(sub *model.Subscription, selected []shareTransferCandidate, seenAt time.Time) []shareTransferCandidate {
	if sub == nil {
		return selected
	}
	for i := range selected {
		entry := selected[i].Entry
		tempRoot := cleanConfigPath(selected[i].Source.Config.TempTransferRoot)
		if tempRoot != "" {
			entry.RootPath = tempRoot
		}
		item := itemFromEntry(sub, entry, seenAt)
		if selected[i].Item != nil {
			item.SourceProvider = selected[i].Item.SourceProvider
			item.SourceURL = selected[i].Item.SourceURL
			item.SourceMessageID = selected[i].Item.SourceMessageID
			item.SourceMessageChannel = selected[i].Item.SourceMessageChannel
			item.SourceMessageURL = selected[i].Item.SourceMessageURL
			item.SourceMessageText = selected[i].Item.SourceMessageText
		}
		selected[i].Entry = entry
		selected[i].Item = item
	}
	return selected
}

func shareTransferCandidatesToTelegramTemp(selected []shareTransferCandidate) []telegramTempCandidate {
	out := make([]telegramTempCandidate, 0, len(selected))
	for _, candidate := range selected {
		out = append(out, telegramTempCandidate{
			Source: candidate.Source,
			Entry:  candidate.Entry,
			Item:   candidate.Item,
		})
	}
	return out
}

func transferSelectedShareCandidates(ctx context.Context, sub *model.Subscription, selected []shareTransferCandidate, transfer bool, seenAt time.Time, resultHash string) ([]model.SubscriptionItem, string, int, int, int, error) {
	selected, skipped, err := filterAcceptedShareTransferCandidates(sub, selected)
	if err != nil {
		return nil, resultHash, 0, 0, 0, err
	}
	skippedItems, skippedAdded, skippedChanged, err := persistSkippedShareTransferCandidates(skipped)
	if err != nil {
		return nil, resultHash, skippedAdded, skippedChanged, 0, err
	}
	if len(selected) == 0 {
		return skippedItems, resultHash, skippedAdded, skippedChanged, 0, nil
	}
	selected, err = saveShareTransferCandidatesFn(ctx, selected)
	if err != nil {
		return skippedItems, resultHash, skippedAdded, skippedChanged, 0, err
	}
	selected = rebuildShareTransferItems(sub, selected, seenAt)
	items, hash, added, changed, transferred, err := applyTelegramTempTransferCandidates(ctx, sub, shareTransferCandidatesToTelegramTemp(selected), transfer, seenAt, resultHash)
	return append(skippedItems, items...), hash, skippedAdded + added, skippedChanged + changed, transferred, err
}

const acceptedEpisodeSkipReason = "skipped: episode already has an accepted transfer"

func filterAcceptedShareTransferCandidates(sub *model.Subscription, selected []shareTransferCandidate) ([]shareTransferCandidate, []shareTransferCandidate, error) {
	if sub == nil || normalizeMediaType(sub.MediaType) == "movie" || len(selected) == 0 {
		return selected, nil, nil
	}
	existing, err := db.ListSubscriptionItems(sub.ID)
	if err != nil {
		return nil, nil, err
	}
	lockedSlots := make(map[string]struct{})
	acceptedKeys := make(map[string]struct{})
	for i := range existing {
		item := &existing[i]
		if !subscriptionItemHasAcceptedTransfer(item) || item.Episode <= 0 {
			continue
		}
		acceptedKeys[item.SourceKey] = struct{}{}
		if slot := mediaSlotKey(sub, item); slot != "" {
			lockedSlots[slot] = struct{}{}
		}
	}
	if len(lockedSlots) == 0 {
		return selected, nil, nil
	}
	filtered := make([]shareTransferCandidate, 0, len(selected))
	skipped := make([]shareTransferCandidate, 0)
	for _, candidate := range selected {
		if candidate.Item == nil || candidate.Item.Episode <= 0 {
			filtered = append(filtered, candidate)
			continue
		}
		if _, locked := lockedSlots[mediaSlotKey(sub, candidate.Item)]; locked {
			if _, alreadyAccepted := acceptedKeys[candidate.Item.SourceKey]; !alreadyAccepted {
				skipped = append(skipped, candidate)
			}
			continue
		}
		filtered = append(filtered, candidate)
	}
	return filtered, skipped, nil
}

func persistSkippedShareTransferCandidates(skipped []shareTransferCandidate) ([]model.SubscriptionItem, int, int, error) {
	items := make([]model.SubscriptionItem, 0, len(skipped))
	added, changed := 0, 0
	for _, candidate := range skipped {
		if candidate.Item == nil {
			continue
		}
		item := *candidate.Item
		item.SourceProvider = normalizeSubscriptionProvider(candidate.Source.Name)
		if item.SourceURL == "" {
			item.SourceURL = candidate.Ref.RawURL
		}
		previous, previousErr := db.GetSubscriptionItem(item.SubscriptionID, item.SourceKey)
		item.Status = model.SubscriptionItemStatusSkipped
		item.ClusterJobID = ""
		item.LastError = acceptedEpisodeSkipReason
		stored, isNew, err := db.UpsertSubscriptionItem(&item)
		if err != nil {
			return items, added, changed, err
		}
		if stored.Status != model.SubscriptionItemStatusSkipped || stored.LastError != acceptedEpisodeSkipReason {
			stored.Status = model.SubscriptionItemStatusSkipped
			stored.ClusterJobID = ""
			stored.LastError = acceptedEpisodeSkipReason
			stored, _, err = db.UpsertSubscriptionItem(stored)
			if err != nil {
				return items, added, changed, err
			}
		}
		if isNew {
			added++
		} else if previousErr == nil && (previous.Status != stored.Status || previous.LastError != stored.LastError) {
			changed++
		}
		items = append(items, *stored)
	}
	return items, added, changed, nil
}

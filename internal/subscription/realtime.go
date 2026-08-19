package subscription

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
)

const realtimeTelegramTrigger = "realtime"

// RealtimeTelegramGroups returns the configured realtime peers. Existing
// provider channel groups remain a compatible fallback when a user enables
// realtime mode without repeating those channel names.
func RealtimeTelegramGroups(cfg model.SubscriptionTelegramSourceConfig) []string {
	groups := append([]string(nil), cfg.RealtimeGroups...)
	if len(groups) == 0 {
		groups = append(groups, cfg.Channels...)
		for _, source := range telegramPanSources(cfg) {
			groups = append(groups, source.Config.Channels...)
		}
	}
	return cleanStringList(groups, false)
}

func EnqueueRealtimeTelegramRow(subscriptionID uint, row telegramCommandRow) (*model.SubscriptionTelegramEvent, bool, error) {
	if subscriptionID == 0 {
		return nil, false, errors.New("subscription id is required")
	}
	messageID := rowMessageID(row)
	if messageID <= 0 {
		return nil, false, errors.New("Telegram message id is required")
	}
	channel := normalizeTelegramChannel(row.Channel)
	if channel == "" {
		return nil, false, errors.New("Telegram channel is required")
	}
	body, err := json.Marshal(row)
	if err != nil {
		return nil, false, err
	}
	sum := sha256.Sum256(body)
	return db.EnqueueSubscriptionTelegramEvent(&model.SubscriptionTelegramEvent{
		SubscriptionID: subscriptionID,
		Channel:        channel,
		MessageID:      fmt.Sprint(messageID),
		PayloadJSON:    string(body),
		PayloadHash:    hex.EncodeToString(sum[:]),
		Status:         model.SubscriptionTelegramEventStatusPending,
		AvailableAt:    time.Now().UTC(),
	})
}

func ProcessPendingRealtimeTelegramEvents(ctx context.Context, limit int) (int, error) {
	events, err := db.ClaimSubscriptionTelegramEvents(limit, time.Now().UTC())
	if err != nil {
		return 0, err
	}
	processed := 0
	for i := range events {
		event := &events[i]
		observationKey, err := processRealtimeTelegramEvent(ctx, event)
		if err == nil {
			if completeErr := db.CompleteSubscriptionTelegramEvent(event.ID, observationKey, time.Now().UTC()); completeErr != nil {
				return processed, completeErr
			}
			processed++
			continue
		}
		if event.Attempts >= 5 {
			_ = db.DeadLetterSubscriptionTelegramEvent(event.ID, err)
			continue
		}
		backoff := time.Duration(1<<min(event.Attempts, 6)) * time.Second
		_ = db.RetrySubscriptionTelegramEvent(event.ID, err, time.Now().UTC().Add(backoff))
	}
	return processed, nil
}

func processRealtimeTelegramEvent(ctx context.Context, event *model.SubscriptionTelegramEvent) (string, error) {
	ctx = ensureClusterObservationRunContext(ctx)
	if event == nil {
		return "", errors.New("realtime Telegram event is nil")
	}
	sub, err := db.GetSubscriptionByID(event.SubscriptionID)
	if err != nil {
		return "", err
	}
	if !sub.Active || !strings.EqualFold(sub.SourceType, model.SubscriptionSourceTelegram) {
		return "", nil
	}
	if err := ApplyDefaults(sub); err != nil {
		return "", err
	}
	cfg, err := parseTelegramConfig(sub.SourceConfig)
	if err != nil {
		return "", err
	}
	if !cfg.RealtimeEnabled {
		return "", nil
	}
	var row telegramCommandRow
	if err := json.Unmarshal([]byte(event.PayloadJSON), &row); err != nil {
		return "", errors.WithMessage(err, "decode realtime Telegram message")
	}
	row.Channel = normalizeTelegramChannel(row.Channel)
	if row.Channel == "" {
		row.Channel = event.Channel
	}
	if !telegramRowMatchesSubscription(sub, row) {
		return "", nil
	}
	links, _ := rowLinksForTelegramPanSources(row, cfg)
	hdhiveLinks, err := resolveTelegramHDHiveLinks(ctx, row, cfg)
	if err != nil {
		return "", err
	}
	for _, link := range hdhiveLinks {
		links = append(links, normalizeTelegramLinkWithAccessCode(link.URL, link.AccessCode))
	}
	if len(links) == 0 {
		links = rowLinks(row)
	}
	message := sourceMessageFromTelegramRow(row)
	accessCode := rowAccessCode(row)
	items := make([]clusterInspectObservationItem, 0, len(links))
	for _, link := range links {
		ref, err := ParseShareURL(normalizeTelegramLinkWithAccessCode(link, accessCode))
		if err != nil {
			continue
		}
		items = append(items, clusterInspectObservationItem{ref: ref, message: message})
	}
	items = dedupeClusterInspectObservationItems(items)
	if len(items) == 0 {
		return "", nil
	}
	observationKey := clusterObservationKey(ctx, sub.ID, "telegram-realtime", items)
	for _, item := range items {
		if _, err := dispatchRealtimeClusterInspectObservation(ctx, sub, item.ref, item.message, observationKey, len(items)); err != nil {
			return observationKey, err
		}
	}
	cursor := parseTelegramCursor(sub.LastCursor)
	cursor.advance(row)
	sub.LastCursor = formatTelegramCursor(cursor)
	sub.LastStatus = model.SubscriptionStatusRunning
	sub.LastError = ""
	if err := db.UpdateSubscription(sub); err != nil {
		return observationKey, err
	}
	return observationKey, nil
}

func dispatchRealtimeClusterInspectObservation(ctx context.Context, sub *model.Subscription, ref ShareRef, message clusterSourceMessage, observationKey string, observationExpected int) (string, error) {
	dispatcher := currentClusterDispatcher()
	if dispatcher == nil {
		return "", errors.New("cluster subscription dispatcher is not registered")
	}
	task := clusterInspectObservationTaskForTrigger(sub, ref, message, observationKey, observationExpected, realtimeTelegramTrigger)
	// Realtime tasks must be selected from the full compatible cluster pool.
	task.PreferredWorkerNodeID = ""
	return dispatcher.DispatchSubscriptionInspect(ctx, task)
}

// ApplyRealtimeClusterInspectObservation persists inspected source candidates
// without immediately dispatching a media transfer. The Coordinator later
// selects one candidate per media slot after the configured priority window.
func ApplyRealtimeClusterInspectObservation(ctx context.Context, manifests []ClusterInspectManifestInput) (int, error) {
	if len(manifests) == 0 {
		return 0, nil
	}
	unlock := lockClusterInspectApply(manifests[0].Task.SubscriptionID)
	defer unlock()
	sub, err := db.GetSubscriptionByID(manifests[0].Task.SubscriptionID)
	if err != nil {
		return 0, err
	}
	if err := ApplyDefaults(sub); err != nil {
		return 0, err
	}
	if !sub.TransferEnabled {
		return 0, nil
	}
	cfg, err := parseTelegramConfig(sub.SourceConfig)
	if err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	candidates := make([]clusterInspectCandidate, 0)
	for _, input := range manifests {
		if input.Task.SubscriptionID != sub.ID || input.Task.Trigger != realtimeTelegramTrigger {
			return 0, errors.New("realtime inspect observation has incompatible task context")
		}
		if !subscriptionTitleMatches(sub, input.Task.SourceMessageText) {
			continue
		}
		ref, err := ParseShareURL(input.Task.ShareURL)
		if err != nil {
			return 0, err
		}
		ref.Passcode = input.Task.SharePasscode
		message := clusterSourceMessage{ID: input.Task.SourceMessageID, Channel: input.Task.SourceMessageChannel, URL: input.Task.SourceMessageURL, Text: input.Task.SourceMessageText}
		for _, object := range input.Objects {
			entry := TreeEntry{ID: object.FileID, Path: "/" + strings.TrimPrefix(object.RelativePath, "/"), Name: pathBase(object.RelativePath), Size: object.Size, Modified: object.ModifiedAt}
			if !isMediaEntry(entry) || !boundShareEntryMatches(sub, entry) {
				continue
			}
			item := clusterItemFromShareEntry(sub, ref, entry, message, now)
			if object.Hash != "" {
				item.FileHash = object.Hash
			}
			candidates = append(candidates, clusterInspectCandidate{item: item, ref: ref, message: message})
		}
	}
	candidates = selectClusterInspectCandidates(sub, candidates, cfg.TransferPriority)
	items := make([]*model.SubscriptionItem, 0, len(candidates))
	bySourceKey := make(map[string]clusterInspectCandidate, len(candidates))
	for _, candidate := range candidates {
		items = append(items, candidate.item)
		bySourceKey[candidate.item.SourceKey] = candidate
	}
	stored, _, _, err := upsertClusterItems(items)
	if err != nil {
		return 0, err
	}
	created := 0
	for _, item := range stored {
		candidate, ok := bySourceKey[item.SourceKey]
		if !ok || item.Status != model.SubscriptionItemStatusPending {
			continue
		}
		slot := realtimeCandidateSlotKey(sub, item)
		readyAt := now
		if realtimeProviderNeedsWait(candidate.item.SourceProvider, cfg) {
			readyAt = now.Add(realtimeCandidateWait(cfg))
		}
		record := &model.SubscriptionRealtimeCandidate{
			SubscriptionID: sub.ID, SlotKey: slot, SourceKey: item.SourceKey, FileHash: item.FileHash, ItemID: item.ID,
			Provider: item.SourceProvider, ShareURL: candidate.ref.RawURL, SharePasscode: candidate.ref.Passcode,
			MessageID: candidate.message.ID, MessageChannel: candidate.message.Channel, MessageURL: candidate.message.URL, MessageText: candidate.message.Text,
			ReadyAt: readyAt, Status: model.SubscriptionRealtimeCandidateStatusPending,
		}
		if _, wasCreated, err := db.CreateSubscriptionRealtimeCandidate(record); err != nil {
			return created, err
		} else if wasCreated {
			created++
		}
	}
	return created, nil
}

func ProcessReadyRealtimeCandidates(ctx context.Context, limit int) (int, error) {
	ready, err := db.ListReadySubscriptionRealtimeCandidates(time.Now().UTC(), limit)
	if err != nil {
		return 0, err
	}
	seen := make(map[string]struct{}, len(ready))
	processed := 0
	for _, candidate := range ready {
		key := fmt.Sprintf("%d\x00%s", candidate.SubscriptionID, candidate.SlotKey)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		count, err := processRealtimeCandidateSlot(ctx, candidate.SubscriptionID, candidate.SlotKey)
		if err != nil {
			return processed, err
		}
		processed += count
	}
	return processed, nil
}

func processRealtimeCandidateSlot(ctx context.Context, subscriptionID uint, slotKey string) (int, error) {
	unlock := lockClusterInspectApply(subscriptionID)
	defer unlock()
	sub, err := db.GetSubscriptionByID(subscriptionID)
	if err != nil {
		return 0, err
	}
	cfg, err := parseTelegramConfig(sub.SourceConfig)
	if err != nil {
		return 0, err
	}
	records, err := db.ListSubscriptionRealtimeCandidates(subscriptionID, slotKey)
	if err != nil || len(records) == 0 {
		return 0, err
	}
	priority := cfg.TransferPriority
	priorityIndex := make(map[string]int, len(priority))
	for i, provider := range normalizeTransferPriority(priority) {
		priorityIndex[provider] = i
	}
	type resolved struct {
		record model.SubscriptionRealtimeCandidate
		item   *model.SubscriptionItem
	}
	resolvedCandidates := make([]resolved, 0, len(records))
	for _, record := range records {
		item, err := db.GetSubscriptionItem(subscriptionID, record.SourceKey)
		if err != nil || item.FileHash != record.FileHash {
			continue
		}
		if subscriptionItemHasAcceptedTransfer(item) {
			ids := candidateIDs(records)
			return 0, db.UpdateSubscriptionRealtimeCandidateStatus(ids, model.SubscriptionRealtimeCandidateStatusSkipped, "skipped: slot already accepted")
		}
		if item.Status != model.SubscriptionItemStatusPending {
			continue
		}
		resolvedCandidates = append(resolvedCandidates, resolved{record: record, item: item})
	}
	if len(resolvedCandidates) == 0 {
		return 0, nil
	}
	sort.SliceStable(resolvedCandidates, func(i, j int) bool {
		return betterClusterInspectCandidate(clusterInspectCandidate{item: resolvedCandidates[i].item}, clusterInspectCandidate{item: resolvedCandidates[j].item}, priorityIndex)
	})
	winner := resolvedCandidates[0]
	ref, err := ParseShareURL(winner.record.ShareURL)
	if err != nil {
		return 0, err
	}
	ref.Passcode = winner.record.SharePasscode
	message := clusterSourceMessage{ID: winner.record.MessageID, Channel: winner.record.MessageChannel, URL: winner.record.MessageURL, Text: winner.record.MessageText}
	task := clusterMediaTask(sub, winner.item, ref, message)
	task.PreferredWorkerNodeID = ""
	task.Trigger = realtimeTelegramTrigger
	dispatcher := currentClusterDispatcher()
	if dispatcher == nil {
		return 0, errors.New("cluster subscription dispatcher is not registered")
	}
	results, err := dispatcher.DispatchSubscriptionMedia(ctx, []ClusterMediaTask{task})
	if err != nil {
		return 0, err
	}
	if len(results) != 1 || results[0].Error != nil || strings.TrimSpace(results[0].JobID) == "" {
		if len(results) == 1 && results[0].Error != nil {
			return 0, results[0].Error
		}
		return 0, errors.New("realtime candidate dispatch returned no job")
	}
	jobID := results[0].JobID
	winner.item.Status = model.SubscriptionItemStatusTransferring
	winner.item.ClusterJobID = jobID
	winner.item.LastError = ""
	if err := persistAcceptedSubscriptionItemAndEpisodeSourceSnapshot(sub, winner.item); err != nil {
		return 0, err
	}
	selected := []uint{winner.record.ID}
	skipped := make([]uint, 0, len(records)-1)
	for _, record := range records {
		if record.ID == winner.record.ID {
			continue
		}
		skipped = append(skipped, record.ID)
		if item, err := db.GetSubscriptionItem(subscriptionID, record.SourceKey); err == nil && item.Status == model.SubscriptionItemStatusPending {
			item.Status = model.SubscriptionItemStatusSkipped
			item.LastError = "skipped: preferred realtime provider selected for the same media slot"
			_, _, _ = db.UpsertSubscriptionItem(item)
		}
	}
	if err := db.UpdateSubscriptionRealtimeCandidateStatus(selected, model.SubscriptionRealtimeCandidateStatusSelected, ""); err != nil {
		return 0, err
	}
	if err := db.UpdateSubscriptionRealtimeCandidateStatus(skipped, model.SubscriptionRealtimeCandidateStatusSkipped, "skipped: preferred realtime provider selected"); err != nil {
		return 0, err
	}
	return 1, nil
}

func realtimeCandidateSlotKey(sub *model.Subscription, item *model.SubscriptionItem) string {
	if slot := mediaSlotKey(sub, item); slot != "" {
		return slot
	}
	return "source:" + item.SourceKey
}

func realtimeProviderNeedsWait(provider string, cfg model.SubscriptionTelegramSourceConfig) bool {
	priority := normalizeTransferPriority(cfg.TransferPriority)
	expected := cfg.RealtimeExpectedProviders
	if len(expected) == 0 {
		expected = priority
	}
	index := make(map[string]int, len(priority))
	for i, value := range priority {
		index[value] = i
	}
	rank := providerPriorityRank(normalizeSubscriptionProvider(provider), index)
	for _, value := range expected {
		if providerPriorityRank(normalizeSubscriptionProvider(value), index) < rank {
			return true
		}
	}
	return false
}

func candidateIDs(items []model.SubscriptionRealtimeCandidate) []uint {
	ids := make([]uint, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	return ids
}

func pathBase(value string) string {
	value = strings.TrimSuffix(strings.TrimSpace(value), "/")
	if index := strings.LastIndex(value, "/"); index >= 0 {
		return value[index+1:]
	}
	return value
}

package subscription

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
)

const (
	ClusterWorkflowVersion       = "subscription-media-v1"
	ClusterSealedManifestVersion = "etf-sha256-v1"
)

// ClusterMediaTask is deliberately owned by the subscription package. The
// cluster runtime adapts it to its wire protocol, avoiding a subscription ->
// cluster -> subscription import cycle.
type ClusterMediaTask struct {
	IdempotencyKey        string
	SubscriptionID        uint
	SubscriptionItemID    uint
	SubscriptionName      string
	PreferredWorkerNodeID string
	SourceKey             string
	SourceMessageID       string
	SourceMessageChannel  string
	SourceMessageURL      string
	SourceMessageText     string
	ShareProvider         string
	ShareURL              string
	SharePasscode         string
	ShareRefFingerprint   string
	SourceFileID          string
	SourceRelativePath    string
	SourceSize            int64
	SourceHash            string
	MediaItemID           string
	MediaType             string
	TMDBID                int64
	TMDBName              string
	TMDBYear              int
	Season                int
	Episode               int
	LogicalMediaRoot      string
	LogicalTargetPath     string
	TargetProfile         string
	WorkflowVersion       string
	SealedManifestVersion string
}

type ClusterDispatchResult struct {
	SourceKey string
	JobID     string
	Error     error
}

type ClusterInspectTask struct {
	IdempotencyKey        string
	SubscriptionID        uint
	SubscriptionName      string
	PreferredWorkerNodeID string
	SourceMessageID       string
	SourceMessageChannel  string
	SourceMessageURL      string
	SourceMessageText     string
	ShareProvider         string
	ShareURL              string
	SharePasscode         string
	ShareRefFingerprint   string
	ObservationKey        string
	ObservationExpected   int
}

type ClusterInspectObject struct {
	FileID       string
	RelativePath string
	Size         int64
	Hash         string
	ModifiedAt   time.Time
}

type ClusterInspectManifestInput struct {
	Task    ClusterInspectTask
	Objects []ClusterInspectObject
}

type ClusterDispatcher interface {
	DispatchSubscriptionMedia(context.Context, []ClusterMediaTask) ([]ClusterDispatchResult, error)
	DispatchSubscriptionInspect(context.Context, ClusterInspectTask) (string, error)
}

var clusterDispatcherRegistry struct {
	sync.RWMutex
	dispatcher ClusterDispatcher
}

// RegisterClusterDispatcher installs the coordinator-side adapter. Passing
// nil unregisters it, which is useful during shutdown and in tests.
func RegisterClusterDispatcher(dispatcher ClusterDispatcher) {
	clusterDispatcherRegistry.Lock()
	clusterDispatcherRegistry.dispatcher = dispatcher
	clusterDispatcherRegistry.Unlock()
}

func currentClusterDispatcher() ClusterDispatcher {
	clusterDispatcherRegistry.RLock()
	defer clusterDispatcherRegistry.RUnlock()
	return clusterDispatcherRegistry.dispatcher
}

func dispatchClusterInspect(ctx context.Context, sub *model.Subscription, ref ShareRef, message clusterSourceMessage) (string, error) {
	return dispatchClusterInspectObservation(ctx, sub, ref, message, "", 1)
}

func dispatchClusterInspectObservation(ctx context.Context, sub *model.Subscription, ref ShareRef, message clusterSourceMessage, observationKey string, observationExpected int) (string, error) {
	dispatcher := currentClusterDispatcher()
	if dispatcher == nil {
		return "", errors.New("cluster subscription dispatcher is not registered")
	}
	task := clusterInspectObservationTask(sub, ref, message, observationKey, observationExpected)
	return dispatcher.DispatchSubscriptionInspect(ctx, task)
}

func clusterInspectTask(sub *model.Subscription, ref ShareRef, message clusterSourceMessage) ClusterInspectTask {
	return clusterInspectObservationTask(sub, ref, message, "", 1)
}

func clusterInspectObservationTask(sub *model.Subscription, ref ShareRef, message clusterSourceMessage, observationKey string, observationExpected int) ClusterInspectTask {
	fingerprint := shortHash(string(ref.Provider) + "\x00" + ref.ShareID + "\x00" + ref.Passcode)
	if strings.TrimSpace(observationKey) == "" {
		observationKey = hashClusterSource("observation", fmt.Sprint(sub.ID), message.ID, fingerprint)
	}
	if observationExpected <= 0 {
		observationExpected = 1
	}
	return ClusterInspectTask{
		IdempotencyKey: hashClusterSource("inspect", fmt.Sprint(sub.ID), string(ref.Provider), ref.ShareID, message.ID),
		SubscriptionID: sub.ID, SubscriptionName: sub.Name, PreferredWorkerNodeID: sub.PreferredWorkerNodeID,
		SourceMessageID: message.ID, SourceMessageChannel: message.Channel,
		SourceMessageURL: message.URL, SourceMessageText: message.Text,
		ShareProvider: string(ref.Provider), ShareURL: ref.RawURL, SharePasscode: ref.Passcode,
		ShareRefFingerprint: fingerprint,
		ObservationKey:      observationKey, ObservationExpected: observationExpected,
	}
}

func ApplyClusterInspectManifest(ctx context.Context, task ClusterInspectTask, objects []ClusterInspectObject) (int, error) {
	return ApplyClusterInspectObservation(ctx, []ClusterInspectManifestInput{{Task: task, Objects: objects}})
}

type clusterInspectCandidate struct {
	item    *model.SubscriptionItem
	ref     ShareRef
	message clusterSourceMessage
}

func ApplyClusterInspectObservation(ctx context.Context, manifests []ClusterInspectManifestInput) (int, error) {
	if len(manifests) == 0 {
		return 0, nil
	}
	sub, err := db.GetSubscriptionByID(manifests[0].Task.SubscriptionID)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	candidates := make([]clusterInspectCandidate, 0)
	for _, input := range manifests {
		task := input.Task
		if task.SubscriptionID != sub.ID {
			return 0, errors.New("cluster inspect observation mixes subscriptions")
		}
		if strings.EqualFold(strings.TrimSpace(sub.SourceType), model.SubscriptionSourceTelegram) && !subscriptionTitleMatches(sub, task.SourceMessageText) {
			continue
		}
		ref, err := ParseShareURL(task.ShareURL)
		if err != nil {
			return 0, err
		}
		if task.SharePasscode != "" {
			ref.Passcode = task.SharePasscode
		}
		message := clusterSourceMessage{ID: task.SourceMessageID, Channel: task.SourceMessageChannel, URL: task.SourceMessageURL, Text: task.SourceMessageText}
		for _, object := range input.Objects {
			entry := TreeEntry{ID: object.FileID, Path: "/" + strings.TrimPrefix(object.RelativePath, "/"), Name: path.Base(object.RelativePath), Size: object.Size, Modified: object.ModifiedAt}
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
	candidates = selectClusterInspectCandidates(sub, candidates, clusterInspectTransferPriority(sub))
	items := make([]*model.SubscriptionItem, 0, len(candidates))
	candidateBySourceKey := make(map[string]clusterInspectCandidate, len(candidates))
	for _, candidate := range candidates {
		items = append(items, candidate.item)
		candidateBySourceKey[candidate.item.SourceKey] = candidate
	}
	stored, _, _, err := upsertClusterItems(items)
	if err != nil {
		return 0, err
	}
	type dispatchGroup struct {
		ref     ShareRef
		message clusterSourceMessage
		items   []*model.SubscriptionItem
	}
	groups := make(map[string]*dispatchGroup)
	groupOrder := make([]string, 0)
	for _, item := range stored {
		candidate, ok := candidateBySourceKey[item.SourceKey]
		if !ok {
			continue
		}
		key := strings.Join([]string{string(candidate.ref.Provider), candidate.ref.RawURL, candidate.ref.Passcode, candidate.message.ID}, "\x00")
		group := groups[key]
		if group == nil {
			group = &dispatchGroup{ref: candidate.ref, message: candidate.message}
			groups[key] = group
			groupOrder = append(groupOrder, key)
		}
		group.items = append(group.items, item)
	}
	dispatched := 0
	var firstErr error
	for _, key := range groupOrder {
		group := groups[key]
		count, err := dispatchClusterItems(ctx, sub, group.items, group.ref, group.message)
		dispatched += count
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return dispatched, firstErr
}

func selectClusterInspectCandidates(sub *model.Subscription, candidates []clusterInspectCandidate, priority []string) []clusterInspectCandidate {
	if sub == nil || len(candidates) <= 1 || strings.EqualFold(strings.TrimSpace(sub.MediaType), "movie") {
		return candidates
	}
	priority = normalizeTransferPriority(priority)
	priorityIndex := make(map[string]int, len(priority))
	for index, provider := range priority {
		priorityIndex[provider] = index
	}
	selected := make([]clusterInspectCandidate, 0, len(candidates))
	bestBySlot := make(map[string]clusterInspectCandidate, len(candidates))
	slotOrder := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.item == nil || candidate.item.Episode <= 0 {
			selected = append(selected, candidate)
			continue
		}
		slot := fmt.Sprintf("%d:%d", candidate.item.Season, candidate.item.Episode)
		existing, ok := bestBySlot[slot]
		if !ok {
			slotOrder = append(slotOrder, slot)
		}
		if !ok || betterClusterInspectCandidate(candidate, existing, priorityIndex) {
			bestBySlot[slot] = candidate
		}
	}
	for _, slot := range slotOrder {
		selected = append(selected, bestBySlot[slot])
	}
	return selected
}

func betterClusterInspectCandidate(candidate, existing clusterInspectCandidate, priorityIndex map[string]int) bool {
	if candidate.item.FileSize != existing.item.FileSize {
		return candidate.item.FileSize > existing.item.FileSize
	}
	candidateProvider := normalizeSubscriptionProvider(candidate.item.SourceProvider)
	existingProvider := normalizeSubscriptionProvider(existing.item.SourceProvider)
	candidateRank := providerPriorityRank(candidateProvider, priorityIndex)
	existingRank := providerPriorityRank(existingProvider, priorityIndex)
	if candidateRank != existingRank {
		return candidateRank < existingRank
	}
	candidateKey := strings.Join([]string{candidateProvider, candidate.item.FilePath, candidate.item.FileID, candidate.item.SourceKey}, "\x00")
	existingKey := strings.Join([]string{existingProvider, existing.item.FilePath, existing.item.FileID, existing.item.SourceKey}, "\x00")
	return candidateKey < existingKey
}

func clusterInspectTransferPriority(sub *model.Subscription) []string {
	if sub != nil && strings.EqualFold(strings.TrimSpace(sub.SourceType), model.SubscriptionSourceTelegram) {
		if cfg, err := parseTelegramConfig(sub.SourceConfig); err == nil {
			return cfg.TransferPriority
		}
	}
	return nil
}

type clusterSourceMessage struct {
	ID      string
	Channel string
	URL     string
	Text    string
}

func dispatchClusterItems(ctx context.Context, sub *model.Subscription, items []*model.SubscriptionItem, ref ShareRef, message clusterSourceMessage) (int, error) {
	if sub == nil || !sub.TransferEnabled {
		return 0, nil
	}
	dispatcher := currentClusterDispatcher()
	if dispatcher == nil {
		return 0, errors.New("cluster subscription dispatcher is not registered")
	}
	tasks := make([]ClusterMediaTask, 0, len(items))
	for _, item := range items {
		if item == nil || item.Status != model.SubscriptionItemStatusPending {
			continue
		}
		tasks = append(tasks, clusterMediaTask(sub, item, ref, message))
	}
	if len(tasks) == 0 {
		return 0, nil
	}
	results, dispatchErr := dispatcher.DispatchSubscriptionMedia(ctx, tasks)
	resultByKey := make(map[string]ClusterDispatchResult, len(results))
	for _, result := range results {
		resultByKey[result.SourceKey] = result
	}
	dispatched := 0
	var firstErr error
	for _, item := range items {
		if item == nil || item.Status != model.SubscriptionItemStatusPending {
			continue
		}
		result, ok := resultByKey[item.SourceKey]
		if !ok {
			if dispatchErr != nil {
				item.Status = model.SubscriptionItemStatusFailed
				item.LastError = dispatchErr.Error()
				if firstErr == nil {
					firstErr = dispatchErr
				}
			}
		} else if result.Error != nil {
			item.Status = model.SubscriptionItemStatusFailed
			item.LastError = result.Error.Error()
			if firstErr == nil {
				firstErr = result.Error
			}
		} else {
			jobID := strings.TrimSpace(result.JobID)
			if jobID == "" {
				err := errors.New("cluster dispatch returned empty job id")
				item.Status = model.SubscriptionItemStatusFailed
				item.LastError = err.Error()
				if firstErr == nil {
					firstErr = err
				}
				if _, _, upsertErr := db.UpsertSubscriptionItem(item); upsertErr != nil && firstErr == nil {
					firstErr = upsertErr
				}
				continue
			}
			item.Status = model.SubscriptionItemStatusTransferring
			item.ClusterJobID = jobID
			item.LastError = ""
			if err := persistAcceptedSubscriptionItemAndEpisodeSourceSnapshot(sub, item); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			dispatched++
			continue
		}
		if _, _, err := db.UpsertSubscriptionItem(item); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return dispatched, firstErr
}

func clusterMediaTask(sub *model.Subscription, item *model.SubscriptionItem, ref ShareRef, message clusterSourceMessage) ClusterMediaTask {
	shareFingerprint := shortHash(string(ref.Provider) + "\x00" + strings.TrimSpace(ref.RawURL) + "\x00" + ref.Passcode)
	mediaItemID := clusterMediaItemID(sub.ID, item.SourceKey, item.FileHash)
	idempotency := hashClusterSource(fmt.Sprint(sub.ID), string(ref.Provider), ref.ShareID, item.FileID, item.FileHash, item.TargetPath)
	return ClusterMediaTask{
		IdempotencyKey: idempotency, SubscriptionID: sub.ID, SubscriptionItemID: item.ID,
		SubscriptionName: sub.Name, PreferredWorkerNodeID: sub.PreferredWorkerNodeID, SourceKey: item.SourceKey,
		SourceMessageID: message.ID, SourceMessageChannel: message.Channel,
		SourceMessageURL: message.URL, SourceMessageText: message.Text,
		ShareProvider: string(ref.Provider), ShareURL: ref.RawURL, SharePasscode: ref.Passcode,
		ShareRefFingerprint: shareFingerprint, SourceFileID: item.FileID,
		SourceRelativePath: strings.TrimPrefix(item.FilePath, "/"), SourceSize: item.FileSize,
		SourceHash: item.FileHash, MediaItemID: mediaItemID, MediaType: sub.MediaType,
		TMDBID: sub.TMDBID, TMDBName: sub.TMDBName, TMDBYear: sub.TMDBYear,
		Season: item.Season, Episode: item.Episode,
		LogicalMediaRoot:  sub.TargetRoot,
		LogicalTargetPath: item.TargetPath,
		WorkflowVersion:   ClusterWorkflowVersion, SealedManifestVersion: ClusterSealedManifestVersion,
	}
}

func clusterMediaItemID(subscriptionID uint, sourceKey, fileHash string) string {
	return shortHash(fmt.Sprintf("%d\x00%s\x00%s", subscriptionID, sourceKey, fileHash))
}

func clusterItemFromShareEntry(sub *model.Subscription, ref ShareRef, entry TreeEntry, message clusterSourceMessage, seenAt time.Time) *model.SubscriptionItem {
	// SourceKey is stable across Telegram messages. FileHash includes size and
	// modified time, so re-posting an unchanged link is idempotent while an
	// updated object becomes pending and gets a new dispatch idempotency key.
	entry.RootPath = string(ref.Provider) + ":" + strings.TrimSpace(ref.ShareID)
	if ref.ParentID != "" {
		entry.RootPath += ":" + strings.TrimSpace(ref.ParentID)
	}
	item := itemFromEntry(sub, entry, seenAt)
	item.SourceProvider = string(ref.Provider)
	item.SourceURL = ref.RawURL
	item.SourceMessageID = message.ID
	item.SourceMessageChannel = message.Channel
	item.SourceMessageURL = message.URL
	item.SourceMessageText = message.Text
	return item
}

func inspectClusterShare(ctx context.Context, sub *model.Subscription, cfg model.SubscriptionTelegramSourceConfig, rawLink string, message clusterSourceMessage, seenAt time.Time) ([]*model.SubscriptionItem, ShareRef, error) {
	ref, err := ParseShareURL(rawLink)
	if err != nil {
		return nil, ref, err
	}
	source, ok := telegramPanSourceForProvider(cfg, ref.Provider)
	if !ok {
		return nil, ref, fmt.Errorf("share provider %s is not configured", ref.Provider)
	}
	source.Config, err = telegramPanSourceConfigWithStorageFallback(ref.Provider, source.Config)
	if err != nil {
		return nil, ref, err
	}
	provider, err := newShareSaverForProvider(ref.Provider, source.Config)
	if err != nil {
		return nil, ref, err
	}
	pairs, err := collectShareTreePairs(ctx, provider, ref)
	if err != nil {
		return nil, ref, err
	}
	pairs = filterLargestSharePairsPerSlot(sub, pairs)
	items := make([]*model.SubscriptionItem, 0, len(pairs))
	for _, pair := range pairs {
		if !isMediaEntry(pair.entry) || !boundShareEntryMatches(sub, pair.entry) {
			continue
		}
		items = append(items, clusterItemFromShareEntry(sub, ref, pair.entry, message, seenAt))
	}
	return items, ref, nil
}

func sourceMessageFromTelegramRow(row telegramCommandRow) clusterSourceMessage {
	// Preserve every title-bearing Telegram field so the downstream manifest
	// guard evaluates the same message context as the initial dispatch gate.
	text := strings.TrimSpace(rowText(row))
	messageID := ""
	if id := rowMessageID(row); id > 0 {
		messageID = strconv.FormatInt(id, 10)
	}
	return clusterSourceMessage{
		ID: messageID, Channel: normalizeTelegramChannel(row.Channel),
		URL: strings.TrimSpace(row.MessageURL), Text: text,
	}
}

func clusterItemsHash(items []*model.SubscriptionItem) string {
	values := make([]string, 0, len(items))
	for _, item := range items {
		if item != nil {
			values = append(values, item.SourceKey+":"+item.FileHash)
		}
	}
	sort.Strings(values)
	return combinedHash("cluster", values)
}

// CompleteClusterTransfer and FailClusterTransfer are the stable callback
// surface used by the Coordinator after ETF materialization succeeds or a job
// reaches a terminal failure.
func CompleteClusterTransfer(subscriptionID uint, sourceKey, jobID string) error {
	return completeClusterTransfer(subscriptionID, sourceKey, jobID, model.SubscriptionItemStatusTransferred, "")
}

func FailClusterTransfer(subscriptionID uint, sourceKey, jobID string, cause error) error {
	lastError := ""
	if cause != nil {
		lastError = cause.Error()
	}
	return completeClusterTransfer(subscriptionID, sourceKey, jobID, model.SubscriptionItemStatusFailed, lastError)
}

func completeClusterTransfer(subscriptionID uint, sourceKey, jobID, status, lastError string) error {
	item, err := db.GetSubscriptionItem(subscriptionID, sourceKey)
	if err != nil {
		return err
	}
	if item.Status == model.SubscriptionItemStatusPending {
		return recoverPendingClusterTerminalItem(item, jobID, status, lastError)
	}
	if item.Status != model.SubscriptionItemStatusTransferring || item.ClusterJobID == "" || item.ClusterJobID != jobID {
		return errors.New("cluster job does not match the active subscription item transfer")
	}
	expectedJobID := jobID
	terminalJobID := jobID
	terminalLastError := item.LastError
	if status == model.SubscriptionItemStatusTransferred {
		terminalLastError = ""
	} else if lastError != "" {
		terminalLastError = lastError
	}
	_, err = persistSubscriptionTerminalItem(db.SubscriptionTerminalItemRequest{
		ItemID:               item.ID,
		SubscriptionID:       item.SubscriptionID,
		SourceKey:            item.SourceKey,
		ExpectedFileHash:     item.FileHash,
		ExpectedStatus:       model.SubscriptionItemStatusTransferring,
		ExpectedClusterJobID: &expectedJobID,
		TerminalStatus:       status,
		TerminalLastError:    terminalLastError,
		TerminalClusterJobID: &terminalJobID,
	})
	return err
}

func recoverPendingClusterTerminalItem(item *model.SubscriptionItem, jobID, status, lastError string) error {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return errors.New("cluster job is required to recover a pending subscription item")
	}
	var job model.ClusterJob
	if err := db.GetDb().First(&job, "id = ?", jobID).Error; err != nil {
		return errors.WithStack(err)
	}
	if job.Type != model.ClusterJobTypeMediaTransfer ||
		job.SubscriptionID != item.SubscriptionID ||
		job.SubscriptionItemID != item.ID ||
		job.MediaItemID != clusterMediaItemID(item.SubscriptionID, item.SourceKey, item.FileHash) {
		return errors.New("cluster callback does not match an accepted pending subscription item")
	}
	subscription, err := db.GetSubscriptionByID(item.SubscriptionID)
	if err != nil {
		return err
	}
	terminalLastError := item.LastError
	if status == model.SubscriptionItemStatusTransferred {
		terminalLastError = ""
	} else if lastError != "" {
		terminalLastError = lastError
	}
	recoveryItem := *item
	recoveryItem.ClusterJobID = jobID
	source, err := subscriptionEpisodeSourceSnapshot(subscription, &recoveryItem)
	if err != nil {
		return err
	}
	terminalJobID := jobID
	_, err = persistSubscriptionTerminalItem(db.SubscriptionTerminalItemRequest{
		ItemID:               item.ID,
		SubscriptionID:       item.SubscriptionID,
		SourceKey:            item.SourceKey,
		ExpectedFileHash:     item.FileHash,
		ExpectedStatus:       model.SubscriptionItemStatusPending,
		TerminalStatus:       status,
		TerminalLastError:    terminalLastError,
		TerminalClusterJobID: &terminalJobID,
		RecoverySource:       source,
	})
	return err
}

func hashClusterSource(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

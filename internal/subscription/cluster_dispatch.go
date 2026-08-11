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
	log "github.com/sirupsen/logrus"
)

const (
	ClusterWorkflowVersion       = "subscription-media-v1"
	ClusterSealedManifestVersion = "etf-sha256-v1"
)

// ErrClusterWorkerUnavailable means dispatch is blocked by worker capability
// or account health, not that the media item itself is invalid. Callers must
// leave the item retryable instead of converting this condition to failure.
var ErrClusterWorkerUnavailable = errors.New("no compatible cluster worker is available")

type ClusterWorkerUnavailableError struct {
	Message string
}

func (e *ClusterWorkerUnavailableError) Error() string {
	if e == nil {
		return ErrClusterWorkerUnavailable.Error()
	}
	return e.Message
}

func (e *ClusterWorkerUnavailableError) Unwrap() error {
	return ErrClusterWorkerUnavailable
}

// ClusterMediaTask is deliberately owned by the subscription package. The
// cluster runtime adapts it to its wire protocol, avoiding a subscription ->
// cluster -> subscription import cycle.
type ClusterMediaTask struct {
	IdempotencyKey        string
	SubscriptionID        uint
	SubscriptionItemID    uint
	SubscriptionName      string
	PreferredWorkerNodeID string
	Trigger               string
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
	SourceProviderData    map[string]string
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
	DeliveryMode          string
}

type ClusterDispatchResult struct {
	SourceKey string
	JobID     string
	Error     error
}

// ClusterRetryResult reports replayed durable media jobs. Unmatched is kept
// separate so a retry endpoint can fail loudly instead of turning an item
// without a replayable job into a misleading pending state.
type ClusterRetryResult struct {
	Requeued  int
	Unmatched int
}

// ClusterFailedSubscriptionRetrier is optional on ClusterDispatcher so old
// test adapters and non-coordinator integrations remain source-compatible.
type ClusterFailedSubscriptionRetrier interface {
	RetryFailedSubscriptionItems(context.Context, uint) (ClusterRetryResult, error)
}

type ClusterInspectTask struct {
	IdempotencyKey        string
	SubscriptionID        uint
	SubscriptionName      string
	PreferredWorkerNodeID string
	Trigger               string
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

var clusterInspectApplyLocks sync.Map // subscriptionID -> *sync.Mutex

func lockClusterInspectApply(subscriptionID uint) func() {
	value, _ := clusterInspectApplyLocks.LoadOrStore(subscriptionID, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
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

// RetryOrphanedClusterSubscriptionItems rebuilds media children from the
// source metadata stored on the item when the original durable job row is
// missing. This is the compensation path for the historical "failed item with
// no task" population; successful items are never included.
func RetryOrphanedClusterSubscriptionItems(ctx context.Context, subscriptionID uint) (ClusterRetryResult, error) {
	var recovered ClusterRetryResult
	if subscriptionID == 0 {
		return recovered, errors.New("subscription id is required")
	}
	if currentClusterDispatcher() == nil {
		return recovered, errors.New("cluster subscription dispatcher is not registered")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	sub, err := db.GetSubscriptionByID(subscriptionID)
	if err != nil {
		return recovered, err
	}
	items, err := db.ListSubscriptionItems(subscriptionID)
	if err != nil {
		return recovered, err
	}
	var jobs []model.ClusterJob
	if err := db.GetDb().WithContext(ctx).Where("subscription_id = ? AND type = ?", subscriptionID, model.ClusterJobTypeMediaTransfer).
		Order("created_at DESC, id DESC").Find(&jobs).Error; err != nil {
		return recovered, err
	}
	activeByItemID := make(map[uint]struct{}, len(jobs))
	for i := range jobs {
		if jobs[i].SubscriptionItemID != 0 && subscriptionJobActive(jobs[i].Status) {
			activeByItemID[jobs[i].SubscriptionItemID] = struct{}{}
		}
	}
	type retryGroup struct {
		ref     ShareRef
		message clusterSourceMessage
		items   []*model.SubscriptionItem
	}
	groups := make(map[string]*retryGroup)
	for i := range items {
		item := &items[i]
		if item.Status != model.SubscriptionItemStatusFailed && item.Status != model.SubscriptionItemStatusPending &&
			item.Status != model.SubscriptionItemStatusRetryWait && item.Status != model.SubscriptionItemStatusUnknown &&
			item.Status != model.SubscriptionItemStatusBlocked {
			continue
		}
		if _, active := activeByItemID[item.ID]; active {
			continue
		}
		ref, parseErr := retryShareRef(item)
		if parseErr != nil {
			recovered.Unmatched++
			continue
		}
		item.Status = model.SubscriptionItemStatusPending
		item.ClusterJobID = ""
		item.LastError = ""
		item.LastErrorCode = ""
		item.RetryAt = nil
		item.BlockedReason = ""
		item.StateVersion++
		if _, _, err := db.UpsertSubscriptionItemForceStatus(item); err != nil {
			return recovered, err
		}
		if item.Season > 0 && item.Episode > 0 {
			if err := db.GetDb().WithContext(ctx).Model(&model.SubscriptionEpisodeSource{}).
				Where("subscription_id = ? AND source_item_id = ? AND file_hash = ?", subscriptionID, item.ID, item.FileHash).
				Updates(map[string]any{"status": model.SubscriptionItemStatusPending, "cluster_job_id": "", "updated_at": time.Now().UTC()}).Error; err != nil {
				return recovered, err
			}
		}
		message := clusterSourceMessage{ID: item.SourceMessageID, Channel: item.SourceMessageChannel, URL: item.SourceMessageURL, Text: item.SourceMessageText}
		key := ref.RawURL + "\x00" + message.ID + "\x00" + message.Channel
		group := groups[key]
		if group == nil {
			group = &retryGroup{ref: ref, message: message}
			groups[key] = group
		}
		group.items = append(group.items, item)
	}
	for _, group := range groups {
		dispatched, dispatchErr := dispatchClusterItems(ctx, sub, group.items, group.ref, group.message)
		recovered.Requeued += dispatched
		if dispatchErr != nil {
			return recovered, dispatchErr
		}
	}
	return recovered, nil
}

func retryShareRef(item *model.SubscriptionItem) (ShareRef, error) {
	if item == nil {
		return ShareRef{}, errors.New("subscription item is nil")
	}
	candidates := make([]string, 0, 2)
	if sourceURL := strings.TrimSpace(item.SourceURL); sourceURL != "" {
		candidates = append(candidates, sourceURL)
	}
	// Older rows may have lost SourceURL while retaining the original source
	// message. Recovering a link from that message keeps those rows replayable
	// without re-scanning the remote source.
	for _, link := range resourceLinksFromText(item.SourceMessageText, "") {
		candidates = append(candidates, link.URL)
	}
	var firstErr error
	for _, candidate := range candidates {
		ref, err := ParseShareURL(candidate)
		if err == nil {
			return ref, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr == nil {
		firstErr = errors.New("subscription item has no persisted replayable share URL")
	}
	return ShareRef{}, firstErr
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
	return clusterInspectObservationTaskForTrigger(sub, ref, message, observationKey, observationExpected, "")
}

func clusterInspectObservationTaskForTrigger(sub *model.Subscription, ref ShareRef, message clusterSourceMessage, observationKey string, observationExpected int, trigger string) ClusterInspectTask {
	fingerprint := shortHash(strings.Join([]string{string(ref.Provider), ref.ShareID, ref.ParentID, ref.Passcode}, "\x00"))
	if strings.TrimSpace(observationKey) == "" {
		observationKey = hashClusterSource("observation", fmt.Sprint(sub.ID), message.ID, fingerprint)
	}
	if observationExpected <= 0 {
		observationExpected = 1
	}
	return ClusterInspectTask{
		IdempotencyKey: hashClusterSource(
			"inspect", fmt.Sprint(sub.ID), observationKey, fingerprint,
			message.ID, message.Channel, message.URL, message.Text,
		),
		SubscriptionID: sub.ID, SubscriptionName: sub.Name, PreferredWorkerNodeID: sub.PreferredWorkerNodeID, Trigger: trigger,
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

// ObservationCloseState tells the incremental apply path how much of the
// observation is currently known. PendingProviders lists the share providers
// of sibling share.inspect jobs that have not yet reported a terminal result;
// AllTerminal means every expected child has reported (successfully or via an
// empty failed manifest), so any slot without a better candidate left to wait
// for must be force-closed with whatever candidate is available.
type ObservationCloseState struct {
	PendingProviders []string
	AllTerminal      bool
}

// ApplyClusterInspectObservation applies a fully known set of manifests: every
// resolved episode/movie winner is dispatched immediately. Callers that only
// have a partial view of an in-flight observation should use
// ApplyClusterInspectObservationIncremental instead.
func ApplyClusterInspectObservation(ctx context.Context, manifests []ClusterInspectManifestInput) (int, error) {
	return applyClusterInspectObservation(ctx, manifests, ObservationCloseState{AllTerminal: true})
}

// ApplyClusterInspectObservationIncremental applies whatever manifests have
// arrived so far for an observation that may still have non-terminal sibling
// share.inspect jobs. Only slots that decideSlotClose considers closed (or,
// once state.AllTerminal is true, every remaining open slot) are dispatched;
// the rest stay pending so a later call can dispatch them once more
// information arrives.
func ApplyClusterInspectObservationIncremental(ctx context.Context, manifests []ClusterInspectManifestInput, state ObservationCloseState) (int, error) {
	return applyClusterInspectObservation(ctx, manifests, state)
}

func applyClusterInspectObservation(ctx context.Context, manifests []ClusterInspectManifestInput, state ObservationCloseState) (int, error) {
	if len(manifests) == 0 {
		return 0, nil
	}
	unlock := lockClusterInspectApply(manifests[0].Task.SubscriptionID)
	defer unlock()
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
	priority := clusterInspectTransferPriority(sub)
	candidates = selectClusterInspectCandidates(sub, candidates, priority)
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
	// Cross-observation dedupe keeps one active file per episode before worker
	// dispatch, preferring the configured provider and then the largest version.
	stored, err = reconcileClusterEpisodeSlots(sub, stored, priority)
	if err != nil {
		return 0, err
	}
	closable, err := filterObservationDispatchCandidates(sub, stored, state, priority)
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
	for _, item := range closable {
		if item == nil || item.Status != model.SubscriptionItemStatusPending {
			continue
		}
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
	if sub == nil || len(candidates) <= 1 {
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
		if candidate.item == nil {
			continue
		}
		slot := mediaSlotKey(sub, candidate.item)
		if slot == "" || (normalizeMediaType(sub.MediaType) != "movie" && candidate.item.Episode <= 0) {
			selected = append(selected, candidate)
			continue
		}
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
	if candidate.item == nil {
		return false
	}
	if existing.item == nil {
		return true
	}
	candidateProvider := normalizeSubscriptionProvider(candidate.item.SourceProvider)
	existingProvider := normalizeSubscriptionProvider(existing.item.SourceProvider)
	candidateRank := providerPriorityRank(candidateProvider, priorityIndex)
	existingRank := providerPriorityRank(existingProvider, priorityIndex)
	if candidateRank != existingRank {
		return candidateRank < existingRank
	}
	if candidate.item.FileSize != existing.item.FileSize {
		return candidate.item.FileSize > existing.item.FileSize
	}
	candidateKey := strings.Join([]string{candidateProvider, candidate.item.FilePath, candidate.item.FileID, candidate.item.SourceKey}, "\x00")
	existingKey := strings.Join([]string{existingProvider, existing.item.FilePath, existing.item.FileID, existing.item.SourceKey}, "\x00")
	return candidateKey < existingKey
}

// filterObservationDispatchCandidates narrows the winners reconcileClusterEpisodeSlots
// resolved down to the subset allowed to dispatch this round. TV items without a
// recognized episode slot pass through unchanged (they never waited on sibling
// inspects). Movie items and recognized TV episode slots only dispatch once
// decideSlotClose says the slot is closed (priority-closed or size-floor, using
// MovieEarlyCloseMinBytes for movies), unless the whole observation is already
// fully terminal, in which case every remaining winner is force-closed.
func filterObservationDispatchCandidates(sub *model.Subscription, items []*model.SubscriptionItem, state ObservationCloseState, priority []string) ([]*model.SubscriptionItem, error) {
	if sub == nil || state.AllTerminal {
		return items, nil
	}
	cfg, err := GetConfig()
	if err != nil {
		return nil, err
	}
	episodeMinBytes := earlyCloseMinBytes(cfg.EpisodeEarlyCloseMinBytes, 1<<30)
	movieMinBytes := earlyCloseMinBytes(cfg.MovieEarlyCloseMinBytes, 20<<30)
	isMovie := normalizeMediaType(sub.MediaType) == "movie"
	eligible := make([]*model.SubscriptionItem, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		if !isMovie && item.Episode <= 0 {
			eligible = append(eligible, item)
			continue
		}
		if mediaSlotKey(sub, item) == "" {
			eligible = append(eligible, item)
			continue
		}
		decision := decideSlotClose(slotCloseInput{
			MediaType:        sub.MediaType,
			Winner:           item,
			PendingProviders: state.PendingProviders,
			EpisodeMinBytes:  episodeMinBytes,
			MovieMinBytes:    movieMinBytes,
			Priority:         priority,
		})
		if decision.Closed {
			eligible = append(eligible, item)
		}
	}
	return eligible, nil
}

const clusterSkippedDuplicateEpisodeReason = "skipped: larger or preferred file selected for the same episode"

// reconcileClusterEpisodeSlots keeps one active transfer candidate per media
// slot (TV episode or movie) across independently inspected shares/messages.
// Non-winners that are still pending or transferring are marked skipped so
// workers never temp-save them. TV files without a recognized episode number
// are left untouched (no cross-share dedupe is attempted for them).
func reconcileClusterEpisodeSlots(sub *model.Subscription, stored []*model.SubscriptionItem, priority []string) ([]*model.SubscriptionItem, error) {
	if sub == nil || len(stored) == 0 {
		return stored, nil
	}
	priority = normalizeTransferPriority(priority)
	priorityIndex := make(map[string]int, len(priority))
	for index, provider := range priority {
		priorityIndex[provider] = index
	}

	touchedSlots := make(map[string]struct{}, len(stored))
	storedByKey := make(map[string]*model.SubscriptionItem, len(stored))
	for _, item := range stored {
		if item == nil {
			continue
		}
		storedByKey[item.SourceKey] = item
		if clusterItemHasDedupSlot(sub, item) {
			touchedSlots[mediaSlotKey(sub, item)] = struct{}{}
		}
	}
	if len(touchedSlots) == 0 {
		return stored, nil
	}

	existing, err := db.ListSubscriptionItems(sub.ID)
	if err != nil {
		return stored, err
	}

	competitorsBySlot := make(map[string][]*model.SubscriptionItem, len(touchedSlots))
	for i := range existing {
		item := &existing[i]
		slot := mediaSlotKey(sub, item)
		if _, ok := touchedSlots[slot]; !ok {
			continue
		}
		if !clusterItemCompetesForSlot(sub, item) {
			continue
		}
		if refreshed, ok := storedByKey[item.SourceKey]; ok {
			item = refreshed
		}
		competitorsBySlot[slot] = append(competitorsBySlot[slot], item)
	}
	for _, item := range stored {
		if item == nil {
			continue
		}
		slot := mediaSlotKey(sub, item)
		if _, ok := touchedSlots[slot]; !ok {
			continue
		}
		if !clusterItemCompetesForSlot(sub, item) {
			continue
		}
		found := false
		for _, existingItem := range competitorsBySlot[slot] {
			if existingItem.SourceKey == item.SourceKey {
				found = true
				break
			}
		}
		if !found {
			competitorsBySlot[slot] = append(competitorsBySlot[slot], item)
		}
	}

	winnerBySlot := make(map[string]*model.SubscriptionItem, len(competitorsBySlot))
	for slot, competitors := range competitorsBySlot {
		winner := acceptedClusterEpisodeWinner(competitors)
		if winner == nil {
			for _, item := range competitors {
				if winner == nil || betterClusterInspectCandidate(clusterInspectCandidate{item: item}, clusterInspectCandidate{item: winner}, priorityIndex) {
					winner = item
				}
			}
		}
		if winner != nil {
			winnerBySlot[slot] = winner
		}
	}

	dispatchable := make([]*model.SubscriptionItem, 0, len(stored))
	for _, item := range stored {
		if item == nil {
			continue
		}
		slot := mediaSlotKey(sub, item)
		if _, touched := touchedSlots[slot]; slot == "" || !touched {
			dispatchable = append(dispatchable, item)
			continue
		}
		winner := winnerBySlot[slot]
		if winner != nil && winner.SourceKey == item.SourceKey {
			if winner.Status == model.SubscriptionItemStatusTransferred ||
				winner.Status == model.SubscriptionItemStatusTransferring {
				// Already accepted for this slot; do not redispatch.
				continue
			}
			dispatchable = append(dispatchable, item)
			continue
		}
		if err := skipClusterDuplicateEpisodeItem(item); err != nil {
			return stored, err
		}
	}

	for slot, competitors := range competitorsBySlot {
		winner := winnerBySlot[slot]
		if winner == nil {
			continue
		}
		for _, item := range competitors {
			if item.SourceKey == winner.SourceKey {
				continue
			}
			if _, fromObservation := storedByKey[item.SourceKey]; fromObservation {
				continue // already handled above
			}
			if err := skipClusterDuplicateEpisodeItem(item); err != nil {
				return stored, err
			}
		}
	}
	return dispatchable, nil
}

func acceptedClusterEpisodeWinner(items []*model.SubscriptionItem) *model.SubscriptionItem {
	var winner *model.SubscriptionItem
	for _, item := range items {
		if !subscriptionItemHasAcceptedTransfer(item) {
			continue
		}
		if winner == nil || item.CreatedAt.Before(winner.CreatedAt) ||
			(item.CreatedAt.Equal(winner.CreatedAt) && item.ID < winner.ID) {
			winner = item
		}
	}
	return winner
}

func subscriptionItemHasAcceptedTransfer(item *model.SubscriptionItem) bool {
	return item != nil && (item.Status == model.SubscriptionItemStatusTransferring || item.Status == model.SubscriptionItemStatusNotifying || item.Status == model.SubscriptionItemStatusTransferred)
}

// clusterItemHasDedupSlot reports whether item participates in cross-share
// slot dedupe: movies always compete by TargetPath, while TV items only
// compete once an episode number was recognized (TV files without one keep
// their independent "path:" pseudo-slot, which is never touched here).
func clusterItemHasDedupSlot(sub *model.Subscription, item *model.SubscriptionItem) bool {
	if item == nil || mediaSlotKey(sub, item) == "" {
		return false
	}
	return normalizeMediaType(sub.MediaType) == "movie" || item.Episode > 0
}

func clusterItemCompetesForSlot(sub *model.Subscription, item *model.SubscriptionItem) bool {
	if !clusterItemHasDedupSlot(sub, item) {
		return false
	}
	switch item.Status {
	case model.SubscriptionItemStatusPending, model.SubscriptionItemStatusTransferring, model.SubscriptionItemStatusNotifying, model.SubscriptionItemStatusTransferred:
		return true
	default:
		return false
	}
}

func skipClusterDuplicateEpisodeItem(item *model.SubscriptionItem) error {
	if item == nil {
		return nil
	}
	switch item.Status {
	case model.SubscriptionItemStatusPending, model.SubscriptionItemStatusTransferring, model.SubscriptionItemStatusFailed:
		// continue
	default:
		return nil
	}
	if item.Status == model.SubscriptionItemStatusSkipped && item.LastError == clusterSkippedDuplicateEpisodeReason {
		return nil
	}
	item.Status = model.SubscriptionItemStatusSkipped
	item.LastError = clusterSkippedDuplicateEpisodeReason
	item.StateVersion++
	// Keep ClusterJobID so a late worker callback can still match and no-op.
	_, _, err := db.UpsertSubscriptionItem(item)
	return err
}

func clusterInspectTransferPriority(sub *model.Subscription) []string {
	if sub == nil {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(sub.SourceType)) {
	case model.SubscriptionSourceTelegram:
		if cfg, err := parseTelegramConfig(sub.SourceConfig); err == nil {
			return cfg.TransferPriority
		}
	case model.SubscriptionSourcePanSou, model.SubscriptionSourceHDHive, model.SubscriptionSourceAuto:
		if cfg, err := GetConfig(); err == nil {
			return cfg.Telegram.TransferPriority
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
	log.Warnf("[cluster-dispatch] dispatchClusterItems: sub=%d items=%d transferEnabled=%v", sub.ID, len(items), sub.TransferEnabled)
	if sub == nil || !sub.TransferEnabled {
		return 0, nil
	}
	dispatcher := currentClusterDispatcher()
	if dispatcher == nil {
		return 0, errors.New("cluster subscription dispatcher is not registered")
	}
	tasks := make([]ClusterMediaTask, 0, len(items))
	dispatchItems := make([]*model.SubscriptionItem, 0, len(items))
	claimedItems := make(map[*model.SubscriptionItem]bool, len(items))
	for _, item := range items {
		if item == nil || item.Status != model.SubscriptionItemStatusPending {
			continue
		}
		if normalizeMediaType(sub.MediaType) != "movie" && item.Episode > 0 {
			source, err := subscriptionEpisodeSourceSnapshot(sub, item)
			if err != nil {
				return 0, err
			}
			claimed, _, err := db.TryClaimSubscriptionEpisodeSource(source, time.Now())
			if err != nil {
				return 0, err
			}
			if !claimed {
				if err := skipClusterDuplicateEpisodeItem(item); err != nil {
					return 0, err
				}
				continue
			}
			claimedItems[item] = true
		}
		task := clusterMediaTask(sub, item, ref, message)
		item.DeliveryMode = task.DeliveryMode
		item.OperationKey = task.IdempotencyKey
		tasks = append(tasks, task)
		dispatchItems = append(dispatchItems, item)
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
	waitingForWorker := errors.Is(dispatchErr, ErrClusterWorkerUnavailable)
	for _, item := range dispatchItems {
		result, ok := resultByKey[item.SourceKey]
		item.StateVersion++
		if !ok {
			if waitingForWorker {
				item.Status = model.SubscriptionItemStatusPending
				item.ClusterJobID = ""
				item.LastError = dispatchErr.Error()
				if _, _, err := db.UpsertSubscriptionItem(item); err != nil && firstErr == nil {
					firstErr = err
				}
				if claimedItems[item] {
					if releaseErr := db.ReleaseSubscriptionEpisodeSourceClaim(item); releaseErr != nil && firstErr == nil {
						firstErr = releaseErr
					}
				}
				continue
			}
			item.Status = model.SubscriptionItemStatusFailed
			missingErr := dispatchErr
			if missingErr == nil {
				missingErr = errors.New("cluster dispatch returned no result for claimed episode source")
			}
			item.LastError = missingErr.Error()
			if firstErr == nil {
				firstErr = missingErr
			}
		} else if result.Error != nil {
			if errors.Is(result.Error, ErrClusterWorkerUnavailable) {
				item.Status = model.SubscriptionItemStatusPending
				item.ClusterJobID = ""
				item.LastError = result.Error.Error()
				if _, _, err := db.UpsertSubscriptionItem(item); err != nil && firstErr == nil {
					firstErr = err
				}
				if claimedItems[item] {
					if releaseErr := db.ReleaseSubscriptionEpisodeSourceClaim(item); releaseErr != nil && firstErr == nil {
						firstErr = releaseErr
					}
				}
				if firstErr == nil {
					firstErr = result.Error
				}
				continue
			}
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
				if claimedItems[item] {
					if releaseErr := db.ReleaseSubscriptionEpisodeSourceClaim(item); releaseErr != nil && firstErr == nil {
						firstErr = releaseErr
					}
				}
				continue
			}
			item.Status = model.SubscriptionItemStatusTransferring
			item.ClusterJobID = jobID
			item.LastError = ""
			item.LastErrorCode = ""
			item.RetryAt = nil
			item.BlockedReason = ""
			if err := persistAcceptedSubscriptionItemAndEpisodeSourceSnapshot(sub, item); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				if claimedItems[item] {
					if releaseErr := db.ReleaseSubscriptionEpisodeSourceClaim(item); releaseErr != nil && firstErr == nil {
						firstErr = releaseErr
					}
				}
				continue
			}
			dispatched++
			continue
		}
		if _, _, err := db.UpsertSubscriptionItem(item); err != nil && firstErr == nil {
			firstErr = err
		}
		if claimedItems[item] {
			if err := db.ReleaseSubscriptionEpisodeSourceClaim(item); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return dispatched, firstErr
}

func clusterMediaTask(sub *model.Subscription, item *model.SubscriptionItem, ref ShareRef, message clusterSourceMessage) ClusterMediaTask {
	shareFingerprint := shortHash(string(ref.Provider) + "\x00" + strings.TrimSpace(ref.RawURL) + "\x00" + ref.Passcode)
	mediaItemID := clusterMediaItemID(sub.ID, item.SourceKey, item.FileHash)
	idempotency := hashClusterSource(fmt.Sprint(sub.ID), string(ref.Provider), ref.ShareID, item.FileID, item.FileHash, item.TargetPath)
	deliveryMode := model.SubscriptionDeliveryModeTransfer
	if cfg, err := GetConfig(); err == nil && cfg.DirectShareLinkEnabled && cfg.DirectDownloadFirstEnabled && ref.Provider == ShareProviderPan123 {
		deliveryMode = model.SubscriptionDeliveryModeDirectDownload
	}
	return ClusterMediaTask{
		IdempotencyKey: idempotency, SubscriptionID: sub.ID, SubscriptionItemID: item.ID,
		SubscriptionName: sub.Name, PreferredWorkerNodeID: sub.PreferredWorkerNodeID, SourceKey: item.SourceKey,
		SourceMessageID: message.ID, SourceMessageChannel: message.Channel,
		SourceMessageURL: message.URL, SourceMessageText: message.Text,
		ShareProvider: string(ref.Provider), ShareURL: ref.RawURL, SharePasscode: ref.Passcode,
		ShareRefFingerprint: shareFingerprint, SourceFileID: item.FileID,
		SourceRelativePath: strings.TrimPrefix(item.FilePath, "/"), SourceSize: item.FileSize,
		SourceHash: item.FileHash, SourceProviderData: cloneShareItemProviderData(item.ProviderData), MediaItemID: mediaItemID, MediaType: sub.MediaType,
		TMDBID: sub.TMDBID, TMDBName: sub.TMDBName, TMDBYear: sub.TMDBYear,
		Season: item.Season, Episode: item.Episode,
		LogicalMediaRoot:  sub.TargetRoot,
		LogicalTargetPath: item.TargetPath,
		WorkflowVersion:   ClusterWorkflowVersion, SealedManifestVersion: ClusterSealedManifestVersion,
		DeliveryMode: deliveryMode,
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
	if item.Status == model.SubscriptionItemStatusSkipped {
		// Superseded by a larger/preferred episode candidate after dispatch.
		return nil
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

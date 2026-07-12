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
	IdempotencyKey       string
	SubscriptionID       uint
	SubscriptionName     string
	SourceMessageID      string
	SourceMessageChannel string
	SourceMessageURL     string
	SourceMessageText    string
	ShareProvider        string
	ShareURL             string
	SharePasscode        string
	ShareRefFingerprint  string
}

type ClusterInspectObject struct {
	FileID       string
	RelativePath string
	Size         int64
	Hash         string
	ModifiedAt   time.Time
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
	dispatcher := currentClusterDispatcher()
	if dispatcher == nil {
		return "", errors.New("cluster subscription dispatcher is not registered")
	}
	task := clusterInspectTask(sub, ref, message)
	return dispatcher.DispatchSubscriptionInspect(ctx, task)
}

func clusterInspectTask(sub *model.Subscription, ref ShareRef, message clusterSourceMessage) ClusterInspectTask {
	fingerprint := shortHash(string(ref.Provider) + "\x00" + ref.ShareID + "\x00" + ref.Passcode)
	return ClusterInspectTask{
		IdempotencyKey: hashClusterSource("inspect", fmt.Sprint(sub.ID), string(ref.Provider), ref.ShareID, message.ID),
		SubscriptionID: sub.ID, SubscriptionName: sub.Name,
		SourceMessageID: message.ID, SourceMessageChannel: message.Channel,
		SourceMessageURL: message.URL, SourceMessageText: message.Text,
		ShareProvider: string(ref.Provider), ShareURL: ref.RawURL, SharePasscode: ref.Passcode,
		ShareRefFingerprint: fingerprint,
	}
}

func ApplyClusterInspectManifest(ctx context.Context, task ClusterInspectTask, objects []ClusterInspectObject) (int, error) {
	sub, err := db.GetSubscriptionByID(task.SubscriptionID)
	if err != nil {
		return 0, err
	}
	ref, err := ParseShareURL(task.ShareURL)
	if err != nil {
		return 0, err
	}
	if task.SharePasscode != "" {
		ref.Passcode = task.SharePasscode
	}
	message := clusterSourceMessage{ID: task.SourceMessageID, Channel: task.SourceMessageChannel, URL: task.SourceMessageURL, Text: task.SourceMessageText}
	now := time.Now()
	items := make([]*model.SubscriptionItem, 0, len(objects))
	for _, object := range objects {
		entry := TreeEntry{ID: object.FileID, Path: "/" + strings.TrimPrefix(object.RelativePath, "/"), Name: path.Base(object.RelativePath), Size: object.Size, Modified: object.ModifiedAt}
		if !isMediaEntry(entry) || !boundShareEntryMatches(sub, entry) {
			continue
		}
		item := clusterItemFromShareEntry(sub, ref, entry, message, now)
		if object.Hash != "" {
			item.FileHash = object.Hash
		}
		items = append(items, item)
	}
	stored, _, _, err := upsertClusterItems(items)
	if err != nil {
		return 0, err
	}
	return dispatchClusterItems(ctx, sub, stored, ref, message)
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
			item.Status = model.SubscriptionItemStatusTransferring
			item.ClusterJobID = strings.TrimSpace(result.JobID)
			item.LastError = ""
			dispatched++
		}
		if _, _, err := db.UpsertSubscriptionItem(item); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return dispatched, firstErr
}

func clusterMediaTask(sub *model.Subscription, item *model.SubscriptionItem, ref ShareRef, message clusterSourceMessage) ClusterMediaTask {
	shareFingerprint := shortHash(string(ref.Provider) + "\x00" + strings.TrimSpace(ref.RawURL) + "\x00" + ref.Passcode)
	mediaItemID := shortHash(fmt.Sprintf("%d\x00%s\x00%s", sub.ID, item.SourceKey, item.FileHash))
	idempotency := hashClusterSource(fmt.Sprint(sub.ID), string(ref.Provider), ref.ShareID, item.FileID, item.FileHash, item.TargetPath)
	return ClusterMediaTask{
		IdempotencyKey: idempotency, SubscriptionID: sub.ID, SubscriptionItemID: item.ID,
		SubscriptionName: sub.Name, SourceKey: item.SourceKey,
		SourceMessageID: message.ID, SourceMessageChannel: message.Channel,
		SourceMessageURL: message.URL, SourceMessageText: message.Text,
		ShareProvider: string(ref.Provider), ShareURL: ref.RawURL, SharePasscode: ref.Passcode,
		ShareRefFingerprint: shareFingerprint, SourceFileID: item.FileID,
		SourceRelativePath: strings.TrimPrefix(item.FilePath, "/"), SourceSize: item.FileSize,
		SourceHash: item.FileHash, MediaItemID: mediaItemID, MediaType: sub.MediaType,
		TMDBID: sub.TMDBID, TMDBName: sub.TMDBName, TMDBYear: sub.TMDBYear,
		Season: item.Season, Episode: item.Episode, LogicalMediaRoot: sub.TargetRoot,
		LogicalTargetPath: item.TargetPath,
		WorkflowVersion:   ClusterWorkflowVersion, SealedManifestVersion: ClusterSealedManifestVersion,
	}
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
	source.Config = telegramPanSourceConfigWithStorageFallback(ref.Provider, source.Config)
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
	text := strings.TrimSpace(row.Text)
	if text == "" {
		text = strings.TrimSpace(row.RawText)
	}
	if text == "" {
		text = strings.TrimSpace(row.Caption)
	}
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
	item, err := db.GetSubscriptionItem(subscriptionID, sourceKey)
	if err != nil {
		return err
	}
	if jobID != "" && item.ClusterJobID != "" && item.ClusterJobID != jobID {
		return errors.New("cluster job does not match the active subscription item transfer")
	}
	item.Status = model.SubscriptionItemStatusTransferred
	item.LastError = ""
	if jobID != "" {
		item.ClusterJobID = jobID
	}
	_, _, err = db.UpsertSubscriptionItem(item)
	return err
}

func FailClusterTransfer(subscriptionID uint, sourceKey, jobID string, cause error) error {
	item, err := db.GetSubscriptionItem(subscriptionID, sourceKey)
	if err != nil {
		return err
	}
	if jobID != "" && item.ClusterJobID != "" && item.ClusterJobID != jobID {
		return errors.New("cluster job does not match the active subscription item transfer")
	}
	item.Status = model.SubscriptionItemStatusFailed
	if cause != nil {
		item.LastError = cause.Error()
	}
	_, _, err = db.UpsertSubscriptionItem(item)
	return err
}

func hashClusterSource(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

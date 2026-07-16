package subscription

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func runClusterBySource(ctx context.Context, sub *model.Subscription) ([]model.SubscriptionItem, string, int, int, int, error) {
	switch strings.ToLower(strings.TrimSpace(sub.SourceType)) {
	case model.SubscriptionSourceTelegram:
		return runTelegramCluster(ctx, sub)
	case model.SubscriptionSourceManual, "":
		return runManualCluster(ctx, sub)
	case model.SubscriptionSourcePanSou:
		return runPanSouCluster(ctx, sub)
	default:
		return nil, sub.LastTreeHash, 0, 0, 0, fmt.Errorf("unsupported subscription source type: %s", sub.SourceType)
	}
}

func runTelegramCluster(ctx context.Context, sub *model.Subscription) ([]model.SubscriptionItem, string, int, int, int, error) {
	cfg, err := parseTelegramConfig(sub.SourceConfig)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	rows, err := runTelegramSearch(ctx, sub, cfg)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	cursor := parseTelegramCursor(sub.LastCursor)
	nextCursor := cursor.clone()
	var saved []model.SubscriptionItem
	added, changed, dispatched := 0, 0, 0
	for _, row := range rows {
		msgID := rowMessageID(row)
		if msgID > 0 && telegramCursorHasSeen(cursor, row) {
			continue
		}
		nextCursor.advance(row)
		if !telegramRowMatchesSubscription(sub, row) {
			continue
		}
		message := sourceMessageFromTelegramRow(row)
		links, _ := rowLinksForTelegramPanSources(row, cfg)
		accessCode := rowAccessCode(row)
		refs := make([]ShareRef, 0, len(links))
		for _, link := range links {
			rawLink := normalizeTelegramLinkWithAccessCode(link, accessCode)
			ref, err := ParseShareURL(rawLink)
			if err != nil {
				return saved, sub.LastTreeHash, added, changed, dispatched, err
			}
			refs = append(refs, ref)
		}
		observationKey := clusterObservationKey(sub.ID, "telegram", message, refs)
		for _, ref := range refs {
			if _, err := dispatchClusterInspectObservation(ctx, sub, ref, message, observationKey, len(refs)); err != nil {
				return saved, sub.LastTreeHash, added, changed, dispatched, err
			}
			dispatched++
		}
	}
	if formatted := formatTelegramCursor(nextCursor); formatted != strings.TrimSpace(sub.LastCursor) {
		sub.LastCursor = formatted
	}
	hash := telegramRowsHash(rows)
	return saved, hash, added, changed, dispatched, nil
}

func runManualCluster(ctx context.Context, sub *model.Subscription) ([]model.SubscriptionItem, string, int, int, int, error) {
	cfg, err := parseManualConfig(sub.SourceConfig)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	if len(cfg.Links) == 0 {
		return runManual(ctx, sub, false)
	}
	var saved []model.SubscriptionItem
	added, changed, dispatched := 0, 0, 0
	observationID := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	type manualObservationItem struct {
		ref     ShareRef
		message clusterSourceMessage
	}
	items := make([]manualObservationItem, 0, len(cfg.Links))
	for _, link := range cfg.Links {
		message := clusterSourceMessage{ID: "manual:" + observationID + ":" + shortHash(link), Text: strings.TrimSpace(link)}
		ref, err := ParseShareURL(link)
		if err != nil {
			return saved, sub.LastTreeHash, added, changed, dispatched, err
		}
		items = append(items, manualObservationItem{ref: ref, message: message})
	}
	observationKey := hashClusterSource("observation", fmt.Sprint(sub.ID), "manual", observationID)
	for _, item := range items {
		if _, err := dispatchClusterInspectObservation(ctx, sub, item.ref, item.message, observationKey, len(items)); err != nil {
			return saved, sub.LastTreeHash, added, changed, dispatched, err
		}
		dispatched++
	}
	return saved, combinedHash("cluster-inspect", cfg.Links), added, changed, dispatched, nil
}

func runPanSouCluster(ctx context.Context, sub *model.Subscription) ([]model.SubscriptionItem, string, int, int, int, error) {
	cfg, err := parsePanSouConfig(sub.SourceConfig)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	query := telegramSearchQuery(sub)
	results, err := searchPanSouResources(ctx, query, cfg.Limit, cfg)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	var saved []model.SubscriptionItem
	added, changed, dispatched := 0, 0, 0
	observationID := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	type panSouObservationItem struct {
		ref     ShareRef
		message clusterSourceMessage
	}
	items := make([]panSouObservationItem, 0)
	for _, result := range results {
		message := clusterSourceMessage{ID: "pansou:" + observationID + ":" + shortHash(result.MessageURL+result.Title), URL: result.MessageURL, Text: strings.TrimSpace(result.Content)}
		for _, link := range result.Links {
			ref, err := ParseShareURL(link.URL)
			if err != nil {
				continue
			}
			items = append(items, panSouObservationItem{ref: ref, message: message})
		}
	}
	observationKey := hashClusterSource("observation", fmt.Sprint(sub.ID), "pansou", observationID)
	for _, item := range items {
		if _, err := dispatchClusterInspectObservation(ctx, sub, item.ref, item.message, observationKey, len(items)); err != nil {
			return saved, sub.LastTreeHash, added, changed, dispatched, err
		}
		dispatched++
	}
	return saved, combinedHash("cluster-inspect", panSouResultLinks(results)), added, changed, dispatched, nil
}

func clusterObservationKey(subscriptionID uint, source string, message clusterSourceMessage, refs []ShareRef) string {
	identities := make([]string, 0, len(refs))
	for _, ref := range refs {
		identities = append(identities, string(ref.Provider)+"\x00"+ref.ShareID+"\x00"+ref.Passcode)
	}
	sort.Strings(identities)
	return hashClusterSource(
		"observation", fmt.Sprint(subscriptionID), source,
		message.ID, message.Channel, message.URL, message.Text,
		strings.Join(identities, "\x01"),
	)
}

func upsertClusterItems(items []*model.SubscriptionItem) ([]*model.SubscriptionItem, int, int, error) {
	stored := make([]*model.SubscriptionItem, 0, len(items))
	added, changed := 0, 0
	for _, item := range items {
		previous, previousErr := db.GetSubscriptionItem(item.SubscriptionID, item.SourceKey)
		saved, isNew, err := db.UpsertSubscriptionItem(item)
		if err != nil {
			return stored, added, changed, err
		}
		if isNew {
			added++
		} else if previousErr == nil && previous.FileHash != saved.FileHash {
			changed++
		} else if saved.Status == model.SubscriptionItemStatusFailed {
			saved.Status = model.SubscriptionItemStatusPending
			saved.ClusterJobID = ""
			saved.LastError = ""
			saved, _, err = db.UpsertSubscriptionItem(saved)
			if err != nil {
				return stored, added, changed, err
			}
			changed++
		} else if saved.Status == model.SubscriptionItemStatusSkipped {
			// Re-selected by a later inspect observation; allow reconcile to
			// compare it again and dispatch if it is now the episode winner.
			saved.Status = model.SubscriptionItemStatusPending
			saved.ClusterJobID = ""
			saved.LastError = ""
			saved, _, err = db.UpsertSubscriptionItem(saved)
			if err != nil {
				return stored, added, changed, err
			}
			changed++
		}
		stored = append(stored, saved)
	}
	return stored, added, changed, nil
}

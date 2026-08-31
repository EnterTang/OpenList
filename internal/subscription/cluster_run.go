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
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func runClusterBySource(ctx context.Context, sub *model.Subscription) ([]model.SubscriptionItem, string, int, int, int, error) {
	switch strings.ToLower(strings.TrimSpace(sub.SourceType)) {
	case model.SubscriptionSourceTelegram:
		return runTelegramCluster(ctx, sub)
	case model.SubscriptionSourceManual, "":
		return runManualCluster(ctx, sub)
	case model.SubscriptionSourcePanSou:
		return runPanSouCluster(ctx, sub)
	case model.SubscriptionSourceHDHive:
		return runHDHiveCluster(ctx, sub)
	case model.SubscriptionSourceAuto:
		return runAutoCluster(ctx, sub)
	case model.SubscriptionSourceMoviePilot:
		return runMoviePilot(ctx, sub, true)
	default:
		return nil, sub.LastTreeHash, 0, 0, 0, fmt.Errorf("unsupported subscription source type: %s", sub.SourceType)
	}
}

func runAutoCluster(ctx context.Context, sub *model.Subscription) ([]model.SubscriptionItem, string, int, int, int, error) {
	if sub == nil {
		return nil, "", 0, 0, 0, errors.New("subscription is required")
	}
	moviePilotSub := *sub
	moviePilotSub.SourceType = model.SubscriptionSourceMoviePilot
	items, hash, added, changed, dispatched, moviePilotErr := runMoviePilotForAuto(ctx, &moviePilotSub, true)
	if moviePilotErr == nil && len(items) > 0 {
		*sub = moviePilotSub
		return items, hash, added, changed, dispatched, nil
	}
	if moviePilotSub.BoundTorrent != nil {
		return items, hash, added, changed, dispatched, moviePilotErr
	}

	items, hash, added, changed, dispatched, fallbackErr := runHDHiveForAuto(ctx, sub, true)
	if fallbackErr != nil && moviePilotErr != nil {
		return items, hash, added, changed, dispatched, fmt.Errorf("MoviePilot priority search failed: %v; fallback source search failed: %w", moviePilotErr, fallbackErr)
	}
	return items, hash, added, changed, dispatched, fallbackErr
}

func runTelegramCluster(ctx context.Context, sub *model.Subscription) ([]model.SubscriptionItem, string, int, int, int, error) {
	ctx = ensureClusterObservationRunContext(ctx)
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
	blockedChannels := make(map[string]struct{})
	var saved []model.SubscriptionItem
	added, changed, dispatched := 0, 0, 0
	var firstErr error
	items := make([]clusterInspectObservationItem, 0)
	for _, row := range rows {
		msgID := rowMessageID(row)
		if msgID > 0 {
			channel := telegramCursorChannelKey(row.Channel)
			if _, blocked := blockedChannels[channel]; blocked {
				continue
			}
			if telegramCursorHasSeen(cursor, row) {
				continue
			}
			nextCursor.advance(row)
		}
		if !telegramRowMatchesSubscription(sub, row) {
			continue
		}
		message := sourceMessageFromTelegramRow(row)
		links, _ := rowLinksForTelegramPanSources(row, cfg)
		hdhiveLinks, err := resolveTelegramHDHiveLinks(ctx, row, cfg)
		if err != nil {
			firstErr = firstNonNilError(firstErr, err)
			if len(links) == 0 {
				nextCursor.holdBefore(row)
				blockedChannels[telegramCursorChannelKey(row.Channel)] = struct{}{}
				continue
			}
		}
		for _, link := range hdhiveLinks {
			links = append(links, normalizeTelegramLinkWithAccessCode(link.URL, link.AccessCode))
		}
		accessCode := rowAccessCode(row)
		for _, link := range links {
			rawLink := normalizeTelegramLinkWithAccessCode(link, accessCode)
			ref, err := ParseShareURL(rawLink)
			if err != nil {
				return saved, sub.LastTreeHash, added, changed, dispatched, err
			}
			items = append(items, clusterInspectObservationItem{ref: ref, message: message})
		}
	}
	items = dedupeClusterInspectObservationItems(items)
	observationKey := clusterObservationKey(ctx, sub.ID, "telegram", items)
	for _, item := range items {
		if _, err := dispatchClusterInspectObservation(ctx, sub, item.ref, item.message, observationKey, len(items)); err != nil {
			return saved, sub.LastTreeHash, added, changed, dispatched, err
		}
		dispatched++
	}
	if formatted := formatTelegramCursor(nextCursor); formatted != strings.TrimSpace(sub.LastCursor) {
		sub.LastCursor = formatted
	}
	hash := telegramRowsHash(rows)
	if firstErr != nil {
		if !isHDHiveRateLimitError(firstErr) {
			return saved, hash, added, changed, dispatched, firstErr
		}
		log.WithError(firstErr).Warn("subscription: HDHive rate limit isolated; direct Telegram sources were dispatched")
	}
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
	items := make([]clusterInspectObservationItem, 0, len(cfg.Links))
	for _, link := range cfg.Links {
		message := clusterSourceMessage{ID: "manual:" + observationID + ":" + shortHash(link), Text: strings.TrimSpace(link)}
		ref, err := ParseShareURL(link)
		if err != nil {
			return saved, sub.LastTreeHash, added, changed, dispatched, err
		}
		items = append(items, clusterInspectObservationItem{ref: ref, message: message})
	}
	items = dedupeClusterInspectObservationItems(items)
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
	ctx = ensureClusterObservationRunContext(ctx)
	cfg, err := parsePanSouConfig(sub.SourceConfig)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	results, err := searchPanSouResourcesForSubscription(ctx, sub, cfg)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	var saved []model.SubscriptionItem
	added, changed, dispatched := 0, 0, 0
	observationID := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	items := make([]clusterInspectObservationItem, 0)
	for _, result := range results {
		message := clusterSourceMessage{ID: "pansou:" + observationID + ":" + shortHash(result.MessageURL+result.Title), URL: result.MessageURL, Text: strings.TrimSpace(result.Content)}
		for _, link := range result.Links {
			ref, err := ParseShareURL(link.URL)
			if err != nil {
				continue
			}
			items = append(items, clusterInspectObservationItem{ref: ref, message: message})
		}
	}
	items = dedupeClusterInspectObservationItems(items)
	observationKey := hashClusterSource("observation", fmt.Sprint(sub.ID), "pansou", observationID)
	for _, item := range items {
		if _, err := dispatchClusterInspectObservation(ctx, sub, item.ref, item.message, observationKey, len(items)); err != nil {
			return saved, sub.LastTreeHash, added, changed, dispatched, err
		}
		dispatched++
	}
	return saved, combinedHash("cluster-inspect", panSouResultLinks(results)), added, changed, dispatched, nil
}

type clusterInspectObservationItem struct {
	ref     ShareRef
	message clusterSourceMessage
}

type subscriptionRunContextKey struct{}

func withSubscriptionRunID(ctx context.Context, runID string) context.Context {
	return context.WithValue(ctx, subscriptionRunContextKey{}, strings.TrimSpace(runID))
}

func ensureClusterObservationRunContext(ctx context.Context) context.Context {
	if runID, _ := ctx.Value(subscriptionRunContextKey{}).(string); strings.TrimSpace(runID) != "" {
		return ctx
	}
	return withSubscriptionRunID(ctx, fmt.Sprintf("run-%d", time.Now().UTC().UnixNano()))
}

func clusterObservationRunID(ctx context.Context) string {
	if runID, _ := ctx.Value(subscriptionRunContextKey{}).(string); strings.TrimSpace(runID) != "" {
		return strings.TrimSpace(runID)
	}
	return fmt.Sprintf("run-%d", time.Now().UTC().UnixNano())
}

func clusterObservationKey(ctx context.Context, subscriptionID uint, source string, items []clusterInspectObservationItem) string {
	return clusterObservationKeyWithRunID(clusterObservationRunID(ctx), subscriptionID, source, items)
}

func clusterObservationKeyWithRunID(runID string, subscriptionID uint, source string, items []clusterInspectObservationItem) string {
	identities := make([]string, 0, len(items))
	for _, item := range items {
		identities = append(identities, clusterInspectObservationItemIdentity(item))
	}
	sort.Strings(identities)
	return "run:" + hashClusterSource(
		"observation", fmt.Sprint(subscriptionID), source, runID,
		strings.Join(identities, "\x01"),
	)
}

func clusterSingleObservationKey(ctx context.Context, subscriptionID uint, source string, identities ...string) string {
	return "run:" + hashClusterSource("observation", fmt.Sprint(subscriptionID), source, clusterObservationRunID(ctx), strings.Join(identities, "\x00"))
}

func dedupeClusterInspectObservationItems(items []clusterInspectObservationItem) []clusterInspectObservationItem {
	seen := make(map[string]struct{}, len(items))
	unique := make([]clusterInspectObservationItem, 0, len(items))
	for _, item := range items {
		identity := clusterInspectObservationItemIdentity(item)
		if _, ok := seen[identity]; ok {
			continue
		}
		seen[identity] = struct{}{}
		unique = append(unique, item)
	}
	return unique
}

func clusterInspectObservationItemIdentity(item clusterInspectObservationItem) string {
	return strings.Join([]string{
		item.message.ID, item.message.Channel, item.message.URL, item.message.Text,
		string(item.ref.Provider), item.ref.ShareID, item.ref.ParentID, item.ref.Passcode,
	}, "\x00")
}

func upsertClusterItems(items []*model.SubscriptionItem) ([]*model.SubscriptionItem, int, int, error) {
	stored := make([]*model.SubscriptionItem, 0, len(items))
	added, changed := 0, 0
	for _, item := range items {
		previous, previousErr := db.GetSubscriptionItem(item.SubscriptionID, item.SourceKey)
		if previousErr == nil && subscriptionItemHasAcceptedTransfer(previous) && previous.FileHash != item.FileHash {
			previous.LastSeenAt = item.LastSeenAt
			saved, _, err := db.UpsertSubscriptionItem(previous)
			if err != nil {
				return stored, added, changed, err
			}
			stored = append(stored, saved)
			continue
		}
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
			saved, _, err = db.UpsertSubscriptionItemForceStatus(saved)
			if err != nil {
				return stored, added, changed, err
			}
			changed++
		} else if saved.Status == model.SubscriptionItemStatusSkipped {
			// Re-selected by a later inspect observation; allow reconcile to
			// compare it again and dispatch if it is now the episode winner.
			// Use ForceStatus so the preservation logic in UpsertSubscriptionItem
			// does not silently revert skipped→pending.
			saved.Status = model.SubscriptionItemStatusPending
			saved.ClusterJobID = ""
			saved.LastError = ""
			saved, _, err = db.UpsertSubscriptionItemForceStatus(saved)
			if err != nil {
				return stored, added, changed, err
			}
			changed++
		}
		stored = append(stored, saved)
	}
	return stored, added, changed, nil
}

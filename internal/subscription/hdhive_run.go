package subscription

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/hdhive"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
)

type hdhiveSubscriptionClient interface {
	Search(context.Context, string, int64) ([]hdhive.Resource, error)
	Share(context.Context, string) (hdhive.ResourceDetails, error)
}

var (
	newHDHiveSubscriptionClient = func(cfg model.SubscriptionTelegramHDHiveConfig) (hdhiveSubscriptionClient, error) {
		return telegramHDHiveClientForConfig(cfg)
	}
	runTelegramForHDHiveSubscription               = runTelegram
	runPanSouForHDHiveSubscription                 = runPanSou
	transferSelectedShareCandidatesForSubscription = transferSelectedShareCandidates
	unlockHDHiveResourceForSubscription            = UnlockHDHiveResource
)

func runHDHive(ctx context.Context, sub *model.Subscription, transfer bool) ([]model.SubscriptionItem, string, int, int, int, error) {
	sourceCfg, err := parseHDHiveSourceConfig(sub.SourceConfig)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	globalCfg, err := GetConfig()
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	return runHDHiveFederated(ctx, sub, sourceCfg, globalCfg, transfer)
}

func parseHDHiveSourceConfig(raw string) (model.SubscriptionHDHiveSourceConfig, error) {
	cfg := model.SubscriptionHDHiveSourceConfig{CloudType: "all", Limit: defaultResourceSearchLimit}
	if strings.TrimSpace(raw) != "" {
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			return cfg, errors.WithMessage(err, "invalid hdhive source config")
		}
	}
	cfg.CloudType = strings.ToLower(strings.TrimSpace(cfg.CloudType))
	if cfg.CloudType == "" {
		cfg.CloudType = "all"
	}
	if !isHDHiveCloudType(cfg.CloudType) {
		return cfg, errors.Errorf("unsupported hdhive cloud_type: %s", cfg.CloudType)
	}
	if cfg.Limit <= 0 {
		cfg.Limit = defaultResourceSearchLimit
	}
	return cfg, nil
}

func runHDHiveFederated(ctx context.Context, sub *model.Subscription, sourceCfg model.SubscriptionHDHiveSourceConfig, globalCfg model.SubscriptionConfig, transfer bool) ([]model.SubscriptionItem, string, int, int, int, error) {
	if sub == nil {
		return nil, "", 0, 0, 0, errors.New("subscription is nil")
	}
	now := time.Now()
	var saved []model.SubscriptionItem
	var hashes []string
	var links []string
	added, changed, transferred := 0, 0, 0
	var firstErr error
	boundCandidate := false

	if sub.BoundShare != nil && strings.TrimSpace(sub.BoundShare.ShareURL) != "" {
		rawShare := NormalizeSubscriptionShareURL(sub.BoundShare.ShareURL, sub.BoundShare.AccessCode)
		_, candidates, handled, err := inspectShareLinkCandidatesFn(ctx, sub, globalCfg.Telegram, rawShare, now)
		if err != nil {
			firstErr = err
		} else if handled {
			boundCandidate = len(candidates) > 0
			selected := selectShareTransferCandidates(sub, candidates, globalCfg.Telegram.TransferPriority)
			items, hash, itemAdded, itemChanged, itemTransferred, transferErr := transferSelectedShareCandidatesForSubscription(ctx, sub, selected, transfer, now, "bound:"+rawShare)
			if transferErr != nil && firstErr == nil {
				firstErr = transferErr
			}
			saved = append(saved, items...)
			added += itemAdded
			changed += itemChanged
			transferred += itemTransferred
			if hash != "" {
				hashes = append(hashes, hash)
			}
			links = append(links, rawShare)
		}
	}

	regularCandidate, regularReady := runHDHiveRegularSources(ctx, sub, globalCfg, transfer, &saved, &hashes, &added, &changed, &transferred, &firstErr)
	regularCandidate = regularCandidate || boundCandidate

	client, err := newHDHiveSubscriptionClient(globalCfg.Telegram.HDHive)
	if err != nil {
		if firstErr == nil {
			firstErr = err
		}
		return saved, hdhiveSubscriptionHash(sub, hashes, links), added, changed, transferred, firstErr
	}
	resources, err := client.Search(ctx, strings.ToLower(strings.TrimSpace(sub.MediaType)), sub.TMDBID)
	if err != nil {
		if firstErr == nil {
			firstErr = err
		}
		return saved, hdhiveSubscriptionHash(sub, hashes, links), added, changed, transferred, firstErr
	}
	resources = filterHDHiveResources(resources, sourceCfg.CloudType)
	if sourceCfg.Limit > 0 && len(resources) > sourceCfg.Limit {
		resources = resources[:sourceCfg.Limit]
	}

	for _, resource := range resources {
		resourceURL := strings.TrimSpace(resource.ResourceURL)
		if resourceURL == "" {
			resourceURL = defaultHDHiveResourceURL(resource)
		}
		resourceRef, ok := hdhive.ResourceRefFromURL(resourceURL, resource.PanType)
		if !ok {
			continue
		}
		resourceURL = resourceRef.URL
		details, shareErr := client.Share(ctx, resourceRef.Slug)
		if shareErr != nil {
			if firstErr == nil {
				firstErr = shareErr
			}
			continue
		}

		shareURL := strings.TrimSpace(details.FullURL)
		accessCode := strings.TrimSpace(details.AccessCode)
		requiresUnlock := false
		if shareURL == "" {
			points := firstHDHiveUnlockPoints(details.UnlockPoints, resource.UnlockPoints)
			free := details.IsFreeForUser || points != nil && *points == 0
			if !free {
				requiresUnlock = true
				if sub.BoundShare != nil && sub.BoundShare.RequiresUnlock && sub.BoundShare.ResourceSlug != resourceRef.Slug {
					continue
				}
				if !regularReady || regularCandidate {
					continue
				}
				if points == nil {
					continue
				}
				if globalCfg.Telegram.HDHive.MaxUnlockPoints > 0 && (points == nil || *points > globalCfg.Telegram.HDHive.MaxUnlockPoints) {
					continue
				}
			}
			unlock, unlockErr := unlockHDHiveResourceForSubscription(ctx, resourceURL, globalCfg.Telegram.HDHive)
			if unlockErr != nil {
				if firstErr == nil {
					firstErr = unlockErr
				}
				continue
			}
			shareURL = strings.TrimSpace(unlock.URL)
			accessCode = firstNonEmpty(unlock.AccessCode, accessCode)
		}
		if shareURL == "" {
			continue
		}
		rawShare := NormalizeSubscriptionShareURL(shareURL, accessCode)
		if requiresUnlock {
			bindHDHiveShare(sub, resourceURL, resourceRef, rawShare, accessCode, resource, details, true, now)
		}
		_, candidates, handled, inspectErr := inspectShareLinkCandidatesFn(ctx, sub, globalCfg.Telegram, rawShare, now)
		if inspectErr != nil {
			if firstErr == nil {
				firstErr = inspectErr
			}
			continue
		}
		if !handled {
			continue
		}
		selected := selectShareTransferCandidates(sub, candidates, globalCfg.Telegram.TransferPriority)
		items, hash, itemAdded, itemChanged, itemTransferred, transferErr := transferSelectedShareCandidatesForSubscription(ctx, sub, selected, transfer, now, "hdhive:"+resourceURL)
		if transferErr != nil && firstErr == nil {
			firstErr = transferErr
		}
		saved = append(saved, items...)
		added += itemAdded
		changed += itemChanged
		transferred += itemTransferred
		if hash != "" {
			hashes = append(hashes, hash)
		}
		links = append(links, rawShare)
		if len(candidates) > 0 {
			bindHDHiveShare(sub, resourceURL, resourceRef, rawShare, accessCode, resource, details, requiresUnlock, now)
			regularCandidate = true
		}
	}

	return saved, hdhiveSubscriptionHash(sub, hashes, links), added, changed, transferred, firstErr
}

func runHDHiveRegularSources(ctx context.Context, sub *model.Subscription, globalCfg model.SubscriptionConfig, transfer bool, saved *[]model.SubscriptionItem, hashes *[]string, added, changed, transferred *int, firstErr *error) (candidate, ready bool) {
	ready = true
	if hasTelegramSearchCommand(globalCfg.Telegram) || hasBuiltinTelegramConfig(globalCfg.Telegram) {
		telegramCfg := globalCfg.Telegram
		telegramCfg.HDHive.Enabled = false
		body, err := json.Marshal(telegramCfg)
		if err != nil {
			return false, false
		}
		telegramSub := *sub
		telegramSub.SourceType = model.SubscriptionSourceTelegram
		telegramSub.SourceConfig = string(body)
		items, hash, itemAdded, itemChanged, itemTransferred, runErr := runTelegramForHDHiveSubscription(ctx, &telegramSub, transfer)
		if runErr != nil {
			ready = false
			if *firstErr == nil {
				*firstErr = runErr
			}
		} else {
			candidate = candidate || subscriptionRunHasCandidate(items)
			if hash != "" {
				*hashes = append(*hashes, "telegram:"+hash)
			}
		}
		*saved = append(*saved, items...)
		*added += itemAdded
		*changed += itemChanged
		*transferred += itemTransferred
		sub.LastCursor = telegramSub.LastCursor
	}

	panSouConfigured := len(globalCfg.PanSou.SearchCommand) > 0 && strings.TrimSpace(globalCfg.PanSou.SearchCommand[0]) != "" || strings.TrimSpace(globalCfg.PanSou.BaseURL) != ""
	if panSouConfigured {
		body, err := json.Marshal(globalCfg.PanSou)
		if err != nil {
			return candidate, false
		}
		panSouSub := *sub
		panSouSub.SourceType = model.SubscriptionSourcePanSou
		panSouSub.SourceConfig = string(body)
		items, hash, itemAdded, itemChanged, itemTransferred, runErr := runPanSouForHDHiveSubscription(ctx, &panSouSub, transfer)
		if runErr != nil {
			ready = false
			if *firstErr == nil {
				*firstErr = runErr
			}
		} else {
			candidate = candidate || subscriptionRunHasCandidate(items)
			if hash != "" {
				*hashes = append(*hashes, "pansou:"+hash)
			}
		}
		*saved = append(*saved, items...)
		*added += itemAdded
		*changed += itemChanged
		*transferred += itemTransferred
	}
	return candidate, ready
}

func subscriptionRunHasCandidate(items []model.SubscriptionItem) bool {
	for _, item := range items {
		if strings.TrimSpace(item.SourcePath) != "" {
			return true
		}
		if item.Status == model.SubscriptionItemStatusPending || item.Status == model.SubscriptionItemStatusTransferring || item.Status == model.SubscriptionItemStatusTransferred {
			return true
		}
	}
	return false
}

func firstHDHiveUnlockPoints(values ...*int) *int {
	for _, value := range values {
		if value != nil {
			copy := *value
			return &copy
		}
	}
	return nil
}

func bindHDHiveShare(sub *model.Subscription, resourceURL string, resourceRef hdhive.ResourceRef, rawShare, accessCode string, resource hdhive.Resource, details hdhive.ResourceDetails, requiresUnlock bool, boundAt time.Time) {
	if sub == nil || sourceProviderFromURL(rawShare) == "" {
		return
	}
	sub.BoundShare = &model.SubscriptionBoundShare{
		SourceType:     model.SubscriptionSourceHDHive,
		Provider:       sourceProviderFromURL(rawShare),
		ShareURL:       rawShare,
		AccessCode:     accessCode,
		ResourceURL:    resourceURL,
		ResourceSlug:   resourceRef.Slug,
		RequiresUnlock: requiresUnlock,
		UnlockPoints:   firstHDHiveUnlockPoints(details.UnlockPoints, resource.UnlockPoints),
		BoundAt:        boundAt,
	}
}

func hdhiveSubscriptionHash(sub *model.Subscription, hashes, links []string) string {
	if len(hashes) == 0 && len(links) == 0 {
		return sub.LastTreeHash
	}
	parts := append(append([]string{}, hashes...), links...)
	return combinedHash("hdhive", parts)
}

func runHDHiveCluster(ctx context.Context, sub *model.Subscription) ([]model.SubscriptionItem, string, int, int, int, error) {
	sourceCfg, err := parseHDHiveSourceConfig(sub.SourceConfig)
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	globalCfg, err := GetConfig()
	if err != nil {
		return nil, sub.LastTreeHash, 0, 0, 0, err
	}
	now := time.Now()
	var saved []model.SubscriptionItem
	var hashes []string
	var links []string
	dispatched := 0
	regularCandidate, regularReady := false, true
	var firstErr error

	if sub.BoundShare != nil && strings.TrimSpace(sub.BoundShare.ShareURL) != "" {
		rawShare := NormalizeSubscriptionShareURL(sub.BoundShare.ShareURL, sub.BoundShare.AccessCode)
		ref, parseErr := ParseShareURL(rawShare)
		if parseErr != nil {
			firstErr = parseErr
		} else if _, dispatchErr := dispatchClusterInspectObservation(ctx, sub, ref, clusterSourceMessage{ID: "bound:" + shortHash(rawShare), Text: rawShare}, "hdhive-bound:"+shortHash(rawShare), 1); dispatchErr != nil {
			firstErr = dispatchErr
		} else {
			dispatched++
			regularCandidate = true
			links = append(links, rawShare)
		}
	}

	if hasTelegramSearchCommand(globalCfg.Telegram) || hasBuiltinTelegramConfig(globalCfg.Telegram) {
		telegramCfg := globalCfg.Telegram
		telegramCfg.HDHive.Enabled = false
		body, marshalErr := json.Marshal(telegramCfg)
		if marshalErr != nil {
			return saved, hdhiveSubscriptionHash(sub, hashes, links), 0, 0, dispatched, marshalErr
		}
		child := *sub
		child.SourceType = model.SubscriptionSourceTelegram
		child.SourceConfig = string(body)
		_, hash, _, _, sourceDispatched, runErr := runTelegramCluster(ctx, &child)
		if runErr != nil {
			regularReady = false
			firstErr = firstNonNilError(firstErr, runErr)
		} else {
			regularCandidate = regularCandidate || sourceDispatched > 0
			if hash != "" {
				hashes = append(hashes, "telegram:"+hash)
			}
		}
		dispatched += sourceDispatched
		sub.LastCursor = child.LastCursor
	}

	panSouConfigured := len(globalCfg.PanSou.SearchCommand) > 0 && strings.TrimSpace(globalCfg.PanSou.SearchCommand[0]) != "" || strings.TrimSpace(globalCfg.PanSou.BaseURL) != ""
	if panSouConfigured {
		body, marshalErr := json.Marshal(globalCfg.PanSou)
		if marshalErr != nil {
			return saved, hdhiveSubscriptionHash(sub, hashes, links), 0, 0, dispatched, marshalErr
		}
		child := *sub
		child.SourceType = model.SubscriptionSourcePanSou
		child.SourceConfig = string(body)
		_, hash, _, _, sourceDispatched, runErr := runPanSouCluster(ctx, &child)
		if runErr != nil {
			regularReady = false
			firstErr = firstNonNilError(firstErr, runErr)
		} else {
			regularCandidate = regularCandidate || sourceDispatched > 0
			if hash != "" {
				hashes = append(hashes, "pansou:"+hash)
			}
		}
		dispatched += sourceDispatched
	}

	client, err := newHDHiveSubscriptionClient(globalCfg.Telegram.HDHive)
	if err != nil {
		return saved, hdhiveSubscriptionHash(sub, hashes, links), 0, 0, dispatched, firstNonNilError(firstErr, err)
	}
	resources, err := client.Search(ctx, strings.ToLower(strings.TrimSpace(sub.MediaType)), sub.TMDBID)
	if err != nil {
		return saved, hdhiveSubscriptionHash(sub, hashes, links), 0, 0, dispatched, firstNonNilError(firstErr, err)
	}
	resources = filterHDHiveResources(resources, sourceCfg.CloudType)
	if sourceCfg.Limit > 0 && len(resources) > sourceCfg.Limit {
		resources = resources[:sourceCfg.Limit]
	}
	for _, resource := range resources {
		resourceURL := strings.TrimSpace(resource.ResourceURL)
		if resourceURL == "" {
			resourceURL = defaultHDHiveResourceURL(resource)
		}
		resourceRef, ok := hdhive.ResourceRefFromURL(resourceURL, resource.PanType)
		if !ok {
			continue
		}
		resourceURL = resourceRef.URL
		details, shareErr := client.Share(ctx, resourceRef.Slug)
		if shareErr != nil {
			firstErr = firstNonNilError(firstErr, shareErr)
			continue
		}
		shareURL := strings.TrimSpace(details.FullURL)
		accessCode := strings.TrimSpace(details.AccessCode)
		requiresUnlock := false
		if shareURL == "" {
			points := firstHDHiveUnlockPoints(details.UnlockPoints, resource.UnlockPoints)
			free := details.IsFreeForUser || points != nil && *points == 0
			if !free {
				requiresUnlock = true
				if sub.BoundShare != nil && sub.BoundShare.RequiresUnlock && sub.BoundShare.ResourceSlug != resourceRef.Slug {
					continue
				}
				if !regularReady || regularCandidate {
					continue
				}
				if points == nil {
					continue
				}
				if globalCfg.Telegram.HDHive.MaxUnlockPoints > 0 && *points > globalCfg.Telegram.HDHive.MaxUnlockPoints {
					continue
				}
			}
			unlock, unlockErr := unlockHDHiveResourceForSubscription(ctx, resourceURL, globalCfg.Telegram.HDHive)
			if unlockErr != nil {
				firstErr = firstNonNilError(firstErr, unlockErr)
				continue
			}
			shareURL = strings.TrimSpace(unlock.URL)
			accessCode = firstNonEmpty(unlock.AccessCode, accessCode)
		}
		if shareURL == "" {
			continue
		}
		rawShare := NormalizeSubscriptionShareURL(shareURL, accessCode)
		ref, parseErr := ParseShareURL(rawShare)
		if parseErr != nil {
			continue
		}
		if _, dispatchErr := dispatchClusterInspectObservation(ctx, sub, ref, clusterSourceMessage{ID: "hdhive:" + resourceRef.Slug, Text: rawShare}, "hdhive:"+resourceRef.Slug, 1); dispatchErr != nil {
			firstErr = firstNonNilError(firstErr, dispatchErr)
			continue
		}
		dispatched++
		links = append(links, rawShare)
		bindHDHiveShare(sub, resourceURL, resourceRef, rawShare, accessCode, resource, details, requiresUnlock, now)
	}

	return saved, hdhiveSubscriptionHash(sub, hashes, links), 0, 0, dispatched, firstErr
}

func firstNonNilError(first, next error) error {
	if first != nil {
		return first
	}
	return next
}

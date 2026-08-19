package subscription

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/hdhive"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type hdhiveSubscriptionClient interface {
	Search(context.Context, string, int64) ([]hdhive.Resource, error)
	Share(context.Context, string) (hdhive.ResourceDetails, error)
}

type hdhiveSubscriptionResource struct {
	resource    hdhive.Resource
	resourceURL string
	resourceRef hdhive.ResourceRef
	details     hdhive.ResourceDetails
	points      *int
	free        bool
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

func loadHDHiveSubscriptionResources(ctx context.Context, client hdhiveSubscriptionClient, sourceCfg model.SubscriptionHDHiveSourceConfig, mediaType string, tmdbID int64) ([]hdhiveSubscriptionResource, error) {
	resources, err := client.Search(ctx, strings.ToLower(strings.TrimSpace(mediaType)), tmdbID)
	if err != nil {
		return nil, err
	}
	resources = filterHDHiveResources(resources, sourceCfg.CloudType)
	if sourceCfg.Limit > 0 && len(resources) > sourceCfg.Limit {
		resources = resources[:sourceCfg.Limit]
	}

	loaded := make([]hdhiveSubscriptionResource, 0, len(resources))
	var firstErr error
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
		points := firstHDHiveUnlockPoints(details.UnlockPoints, resource.UnlockPoints)
		loaded = append(loaded, hdhiveSubscriptionResource{
			resource:    resource,
			resourceURL: resourceURL,
			resourceRef: resourceRef,
			details:     details,
			points:      points,
			free:        details.IsFreeForUser || points != nil && *points == 0,
		})
	}
	return loaded, firstErr
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
			// If the bound share is permanently invalid (cancelled, expired,
			// removed, banned), clear it so the next run re-searches HDHive
			// or Telegram for a fresh share link instead of retrying the dead
			// one forever.
			if isShareSourceInvalidError(err) {
				log.Warnf("subscription %d bound share %s is invalid (%v); clearing for re-search", sub.ID, rawShare, err)
				sub.BoundShare = nil
				if updateErr := db.UpdateSubscription(sub); updateErr != nil && firstErr == nil {
					firstErr = updateErr
				}
			}
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

	var resources []hdhiveSubscriptionResource
	if client, clientErr := newHDHiveSubscriptionClient(globalCfg.Telegram.HDHive); clientErr != nil {
		firstErr = firstNonNilError(firstErr, clientErr)
	} else {
		loaded, loadErr := loadHDHiveSubscriptionResources(ctx, client, sourceCfg, sub.MediaType, sub.TMDBID)
		resources = loaded
		if isHDHiveRateLimitError(loadErr) {
			log.WithError(loadErr).Warn("subscription: HDHive rate limit isolated; regular sources will continue")
		} else {
			firstErr = firstNonNilError(firstErr, loadErr)
		}
	}

	freeCandidate := false
	for _, resource := range resources {
		if !resource.free && strings.TrimSpace(resource.details.FullURL) == "" {
			continue
		}
		matched, processErr := processHDHiveSubscriptionResource(ctx, sub, globalCfg, transfer, now, resource, false, true, false, &saved, &hashes, &links, &added, &changed, &transferred)
		firstErr = firstNonNilError(firstErr, processErr)
		freeCandidate = freeCandidate || matched
	}

	regularCandidate, regularReady := runHDHiveRegularSources(ctx, sub, globalCfg, transfer, &saved, &hashes, &added, &changed, &transferred, &firstErr)
	candidateAvailable := boundCandidate || freeCandidate || regularCandidate
	for _, resource := range resources {
		if resource.free || strings.TrimSpace(resource.details.FullURL) != "" {
			continue
		}
		matched, processErr := processHDHiveSubscriptionResource(ctx, sub, globalCfg, transfer, now, resource, true, regularReady, candidateAvailable, &saved, &hashes, &links, &added, &changed, &transferred)
		firstErr = firstNonNilError(firstErr, processErr)
		candidateAvailable = candidateAvailable || matched
	}

	return saved, hdhiveSubscriptionHash(sub, hashes, links), added, changed, transferred, isolateHDHiveRateLimitError(firstErr)
}

func processHDHiveSubscriptionResource(ctx context.Context, sub *model.Subscription, globalCfg model.SubscriptionConfig, transfer bool, now time.Time, resource hdhiveSubscriptionResource, allowPaid, regularReady, candidateAvailable bool, saved *[]model.SubscriptionItem, hashes, links *[]string, added, changed, transferred *int) (bool, error) {
	shareURL := strings.TrimSpace(resource.details.FullURL)
	accessCode := strings.TrimSpace(resource.details.AccessCode)
	requiresUnlock := false
	if shareURL == "" {
		if !resource.free {
			if !allowPaid {
				return false, nil
			}
			requiresUnlock = true
			if sub.BoundShare != nil && sub.BoundShare.RequiresUnlock && sub.BoundShare.ResourceSlug != resource.resourceRef.Slug {
				return false, nil
			}
			if !regularReady || candidateAvailable {
				return false, nil
			}
			if resource.points == nil {
				return false, nil
			}
			if globalCfg.Telegram.HDHive.MaxUnlockPoints > 0 && *resource.points > globalCfg.Telegram.HDHive.MaxUnlockPoints {
				return false, nil
			}
		}
		unlock, unlockErr := unlockHDHiveResourceForSubscription(ctx, resource.resourceURL, globalCfg.Telegram.HDHive)
		if unlockErr != nil {
			return false, unlockErr
		}
		shareURL = strings.TrimSpace(unlock.URL)
		accessCode = firstNonEmpty(unlock.AccessCode, accessCode)
	}
	if shareURL == "" {
		return false, nil
	}
	rawShare := NormalizeSubscriptionShareURL(shareURL, accessCode)
	if requiresUnlock {
		bindHDHiveShare(sub, resource.resourceURL, resource.resourceRef, rawShare, accessCode, resource.resource, resource.details, true, now)
	}
	_, candidates, handled, inspectErr := inspectShareLinkCandidatesFn(ctx, sub, globalCfg.Telegram, rawShare, now)
	if inspectErr != nil {
		return false, inspectErr
	}
	if !handled {
		return false, nil
	}
	selected := selectShareTransferCandidates(sub, candidates, globalCfg.Telegram.TransferPriority)
	items, hash, itemAdded, itemChanged, itemTransferred, transferErr := transferSelectedShareCandidatesForSubscription(ctx, sub, selected, transfer, now, "hdhive:"+resource.resourceURL)
	*saved = append(*saved, items...)
	*added += itemAdded
	*changed += itemChanged
	*transferred += itemTransferred
	if hash != "" {
		*hashes = append(*hashes, hash)
	}
	*links = append(*links, rawShare)
	matched := len(candidates) > 0
	if transferErr != nil {
		return matched, transferErr
	}
	if matched {
		bindHDHiveShare(sub, resource.resourceURL, resource.resourceRef, rawShare, accessCode, resource.resource, resource.details, requiresUnlock, now)
	}
	return matched, nil
}

func runHDHiveRegularSources(ctx context.Context, sub *model.Subscription, globalCfg model.SubscriptionConfig, transfer bool, saved *[]model.SubscriptionItem, hashes *[]string, added, changed, transferred *int, firstErr *error) (candidate, ready bool) {
	ready = true
	if hasTelegramSearchCommand(globalCfg.Telegram) || hasBuiltinTelegramConfig(globalCfg.Telegram) {
		telegramCfg := globalCfg.Telegram
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
	ctx = ensureClusterObservationRunContext(ctx)
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

	var resources []hdhiveSubscriptionResource
	if client, clientErr := newHDHiveSubscriptionClient(globalCfg.Telegram.HDHive); clientErr != nil {
		firstErr = firstNonNilError(firstErr, clientErr)
	} else {
		resources, err = loadHDHiveSubscriptionResources(ctx, client, sourceCfg, sub.MediaType, sub.TMDBID)
		if isHDHiveRateLimitError(err) {
			log.WithError(err).Warn("subscription: HDHive rate limit isolated; regular cluster sources will continue")
		} else {
			firstErr = firstNonNilError(firstErr, err)
		}
	}

	freeCandidate := false
	for _, resource := range resources {
		if !resource.free && strings.TrimSpace(resource.details.FullURL) == "" {
			continue
		}
		matched, processErr := processHDHiveClusterResource(ctx, sub, globalCfg, now, resource, false, true, false, &links, &dispatched)
		firstErr = firstNonNilError(firstErr, processErr)
		freeCandidate = freeCandidate || matched
	}

	if hasTelegramSearchCommand(globalCfg.Telegram) || hasBuiltinTelegramConfig(globalCfg.Telegram) {
		telegramCfg := globalCfg.Telegram
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

	candidateAvailable := regularCandidate || freeCandidate
	for _, resource := range resources {
		if resource.free || strings.TrimSpace(resource.details.FullURL) != "" {
			continue
		}
		matched, processErr := processHDHiveClusterResource(ctx, sub, globalCfg, now, resource, true, regularReady, candidateAvailable, &links, &dispatched)
		firstErr = firstNonNilError(firstErr, processErr)
		candidateAvailable = candidateAvailable || matched
	}

	return saved, hdhiveSubscriptionHash(sub, hashes, links), 0, 0, dispatched, isolateHDHiveRateLimitError(firstErr)
}

func processHDHiveClusterResource(ctx context.Context, sub *model.Subscription, globalCfg model.SubscriptionConfig, now time.Time, resource hdhiveSubscriptionResource, allowPaid, regularReady, candidateAvailable bool, links *[]string, dispatched *int) (bool, error) {
	shareURL := strings.TrimSpace(resource.details.FullURL)
	accessCode := strings.TrimSpace(resource.details.AccessCode)
	requiresUnlock := false
	if shareURL == "" {
		if !resource.free {
			if !allowPaid {
				return false, nil
			}
			requiresUnlock = true
			if sub.BoundShare != nil && sub.BoundShare.RequiresUnlock && sub.BoundShare.ResourceSlug != resource.resourceRef.Slug {
				return false, nil
			}
			if !regularReady || candidateAvailable {
				return false, nil
			}
			if resource.points == nil {
				return false, nil
			}
			if globalCfg.Telegram.HDHive.MaxUnlockPoints > 0 && *resource.points > globalCfg.Telegram.HDHive.MaxUnlockPoints {
				return false, nil
			}
		}
		unlock, unlockErr := unlockHDHiveResourceForSubscription(ctx, resource.resourceURL, globalCfg.Telegram.HDHive)
		if unlockErr != nil {
			return false, unlockErr
		}
		shareURL = strings.TrimSpace(unlock.URL)
		accessCode = firstNonEmpty(unlock.AccessCode, accessCode)
	}
	if shareURL == "" {
		return false, nil
	}
	rawShare := NormalizeSubscriptionShareURL(shareURL, accessCode)
	ref, parseErr := ParseShareURL(rawShare)
	if parseErr != nil {
		return false, nil
	}
	observationKey := clusterSingleObservationKey(ctx, sub.ID, "hdhive", rawShare, resource.resourceRef.Slug)
	if _, dispatchErr := dispatchClusterInspectObservation(ctx, sub, ref, clusterSourceMessage{ID: "hdhive:" + resource.resourceRef.Slug, Text: rawShare}, observationKey, 1); dispatchErr != nil {
		if strings.Contains(dispatchErr.Error(), "no compatible cluster worker is connected") {
			return false, nil
		}
		return false, dispatchErr
	}
	(*dispatched)++
	*links = append(*links, rawShare)
	bindHDHiveShare(sub, resource.resourceURL, resource.resourceRef, rawShare, accessCode, resource.resource, resource.details, requiresUnlock, now)
	return true, nil
}

func firstNonNilError(first, next error) error {
	if first == nil {
		return next
	}
	if next == nil {
		return first
	}
	if isHDHiveRateLimitError(first) && !isHDHiveRateLimitError(next) {
		return next
	}
	return first
}

func isolateHDHiveRateLimitError(err error) error {
	if !isHDHiveRateLimitError(err) {
		return err
	}
	log.WithError(err).Warn("subscription: HDHive rate limit isolated from subscription result")
	return nil
}

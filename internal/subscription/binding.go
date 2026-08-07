package subscription

import (
	"context"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/hdhive"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
)

func BindSubscriptionResource(ctx context.Context, req model.SubscriptionResourceBindReq) (*model.Subscription, error) {
	if req.SubscriptionID == 0 {
		return nil, errors.New("subscription_id is required")
	}
	if strings.TrimSpace(req.ShareURL) == "" {
		return nil, errors.New("share_url is required")
	}
	sub, err := db.GetSubscriptionByID(req.SubscriptionID)
	if err != nil {
		return nil, err
	}
	shareURL := NormalizeSubscriptionShareURL(req.ShareURL, req.AccessCode)
	ref, err := ParseShareURL(shareURL)
	if err != nil {
		return nil, err
	}
	provider := strings.ToLower(strings.TrimSpace(req.Provider))
	if provider == "" {
		provider = string(ref.Provider)
	}
	if provider != string(ref.Provider) {
		return nil, errors.Errorf("provider %q does not match share URL provider %q", provider, ref.Provider)
	}

	resourceSlug := ""
	if resourceURL := strings.TrimSpace(req.ResourceURL); resourceURL != "" {
		resourceRef, ok := hdhive.ResourceRefFromURL(resourceURL, "189")
		if !ok {
			return nil, errors.New("resource_url is not a valid HDHive resource URL")
		}
		resourceSlug = resourceRef.Slug
	}
	accessCode := strings.TrimSpace(req.AccessCode)
	if accessCode == "" {
		accessCode = ref.Passcode
	}
	sourceType := strings.ToLower(strings.TrimSpace(req.SourceType))
	if sourceType == "" {
		sourceType = model.SubscriptionSourceManual
	}
	sub.BoundShare = &model.SubscriptionBoundShare{
		SourceType:     sourceType,
		Provider:       provider,
		ShareURL:       shareURL,
		AccessCode:     accessCode,
		ResourceURL:    strings.TrimSpace(req.ResourceURL),
		ResourceSlug:   resourceSlug,
		RequiresUnlock: req.RequiresUnlock,
		UnlockPoints:   req.UnlockPoints,
		BoundAt:        time.Now(),
	}
	if err := db.UpdateSubscription(sub); err != nil {
		return nil, err
	}
	return sub, nil
}

func UnbindSubscriptionResource(ctx context.Context, req model.SubscriptionResourceUnbindReq) (*model.Subscription, error) {
	if req.SubscriptionID == 0 {
		return nil, errors.New("subscription_id is required")
	}
	sub, err := db.GetSubscriptionByID(req.SubscriptionID)
	if err != nil {
		return nil, err
	}
	sub.BoundShare = nil
	if err := db.UpdateSubscription(sub); err != nil {
		return nil, err
	}
	return sub, nil
}

func NormalizeSubscriptionShareURL(link, accessCode string) string {
	link = strings.TrimSpace(link)
	accessCode = strings.TrimSpace(accessCode)
	if link == "" || accessCode == "" || strings.Contains(link, ",") || isPan123FastLink(link) {
		return link
	}
	return link + "," + accessCode
}

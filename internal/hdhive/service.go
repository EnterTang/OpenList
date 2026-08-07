package hdhive

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Enabled         bool
	MaxUnlockPoints int
}

type ResolvedItem struct {
	ResourceRef
	Success      bool
	FullURL      string
	AccessCode   string
	AlreadyOwned bool
	PointsSpent  *int
	FromCache    bool
}

type Failure struct {
	ResourceRef
	ErrorCode string
	Message   string
}

type Skipped struct {
	ResourceRef
	Reason          string
	UnlockPoints    int
	MaxUnlockPoints int
}

type Result struct {
	CloudLinks []string
	Items      []ResolvedItem
	Failures   []Failure
	Skipped    []Skipped
	Authorized bool
	AuthMode   string
}

type cachedUnlock struct {
	FullURL    string
	AccessCode string
}

type Service struct {
	client Client

	mu       sync.Mutex
	cache    map[string]cachedUnlock
	resolveM sync.Mutex
}

func NewService(client Client) *Service {
	return &Service{client: client, cache: make(map[string]cachedUnlock)}
}

func (s *Service) Resolve(ctx context.Context, refs []ResourceRef, cfg Config) (Result, error) {
	result := Result{AuthMode: "symedia"}
	if !cfg.Enabled {
		for _, ref := range uniqueRefs(refs) {
			result.Skipped = append(result.Skipped, Skipped{ResourceRef: ref, Reason: "disabled"})
		}
		return result, nil
	}
	if s == nil || s.client == nil {
		for _, ref := range uniqueRefs(refs) {
			result.Skipped = append(result.Skipped, Skipped{ResourceRef: ref, Reason: "not_configured"})
		}
		return result, nil
	}
	refs = uniqueRefs(refs)
	if len(refs) == 0 {
		return result, nil
	}

	// A single resolver lock prevents two subscription workers from both
	// observing an unpaid resource and charging it concurrently.
	s.resolveM.Lock()
	defer s.resolveM.Unlock()

	pending := make([]ResourceRef, 0, len(refs))
	for _, ref := range refs {
		key := ref.SiteID + ":" + ref.Slug
		if cached, ok := s.cached(key); ok {
			appendResolved(&result, ref, UnlockResult{FullURL: cached.FullURL, AccessCode: cached.AccessCode}, true)
			continue
		}
		pending = append(pending, ref)
	}
	if len(pending) == 0 {
		return result, nil
	}

	status, err := s.client.Status(ctx)
	if err != nil {
		for _, ref := range pending {
			result.Failures = append(result.Failures, failureFor(ref, err, "HDHIVE_SYMEDIA_AUTH_FAILED"))
		}
		return result, nil
	}
	if !status.Authorized {
		err := &Error{Code: "HDHIVE_SYMEDIA_AUTH_FAILED", Message: "Symedia HDHive account is not authorized"}
		for _, ref := range pending {
			result.Failures = append(result.Failures, failureFor(ref, err, "HDHIVE_SYMEDIA_AUTH_FAILED"))
		}
		return result, nil
	}
	result.Authorized = true
	for _, ref := range pending {
		key := ref.SiteID + ":" + ref.Slug
		details, err := s.client.Share(ctx, ref.Slug)
		if err != nil {
			result.Failures = append(result.Failures, failureFor(ref, err, "HDHIVE_SYMEDIA_SHARE_FAILED"))
			continue
		}
		// Symedia may return the existing URL without an explicit ownership
		// flag. A non-empty URL is authoritative and must not trigger a paid
		// unlock request.
		if details.FullURL != "" {
			resolved := UnlockResult{FullURL: details.FullURL, AccessCode: details.AccessCode, AlreadyOwned: true}
			s.store(key, resolved)
			appendResolved(&result, ref, resolved, false)
			continue
		}
		if cfg.MaxUnlockPoints > 0 {
			if details.UnlockPoints == nil {
				result.Failures = append(result.Failures, Failure{ResourceRef: ref, ErrorCode: "HDHIVE_UNLOCK_POINTS_UNAVAILABLE", Message: "unlock points could not be confirmed"})
				continue
			}
			if *details.UnlockPoints > cfg.MaxUnlockPoints {
				result.Skipped = append(result.Skipped, Skipped{ResourceRef: ref, Reason: "unlock_points_exceeded", UnlockPoints: *details.UnlockPoints, MaxUnlockPoints: cfg.MaxUnlockPoints})
				continue
			}
		}

		unlocked, err := s.unlockWithRetry(ctx, ref.Slug)
		if err != nil {
			result.Failures = append(result.Failures, failureFor(ref, err, "HDHIVE_SYMEDIA_UNLOCK_FAILED"))
			continue
		}
		if unlocked.AccessCode == "" {
			unlocked.AccessCode = details.AccessCode
		}
		s.store(key, unlocked)
		appendResolved(&result, ref, unlocked, false)
	}
	return result, nil
}

func (s *Service) unlockWithRetry(ctx context.Context, slug string) (UnlockResult, error) {
	for attempt := 0; attempt < 2; attempt++ {
		result, err := s.client.Unlock(ctx, slug)
		if err == nil {
			return result, nil
		}
		proxyErr, ok := err.(*Error)
		if !ok || proxyErr.Code != "HDHIVE_SYMEDIA_RATE_LIMITED" || proxyErr.RetryAfter <= 0 || attempt == 1 {
			return UnlockResult{}, err
		}
		wait := proxyErr.RetryAfter
		if wait > 120*time.Second {
			wait = 120 * time.Second
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return UnlockResult{}, ctx.Err()
		case <-timer.C:
		}
	}
	return UnlockResult{}, fmt.Errorf("HDHive unlock retry exhausted")
}

func appendResolved(result *Result, ref ResourceRef, unlocked UnlockResult, fromCache bool) {
	result.CloudLinks = appendUnique(result.CloudLinks, unlocked.FullURL)
	result.Items = append(result.Items, ResolvedItem{
		ResourceRef:  ref,
		Success:      true,
		FullURL:      unlocked.FullURL,
		AccessCode:   unlocked.AccessCode,
		AlreadyOwned: unlocked.AlreadyOwned,
		PointsSpent:  unlocked.PointsSpent,
		FromCache:    fromCache,
	})
}

func failureFor(ref ResourceRef, err error, fallback string) Failure {
	code := fallback
	message := "HDHive unlock failed"
	if proxyErr, ok := err.(*Error); ok {
		if proxyErr.Code != "" {
			code = proxyErr.Code
		}
		if proxyErr.Message != "" {
			message = proxyErr.Message
		}
	} else if err != nil && strings.TrimSpace(err.Error()) != "" {
		message = err.Error()
	}
	return Failure{ResourceRef: ref, ErrorCode: code, Message: message}
}

func (s *Service) cached(key string) (cachedUnlock, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.cache[key]
	return value, ok
}

func (s *Service) store(key string, result UnlockResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache[key] = cachedUnlock{FullURL: result.FullURL, AccessCode: result.AccessCode}
}

func uniqueRefs(refs []ResourceRef) []ResourceRef {
	result := make([]ResourceRef, 0, len(refs))
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		ref.Slug = normalizeSlug(ref.Slug)
		ref.SiteID = normalizeSiteID(ref.SiteID, defaultSiteID)
		if ref.Slug == "" {
			continue
		}
		if ref.URL == "" {
			ref.URL = "https://hdhive.com/resource/" + ref.SiteID + "/" + ref.Slug
		}
		key := ref.SiteID + ":" + ref.Slug
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, ref)
	}
	return result
}

func appendUnique(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

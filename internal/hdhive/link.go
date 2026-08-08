package hdhive

import (
	"regexp"
	"strings"
)

const defaultSiteID = "189"

var resourcePattern = regexp.MustCompile(`(?i)https?://(?:www\.)?hdhive\.com/resource/(?:([a-z0-9-]+)/)?([a-f0-9-]{32,36})`)

// ResourceRef is the stable HDHive resource identity used by the unlock API.
// SiteID is kept separate from Slug because HDHive can host resources for
// multiple cloud providers, including the 115 resources used by Telegram.
type ResourceRef struct {
	SiteID string
	Slug   string
	URL    string
}

func ResourceRefFromURL(raw, fallbackSiteID string) (ResourceRef, bool) {
	match := resourcePattern.FindStringSubmatch(strings.TrimSpace(raw))
	if len(match) != 3 {
		return ResourceRef{}, false
	}
	siteID := normalizeSiteID(match[1], fallbackSiteID)
	slug := normalizeSlug(match[2])
	if slug == "" {
		return ResourceRef{}, false
	}
	return ResourceRef{SiteID: siteID, Slug: slug, URL: "https://hdhive.com/resource/" + siteID + "/" + slug}, true
}

// ExtractResourceRefs scans both free text and URL-bearing Telegram entities
// or buttons. Duplicates are removed by site ID plus normalized slug.
func ExtractResourceRefs(text string, urls []string, fallbackSiteID string) []ResourceRef {
	refs := make([]ResourceRef, 0)
	seen := make(map[string]struct{})
	appendURL := func(raw string) {
		ref, ok := ResourceRefFromURL(raw, fallbackSiteID)
		if !ok {
			return
		}
		key := ref.SiteID + ":" + ref.Slug
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		refs = append(refs, ref)
	}
	for _, match := range resourcePattern.FindAllString(text, -1) {
		appendURL(match)
	}
	for _, raw := range urls {
		appendURL(raw)
	}
	return refs
}

func normalizeSlug(raw string) string {
	value := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(raw), "-", ""))
	if len(value) != 32 {
		return ""
	}
	for _, r := range value {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return ""
		}
	}
	return value
}

func normalizeSiteID(raw, fallback string) string {
	if value := normalizeSiteIDValue(raw); value != "" {
		return value
	}
	if value := normalizeSiteIDValue(fallback); value != "" {
		return value
	}
	return defaultSiteID
}

func normalizeSiteIDValue(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	for _, r := range value {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || r == '-') {
			return ""
		}
	}
	return value
}

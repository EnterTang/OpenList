package static

import (
	"net/url"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

type SiteConfig struct {
	BasePath string
	Cdn      string
}

func getSiteConfig() SiteConfig {
	basePath := conf.URL.Path
	if basePath != "" {
		basePath = utils.FixAndCleanPath(basePath)
		// Keep consistent with frontend: trim trailing slash unless it's root
		if basePath != "/" && strings.HasSuffix(basePath, "/") {
			basePath = strings.TrimSuffix(basePath, "/")
		}
	}
	if basePath == "" {
		basePath = "/"
	}

	siteConfig := SiteConfig{
		BasePath: basePath,
		Cdn:      normalizeDynamicBase(conf.Conf.Cdn, basePath),
	}
	return siteConfig
}

func normalizeDynamicBase(rawCdn string, basePath string) string {
	fallback := strings.TrimSuffix(basePath, "/")
	cdn := strings.TrimSpace(strings.ReplaceAll(rawCdn, "$version", strings.TrimPrefix(conf.WebVersion, "v")))
	if cdn == "" {
		return fallback
	}

	cdn = strings.TrimRight(cdn, "/")
	if cdn == "" {
		return fallback
	}

	parsed, err := url.Parse(cdn)
	if err != nil {
		return fallback
	}
	if parsed.Scheme != "" && parsed.Host == "" {
		return fallback
	}
	if strings.HasPrefix(cdn, "//") && parsed.Host == "" {
		return fallback
	}
	if parsed.Scheme == "" && !strings.HasPrefix(cdn, "/") {
		return fallback
	}
	return cdn
}

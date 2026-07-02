package static

import (
	"net/url"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
)

func TestGetSiteConfigNormalizesDynamicBase(t *testing.T) {
	oldConf := conf.Conf
	oldURL := conf.URL
	oldWebVersion := conf.WebVersion
	t.Cleanup(func() {
		conf.Conf = oldConf
		conf.URL = oldURL
		conf.WebVersion = oldWebVersion
	})

	conf.WebVersion = "v4.2.3"
	tests := []struct {
		name     string
		siteURL  string
		cdn      string
		wantBase string
		wantCdn  string
	}{
		{
			name:     "root without CDN uses site root assets",
			siteURL:  "http://localhost:5244",
			wantBase: "/",
			wantCdn:  "",
		},
		{
			name:     "subpath without CDN uses subpath assets",
			siteURL:  "http://localhost:5244/openlist/",
			wantBase: "/openlist",
			wantCdn:  "/openlist",
		},
		{
			name:     "invalid scheme only CDN falls back",
			siteURL:  "http://localhost:5244",
			cdn:      "http://",
			wantBase: "/",
			wantCdn:  "",
		},
		{
			name:     "invalid missing host CDN falls back to subpath",
			siteURL:  "http://localhost:5244/openlist",
			cdn:      "https://",
			wantBase: "/openlist",
			wantCdn:  "/openlist",
		},
		{
			name:     "absolute CDN is trimmed and keeps version replacement",
			siteURL:  "http://localhost:5244",
			cdn:      "https://cdn.example.com/openlist/$version/",
			wantBase: "/",
			wantCdn:  "https://cdn.example.com/openlist/4.2.3",
		},
		{
			name:     "protocol relative CDN is allowed",
			siteURL:  "http://localhost:5244",
			cdn:      "//cdn.example.com/openlist/",
			wantBase: "/",
			wantCdn:  "//cdn.example.com/openlist",
		},
		{
			name:     "host without scheme is ignored",
			siteURL:  "http://localhost:5244",
			cdn:      "cdn.example.com/openlist",
			wantBase: "/",
			wantCdn:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := url.Parse(tt.siteURL)
			if err != nil {
				t.Fatal(err)
			}
			conf.URL = parsed
			conf.Conf = &conf.Config{Cdn: tt.cdn}

			got := getSiteConfig()
			if got.BasePath != tt.wantBase {
				t.Fatalf("BasePath = %q, want %q", got.BasePath, tt.wantBase)
			}
			if got.Cdn != tt.wantCdn {
				t.Fatalf("Cdn = %q, want %q", got.Cdn, tt.wantCdn)
			}
		})
	}
}

package subscription

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ProbePanSouStatus checks whether the configured PanSou base URL is reachable.
// An empty URL is treated as unconfigured (ok=false). Any completed HTTP response
// with status < 500 counts as reachable, because PanSou roots often return 404.
func ProbePanSouStatus(ctx context.Context, baseURL string) (ok bool, message string, latencyMs int64) {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return false, "base_url is empty", 0
	}
	u, err := url.Parse(baseURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false, "invalid base_url", 0
	}

	endpoint := strings.TrimRight(baseURL, "/") + "/"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false, err.Error(), 0
	}

	client := &http.Client{Timeout: 5 * time.Second}
	start := time.Now()
	resp, err := client.Do(req)
	latencyMs = time.Since(start).Milliseconds()
	if err != nil {
		return false, err.Error(), latencyMs
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		return false, fmt.Sprintf("status=%d", resp.StatusCode), latencyMs
	}
	return true, fmt.Sprintf("status=%d", resp.StatusCode), latencyMs
}

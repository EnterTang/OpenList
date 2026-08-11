package worker

import (
	"errors"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
	"github.com/stretchr/testify/require"
)

func TestDirectDownloadErrorsDoNotExposeProviderURL(t *testing.T) {
	err := classifyDirectShareProviderError(errors.New("request https://example.invalid/share?token=secret returned HTTP 503"))
	if !directDownloadCanFallback(err) {
		t.Fatal("transient provider error should be eligible for transfer fallback")
	}
	if strings.Contains(err.Error(), "example.invalid") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("direct error leaked provider URL or token: %v", err)
	}
}

func TestDirectDownloadProviderDataIsLimitedToNonSecretFields(t *testing.T) {
	data := subscription.SanitizeShareItemProviderData(map[string]any{
		"etag":         "etag-1",
		"s3key_flag":   "flag-1",
		"access_token": "secret",
		"share_pwd":    "secret",
	})
	require.Equal(t, subscription.ShareItemProviderData{"etag": "etag-1", "s3key_flag": "flag-1"}, data)
}

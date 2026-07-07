package subscription

import (
	"fmt"
	"net/url"
	"strings"
)

func DetectShareProvider(raw string) (ShareProviderName, bool) {
	cleaned, _ := splitShareURLPasscode(raw)
	parsed, err := url.Parse(cleaned)
	if err != nil {
		return "", false
	}
	host := strings.ToLower(parsed.Hostname())
	switch {
	case hostMatchesDomain(host, "pan.quark.cn"):
		return ShareProviderQuark, true
	case hostMatchesDomain(host, "alipan.com") || hostMatchesDomain(host, "aliyundrive.com"):
		return ShareProviderAliyunDrive, true
	case hostMatchesDomain(host, "123pan.com"):
		return ShareProviderPan123, true
	case hostMatchesDomain(host, "115cdn.com") || hostMatchesDomain(host, "115.com"):
		return ShareProviderPan115, true
	default:
		return "", false
	}
}

func ParseShareURL(raw string) (ShareRef, error) {
	cleaned, fallbackPasscode := splitShareURLPasscode(raw)
	provider, ok := DetectShareProvider(cleaned)
	if !ok {
		return ShareRef{}, fmt.Errorf("unsupported share URL: %s", cleaned)
	}
	parsed, err := url.Parse(cleaned)
	if err != nil {
		return ShareRef{}, fmt.Errorf("invalid share URL: %w", err)
	}
	shareID := shareIDFromPath(parsed.EscapedPath())
	if shareID == "" {
		return ShareRef{}, fmt.Errorf("share URL missing share ID: %s", cleaned)
	}
	ref := ShareRef{
		Provider: provider,
		RawURL:   cleaned,
		ShareID:  shareID,
	}
	query := parsed.Query()
	switch provider {
	case ShareProviderQuark, ShareProviderAliyunDrive:
		ref.Passcode = fallbackPasscode
	case ShareProviderPan123:
		ref.Passcode = firstNonEmpty(query.Get("pwd"), fallbackPasscode)
	case ShareProviderPan115:
		ref.Passcode = firstNonEmpty(query.Get("password"), query.Get("pwd"), fallbackPasscode)
	}
	return ref, nil
}

func splitShareURLPasscode(raw string) (string, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	urlPart, passcode, found := strings.Cut(raw, ",")
	if !found {
		return raw, ""
	}
	return strings.TrimSpace(urlPart), strings.TrimSpace(passcode)
}

func shareIDFromPath(escapedPath string) string {
	parts := strings.Split(strings.Trim(escapedPath, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] != "s" {
			continue
		}
		id, err := url.PathUnescape(parts[i+1])
		if err != nil {
			return ""
		}
		return strings.TrimSpace(id)
	}
	return ""
}

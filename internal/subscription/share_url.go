package subscription

import (
	"encoding/hex"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const pan123FastLinkPrefix = "123FSLinkV2$"

var pan123FastLinkPattern = regexp.MustCompile(`123FSLinkV2\$[A-Fa-f0-9]{32}#[0-9]+#[^\r\n]+`)

type pan123FastLinkFile struct {
	Etag string
	Size int64
	Name string
}

func DetectShareProvider(raw string) (ShareProviderName, bool) {
	if _, err := parsePan123FastLink(raw); err == nil {
		return ShareProviderPan123, true
	}
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
	if ref, err := parsePan123FastLink(raw); err == nil {
		return ref, nil
	} else if strings.HasPrefix(strings.TrimSpace(raw), pan123FastLinkPrefix) {
		return ShareRef{}, err
	}
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

func parsePan123FastLink(raw string) (ShareRef, error) {
	file, err := parsePan123FastLinkFile(raw)
	if err != nil {
		return ShareRef{}, err
	}
	return ShareRef{
		Provider: ShareProviderPan123,
		RawURL:   strings.TrimSpace(raw),
		ShareID:  file.Etag,
		ParentID: "0",
	}, nil
}

func parsePan123FastLinkFile(raw string) (pan123FastLinkFile, error) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, pan123FastLinkPrefix) {
		return pan123FastLinkFile{}, fmt.Errorf("unsupported share URL: %s", raw)
	}
	payload := strings.TrimPrefix(raw, pan123FastLinkPrefix)
	parts := strings.SplitN(payload, "#", 3)
	if len(parts) != 3 {
		return pan123FastLinkFile{}, fmt.Errorf("invalid 123 fastlink: %s", raw)
	}
	etag := strings.TrimSpace(parts[0])
	if len(etag) != 32 {
		return pan123FastLinkFile{}, fmt.Errorf("123 fastlink etag is invalid: %s", raw)
	}
	if _, err := hex.DecodeString(etag); err != nil {
		return pan123FastLinkFile{}, fmt.Errorf("123 fastlink etag is invalid: %s", raw)
	}
	size, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil || size < 0 {
		return pan123FastLinkFile{}, fmt.Errorf("123 fastlink size is invalid: %s", raw)
	}
	name := strings.TrimSpace(parts[2])
	if name == "" {
		return pan123FastLinkFile{}, fmt.Errorf("123 fastlink filename is empty: %s", raw)
	}
	return pan123FastLinkFile{
		Etag: etag,
		Size: size,
		Name: name,
	}, nil
}

func (f pan123FastLinkFile) shareItem(parentID string) ShareItem {
	return ShareItem{
		ID:       f.Etag,
		ParentID: firstNonEmpty(parentID, "0"),
		Name:     f.Name,
		Size:     f.Size,
		Raw: map[string]any{
			"etag":      f.Etag,
			"size":      f.Size,
			"file_name": f.Name,
			"type":      0,
		},
	}
}

func isPan123FastLink(raw string) bool {
	_, err := parsePan123FastLinkFile(raw)
	return err == nil
}

func extractPan123FastLinks(text string) []string {
	matches := pan123FastLinkPattern.FindAllString(text, -1)
	if len(matches) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	links := make([]string, 0, len(matches))
	for _, match := range matches {
		match = strings.TrimSpace(match)
		if match == "" || !isPan123FastLink(match) {
			continue
		}
		if _, ok := seen[match]; ok {
			continue
		}
		seen[match] = struct{}{}
		links = append(links, match)
	}
	return links
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

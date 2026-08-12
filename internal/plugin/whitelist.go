package plugin

import (
	"path"
	"strings"
)

var tempSuffixes = []string{".tmp", ".part", ".aria2", ".!qb", ".crdownload"}

func ParseWhitelist(raw string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		ext := strings.ToLower(strings.TrimSpace(part))
		ext = strings.TrimPrefix(ext, ".")
		if ext == "" {
			continue
		}
		out[ext] = struct{}{}
	}
	return out
}

func ExtensionAllowed(name string, whitelist map[string]struct{}) bool {
	if len(whitelist) == 0 {
		return false
	}
	ext := strings.ToLower(strings.TrimPrefix(path.Ext(name), "."))
	if ext == "" {
		return false
	}
	_, ok := whitelist[ext]
	return ok
}

func IsTempIncompleteName(name string) bool {
	lower := strings.ToLower(name)
	for _, suf := range tempSuffixes {
		if strings.HasSuffix(lower, suf) {
			return true
		}
	}
	return false
}

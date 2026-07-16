package bootstrap

import (
	"net/url"
	"strings"
)

func sqliteDSN(target string, options url.Values) string {
	databaseURL := parseSQLiteTarget(target)

	query := databaseURL.Query()
	for key, values := range options {
		for _, value := range values {
			query.Add(key, value)
		}
	}
	databaseURL.RawQuery = query.Encode()
	return databaseURL.String()
}

func parseSQLiteTarget(target string) *url.URL {
	targetPath, rawQuery, _ := strings.Cut(target, "?")
	if len(targetPath) >= len("file:") && strings.EqualFold(targetPath[:len("file:")], "file:") {
		if parsed, err := url.Parse(strings.ReplaceAll(targetPath, "#", "%23")); err == nil {
			parsed.RawQuery = rawQuery
			return parsed
		}
	}

	if isWindowsDrivePath(targetPath) {
		targetPath = "/" + strings.ReplaceAll(targetPath, `\`, "/")
	}
	return &url.URL{
		Scheme:   "file",
		Path:     targetPath,
		RawQuery: rawQuery,
	}
}

func isWindowsDrivePath(path string) bool {
	if len(path) < 3 || path[1] != ':' || (path[2] != '/' && path[2] != '\\') {
		return false
	}
	return (path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z')
}

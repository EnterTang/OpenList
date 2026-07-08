package subscription

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	stdpath "path"
	"regexp"
	"strconv"
	"strings"
)

const pan123CommonPathFastLinkPrefix = "123FLCPV2$"

var (
	pan123CommonPathFastLinkPattern = regexp.MustCompile(`123FLCPV2\$[^\r\n]+`)
	pan123ImportFastLinkPattern     = regexp.MustCompile(`123FSLinkV2\$[A-Za-z0-9]{22,32}#[0-9]+#[^\r\n]+`)
)

type pan123ImportedFile struct {
	Etag string
	Size int64
	Path string
	Name string
}

type ImportParseIssue struct {
	Input  string
	Reason string
}

func parseManualImportText(raw string) ([]pan123ImportedFile, []ImportParseIssue, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil, nil
	}
	if files, issues, err := parsePan123FastLinkJSON(raw); err == nil {
		return dedupeImportedFiles(files), issues, nil
	}
	var all []pan123ImportedFile
	var issues []ImportParseIssue
	for _, match := range extractPan123CommonPathFastLinks(raw) {
		files, parseIssues, err := parsePan123CommonPathFastLink(match)
		if err != nil {
			issues = append(issues, ImportParseIssue{Input: match, Reason: err.Error()})
			continue
		}
		all = append(all, files...)
		issues = append(issues, parseIssues...)
	}
	for _, match := range extractPan123ImportFastLinks(raw) {
		file, err := parsePan123SingleImport(match)
		if err != nil {
			issues = append(issues, ImportParseIssue{Input: match, Reason: err.Error()})
			continue
		}
		all = append(all, file)
	}
	all = dedupeImportedFiles(all)
	if len(all) == 0 {
		return nil, issues, fmt.Errorf("no supported pan123 fastlink imports found")
	}
	return all, issues, nil
}

func parsePan123CommonPathFastLink(raw string) ([]pan123ImportedFile, []ImportParseIssue, error) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, pan123CommonPathFastLinkPrefix) {
		return nil, nil, fmt.Errorf("invalid 123 common-path fastlink")
	}
	payload := strings.TrimPrefix(raw, pan123CommonPathFastLinkPrefix)
	commonPath, fileInfo, found := strings.Cut(payload, "%")
	if !found {
		return nil, nil, fmt.Errorf("123 common-path fastlink missing common path separator")
	}
	var files []pan123ImportedFile
	var issues []ImportParseIssue
	for _, record := range strings.Split(fileInfo, "$") {
		record = strings.TrimSpace(record)
		if record == "" {
			continue
		}
		parts := strings.SplitN(record, "#", 3)
		if len(parts) != 3 {
			issues = append(issues, ImportParseIssue{Input: record, Reason: "invalid fastlink record"})
			continue
		}
		file, err := buildImportedFile(parts[0], parts[1], commonPath, parts[2])
		if err != nil {
			issues = append(issues, ImportParseIssue{Input: record, Reason: err.Error()})
			continue
		}
		files = append(files, file)
	}
	if len(files) == 0 {
		return nil, issues, fmt.Errorf("123 common-path fastlink contained no valid files")
	}
	return dedupeImportedFiles(files), issues, nil
}

func parsePan123FastLinkJSON(raw string) ([]pan123ImportedFile, []ImportParseIssue, error) {
	decoder := json.NewDecoder(strings.NewReader(strings.TrimSpace(raw)))
	decoder.UseNumber()
	var payload any
	if err := decoder.Decode(&payload); err != nil {
		return nil, nil, err
	}
	var commonPath string
	usesBase62 := false
	var rawFiles []any
	switch value := payload.(type) {
	case []any:
		rawFiles = value
	case map[string]any:
		if v, ok := value["commonPath"].(string); ok {
			commonPath = v
		}
		if v, ok := value["usesBase62EtagsInExport"].(bool); ok {
			usesBase62 = v
		}
		filesValue, ok := value["files"]
		if !ok {
			return nil, nil, fmt.Errorf("123 fastlink JSON missing files")
		}
		list, ok := filesValue.([]any)
		if !ok {
			return nil, nil, fmt.Errorf("123 fastlink JSON files must be an array")
		}
		rawFiles = list
	default:
		return nil, nil, fmt.Errorf("unsupported 123 fastlink JSON payload")
	}
	var files []pan123ImportedFile
	var issues []ImportParseIssue
	for _, item := range rawFiles {
		entry, ok := item.(map[string]any)
		if !ok {
			issues = append(issues, ImportParseIssue{Input: fmt.Sprintf("%v", item), Reason: "JSON file entry must be an object"})
			continue
		}
		etag, ok := entry["etag"].(string)
		if !ok {
			issues = append(issues, ImportParseIssue{Input: fmt.Sprintf("%v", item), Reason: "JSON file entry missing etag"})
			continue
		}
		pathValue, ok := entry["path"].(string)
		if !ok {
			issues = append(issues, ImportParseIssue{Input: fmt.Sprintf("%v", item), Reason: "JSON file entry missing path"})
			continue
		}
		normalizedEtag, err := normalizeImportedEtagWithMode(etag, usesBase62)
		if err != nil {
			issues = append(issues, ImportParseIssue{Input: fmt.Sprintf("%v", item), Reason: err.Error()})
			continue
		}
		size, err := parseImportedSize(entry["size"])
		if err != nil {
			issues = append(issues, ImportParseIssue{Input: fmt.Sprintf("%v", item), Reason: err.Error()})
			continue
		}
		cleanPath, err := cleanImportedPath(commonPath, pathValue)
		if err != nil {
			issues = append(issues, ImportParseIssue{Input: fmt.Sprintf("%v", item), Reason: err.Error()})
			continue
		}
		files = append(files, pan123ImportedFile{
			Etag: normalizedEtag,
			Size: size,
			Path: cleanPath,
			Name: stdpath.Base(cleanPath),
		})
	}
	if len(files) == 0 {
		return nil, issues, fmt.Errorf("123 fastlink JSON contained no valid files")
	}
	return dedupeImportedFiles(files), issues, nil
}

func parsePan123SingleImport(raw string) (pan123ImportedFile, error) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, pan123FastLinkPrefix) {
		return pan123ImportedFile{}, fmt.Errorf("invalid 123 fastlink")
	}
	payload := strings.TrimPrefix(raw, pan123FastLinkPrefix)
	parts := strings.SplitN(payload, "#", 3)
	if len(parts) != 3 {
		return pan123ImportedFile{}, fmt.Errorf("invalid 123 fastlink")
	}
	return buildImportedFile(parts[0], parts[1], "", parts[2])
}

func buildImportedFile(rawEtag, rawSize, commonPath, rawPath string) (pan123ImportedFile, error) {
	etag, err := normalizeImportedEtag(rawEtag)
	if err != nil {
		return pan123ImportedFile{}, err
	}
	size, err := parseImportedSize(rawSize)
	if err != nil {
		return pan123ImportedFile{}, err
	}
	cleanPath, err := cleanImportedPath(commonPath, rawPath)
	if err != nil {
		return pan123ImportedFile{}, err
	}
	return pan123ImportedFile{
		Etag: etag,
		Size: size,
		Path: cleanPath,
		Name: stdpath.Base(cleanPath),
	}, nil
}

func cleanImportedPath(commonPath, relPath string) (string, error) {
	parts := make([]string, 0)
	for _, part := range []string{commonPath, relPath} {
		for _, segment := range strings.Split(strings.ReplaceAll(strings.TrimSpace(part), "\\", "/"), "/") {
			segment = strings.TrimSpace(segment)
			if segment == "" {
				continue
			}
			if segment == "." || segment == ".." {
				return "", fmt.Errorf("import path contains invalid path segment")
			}
			parts = append(parts, segment)
		}
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("import path is empty")
	}
	return strings.Join(parts, "/"), nil
}

func normalizeImportedEtag(raw string) (string, error) {
	return normalizeImportedEtagWithMode(raw, false)
}

func normalizeImportedEtagWithMode(raw string, forceBase62 bool) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("import etag is empty")
	}
	if !forceBase62 && len(raw) == 32 {
		if _, err := hex.DecodeString(raw); err == nil {
			return strings.ToLower(raw), nil
		}
	}
	if len(raw) != 22 {
		return "", fmt.Errorf("import etag is invalid")
	}
	hexValue, err := base62ToHex32(raw)
	if err != nil {
		return "", fmt.Errorf("import etag is invalid")
	}
	return hexValue, nil
}

func base62ToHex32(raw string) (string, error) {
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	value := big.NewInt(0)
	base := big.NewInt(62)
	for _, r := range raw {
		idx := strings.IndexRune(alphabet, r)
		if idx < 0 {
			return "", fmt.Errorf("invalid base62")
		}
		value.Mul(value, base)
		value.Add(value, big.NewInt(int64(idx)))
	}
	hexValue := value.Text(16)
	if len(hexValue)%2 != 0 {
		hexValue = "0" + hexValue
	}
	for len(hexValue) < 32 {
		hexValue = "0" + hexValue
	}
	if len(hexValue) > 32 {
		return "", fmt.Errorf("invalid base62")
	}
	return strings.ToLower(hexValue), nil
}

func parseImportedSize(raw any) (int64, error) {
	switch value := raw.(type) {
	case nil:
		return 0, fmt.Errorf("import size is missing")
	case int64:
		if value < 0 {
			return 0, fmt.Errorf("import size is invalid")
		}
		return value, nil
	case int:
		if value < 0 {
			return 0, fmt.Errorf("import size is invalid")
		}
		return int64(value), nil
	case float64:
		if value < 0 {
			return 0, fmt.Errorf("import size is invalid")
		}
		return int64(value), nil
	case json.Number:
		n, err := value.Int64()
		if err != nil || n < 0 {
			return 0, fmt.Errorf("import size is invalid")
		}
		return n, nil
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("import size is invalid")
		}
		return n, nil
	default:
		return 0, fmt.Errorf("import size is invalid")
	}
}

func extractPan123CommonPathFastLinks(text string) []string {
	return uniqueTrimmedMatches(pan123CommonPathFastLinkPattern.FindAllString(text, -1))
}

func extractPan123ImportFastLinks(text string) []string {
	return uniqueTrimmedMatches(pan123ImportFastLinkPattern.FindAllString(text, -1))
}

func uniqueTrimmedMatches(matches []string) []string {
	if len(matches) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	results := make([]string, 0, len(matches))
	for _, match := range matches {
		match = strings.TrimSpace(match)
		if match == "" {
			continue
		}
		if _, ok := seen[match]; ok {
			continue
		}
		seen[match] = struct{}{}
		results = append(results, match)
	}
	return results
}

func dedupeImportedFiles(files []pan123ImportedFile) []pan123ImportedFile {
	if len(files) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	result := make([]pan123ImportedFile, 0, len(files))
	for _, file := range files {
		key := file.Etag + "#" + strconv.FormatInt(file.Size, 10) + "#" + file.Path
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, file)
	}
	return result
}

package etfmeta

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

var sha256Pattern = regexp.MustCompile(`^[A-Fa-f0-9]{64}$`)

type Info struct {
	Name       string `json:"name"`
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`
	CreateTime string `json:"create_time,omitempty"`
}

func Encode(info *Info) ([]byte, error) {
	normalized, err := normalize(info)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	return []byte(base64.StdEncoding.EncodeToString(data)), nil
}

func Decode(data []byte) (*Info, error) {
	infos, err := DecodeAll(data)
	if err != nil {
		return nil, err
	}
	if len(infos) == 0 {
		return nil, fmt.Errorf("etf metadata is empty")
	}
	return &infos[0], nil
}

func DecodeAll(data []byte) ([]Info, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	var infos []Info
	var firstErr error
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		info, err := decodeLine(line)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		infos = append(infos, *info)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(infos) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return infos, nil
}

func FileName(sourceName string) string {
	if IsName(sourceName) {
		return sourceName
	}
	return sourceName + ".etf"
}

func IsName(name string) bool {
	return strings.EqualFold(filepath.Ext(name), ".etf")
}

func ResolveRestoreName(etfName string, info *Info) (string, error) {
	normalized, err := normalize(info)
	if err != nil {
		return "", err
	}
	if normalized.Name != "" {
		return normalized.Name, nil
	}
	name := strings.TrimSuffix(etfName, filepath.Ext(etfName))
	if strings.TrimSpace(name) == "" {
		return "", fmt.Errorf("restore name is empty")
	}
	return name, nil
}

func decodeLine(line string) (*Info, error) {
	raw, err := base64.StdEncoding.DecodeString(line)
	if err != nil {
		return nil, err
	}
	var info Info
	if err := json.Unmarshal(raw, &info); err != nil {
		return nil, err
	}
	return normalize(&info)
}

func normalize(info *Info) (*Info, error) {
	if info == nil {
		return nil, fmt.Errorf("etf metadata is nil")
	}
	normalized := *info
	normalized.Name = strings.TrimSpace(normalized.Name)
	normalized.SHA256 = strings.ToUpper(strings.TrimSpace(normalized.SHA256))
	if normalized.Name == "" {
		return nil, fmt.Errorf("etf name is empty")
	}
	if normalized.Size <= 0 {
		return nil, fmt.Errorf("etf size must be positive")
	}
	if !sha256Pattern.MatchString(normalized.SHA256) {
		return nil, fmt.Errorf("etf sha256 is invalid")
	}
	return &normalized, nil
}

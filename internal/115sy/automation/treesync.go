package automation

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"

	sy "github.com/OpenListTeam/OpenList/v4/internal/115sy"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

type TreeRecord struct {
	ID         string `json:"id"`
	ParentID   string `json:"parent_id"`
	Path       string `json:"path"`
	Name       string `json:"name"`
	IsDir      bool   `json:"is_dir"`
	ModifyTime int64  `json:"modify_time,omitempty"`
}

type TreeIndex struct {
	Records []TreeRecord `json:"records"`
}

func ParseTree(reader io.Reader) ([]TreeRecord, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	data, err = decodeText(data)
	if err != nil {
		return nil, err
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("tree input is empty")
	}
	if trimmed[0] == '[' {
		var records []TreeRecord
		if err := json.Unmarshal(trimmed, &records); err != nil {
			return nil, fmt.Errorf("invalid tree json: %w", err)
		}
		return validateTree(records)
	}
	return parseTreeDelimited(trimmed)
}

func decodeText(data []byte) ([]byte, error) {
	switch {
	case len(data) >= 2 && binary.LittleEndian.Uint16(data[:2]) == 0xfeff:
		reader := transform.NewReader(bytes.NewReader(data[2:]), unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewDecoder())
		return io.ReadAll(reader)
	case len(data) >= 2 && binary.BigEndian.Uint16(data[:2]) == 0xfeff:
		reader := transform.NewReader(bytes.NewReader(data[2:]), unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewDecoder())
		return io.ReadAll(reader)
	case bytes.HasPrefix(data, []byte{0xef, 0xbb, 0xbf}):
		return data[3:], nil
	default:
		return data, nil
	}
}

func parseTreeDelimited(data []byte) ([]TreeRecord, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	records := make([]TreeRecord, 0)
	first := true
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var fields []string
		if strings.Contains(line, "\t") {
			fields = strings.Split(line, "\t")
		} else {
			parsed, err := csv.NewReader(strings.NewReader(line)).Read()
			if err != nil {
				return nil, fmt.Errorf("invalid tree record: %w", err)
			}
			fields = parsed
		}
		if first && strings.EqualFold(strings.TrimSpace(fields[0]), "id") {
			first = false
			continue
		}
		first = false
		if len(fields) < 4 {
			return nil, fmt.Errorf("tree record has too few fields")
		}
		isDir := false
		if len(fields) >= 5 {
			parsed, err := strconv.ParseBool(strings.TrimSpace(fields[4]))
			if err != nil {
				isDir = strings.TrimSpace(fields[4]) == "0"
			} else {
				isDir = parsed
			}
		}
		records = append(records, TreeRecord{ID: strings.TrimSpace(fields[0]), ParentID: strings.TrimSpace(fields[1]), Path: strings.TrimSpace(fields[2]), Name: strings.TrimSpace(fields[3]), IsDir: isDir})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return validateTree(records)
}

func validateTree(records []TreeRecord) ([]TreeRecord, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("tree input has no records")
	}
	ids := make(map[string]struct{}, len(records))
	for _, record := range records {
		if record.ID == "" || record.Name == "" {
			return nil, fmt.Errorf("tree record is missing id or name")
		}
		if _, exists := ids[record.ID]; exists {
			return nil, fmt.Errorf("tree contains duplicate id %q", record.ID)
		}
		ids[record.ID] = struct{}{}
		cleaned := path.Clean(record.Path)
		if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
			return nil, fmt.Errorf("tree path escapes root")
		}
	}
	for _, record := range records {
		if record.ParentID != "" && record.ParentID != "0" {
			if _, exists := ids[record.ParentID]; !exists {
				return nil, fmt.Errorf("tree record %q has missing parent", record.ID)
			}
		}
	}
	return append([]TreeRecord(nil), records...), nil
}

func (i *TreeIndex) Replace(reader io.Reader) error {
	if i == nil {
		return fmt.Errorf("tree index is nil")
	}
	records, err := ParseTree(reader)
	if err != nil {
		return err
	}
	i.Records = records
	return nil
}

func TreeSync(index *TreeIndex, reader io.Reader) error {
	if index == nil {
		return fmt.Errorf("tree index is nil")
	}
	return index.Replace(reader)
}

func StarSync(ctx context.Context, client *sy.Client, rootCID string) ([]TreeRecord, error) {
	if client == nil {
		return nil, fmt.Errorf("115-sy client is nil")
	}
	rootCID = strings.TrimSpace(rootCID)
	if rootCID == "" {
		rootCID = "0"
	}
	type pending struct{ cid, parent, path string }
	queue := []pending{{cid: rootCID, path: "/"}}
	seen := map[string]struct{}{rootCID: {}}
	records := []TreeRecord{{ID: rootCID, Name: "/", Path: "/", IsDir: true}}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		current := queue[0]
		queue = queue[1:]
		items, err := client.ListFiles(ctx, current.cid, sy.ListOptions{PageSize: 1150})
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			itemParent := item.ParentCID
			if itemParent == "" {
				itemParent = current.cid
			}
			itemPath := path.Join(current.path, item.Name)
			records = append(records, TreeRecord{ID: item.ID, ParentID: itemParent, Path: itemPath, Name: item.Name, IsDir: item.IsDir, ModifyTime: item.ModifyTime})
			if item.IsDir {
				if _, exists := seen[item.ID]; exists {
					return nil, fmt.Errorf("star sync found directory cycle")
				}
				seen[item.ID] = struct{}{}
				queue = append(queue, pending{cid: item.ID, parent: itemParent, path: itemPath})
			}
		}
	}
	return records, nil
}

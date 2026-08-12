package etfauto

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
)

type RapidJSONArchiveItem struct {
	FileName string `json:"file_name"`
	FilePath string `json:"file_path,omitempty"`
	FileSize int64  `json:"file_size"`
	SHA256   string `json:"sha256"`
	Season   *int   `json:"season,omitempty"`
	Episode  *int   `json:"episode,omitempty"`
}

type RapidJSONArchivePayload struct {
	TMDBID    int64                  `json:"tmdb_id"`
	MediaType string                 `json:"media_type"`
	Title     string                 `json:"title,omitempty"`
	Items     []RapidJSONArchiveItem `json:"items"`
}

func RapidJSONPayloadFromRecord(record *model.ETFArchiveRecord) (RapidJSONArchivePayload, error) {
	if record == nil || record.TMDBID <= 0 {
		return RapidJSONArchivePayload{}, errors.New("ETF record tmdb id is required")
	}
	mediaType := strings.ToLower(strings.TrimSpace(record.MediaType))
	if mediaType != "tv" && mediaType != "movie" {
		return RapidJSONArchivePayload{}, fmt.Errorf("unsupported ETF media type %q", mediaType)
	}
	name := strings.TrimSpace(record.SourceName)
	hash := strings.ToUpper(strings.TrimSpace(record.SourceSHA256))
	if name == "" || record.SourceSize <= 0 || len(hash) != 64 {
		return RapidJSONArchivePayload{}, errors.New("ETF record has incomplete rapid metadata")
	}
	if _, err := hex.DecodeString(hash); err != nil {
		return RapidJSONArchivePayload{}, errors.New("ETF record has invalid SHA256")
	}
	item := RapidJSONArchiveItem{FileName: name, FilePath: name, FileSize: record.SourceSize, SHA256: hash}
	if mediaType == "tv" {
		if record.Season > 0 {
			item.Season = &record.Season
		}
		if record.Episode > 0 {
			item.Episode = &record.Episode
		}
	}
	return RapidJSONArchivePayload{
		TMDBID: record.TMDBID, MediaType: mediaType, Title: strings.TrimSpace(record.TMDBName), Items: []RapidJSONArchiveItem{item},
	}, nil
}

func MergeRapidJSONPayloads(payloads []RapidJSONArchivePayload) (RapidJSONArchivePayload, error) {
	if len(payloads) == 0 {
		return RapidJSONArchivePayload{}, errors.New("no rapid metadata to import")
	}
	result := payloads[0]
	seen := map[string]struct{}{}
	result.Items = nil
	for _, payload := range payloads {
		if payload.TMDBID != result.TMDBID || payload.MediaType != result.MediaType {
			return RapidJSONArchivePayload{}, errors.New("rapid metadata contains multiple media items")
		}
		for _, item := range payload.Items {
			if item.FileName == "" || item.FileSize <= 0 || len(item.SHA256) != 64 {
				return RapidJSONArchivePayload{}, errors.New("rapid metadata item is incomplete")
			}
			if _, err := hex.DecodeString(item.SHA256); err != nil {
				return RapidJSONArchivePayload{}, errors.New("rapid metadata item has invalid SHA256")
			}
			key := strings.ToUpper(item.SHA256) + ":" + item.FileName
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			result.Items = append(result.Items, item)
		}
	}
	if len(result.Items) == 0 {
		return RapidJSONArchivePayload{}, errors.New("no rapid metadata to import")
	}
	return result, nil
}

func (c *TargetClient) CreateRapidJSONArchive(ctx context.Context, payload RapidJSONArchivePayload) (string, error) {
	if payload.TMDBID <= 0 || (payload.MediaType != "tv" && payload.MediaType != "movie") || len(payload.Items) == 0 {
		return "", errors.New("invalid rapid-json archive payload")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/resource-ingest/rapid-json/archive"), bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	raw, err := c.do(req)
	if err != nil {
		return "", err
	}
	var accepted struct {
		ID     string `json:"id"`
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(raw, &accepted); err != nil {
		return "", err
	}
	taskID := strings.TrimSpace(accepted.ID)
	if taskID == "" {
		taskID = strings.TrimSpace(accepted.TaskID)
	}
	if taskID == "" {
		return "", fmt.Errorf("rapid-json archive response missing task id: %s", string(raw))
	}
	return string(raw), nil
}

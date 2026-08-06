package _115sy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	sharePageSize  int64 = 1000
	shareItemLimit       = 10000
)

func ParseShareURL(raw string) (ShareURL, error) {
	trimmed := strings.TrimSpace(raw)
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Scheme != "https" {
		return ShareURL{}, fmt.Errorf("invalid 115 share url")
	}
	host := strings.ToLower(strings.TrimPrefix(parsed.Hostname(), "www."))
	if host != "115.com" && host != "115cdn.com" {
		return ShareURL{}, fmt.Errorf("unsupported 115 share host")
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) != 2 || parts[0] != "s" || !validShareToken(parts[1]) {
		return ShareURL{}, fmt.Errorf("invalid 115 share path")
	}
	receiveCode := strings.TrimSpace(parsed.Query().Get("password"))
	if receiveCode == "" {
		receiveCode = strings.TrimSpace(parsed.Query().Get("receive_code"))
	}
	if !validShareToken(receiveCode) {
		return ShareURL{}, fmt.Errorf("115 share receive code is required")
	}
	return ShareURL{ShareCode: parts[1], ReceiveCode: receiveCode, SourceURL: trimmed}, nil
}

func validShareToken(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

func (c *Client) ShareSnapshot(ctx context.Context, share ShareURL) (ShareSnapshot, error) {
	if !validShareToken(share.ShareCode) || !validShareToken(share.ReceiveCode) {
		return ShareSnapshot{}, fmt.Errorf("invalid 115 share credentials")
	}
	items := make([]ShareItem, 0)
	seen := make(map[string]struct{})
	parents := map[string]string{"0": ""}
	var total int64
	var name, rootID string
	for offset := int64(0); ; offset += sharePageSize {
		query := url.Values{
			"share_code":   {share.ShareCode},
			"receive_code": {share.ReceiveCode},
			"cid":          {"0"},
			"limit":        {strconv.FormatInt(sharePageSize, 10)},
			"offset":       {strconv.FormatInt(offset, 10)},
			"asc":          {"0"},
		}
		var envelope responseEnvelope
		if err := c.doJSON(ctx, OperationShareSnapshot, ProfileAndroid, http.MethodGet, EndpointShareSnapshot, query, nil, &envelope); err != nil {
			return ShareSnapshot{}, err
		}
		page, err := decodeSharePage(envelope.Data)
		if err != nil {
			return ShareSnapshot{}, err
		}
		if page.Total > total {
			total = page.Total
		}
		if page.Name != "" {
			name = page.Name
		}
		if page.RootID != "" {
			rootID = page.RootID
		}
		for _, item := range page.Items {
			if item.ID == "" {
				return ShareSnapshot{}, &ProtocolError{Endpoint: EndpointShareSnapshot, Message: "share item is missing id"}
			}
			if item.ParentID == "" {
				item.ParentID = "0"
			}
			if item.IsDir && shareDirectoryCycle(item.ID, item.ParentID, parents) {
				return ShareSnapshot{}, &ProtocolError{Endpoint: EndpointShareSnapshot, Message: "share directory cycle detected"}
			}
			key := item.ID + "\x00" + item.ParentID
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			if item.IsDir {
				parents[item.ID] = item.ParentID
			}
			items = append(items, item)
			if len(items) > shareItemLimit {
				return ShareSnapshot{}, &ProtocolError{Endpoint: EndpointShareSnapshot, Message: "share item limit exceeded"}
			}
		}
		if len(page.Items) == 0 || (page.Total > 0 && int64(len(items)) >= page.Total) || int64(len(page.Items)) < sharePageSize {
			break
		}
	}
	if rootID == "" {
		rootID = "0"
	}
	return ShareSnapshot{
		ShareCode:   share.ShareCode,
		ReceiveCode: share.ReceiveCode,
		RootID:      rootID,
		Name:        name,
		FileCount:   int64(len(items)),
		TotalSize:   totalSize(items),
		Items:       items,
	}, nil
}

type sharePage struct {
	Items  []ShareItem
	Total  int64
	RootID string
	Name   string
}

func decodeSharePage(raw json.RawMessage) (sharePage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return sharePage{}, nil
	}
	var data struct {
		List       []RemoteItem   `json:"list"`
		Items      []RemoteItem   `json:"items"`
		Files      []RemoteItem   `json:"files"`
		Count      flexibleInt    `json:"count"`
		Total      flexibleInt    `json:"total"`
		CID        flexibleString `json:"cid"`
		RootID     flexibleString `json:"root_id"`
		ShareTitle string         `json:"share_title"`
		ShareInfo  struct {
			ShareTitle string `json:"share_title"`
		} `json:"shareinfo"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return sharePage{}, &ProtocolError{Endpoint: EndpointShareSnapshot, Message: err.Error()}
	}
	remoteItems := data.List
	if len(remoteItems) == 0 {
		remoteItems = data.Items
	}
	if len(remoteItems) == 0 {
		remoteItems = data.Files
	}
	items := make([]ShareItem, 0, len(remoteItems))
	for _, item := range remoteItems {
		items = append(items, ShareItem{ID: item.ID, ParentID: item.ParentCID, Name: item.Name, IsDir: item.IsDir, Size: item.Size})
	}
	total := int64(data.Total.value)
	if total == 0 {
		total = int64(data.Count.value)
	}
	return sharePage{Items: items, Total: total, RootID: firstNonEmpty(string(data.RootID), string(data.CID)), Name: firstNonEmpty(data.ShareTitle, data.ShareInfo.ShareTitle)}, nil
}

func shareDirectoryCycle(id, parent string, parents map[string]string) bool {
	if id == parent {
		return true
	}
	seen := map[string]struct{}{id: {}}
	for parent != "" {
		if _, exists := seen[parent]; exists {
			return true
		}
		seen[parent] = struct{}{}
		next, exists := parents[parent]
		if !exists {
			return false
		}
		parent = next
	}
	return false
}

func totalSize(items []ShareItem) int64 {
	var total int64
	for _, item := range items {
		if !item.IsDir {
			total += item.Size
		}
	}
	return total
}

func (c *Client) ReceiveShare(ctx context.Context, req ReceiveShareRequest) (ReceiveResult, error) {
	if !validShareToken(req.ShareCode) || !validShareToken(req.ReceiveCode) || strings.TrimSpace(req.TargetCID) == "" || strings.TrimSpace(req.TargetCID) == "0" {
		return ReceiveResult{}, fmt.Errorf("invalid share receive request")
	}
	if strings.TrimSpace(req.FileID) == "" {
		return ReceiveResult{}, fmt.Errorf("share receive file id is required")
	}
	form := url.Values{
		"share_code":   {req.ShareCode},
		"receive_code": {req.ReceiveCode},
		"file_id":      {req.FileID},
		"cid":          {req.TargetCID},
		"is_check":     {"0"},
	}
	var envelope responseEnvelope
	if err := c.doForm(ctx, OperationShareReceive, ProfileWeb, http.MethodPost, EndpointShareReceive, nil, form, &envelope); err != nil {
		return ReceiveResult{}, err
	}
	result := ReceiveResult{State: envelope.State == nil || bool(*envelope.State), Message: firstNonEmpty(envelope.Message, envelope.Error), Data: envelope.Data}
	if len(envelope.Data) > 0 {
		var data struct {
			TaskID string         `json:"task_id"`
			CID    flexibleString `json:"cid"`
		}
		if err := json.Unmarshal(envelope.Data, &data); err == nil {
			result.TaskID = data.TaskID
			result.CID = string(data.CID)
		}
	}
	return result, nil
}

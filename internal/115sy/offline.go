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

func (c *Client) AddOfflineTasks(ctx context.Context, req OfflineRequest) (OfflineResult, error) {
	urls, err := normalizeOfflineURLs(req.URLs)
	if err != nil {
		return OfflineResult{}, err
	}
	targetCID := strings.TrimSpace(req.TargetCID)
	if targetCID == "" || targetCID == "0" {
		return OfflineResult{}, fmt.Errorf("offline target cid is required")
	}
	form := url.Values{"wp_path_id": {targetCID}}
	for index, value := range urls {
		form.Set(fmt.Sprintf("url[%d]", index), value)
	}
	var envelope responseEnvelope
	if err := c.doForm(ctx, OperationOffline, ProfileAndroid, http.MethodPost, EndpointOfflineAdd, nil, form, &envelope); err != nil {
		return OfflineResult{}, err
	}
	return decodeOfflineResult(envelope.Data, urls), nil
}

func normalizeOfflineURLs(values []string) ([]string, error) {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if !validOfflineURL(value) {
			return nil, fmt.Errorf("unsupported offline url")
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("offline url list is empty")
	}
	return result, nil
}

func validOfflineURL(value string) bool {
	lower := strings.ToLower(value)
	if strings.HasPrefix(lower, "magnet:?xt=urn:btih:") || strings.HasPrefix(lower, "ed2k://|file|") {
		return true
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return false
	}
	return (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != ""
}

func decodeOfflineResult(raw json.RawMessage, urls []string) OfflineResult {
	result := OfflineResult{Items: make([]OfflineItemResult, len(urls))}
	for index, value := range urls {
		result.Items[index].URL = value
	}
	if len(raw) == 0 || string(raw) == "null" {
		for index := range result.Items {
			result.Items[index].Success = true
		}
		return result
	}
	var list []struct {
		URL      string         `json:"url"`
		InfoHash string         `json:"info_hash"`
		TaskID   flexibleString `json:"task_id"`
		Error    string         `json:"error"`
		ErrorMsg string         `json:"error_msg"`
		State    *flexibleBool  `json:"state"`
	}
	if json.Unmarshal(raw, &list) != nil {
		var wrapper struct {
			Result []struct {
				URL      string         `json:"url"`
				InfoHash string         `json:"info_hash"`
				TaskID   flexibleString `json:"task_id"`
				Error    string         `json:"error"`
				ErrorMsg string         `json:"error_msg"`
				State    *flexibleBool  `json:"state"`
			} `json:"result"`
			Tasks []struct {
				URL      string         `json:"url"`
				InfoHash string         `json:"info_hash"`
				TaskID   flexibleString `json:"task_id"`
				Error    string         `json:"error"`
				ErrorMsg string         `json:"error_msg"`
				State    *flexibleBool  `json:"state"`
			} `json:"tasks"`
		}
		if json.Unmarshal(raw, &wrapper) == nil {
			list = wrapper.Result
			if len(list) == 0 {
				list = wrapper.Tasks
			}
		}
	}
	for index, entry := range list {
		if index >= len(result.Items) {
			break
		}
		if entry.URL != "" {
			result.Items[index].URL = entry.URL
		}
		result.Items[index].TaskID = string(entry.TaskID)
		result.Items[index].Error = firstNonEmpty(entry.Error, entry.ErrorMsg)
		result.Items[index].ErrorMsg = entry.ErrorMsg
		result.Items[index].Success = result.Items[index].Error == "" && (entry.State == nil || bool(*entry.State))
		if result.Items[index].TaskID == "" {
			result.Items[index].TaskID = entry.InfoHash
		}
	}
	for index := len(list); index < len(result.Items); index++ {
		result.Items[index].Error = "offline task result missing"
	}
	return result
}

func (c *Client) ListOfflineTasks(ctx context.Context) ([]OfflineTask, error) {
	query := url.Values{"page": {"1"}, "page_size": {"100"}}
	var envelope responseEnvelope
	if err := c.doJSON(ctx, OperationOffline, ProfileAndroid, http.MethodGet, EndpointOfflineList, query, nil, &envelope); err != nil {
		return nil, err
	}
	return decodeOfflineTasks(envelope.Data)
}

func decodeOfflineTasks(raw json.RawMessage) ([]OfflineTask, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var entries []json.RawMessage
	if raw[0] == '[' {
		if err := json.Unmarshal(raw, &entries); err != nil {
			return nil, &ProtocolError{Endpoint: EndpointOfflineList, Message: err.Error()}
		}
	} else {
		var wrapper struct {
			Tasks []json.RawMessage `json:"tasks"`
			List  []json.RawMessage `json:"list"`
			Items []json.RawMessage `json:"items"`
		}
		if err := json.Unmarshal(raw, &wrapper); err != nil {
			return nil, &ProtocolError{Endpoint: EndpointOfflineList, Message: err.Error()}
		}
		entries = wrapper.Tasks
		if len(entries) == 0 {
			entries = wrapper.List
		}
		if len(entries) == 0 {
			entries = wrapper.Items
		}
	}
	result := make([]OfflineTask, 0, len(entries))
	for _, entry := range entries {
		var rawTask struct {
			ID        flexibleString `json:"id"`
			InfoHash  flexibleString `json:"info_hash"`
			Name      string         `json:"name"`
			URL       string         `json:"url"`
			Size      flexibleInt    `json:"size"`
			Progress  flexibleInt    `json:"progress"`
			Percent   flexibleInt    `json:"percentDone"`
			Status    flexibleInt    `json:"status"`
			Error     string         `json:"error"`
			FileID    flexibleString `json:"file_id"`
			TargetCID flexibleString `json:"wp_path_id"`
			UpdatedAt flexibleInt    `json:"last_update"`
		}
		if err := json.Unmarshal(entry, &rawTask); err != nil {
			return nil, &ProtocolError{Endpoint: EndpointOfflineList, Message: err.Error()}
		}
		progress := rawTask.Progress.value
		if rawTask.Percent.set {
			progress = rawTask.Percent.value
		}
		result = append(result, OfflineTask{
			ID: string(rawTask.ID), TaskID: string(rawTask.ID), InfoHash: string(rawTask.InfoHash), Name: rawTask.Name, URL: rawTask.URL,
			Size: rawTask.Size.value, Progress: float64(progress), Status: int(rawTask.Status.value),
			Error: rawTask.Error, FileID: string(rawTask.FileID), TargetCID: string(rawTask.TargetCID), UpdatedAt: rawTask.UpdatedAt.value,
		})
	}
	return result, nil
}

func (c *Client) DeleteOfflineTasks(ctx context.Context, ids []string, deleteFiles bool) error {
	if len(ids) == 0 {
		return nil
	}
	form := url.Values{"del_source_file": {strconv.Itoa(boolToInt(deleteFiles))}}
	seen := make(map[string]struct{}, len(ids))
	index := 0
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		form.Set(fmt.Sprintf("info_hash[%d]", index), id)
		index++
	}
	if index == 0 {
		return nil
	}
	return c.doForm(ctx, OperationOffline, ProfileAndroid, http.MethodPost, EndpointOfflineDelete, nil, form, nil)
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

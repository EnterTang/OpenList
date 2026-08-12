package _115sy

import (
	"context"
	"encoding/json"
	"errors"
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

// ShareChildren lists one directory from a share. It uses the same Android
// first / HTTP 405 Web fallback policy as ShareSnapshot, while preserving the
// requested cid so callers do not need to download the entire share tree for
// every recursive directory visit.
func (c *Client) ShareChildren(ctx context.Context, share ShareURL, parentID string) ([]ShareItem, error) {
	if !validShareToken(share.ShareCode) || !validShareToken(share.ReceiveCode) {
		return nil, fmt.Errorf("invalid 115 share credentials")
	}
	parentID = strings.TrimSpace(parentID)
	if parentID == "" {
		parentID = "0"
	}
	items := make([]ShareItem, 0)
	seen := make(map[string]struct{})
	seenOffsets := make(map[int64]struct{})
	var total int64
	for offset := int64(0); ; offset += sharePageSize {
		if _, exists := seenOffsets[offset]; exists {
			return nil, &ProtocolError{Endpoint: EndpointShareSnapshot, Message: "server repeated share pagination offset"}
		}
		seenOffsets[offset] = struct{}{}
		query := url.Values{
			"share_code":   {share.ShareCode},
			"receive_code": {share.ReceiveCode},
			"cid":          {parentID},
			"limit":        {strconv.FormatInt(sharePageSize, 10)},
			"offset":       {strconv.FormatInt(offset, 10)},
			"asc":          {"0"},
		}
		var envelope responseEnvelope
		if err := c.doJSON(ctx, OperationShareSnapshot, ProfileAndroid, http.MethodGet, EndpointShareSnapshot, query, nil, &envelope); err != nil {
			return nil, err
		}
		page, err := decodeSharePage(envelope.Data)
		if err != nil {
			return nil, err
		}
		if page.Total == 0 {
			if envelope.Count.set {
				page.Total = envelope.Count.value
			}
			if envelope.Total.set {
				page.Total = envelope.Total.value
			}
		}
		if page.Total > total {
			total = page.Total
		}
		for _, item := range page.Items {
			if item.ID == "" {
				return nil, &ProtocolError{Endpoint: EndpointShareSnapshot, Message: "share item is missing id"}
			}
			item.ParentID = firstNonEmpty(item.ParentID, parentID)
			key := item.ID + "\x00" + item.ParentID
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			items = append(items, item)
			if len(items) > shareItemLimit {
				return nil, &ProtocolError{Endpoint: EndpointShareSnapshot, Message: "share item limit exceeded"}
			}
		}
		if len(page.Items) == 0 || (total > 0 && int64(len(items)) >= total) || len(page.Items) < int(sharePageSize) {
			break
		}
	}
	return items, nil
}

// ShareDownloadURL obtains the short-lived URL for one file in a share. The
// app endpoint follows the RSA envelope used by the official 115 client; a
// 405 is the only automatic profile fallback and then uses the web endpoint.
// The returned URL is intentionally not cached by this package.
func (c *Client) ShareDownloadURL(ctx context.Context, share ShareURL, fileID, userAgent string) (DownloadLink, error) {
	if c == nil || !validShareToken(share.ShareCode) || !validShareToken(share.ReceiveCode) || strings.TrimSpace(fileID) == "" {
		return DownloadLink{}, fmt.Errorf("invalid 115 share download request")
	}
	requestClient := c
	if strings.TrimSpace(userAgent) != "" && userAgent != c.userAgent {
		requestClient = c.cloneForUserAgent(userAgent)
	}
	payloadJSON, err := json.Marshal(map[string]any{
		"share_code":   share.ShareCode,
		"receive_code": share.ReceiveCode,
		"file_id":      strings.TrimSpace(fileID),
		"dl":           1,
	})
	if err != nil {
		return DownloadLink{}, &ProtocolError{Endpoint: EndpointShareDownloadURLApp, Message: err.Error()}
	}
	encrypted, err := p115RSAEncrypt(payloadJSON)
	if err != nil {
		return DownloadLink{}, &ProtocolError{Endpoint: EndpointShareDownloadURLApp, Message: err.Error()}
	}
	var envelope responseEnvelope
	form := url.Values{"data": {encrypted}}
	err = requestClient.do(ctx, OperationShareDownloadURL, ProfileAndroid, http.MethodPost, EndpointShareDownloadURLApp, nil, []byte(form.Encode()), "application/x-www-form-urlencoded", &envelope)
	if err != nil {
		var httpErr *HTTPError
		if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusMethodNotAllowed {
			return DownloadLink{}, err
		}
		query := url.Values{
			"share_code":   {share.ShareCode},
			"receive_code": {share.ReceiveCode},
			"file_id":      {strings.TrimSpace(fileID)},
		}
		if err := requestClient.do(ctx, OperationShareDownloadURL, ProfileWeb, http.MethodGet, EndpointShareDownloadURLWeb, query, nil, "", &envelope); err != nil {
			return DownloadLink{}, err
		}
		link, err := decodeShareDownloadLink(envelope.Data)
		if err != nil {
			return DownloadLink{}, err
		}
		setShareDownloadUserAgent(&link, userAgent)
		return link, nil
	}

	var encryptedResponse string
	if err := json.Unmarshal(envelope.Data, &encryptedResponse); err == nil && strings.TrimSpace(encryptedResponse) != "" {
		decrypted, decryptErr := p115RSADecrypt(encryptedResponse)
		if decryptErr != nil {
			return DownloadLink{}, &ProtocolError{Endpoint: EndpointShareDownloadURLApp, Message: decryptErr.Error()}
		}
		link, decodeErr := decodeShareDownloadLink(decrypted)
		if decodeErr != nil {
			return DownloadLink{}, decodeErr
		}
		setShareDownloadUserAgent(&link, userAgent)
		return link, nil
	}
	// Some compatible gateways expose the same endpoint without the RSA
	// envelope. Accept that response shape while keeping the encrypted path as
	// the default official-client protocol.
	link, err := decodeShareDownloadLink(envelope.Data)
	if err != nil {
		return DownloadLink{}, err
	}
	setShareDownloadUserAgent(&link, userAgent)
	return link, nil
}

func setShareDownloadUserAgent(link *DownloadLink, userAgent string) {
	if link == nil || strings.TrimSpace(userAgent) == "" {
		return
	}
	if link.Header == nil {
		link.Header = make(http.Header)
	}
	link.Header.Set("User-Agent", userAgent)
}

func decodeShareDownloadLink(raw []byte) (DownloadLink, error) {
	payload, err := decodeDownloadPayload(raw)
	if err != nil {
		return DownloadLink{}, err
	}
	link, err := payload.downloadURL()
	if err != nil {
		return DownloadLink{}, err
	}
	header := payload.Header
	if header == nil {
		header = make(http.Header)
	}
	if header.Get("User-Agent") == "" {
		header.Set("User-Agent", DefaultAndroidUA)
	}
	return DownloadLink{URL: link, Header: header}, nil
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
		items = append(items, ShareItem{
			ID:         item.ID,
			ParentID:   item.ParentCID,
			Name:       item.Name,
			IsDir:      item.IsDir,
			Size:       item.Size,
			ModifyTime: item.ModifyTime,
		})
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
	if !validShareToken(req.ShareCode) || !validShareToken(req.ReceiveCode) || strings.TrimSpace(req.TargetCID) == "" {
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
	// Match p115client.share_receive_app: use the Android-compatible API
	// first, and let the request policy fall back to the web endpoint only
	// when the app route explicitly rejects the method with HTTP 405.
	if err := c.doForm(ctx, OperationShareReceive, ProfileAndroid, http.MethodPost, EndpointShareReceive, nil, form, &envelope); err != nil {
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

func (c *Client) CreateShare(ctx context.Context, req CreateShareRequest) (CreateShareResult, error) {
	ids := make([]string, 0, len(req.FileIDs))
	seen := make(map[string]struct{}, len(req.FileIDs))
	for _, id := range req.FileIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return CreateShareResult{}, &ProtocolError{Endpoint: EndpointShareSend, Message: "file ids are required"}
	}
	order := strings.TrimSpace(req.Order)
	if order == "" {
		order = "file_name"
	}
	form := url.Values{
		"file_ids":    {strings.Join(ids, ",")},
		"ignore_warn": {"1"},
		"is_asc":      {"1"},
		"order":       {order},
	}
	var envelope responseEnvelope
	if err := c.doForm(ctx, OperationShareReceive, ProfileWeb, http.MethodPost, EndpointShareSend, nil, form, &envelope); err != nil {
		return CreateShareResult{}, err
	}
	var result CreateShareResult
	if err := json.Unmarshal(envelope.Data, &result); err != nil {
		return CreateShareResult{}, &ProtocolError{Endpoint: EndpointShareSend, Message: "share response is invalid"}
	}
	result.ShareCode = strings.TrimSpace(result.ShareCode)
	result.ReceiveCode = strings.TrimSpace(result.ReceiveCode)
	result.ShareURL = strings.TrimSpace(result.ShareURL)
	if result.ShareCode == "" {
		return CreateShareResult{}, &ProtocolError{Endpoint: EndpointShareSend, Message: "share response is missing share code"}
	}
	if result.ShareURL == "" {
		result.ShareURL = "https://115.com/s/" + url.PathEscape(result.ShareCode)
		if result.ReceiveCode != "" {
			result.ShareURL += "?password=" + url.QueryEscape(result.ReceiveCode)
		}
	}
	return result, nil
}

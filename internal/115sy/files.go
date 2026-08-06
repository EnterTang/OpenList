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

const maxFilePageSize int64 = 1150

type ListOptions struct {
	PageSize int64
	Offset   int64
}

type ProtocolError struct {
	Endpoint string
	Message  string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("invalid 115 response from %s: %s", sanitizeEndpoint(e.Endpoint), sanitizeMessage(e.Message))
}

type filePage struct {
	Items      []RemoteItem
	Offset     int64
	Limit      int64
	Total      int64
	HasMore    bool
	HasMoreSet bool
	Next       int64
	OffsetSet  bool
	NextSet    bool
}

func (c *Client) ListFiles(ctx context.Context, cid string, opts ListOptions) ([]RemoteItem, error) {
	cid = strings.TrimSpace(cid)
	if cid == "" {
		cid = "0"
	}
	limit := opts.PageSize
	if limit <= 0 {
		limit = maxFilePageSize
	}
	if limit > maxFilePageSize {
		limit = maxFilePageSize
	}
	offset := opts.Offset
	if offset < 0 {
		return nil, &ProtocolError{Endpoint: EndpointFileList, Message: "offset must not be negative"}
	}

	items := make([]RemoteItem, 0)
	seenOffsets := make(map[int64]struct{})
	seenItems := make(map[string]struct{})
	for {
		if _, exists := seenOffsets[offset]; exists {
			return nil, &ProtocolError{Endpoint: EndpointFileList, Message: "server repeated pagination offset"}
		}
		seenOffsets[offset] = struct{}{}

		query := url.Values{
			"aid":              {"1"},
			"cid":              {cid},
			"offset":           {strconv.FormatInt(offset, 10)},
			"limit":            {strconv.FormatInt(limit, 10)},
			"show_dir":         {"1"},
			"snap":             {"0"},
			"natsort":          {"0"},
			"record_open_time": {"1"},
			"format":           {"json"},
		}
		var envelope responseEnvelope
		if err := c.doJSON(ctx, OperationFileList, ProfileAndroid, http.MethodGet, EndpointFileList, query, nil, &envelope); err != nil {
			return nil, err
		}
		page, err := decodeFilePage(envelope.Data)
		if err != nil {
			return nil, err
		}
		if envelope.Count.set {
			page.Total = envelope.Count.value
		}
		if envelope.Total.set {
			page.Total = envelope.Total.value
		}
		if envelope.Offset.set {
			page.Offset, page.OffsetSet = envelope.Offset.value, true
		}
		if envelope.Limit.set {
			page.Limit = envelope.Limit.value
		}
		if envelope.Next.set {
			page.Next, page.NextSet = envelope.Next.value, true
		}
		if envelope.HasMore != nil {
			page.HasMore, page.HasMoreSet = bool(*envelope.HasMore), true
		}
		if page.OffsetSet && page.Offset != offset {
			return nil, &ProtocolError{Endpoint: EndpointFileList, Message: "server repeated pagination offset"}
		}
		for _, item := range page.Items {
			if item.ID == "" {
				return nil, &ProtocolError{Endpoint: EndpointFileList, Message: "file item is missing id"}
			}
			if item.ParentCID == "" {
				item.ParentCID = cid
			}
			key := item.ID + "\x00" + item.ParentCID
			if _, exists := seenItems[key]; exists {
				continue
			}
			seenItems[key] = struct{}{}
			items = append(items, item)
		}

		if len(page.Items) == 0 {
			if page.HasMore {
				return nil, &ProtocolError{Endpoint: EndpointFileList, Message: "server returned empty page with more results"}
			}
			break
		}
		if page.Total > 0 && int64(len(items)) >= page.Total {
			break
		}
		if page.HasMoreSet && !page.HasMore {
			break
		}
		if !page.HasMoreSet && page.Total == 0 && int64(len(page.Items)) < limit {
			break
		}

		next := page.Next
		if !page.NextSet {
			next = offset + limit
		}
		if next <= offset {
			return nil, &ProtocolError{Endpoint: EndpointFileList, Message: "server returned non-advancing pagination offset"}
		}
		offset = next
	}
	return items, nil
}

func decodeFilePage(raw json.RawMessage) (filePage, error) {
	raw = json.RawMessage(strings.TrimSpace(string(raw)))
	if len(raw) == 0 || string(raw) == "null" {
		return filePage{}, nil
	}
	if raw[0] == '[' {
		var items []RemoteItem
		if err := json.Unmarshal(raw, &items); err != nil {
			return filePage{}, &ProtocolError{Endpoint: EndpointFileList, Message: err.Error()}
		}
		return filePage{Items: items}, nil
	}
	var wire struct {
		Items   []RemoteItem    `json:"items"`
		Files   []RemoteItem    `json:"files"`
		List    []RemoteItem    `json:"list"`
		Data    json.RawMessage `json:"data"`
		Offset  flexibleInt64   `json:"offset"`
		Limit   flexibleInt64   `json:"limit"`
		Count   flexibleInt64   `json:"count"`
		Total   flexibleInt64   `json:"total"`
		HasMore flexibleBool    `json:"has_more"`
		Next    flexibleInt64   `json:"next_offset"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return filePage{}, &ProtocolError{Endpoint: EndpointFileList, Message: err.Error()}
	}
	items := wire.Items
	if len(items) == 0 {
		items = wire.Files
	}
	if len(items) == 0 {
		items = wire.List
	}
	if len(items) == 0 && len(wire.Data) > 0 && string(wire.Data) != "null" {
		if wire.Data[0] == '[' {
			if err := json.Unmarshal(wire.Data, &items); err != nil {
				return filePage{}, &ProtocolError{Endpoint: EndpointFileList, Message: err.Error()}
			}
		} else {
			return decodeFilePage(wire.Data)
		}
	}
	page := filePage{
		Items:      items,
		Offset:     int64(wire.Offset),
		Limit:      int64(wire.Limit),
		Total:      int64(wire.Total),
		HasMore:    bool(wire.HasMore),
		HasMoreSet: strings.Contains(string(raw), "has_more"),
		Next:       int64(wire.Next),
		OffsetSet:  strings.Contains(string(raw), "offset"),
		NextSet:    strings.Contains(string(raw), "next_offset"),
	}
	if page.Total == 0 {
		page.Total = int64(wire.Count)
	}
	if page.Limit <= 0 {
		page.Limit = maxFilePageSize
	}
	if page.Next == 0 && page.Offset > 0 {
		page.Next = page.Offset + page.Limit
	}
	return page, nil
}

func (c *Client) GetFile(ctx context.Context, fid string) (RemoteItem, error) {
	query := url.Values{"pick_code": {strings.TrimSpace(fid)}, "fid": {strings.TrimSpace(fid)}}
	var raw json.RawMessage
	if err := c.doJSON(ctx, OperationShareReceive, ProfileWeb, http.MethodGet, EndpointFileInfo, query, nil, &raw); err != nil {
		return RemoteItem{}, err
	}
	var item RemoteItem
	if len(raw) > 0 && raw[0] == '[' {
		var items []RemoteItem
		if err := json.Unmarshal(raw, &items); err != nil || len(items) == 0 {
			return RemoteItem{}, &ProtocolError{Endpoint: EndpointFileInfo, Message: "file info response is empty"}
		}
		return items[0], nil
	}
	if err := json.Unmarshal(raw, &item); err != nil || item.ID == "" {
		return RemoteItem{}, &ProtocolError{Endpoint: EndpointFileInfo, Message: "file info response is invalid"}
	}
	return item, nil
}

func (c *Client) MakeDir(ctx context.Context, cid, name string) (RemoteItem, error) {
	var response struct {
		ID  string `json:"file_id"`
		CID string `json:"cid"`
	}
	form := url.Values{"pid": {strings.TrimSpace(cid)}, "cname": {name}}
	if err := c.doForm(ctx, OperationShareReceive, ProfileWeb, http.MethodPost, EndpointDirAdd, nil, form, &response); err != nil {
		return RemoteItem{}, err
	}
	c.invalidatePathCache()
	id := firstNonEmpty(response.ID, response.CID)
	if id == "" {
		return RemoteItem{}, &ProtocolError{Endpoint: EndpointDirAdd, Message: "mkdir response is missing id"}
	}
	return RemoteItem{ID: id, Name: name, IsDir: true, ParentCID: cid}, nil
}

func (c *Client) Move(ctx context.Context, fid, cid string) error {
	return c.mutateFile(ctx, EndpointFileMove, url.Values{"pid": {cid}, "fid[0]": {fid}})
}

func (c *Client) Rename(ctx context.Context, fid, name string) error {
	return c.mutateFile(ctx, EndpointFileRename, url.Values{"fid": {fid}, "file_name": {name}, "files_new_name[" + fid + "]": {name}})
}

func (c *Client) Copy(ctx context.Context, fid, cid string) error {
	return c.mutateFile(ctx, EndpointFileCopy, url.Values{"pid": {cid}, "fid[0]": {fid}})
}

func (c *Client) Remove(ctx context.Context, fid, parentCID string) error {
	return c.mutateFile(ctx, EndpointFileDelete, url.Values{"fid[0]": {fid}})
}

func (c *Client) mutateFile(ctx context.Context, endpoint string, form url.Values) error {
	if err := c.doForm(ctx, OperationShareReceive, ProfileWeb, http.MethodPost, endpoint, nil, form, nil); err != nil {
		return err
	}
	c.invalidatePathCache()
	return nil
}

func (c *Client) GetCapacity(ctx context.Context) (Capacity, error) {
	var payload rootProbePayload
	if err := c.doJSON(ctx, OperationUserInfo, ProfileWeb, http.MethodGet, EndpointCategory, nil, nil, &payload); err != nil {
		return Capacity{}, err
	}
	return Capacity{
		Total:     int64(payload.SpaceTotal),
		Used:      int64(payload.SpaceUsed),
		Remaining: int64(payload.SpaceRemain),
	}, nil
}

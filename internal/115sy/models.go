package _115sy

import (
	"encoding/json"
	"strconv"
	"strings"
)

type Credential struct {
	UID  string `json:"uid"`
	CID  string `json:"cid"`
	SEID string `json:"seid"`
	KID  string `json:"kid,omitempty"`
}

type Capacity struct {
	Total     int64 `json:"total"`
	Used      int64 `json:"used"`
	Remaining int64 `json:"remaining"`
}

type AuthState struct {
	Credential Credential `json:"credential"`
	User       UserInfo   `json:"user"`
	UserID     string     `json:"user_id"`
	RootCID    string     `json:"root_cid"`
	Capacity   Capacity   `json:"capacity"`
}

type UserInfo struct {
	ID       string `json:"id"`
	Nickname string `json:"nickname"`
}

type RemoteItem struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	IsDir      bool   `json:"is_dir"`
	Size       int64  `json:"size"`
	SHA1       string `json:"sha1"`
	PickCode   string `json:"pickcode"`
	ParentCID  string `json:"parent_cid"`
	ModifyTime int64  `json:"modify_time"`
	Thumbnail  string `json:"thumbnail"`
}

func (i *RemoteItem) UnmarshalJSON(data []byte) error {
	type remoteItemAlias RemoteItem
	var raw struct {
		remoteItemAlias
		ID           flexibleString `json:"id"`
		FileID       flexibleString `json:"file_id"`
		FID          flexibleString `json:"fid"`
		CID          flexibleString `json:"cid"`
		Name         string         `json:"name"`
		N            string         `json:"n"`
		FileName     string         `json:"file_name"`
		IsDir        flexibleBool   `json:"is_dir"`
		IsFolder     flexibleBool   `json:"is_folder"`
		Directory    flexibleBool   `json:"directory"`
		FileCategory flexibleString `json:"file_category"`
		FC           flexibleString `json:"fc"`
		Category     flexibleString `json:"category"`
		Size         flexibleInt64  `json:"size"`
		S            flexibleInt64  `json:"s"`
		SHA1         string         `json:"sha1"`
		Sha          string         `json:"sha"`
		PickCode     string         `json:"pickcode"`
		PC           string         `json:"pc"`
		ParentCID    flexibleString `json:"parent_cid"`
		ParentID     flexibleString `json:"parent_id"`
		PID          flexibleString `json:"pid"`
		ModifyTime   flexibleInt64  `json:"modify_time"`
		UT           flexibleInt64  `json:"utime"`
		T            flexibleInt64  `json:"t"`
		Thumbnail    string         `json:"thumbnail"`
		Thumb        string         `json:"thumb"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	*i = RemoteItem(raw.remoteItemAlias)
	i.IsDir = bool(raw.IsDir) || bool(raw.IsFolder) || bool(raw.Directory) || string(raw.Category) == "0" || string(raw.FileCategory) == "0" || string(raw.FC) == "0"
	i.ID = strings.TrimSpace(firstNonEmpty(
		string(raw.ID),
		string(raw.FileID),
		string(raw.FID),
		string(raw.CID),
		i.ID,
	))
	i.Name = firstNonEmpty(raw.Name, raw.N, raw.FileName, i.Name)
	i.Size = int64(raw.Size)
	if i.Size == 0 {
		i.Size = int64(raw.S)
	}
	i.SHA1 = firstNonEmpty(raw.SHA1, raw.Sha, i.SHA1)
	i.PickCode = firstNonEmpty(raw.PickCode, raw.PC, i.PickCode)
	i.ParentCID = strings.TrimSpace(firstNonEmpty(
		string(raw.ParentCID),
		string(raw.ParentID),
		string(raw.PID),
		i.ParentCID,
	))
	i.ModifyTime = int64(raw.ModifyTime)
	if i.ModifyTime == 0 {
		i.ModifyTime = int64(raw.UT)
	}
	if i.ModifyTime == 0 {
		i.ModifyTime = int64(raw.T)
	}
	i.Thumbnail = firstNonEmpty(raw.Thumbnail, raw.Thumb, i.Thumbnail)
	return nil
}

type ListFilesOptions struct {
	Limit  int
	Offset int
	Order  string
	Asc    bool
}

type fileListPayload struct {
	Items      []RemoteItem
	Total      int
	Offset     int
	NextOffset int
	HasMore    *bool

	hasTotal      bool
	hasOffset     bool
	hasNextOffset bool
}

func (p *fileListPayload) UnmarshalJSON(data []byte) error {
	var items []RemoteItem
	if err := json.Unmarshal(data, &items); err == nil {
		p.Items = items
		return nil
	}

	var raw struct {
		Data       json.RawMessage `json:"data"`
		Items      []RemoteItem    `json:"items"`
		Files      []RemoteItem    `json:"files"`
		List       []RemoteItem    `json:"list"`
		Total      flexibleInt     `json:"total"`
		Count      flexibleInt     `json:"count"`
		FileCount  flexibleInt     `json:"file_count"`
		Offset     flexibleInt     `json:"offset"`
		NextOffset flexibleInt     `json:"next_offset"`
		HasMore    *flexibleBool   `json:"has_more"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if len(raw.Data) > 0 && string(raw.Data) != "null" && len(raw.Items) == 0 && len(raw.Files) == 0 && len(raw.List) == 0 {
		var nested fileListPayload
		if err := json.Unmarshal(raw.Data, &nested); err != nil {
			return err
		}
		*p = nested
		return nil
	}

	p.Items = firstNonEmptyItems(raw.Items, raw.Files, raw.List)
	p.Total, p.hasTotal = firstPositiveInt(raw.Total, raw.Count, raw.FileCount)
	p.Offset = int(raw.Offset.value)
	p.hasOffset = raw.Offset.set
	p.NextOffset = int(raw.NextOffset.value)
	p.hasNextOffset = raw.NextOffset.set
	if raw.HasMore != nil {
		value := bool(*raw.HasMore)
		p.HasMore = &value
	}
	return nil
}

func firstNonEmptyItems(values ...[]RemoteItem) []RemoteItem {
	for _, value := range values {
		if len(value) > 0 {
			return value
		}
	}
	return nil
}

func firstPositiveInt(values ...flexibleInt) (int, bool) {
	for _, value := range values {
		if value.set {
			return int(value.value), true
		}
	}
	return 0, false
}

type flexibleString string

func (v *flexibleString) UnmarshalJSON(data []byte) error {
	raw := strings.TrimSpace(string(data))
	if raw == "" || raw == "null" {
		*v = ""
		return nil
	}
	if strings.HasPrefix(raw, `"`) {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		*v = flexibleString(s)
		return nil
	}
	*v = flexibleString(raw)
	return nil
}

type flexibleInt struct {
	value int64
	set   bool
}

func (v *flexibleInt) UnmarshalJSON(data []byte) error {
	raw := strings.TrimSpace(string(data))
	if raw == "" || raw == "null" {
		v.value = 0
		v.set = false
		return nil
	}
	raw = strings.Trim(raw, `"`)
	if raw == "" {
		v.value = 0
		v.set = true
		return nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return err
	}
	v.value = n
	v.set = true
	return nil
}

type flexibleBool bool

func (v *flexibleBool) UnmarshalJSON(data []byte) error {
	raw := strings.ToLower(strings.Trim(strings.TrimSpace(string(data)), `"`))
	switch raw {
	case "", "null", "0", "false", "n", "no":
		*v = false
		return nil
	case "1", "true", "y", "yes":
		*v = true
		return nil
	default:
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return err
		}
		*v = flexibleBool(parsed)
		return nil
	}
}

type ShareURL struct {
	ShareCode   string `json:"share_code"`
	ReceiveCode string `json:"receive_code"`
	SourceURL   string `json:"source_url"`
}

type ShareItem struct {
	ID       string `json:"id"`
	ParentID string `json:"parent_id"`
	Name     string `json:"name"`
	IsDir    bool   `json:"is_dir"`
	Size     int64  `json:"size"`
}

type ShareSnapshot struct {
	ShareCode   string      `json:"share_code"`
	ReceiveCode string      `json:"receive_code"`
	RootID      string      `json:"root_id"`
	Name        string      `json:"name"`
	FileCount   int64       `json:"file_count"`
	TotalSize   int64       `json:"total_size"`
	Items       []ShareItem `json:"items"`
}

type ReceiveShareRequest struct {
	ShareCode   string `json:"share_code"`
	ReceiveCode string `json:"receive_code"`
	TargetCID   string `json:"target_cid"`
	FileID      string `json:"file_id,omitempty"`
}

type OfflineRequest struct {
	TargetCID string   `json:"target_cid"`
	URLs      []string `json:"urls"`
}

type OfflineTask struct {
	TaskID  string `json:"task_id"`
	URL     string `json:"url"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

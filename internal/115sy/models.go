package _115sy

import (
	"encoding/json"
	"fmt"
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

type UploadAvailability struct {
	UserID           int64    `json:"user_id"`
	UserKey          string   `json:"userkey"`
	SizeLimit        int64    `json:"size_limit"`
	UploadAllowed    bool     `json:"upload_allowed"`
	UploadAllowedMsg string   `json:"upload_allowed_msg"`
	TypeLimit        []string `json:"type_limit"`
}

func (a UploadAvailability) available() bool {
	return a.UserID != 0 && strings.TrimSpace(a.UserKey) != ""
}

type UploadHashes struct {
	SHA1       string
	PreSHA1    string
	Size       int64
	PreHashLen int64
}

type RapidUploadRequest struct {
	FileName  string
	ParentCID string
	Size      int64
	SHA1      string
	PreSHA1   string
}

type UploadInitResponse struct {
	Request   string `json:"request"`
	ErrorCode int    `json:"statuscode"`
	ErrorMsg  string `json:"statusmsg"`
	State     *bool  `json:"state,omitempty"`
	Errno     int    `json:"errno,omitempty"`
	Error     string `json:"error,omitempty"`
	Status    int    `json:"status"`
	PickCode  string `json:"pickcode"`
	Target    string `json:"target"`
	Version   string `json:"version"`
	FileID    string `json:"fileid"`
	FileInfo  string `json:"fileinfo"`
	SignKey   string `json:"sign_key"`
	SignCheck string `json:"sign_check"`
	Bucket    string `json:"bucket"`
	Object    string `json:"object"`
	Callback  struct {
		Callback    string `json:"callback"`
		CallbackVar string `json:"callback_var"`
	} `json:"callback"`
	SHA1 string `json:"-"`
}

func (r *UploadInitResponse) UnmarshalJSON(data []byte) error {
	type uploadInitAlias UploadInitResponse
	var raw struct {
		uploadInitAlias
		Status    flexibleInt    `json:"status"`
		FileID    flexibleString `json:"fileid"`
		ErrorCode flexibleInt    `json:"statuscode"`
		Errno     flexibleInt    `json:"errno"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*r = UploadInitResponse(raw.uploadInitAlias)
	r.Status = int(raw.Status.value)
	r.FileID = string(raw.FileID)
	r.ErrorCode = int(raw.ErrorCode.value)
	r.Errno = int(raw.Errno.value)
	return nil
}

func (r UploadInitResponse) RapidMatched() (bool, error) {
	switch r.Status {
	case 2:
		return true, nil
	case 1:
		return false, nil
	default:
		return false, &ProtocolError{Endpoint: EndpointUploadInit, Message: "unexpected upload init status"}
	}
}

func (r UploadInitResponse) needsRangeSignature() bool {
	switch r.Status {
	case 6, 7, 8:
		return strings.TrimSpace(r.SignKey) != "" && strings.TrimSpace(r.SignCheck) != ""
	default:
		return false
	}
}

type UploadOSSToken struct {
	AccessKeyID     string `json:"AccessKeyID"`
	AccessKeySecret string `json:"AccessKeySecret"`
	SecurityToken   string `json:"SecurityToken"`
	StatusCode      string `json:"StatusCode"`
	Endpoint        string `json:"Endpoint,omitempty"`
}

type UploadResult struct {
	State   bool   `json:"state"`
	Code    int    `json:"code"`
	Errno   int    `json:"errno"`
	Message string `json:"message"`
	Error   string `json:"error"`
	Data    struct {
		PickCode string `json:"pick_code"`
		FileName string `json:"file_name"`
		FileSize int64  `json:"file_size"`
		FileID   string `json:"file_id"`
		FID      string `json:"fid"`
		ThumbURL string `json:"thumb_url"`
		SHA1     string `json:"sha1"`
		CID      string `json:"cid"`
	} `json:"data"`
}

func (r UploadResult) RemoteItem(parentCID string) RemoteItem {
	id := firstNonEmpty(r.Data.FileID, r.Data.FID)
	return RemoteItem{
		ID:        id,
		Name:      r.Data.FileName,
		IsDir:     false,
		Size:      r.Data.FileSize,
		SHA1:      r.Data.SHA1,
		PickCode:  r.Data.PickCode,
		ParentCID: firstNonEmpty(r.Data.CID, parentCID),
		Thumbnail: r.Data.ThumbURL,
	}
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

type ShareTarget struct {
	Name string `json:"name"`
	CID  string `json:"cid"`
}

type ReceiveResult struct {
	State   bool            `json:"state"`
	Message string          `json:"message,omitempty"`
	TaskID  string          `json:"task_id,omitempty"`
	CID     string          `json:"cid,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type OfflineItemResult struct {
	URL      string `json:"url"`
	TaskID   string `json:"task_id,omitempty"`
	Success  bool   `json:"success"`
	Error    string `json:"error,omitempty"`
	ErrorMsg string `json:"error_msg,omitempty"`
}

type OfflineResult struct {
	Items []OfflineItemResult `json:"items"`
}

type OfflineTask struct {
	ID        string  `json:"id"`
	TaskID    string  `json:"task_id,omitempty"`
	InfoHash  string  `json:"info_hash"`
	Name      string  `json:"name"`
	URL       string  `json:"url"`
	Size      int64   `json:"size"`
	Progress  float64 `json:"progress"`
	Status    int     `json:"status"`
	Error     string  `json:"error,omitempty"`
	Message   string  `json:"message,omitempty"`
	FileID    string  `json:"file_id,omitempty"`
	TargetCID string  `json:"target_cid,omitempty"`
	UpdatedAt int64   `json:"updated_at,omitempty"`
}

func (t OfflineTask) Done() bool { return t.Status == 2 || t.Status == 11 }

func (t OfflineTask) Failed() bool { return t.Status < 0 || t.Status == 9 }

func ParseShareTargets(raw string) ([]ShareTarget, error) {
	parts := strings.Split(raw, ",")
	if len(parts) == 1 && strings.TrimSpace(parts[0]) == "" {
		return nil, fmt.Errorf("share target list is empty")
	}
	targets := make([]ShareTarget, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		name, cid, ok := strings.Cut(strings.TrimSpace(part), ":")
		name, cid = strings.TrimSpace(name), strings.TrimSpace(cid)
		if !ok || name == "" || cid == "" || cid == "0" {
			return nil, fmt.Errorf("invalid share target %q", part)
		}
		if _, err := strconv.ParseInt(cid, 10, 64); err != nil {
			return nil, fmt.Errorf("invalid share target cid %q", cid)
		}
		if _, exists := seen[cid]; exists {
			continue
		}
		seen[cid] = struct{}{}
		targets = append(targets, ShareTarget{Name: name, CID: cid})
	}
	return targets, nil
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

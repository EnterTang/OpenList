package _115sy

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

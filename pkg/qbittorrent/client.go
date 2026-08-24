package qbittorrent

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

type Client interface {
	AddFromLink(link string, savePath string, id string) error
	GetInfo(id string) (TorrentInfo, error)
	GetFiles(id string) ([]FileInfo, error)
	GetTorrentByHash(context.Context, string) (TorrentInfo, error)
	GetFilesByHash(context.Context, string) ([]FileInfo, error)
	StartByHash(context.Context, string) error
	StopByHash(context.Context, string) error
	DeleteByHash(context.Context, string, bool) error
	Delete(id string, deleteFiles bool) error
}

type client struct {
	url    *url.URL
	client http.Client
	Client
}

func New(webuiUrl string) (Client, error) {
	u, err := url.Parse(webuiUrl)
	if err != nil {
		return nil, err
	}

	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 2,
		IdleConnTimeout:     30 * time.Second,
		DisableKeepAlives:   false, // Enable connection reuse
	}

	var c = &client{
		url: u,
		client: http.Client{
			Jar:       jar,
			Transport: transport,
			Timeout:   30 * time.Second, // Set overall timeout
		},
	}

	err = c.checkAuthorization()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *client) checkAuthorization() error {
	// check authorization
	if c.authorized() {
		return nil
	}

	// check authorization after logging in
	err := c.login()
	if err != nil {
		return err
	}
	if c.authorized() {
		return nil
	}
	return errors.New("unauthorized qbittorrent url")
}

func (c *client) authorized() bool {
	resp, err := c.post("/api/v2/app/version", nil)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200 // the status code will be 403 if not authorized
}

func (c *client) login() error {
	// prepare HTTP request
	v := url.Values{}
	v.Set("username", c.url.User.Username())
	passwd, _ := c.url.User.Password()
	v.Set("password", passwd)
	resp, err := c.post("/api/v2/auth/login", v)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// avoid long waiting time if being upgraded to websocket connections (e.g. 101 responses)
	// as per API documentation, qBittorrent returns only 200 on successful login (qBittorrent < 5.2.0)
	// qBittorrent 5.2.0 /api/v2/auth/login returns HTTP 204 on success
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return errors.New("failed to login into qBittorrent webui with status code: " + resp.Status)
	}
	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	// check result
	body := make([]byte, 2)
	_, err = resp.Body.Read(body)
	if err != nil {
		return err
	}
	if string(body) != "Ok" {
		return errors.New("failed to login into qBittorrent webui: credentials were rejected")
	}
	return nil
}

func (c *client) post(path string, data url.Values) (*http.Response, error) {
	return c.postContext(context.Background(), path, data)
}

func (c *client) postContext(ctx context.Context, path string, data url.Values) (*http.Response, error) {
	u := c.url.JoinPath(path)
	u.User = nil // remove userinfo for requests

	var body io.Reader
	if data != nil {
		body = bytes.NewReader([]byte(data.Encode()))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return nil, err
	}
	if data != nil {
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.Cookies() != nil {
		c.client.Jar.SetCookies(u, resp.Cookies())
	}
	return resp, nil
}

func (c *client) AddFromLink(link string, savePath string, id string) error {
	err := c.checkAuthorization()
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	writer := multipart.NewWriter(buf)

	addField := func(name string, value string) {
		if err != nil {
			return
		}
		err = writer.WriteField(name, value)
	}
	addField("urls", link)
	addField("savepath", savePath)
	addField("tags", "openlist-"+id)
	addField("autoTMM", "false")
	if err != nil {
		return err
	}

	err = writer.Close()
	if err != nil {
		return err
	}

	u := c.url.JoinPath("/api/v2/torrents/add")
	u.User = nil // remove userinfo for requests
	req, err := http.NewRequest(http.MethodPost, u.String(), buf)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", writer.FormDataContentType())

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// qBittorrent 5.2.0 returns 204; older supported versions return 200
	// with an "Ok." body. Never include the source URL in failures because PT
	// download links may contain a passkey.
	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to add qBittorrent task: HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64))
	if err != nil {
		return err
	}
	result := strings.TrimSpace(string(body))
	if result != "Ok" && result != "Ok." {
		return errors.New("failed to add qBittorrent task: qBittorrent rejected the request")
	}
	return nil
}

type TorrentStatus string

const (
	ERROR              TorrentStatus = "error"
	MISSINGFILES       TorrentStatus = "missingFiles"
	UPLOADING          TorrentStatus = "uploading"
	PAUSEDUP           TorrentStatus = "pausedUP"
	QUEUEDUP           TorrentStatus = "queuedUP"
	STALLEDUP          TorrentStatus = "stalledUP"
	CHECKINGUP         TorrentStatus = "checkingUP"
	FORCEDUP           TorrentStatus = "forcedUP"
	ALLOCATING         TorrentStatus = "allocating"
	DOWNLOADING        TorrentStatus = "downloading"
	METADL             TorrentStatus = "metaDL"
	PAUSEDDL           TorrentStatus = "pausedDL"
	QUEUEDDL           TorrentStatus = "queuedDL"
	STALLEDDL          TorrentStatus = "stalledDL"
	CHECKINGDL         TorrentStatus = "checkingDL"
	FORCEDDL           TorrentStatus = "forcedDL"
	CHECKINGRESUMEDATA TorrentStatus = "checkingResumeData"
	MOVING             TorrentStatus = "moving"
	UNKNOWN            TorrentStatus = "unknown"
)

// https://github.com/DGuang21/PTGo/blob/main/app/client/client_distributer.go
type TorrentInfo struct {
	AddedOn           int           `json:"added_on"`           // 将 torrent 添加到客户端的时间（Unix Epoch）
	AmountLeft        int64         `json:"amount_left"`        // 剩余大小（字节）
	AutoTmm           bool          `json:"auto_tmm"`           // 此 torrent 是否由 Automatic Torrent Management 管理
	Availability      float64       `json:"availability"`       // 当前百分比
	Category          string        `json:"category"`           //
	Completed         int64         `json:"completed"`          // 完成的传输数据量（字节）
	CompletionOn      int           `json:"completion_on"`      // Torrent 完成的时间（Unix Epoch）
	ContentPath       string        `json:"content_path"`       // torrent 内容的绝对路径（多文件 torrent 的根路径，单文件 torrent 的绝对文件路径）
	DlLimit           int           `json:"dl_limit"`           // Torrent 下载速度限制（字节/秒）
	Dlspeed           int           `json:"dlspeed"`            // Torrent 下载速度（字节/秒）
	Downloaded        int64         `json:"downloaded"`         // 已经下载大小
	DownloadedSession int64         `json:"downloaded_session"` // 此会话下载的数据量
	Eta               int           `json:"eta"`                //
	FLPiecePrio       bool          `json:"f_l_piece_prio"`     // 如果第一个最后一块被优先考虑，则为true
	ForceStart        bool          `json:"force_start"`        // 如果为此 torrent 启用了强制启动，则为true
	Hash              string        `json:"hash"`               //
	LastActivity      int           `json:"last_activity"`      // 上次活跃的时间（Unix Epoch）
	MagnetURI         string        `json:"magnet_uri"`         // 与此 torrent 对应的 Magnet URI
	MaxRatio          float64       `json:"max_ratio"`          // 种子/上传停止种子前的最大共享比率
	MaxSeedingTime    int           `json:"max_seeding_time"`   // 停止种子种子前的最长种子时间（秒）
	Name              string        `json:"name"`               //
	NumComplete       int           `json:"num_complete"`       //
	NumIncomplete     int           `json:"num_incomplete"`     //
	NumLeechs         int           `json:"num_leechs"`         // 连接到的 leechers 的数量
	NumSeeds          int           `json:"num_seeds"`          // 连接到的种子数
	Priority          int           `json:"priority"`           // 速度优先。如果队列被禁用或 torrent 处于种子模式，则返回 -1
	Progress          float64       `json:"progress"`           // 进度
	Ratio             float64       `json:"ratio"`              // Torrent 共享比率
	RatioLimit        int           `json:"ratio_limit"`        //
	SavePath          string        `json:"save_path"`
	SeedingTime       int           `json:"seeding_time"`       // Torrent 完成用时（秒）
	SeedingTimeLimit  int           `json:"seeding_time_limit"` // max_seeding_time
	SeenComplete      int           `json:"seen_complete"`      // 上次 torrent 完成的时间
	SeqDl             bool          `json:"seq_dl"`             // 如果启用顺序下载，则为true
	Size              int64         `json:"size"`               //
	State             TorrentStatus `json:"state"`              // 参见https://github.com/qbittorrent/qBittorrent/wiki/WebUI-API-(qBittorrent-4.1)#get-torrent-list
	SuperSeeding      bool          `json:"super_seeding"`      // 如果启用超级播种，则为true
	Tags              string        `json:"tags"`               // Torrent 的逗号连接标签列表
	TimeActive        int           `json:"time_active"`        // 总活动时间（秒）
	TotalSize         int64         `json:"total_size"`         // 此 torrent 中所有文件的总大小（字节）（包括未选择的文件）
	Tracker           string        `json:"tracker"`            // 第一个具有工作状态的tracker。如果没有tracker在工作，则返回空字符串。
	TrackersCount     int           `json:"trackers_count"`     //
	UpLimit           int           `json:"up_limit"`           // 上传限制
	Uploaded          int64         `json:"uploaded"`           // 累计上传
	UploadedSession   int64         `json:"uploaded_session"`   // 当前session累计上传
	Upspeed           int           `json:"upspeed"`            // 上传速度（字节/秒）
}

type InfoNotFoundError struct {
	Id  string
	Err error
}

func (i InfoNotFoundError) Error() string {
	return "there should be exactly one task with tag \"openlist-" + i.Id + "\""
}

func NewInfoNotFoundError(id string) InfoNotFoundError {
	return InfoNotFoundError{Id: id}
}

func (c *client) GetInfo(id string) (TorrentInfo, error) {
	var infos []TorrentInfo

	err := c.checkAuthorization()
	if err != nil {
		return TorrentInfo{}, err
	}

	v := url.Values{}
	v.Set("tag", "openlist-"+id)
	response, err := c.post("/api/v2/torrents/info", v)
	if err != nil {
		return TorrentInfo{}, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return TorrentInfo{}, err
	}
	err = utils.Json.Unmarshal(body, &infos)
	if err != nil {
		return TorrentInfo{}, err
	}
	if len(infos) != 1 {
		return TorrentInfo{}, NewInfoNotFoundError(id)
	}
	return infos[0], nil
}

type FileInfo struct {
	Index        int     `json:"index"`
	Name         string  `json:"name"`
	Size         int64   `json:"size"`
	Progress     float32 `json:"progress"`
	Priority     int     `json:"priority"`
	IsSeed       bool    `json:"is_seed"`
	PieceRange   []int   `json:"piece_range"`
	Availability float32 `json:"availability"`
}

func (c *client) GetFiles(id string) ([]FileInfo, error) {
	var infos []FileInfo

	err := c.checkAuthorization()
	if err != nil {
		return []FileInfo{}, err
	}

	tInfo, err := c.GetInfo(id)
	if err != nil {
		return []FileInfo{}, err
	}

	v := url.Values{}
	v.Set("hash", tInfo.Hash)
	response, err := c.post("/api/v2/torrents/files", v)
	if err != nil {
		return []FileInfo{}, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return []FileInfo{}, err
	}
	err = utils.Json.Unmarshal(body, &infos)
	if err != nil {
		return []FileInfo{}, err
	}
	return infos, nil
}

func (c *client) GetTorrentByHash(ctx context.Context, hash string) (TorrentInfo, error) {
	normalized, err := normalizeTorrentHash(hash)
	if err != nil {
		return TorrentInfo{}, err
	}
	if err := c.checkAuthorization(); err != nil {
		return TorrentInfo{}, err
	}
	return c.getTorrentByHash(ctx, normalized)
}

func (c *client) getTorrentByHash(ctx context.Context, hash string) (TorrentInfo, error) {
	v := url.Values{}
	v.Set("hashes", hash)
	response, err := c.postContext(ctx, "/api/v2/torrents/info", v)
	if err != nil {
		return TorrentInfo{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return TorrentInfo{}, fmt.Errorf("failed to query qBittorrent torrent info: %s", response.Status)
	}
	var infos []TorrentInfo
	if err := json.NewDecoder(response.Body).Decode(&infos); err != nil {
		return TorrentInfo{}, err
	}
	if len(infos) != 1 || !strings.EqualFold(strings.TrimSpace(infos[0].Hash), hash) {
		return TorrentInfo{}, NewInfoNotFoundError(hash)
	}
	return infos[0], nil
}

func (c *client) GetFilesByHash(ctx context.Context, hash string) ([]FileInfo, error) {
	normalized, err := normalizeTorrentHash(hash)
	if err != nil {
		return nil, err
	}
	if err := c.checkAuthorization(); err != nil {
		return nil, err
	}
	v := url.Values{}
	v.Set("hash", normalized)
	response, err := c.postContext(ctx, "/api/v2/torrents/files", v)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to query qBittorrent torrent files: %s", response.Status)
	}
	var files []FileInfo
	if err := json.NewDecoder(response.Body).Decode(&files); err != nil {
		return nil, err
	}
	return files, nil
}

func (c *client) StartByHash(ctx context.Context, hash string) error {
	return c.controlByHash(ctx, "/api/v2/torrents/start", "/api/v2/torrents/resume", hash)
}

func (c *client) StopByHash(ctx context.Context, hash string) error {
	return c.controlByHash(ctx, "/api/v2/torrents/stop", "/api/v2/torrents/pause", hash)
}

func (c *client) controlByHash(ctx context.Context, endpoint, legacyEndpoint, hash string) error {
	normalized, err := normalizeTorrentHash(hash)
	if err != nil {
		return err
	}
	if err := c.checkAuthorization(); err != nil {
		return err
	}
	v := url.Values{}
	v.Set("hashes", normalized)
	response, err := c.postContext(ctx, endpoint, v)
	if err != nil {
		return err
	}
	if response.StatusCode == http.StatusNotFound && legacyEndpoint != "" {
		_ = response.Body.Close()
		response, err = c.postContext(ctx, legacyEndpoint, v)
		if err != nil {
			return err
		}
		endpoint = legacyEndpoint
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("qBittorrent %s failed: %s", path.Base(endpoint), response.Status)
	}
	return nil
}

func (c *client) DeleteByHash(ctx context.Context, hash string, deleteFiles bool) error {
	normalized, err := normalizeTorrentHash(hash)
	if err != nil {
		return err
	}
	if err := c.checkAuthorization(); err != nil {
		return err
	}
	v := url.Values{}
	v.Set("hashes", normalized)
	v.Set("deleteFiles", strconv.FormatBool(deleteFiles))
	response, err := c.postContext(ctx, "/api/v2/torrents/delete", v)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed to delete qBittorrent torrent: %s", response.Status)
	}
	return nil
}

func normalizeTorrentHash(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) != 40 && len(value) != 64 {
		return "", errors.New("torrent hash must contain 40 or 64 hexadecimal characters")
	}
	if _, err := hex.DecodeString(value); err != nil {
		return "", errors.New("torrent hash must contain only hexadecimal characters")
	}
	return value, nil
}

func (c *client) Delete(id string, deleteFiles bool) error {
	err := c.checkAuthorization()
	if err != nil {
		return err
	}

	info, err := c.GetInfo(id)
	if err != nil {
		return err
	}
	v := url.Values{}
	v.Set("hashes", info.Hash)
	if deleteFiles {
		v.Set("deleteFiles", "true")
	} else {
		v.Set("deleteFiles", "false")
	}
	deleteResp, err := c.post("/api/v2/torrents/delete", v)
	if err != nil {
		return err
	}
	defer deleteResp.Body.Close()
	if deleteResp.StatusCode != 200 {
		return errors.New("failed to delete qbittorrent task")
	}

	v = url.Values{}
	v.Set("tags", "openlist-"+id)
	deleteTagsResp, err := c.post("/api/v2/torrents/deleteTags", v)
	if err != nil {
		return err
	}
	defer deleteTagsResp.Body.Close()
	if deleteTagsResp.StatusCode != 200 {
		return errors.New("failed to delete qbittorrent tag")
	}
	return nil
}

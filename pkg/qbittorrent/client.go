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
	GetTorrents(context.Context) ([]TorrentInfo, error)
	GetTorrentByHash(context.Context, string) (TorrentInfo, error)
	GetFilesByHash(context.Context, string) ([]FileInfo, error)
	StartByHash(context.Context, string) error
	StopByHash(context.Context, string) error
	DeleteByHash(context.Context, string, bool) error
	Delete(id string, deleteFiles bool) error
}

// FreeSpaceClient is implemented by qBittorrent clients that support the
// path-aware free-space API. It is kept separate from Client so older test
// doubles and integrations can continue to use the core torrent operations.
type FreeSpaceClient interface {
	GetFreeSpaceAtPath(context.Context, string) (uint64, error)
}

// GlobalFreeSpaceClient is implemented by qBittorrent clients that expose the
// free space for qBittorrent's default save path. It is a safe compatibility
// fallback only when the caller has a single qB path mapping; unlike
// FreeSpaceClient, this API cannot distinguish multiple qB volumes.
type GlobalFreeSpaceClient interface {
	GetFreeSpace(context.Context) (uint64, error)
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
	resp, err := c.getContext(context.Background(), "/api/v2/app/version", nil)
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
	return c.requestContext(ctx, http.MethodPost, path, data)
}

func (c *client) getContext(ctx context.Context, path string, data url.Values) (*http.Response, error) {
	return c.requestContext(ctx, http.MethodGet, path, data)
}

func (c *client) requestContext(ctx context.Context, method, endpoint string, data url.Values) (*http.Response, error) {
	var body []byte
	contentType := ""
	if method != http.MethodGet && data != nil {
		body = []byte(data.Encode())
		contentType = "application/x-www-form-urlencoded"
	}
	return c.requestBytesContext(ctx, method, endpoint, data, body, contentType)
}

func (c *client) requestBytesContext(ctx context.Context, method, endpoint string, query url.Values, body []byte, contentType string) (*http.Response, error) {
	base := c.url.JoinPath(endpoint)
	base.User = nil // remove userinfo for requests
	resp, err := c.doRequest(ctx, method, base, query, body, contentType)
	if err != nil {
		return nil, err
	}

	// Some reverse proxies expose qBittorrent over HTTPS while an old or
	// misconfigured caller still uses an HTTP WebUI URL. qBittorrent/Caddy
	// reports this explicitly. Retry once over HTTPS so health checks and
	// ordinary requests can recover without weakening a correctly configured
	// HTTPS endpoint or silently downgrading one.
	if strings.EqualFold(c.url.Scheme, "http") && responseSaysHTTPSRequired(resp) {
		_ = resp.Body.Close()
		retryURL := *base
		retryURL.Scheme = "https"
		resp, err = c.doRequest(ctx, method, &retryURL, query, body, contentType)
		if err != nil {
			return nil, err
		}
		base = &retryURL
	}
	if resp.Cookies() != nil {
		c.client.Jar.SetCookies(base, resp.Cookies())
	}
	return resp, nil
}

func (c *client) doRequest(ctx context.Context, method string, endpoint *url.URL, params url.Values, body []byte, contentType string) (*http.Response, error) {
	u := *endpoint
	if method == http.MethodGet && params != nil {
		query := u.Query()
		for key, values := range params {
			for _, value := range values {
				query.Add(key, value)
			}
		}
		u.RawQuery = query.Encode()
	}

	var requestBody io.Reader
	if len(body) > 0 {
		requestBody = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), requestBody)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	origin := u.Scheme + "://" + u.Host + "/"
	req.Header.Set("Origin", strings.TrimSuffix(origin, "/"))
	req.Header.Set("Referer", origin)
	return c.client.Do(req)
}

func responseSaysHTTPSRequired(resp *http.Response) bool {
	if resp == nil || resp.Body == nil || resp.StatusCode < http.StatusBadRequest {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(body))
	if err != nil {
		return false
	}
	message := strings.ToLower(string(body))
	return strings.Contains(message, "client sent an http request to an https server") ||
		strings.Contains(message, "http request was sent to an https server")
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

	resp, err := c.requestBytesContext(context.Background(), http.MethodPost, "/api/v2/torrents/add", nil, buf.Bytes(), writer.FormDataContentType())
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
	if i.Err != nil {
		return i.Err.Error()
	}
	return "there should be exactly one task with tag \"openlist-" + i.Id + "\""
}

func NewInfoNotFoundError(id string) InfoNotFoundError {
	return InfoNotFoundError{Id: id}
}

func NewTorrentNotFoundError(hash string) InfoNotFoundError {
	return InfoNotFoundError{Id: hash, Err: fmt.Errorf("qBittorrent torrent hash %q was not found", hash)}
}

func (c *client) GetInfo(id string) (TorrentInfo, error) {
	if normalized, err := normalizeTorrentHash(id); err == nil {
		return c.GetTorrentByHash(context.Background(), normalized)
	}
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

// GetTorrents returns the complete qB torrent list. It is used by Worker
// capacity policies because those policies must also cover downloads that
// have not yet been associated with a MoviePilot binding.
func (c *client) GetTorrents(ctx context.Context) ([]TorrentInfo, error) {
	if err := c.checkAuthorization(); err != nil {
		return nil, err
	}
	response, err := c.getContext(ctx, "/api/v2/torrents/info", nil)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to query qBittorrent torrent list: %s", response.Status)
	}
	var infos []TorrentInfo
	if err := json.NewDecoder(response.Body).Decode(&infos); err != nil {
		return nil, err
	}
	return infos, nil
}

func (c *client) getTorrentByHash(ctx context.Context, hash string) (TorrentInfo, error) {
	v := url.Values{}
	v.Set("hashes", hash)
	response, err := c.getContext(ctx, "/api/v2/torrents/info", v)
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
		return TorrentInfo{}, NewTorrentNotFoundError(hash)
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
	response, err := c.getContext(ctx, "/api/v2/torrents/files", v)
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

// GetFreeSpaceAtPath returns the number of free bytes on the qBittorrent host
// for a qB-visible path. The endpoint was added in WebAPI 2.15.2; callers can
// use ErrFreeSpaceAtPathUnsupported to fall back for older qBittorrent builds.
func (c *client) GetFreeSpaceAtPath(ctx context.Context, qbPath string) (uint64, error) {
	qbPath = strings.TrimSpace(qbPath)
	if qbPath == "" {
		return 0, errors.New("qB path is required for free-space lookup")
	}
	if err := c.checkAuthorization(); err != nil {
		return 0, err
	}
	values := url.Values{}
	values.Set("path", qbPath)
	response, err := c.postContext(ctx, "/api/v2/app/getFreeSpaceAtPathAction", values)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return 0, fmt.Errorf("%w: %s", ErrFreeSpaceAtPathUnsupported, response.Status)
	}
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("failed to query qBittorrent free space at path: %s", response.Status)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 128))
	if err != nil {
		return 0, err
	}
	free, err := strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
	if err != nil || free < 0 {
		if err == nil {
			err = errors.New("qBittorrent returned a negative free-space value")
		}
		return 0, fmt.Errorf("invalid qBittorrent free-space response: %w", err)
	}
	return uint64(free), nil
}

var ErrFreeSpaceAtPathUnsupported = errors.New("qBittorrent path-aware free-space API is unsupported")

// GetFreeSpace returns qBittorrent's free space for its default save path.
// This endpoint predates the path-aware API and is available on older qB
// versions, including WebAPI 2.15.1.
func (c *client) GetFreeSpace(ctx context.Context) (uint64, error) {
	if err := c.checkAuthorization(); err != nil {
		return 0, err
	}
	response, err := c.getContext(ctx, "/api/v2/sync/maindata", nil)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("failed to query qBittorrent global free space: %s", response.Status)
	}
	var data struct {
		ServerState struct {
			FreeSpaceOnDisk *int64 `json:"free_space_on_disk"`
		} `json:"server_state"`
	}
	if err := json.NewDecoder(response.Body).Decode(&data); err != nil {
		return 0, err
	}
	if data.ServerState.FreeSpaceOnDisk == nil {
		return 0, errors.New("qBittorrent global free-space value is missing")
	}
	if *data.ServerState.FreeSpaceOnDisk < 0 {
		return 0, errors.New("qBittorrent returned a negative global free-space value")
	}
	return uint64(*data.ServerState.FreeSpaceOnDisk), nil
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
	if hash, err := normalizeTorrentHash(id); err == nil {
		return c.DeleteByHash(context.Background(), hash, deleteFiles)
	}
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

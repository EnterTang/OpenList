package subscription

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
)

const pan123APIURL = "https://yun.123pan.com/b/api"

const (
	pan123ShareSaveStatusConfirmed     = "confirmed"
	pan123ShareSaveStatusRetryable     = "retryable"
	pan123ShareSaveStatusResultUnknown = "result_unknown"
	pan123ShareSaveStatusTerminal      = "terminal"

	pan123ClusterErrorCodeShareSaveRetryable     = "share_save_retryable"
	pan123ClusterErrorCodeShareSaveResultUnknown = "share_save_result_unknown"
	pan123ClusterErrorCodeShareSaveTerminal      = "share_save_terminal"

	pan123ShareSaveResultTokenPrefix = "pan123_result:"
)

type pan123ShareProvider struct {
	cfg     model.SubscriptionTelegramPanConfig
	apiURL  string
	client  *resty.Client
	limiter *rate.Limiter
}

var pan123ShareLimiters sync.Map

const pan123ShareRequestInterval = 350 * time.Millisecond

func NewPan123ShareProvider(cfg model.SubscriptionTelegramPanConfig) ShareSaver {
	cfg = normalizeTelegramPanConfig(cfg)
	key := strings.TrimSpace(cfg.AccessToken)
	if key == "" {
		key = "anonymous"
	}
	digest := sha256.Sum256([]byte(key))
	limiterKey := fmt.Sprintf("%x", digest[:])
	limiterValue, _ := pan123ShareLimiters.LoadOrStore(limiterKey, rate.NewLimiter(rate.Every(pan123ShareRequestInterval), 1))
	return &pan123ShareProvider{
		cfg:     cfg,
		apiURL:  pan123APIURL,
		client:  newShareHTTPClient(),
		limiter: limiterValue.(*rate.Limiter),
	}
}

func (p *pan123ShareProvider) Name() ShareProviderName {
	return ShareProviderPan123
}

func (p *pan123ShareProvider) ParseURL(raw string) (ShareRef, error) {
	ref, err := ParseShareURL(raw)
	if err != nil {
		return ShareRef{}, err
	}
	if ref.Provider != ShareProviderPan123 {
		return ShareRef{}, fmt.Errorf("share URL provider = %s, want %s", ref.Provider, ShareProviderPan123)
	}
	return ref, nil
}

func (p *pan123ShareProvider) EnsureDir(ctx context.Context, path string) (string, error) {
	path = utils.FixAndCleanPath(path)
	if path == "" || path == "/" {
		return "", errors.New("temp transfer root is empty")
	}
	if err := ensureDir(ctx, path); err != nil {
		return "", err
	}
	obj, err := fs.Get(ctx, path, &fs.GetArgs{NoLog: true})
	if err != nil {
		return "", err
	}
	if obj == nil || obj.GetID() == "" {
		return "", errors.Errorf("temp transfer root has no remote id: %s", path)
	}
	return obj.GetID(), nil
}

func (p *pan123ShareProvider) ListShareChildren(ctx context.Context, ref ShareRef, parentID string) ([]ShareItem, error) {
	if fastLink, err := parsePan123FastLinkFile(ref.RawURL); err == nil {
		if parentID != "" && parentID != "0" {
			return nil, nil
		}
		return []ShareItem{fastLink.shareItem("0")}, nil
	}
	parentID = firstNonEmpty(parentID, "0")
	items := make([]ShareItem, 0)
	seen := make(map[string]struct{})
	for page := 1; ; page++ {
		var resp pan123ListResp
		req := p.request(ctx).
			SetQueryParams(map[string]string{
				"limit":          "100",
				"next":           "0",
				"orderBy":        "file_name",
				"orderDirection": "asc",
				"shareKey":       ref.ShareID,
				"ParentFileId":   parentID,
				"Page":           strconv.Itoa(page),
				"event":          "homeListFile",
				"operateType":    "1",
			})
		if ref.Passcode != "" {
			req.SetQueryParam("SharePwd", ref.Passcode)
		}
		httpResp, err := req.Get(p.apiURL + "/share/get")
		if err != nil {
			return nil, err
		}
		if err := decodePan123JSON(httpResp, &resp); err != nil {
			return nil, err
		}
		if resp.Code != 0 {
			return nil, pan123Error(resp.Message)
		}
		for _, item := range resp.Data.InfoList {
			key := strconv.FormatInt(item.FileID, 10)
			if key == "0" {
				key = fmt.Sprintf("%s|%d|%d", item.FileName, item.Type, item.Size)
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			items = append(items, item.shareItem(parentID))
		}
		if len(resp.Data.InfoList) == 0 || strings.TrimSpace(resp.Data.Next) == "-1" {
			break
		}
	}
	return items, nil
}

// GetShareDownloadURL resolves a short-lived 123Pan share URL. The URL is an
// execution-time value: callers must keep it in worker memory and reacquire it
// after expiry instead of persisting or forwarding it through the coordinator.
func (p *pan123ShareProvider) GetShareDownloadURL(ctx context.Context, ref ShareRef, item ShareItem) (ShareDownloadLink, error) {
	raw := shareItemRawMap(item)
	request := p.request(ctx).SetBody(map[string]any{
		"shareKey":  ref.ShareID,
		"SharePwd":  ref.Passcode,
		"etag":      rawString(raw, "etag"),
		"fileId":    item.ID,
		"s3keyFlag": firstNonEmpty(rawString(raw, "s3key_flag"), rawString(raw, "s3KeyFlag")),
		"size":      item.Size,
	})
	if token := strings.TrimSpace(p.cfg.AccessToken); token != "" {
		request.SetHeader("authorization", "Bearer "+token)
	}
	resp, err := request.Post(p.apiURL + "/share/download/info")
	if err != nil {
		return ShareDownloadLink{}, err
	}
	var payload struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			DownloadURL string `json:"DownloadURL"`
		} `json:"data"`
	}
	if err := decodePan123JSON(resp, &payload); err != nil {
		return ShareDownloadLink{}, err
	}
	if payload.Code != 0 {
		return ShareDownloadLink{}, pan123Error(payload.Message)
	}
	downloadURL := strings.TrimSpace(payload.Data.DownloadURL)
	if downloadURL == "" {
		return ShareDownloadLink{}, errors.New("123pan share download URL is empty")
	}
	resolved, headers, err := resolvePan123ShareRedirect(ctx, p.client, downloadURL)
	if err != nil {
		return ShareDownloadLink{}, err
	}
	return ShareDownloadLink{
		URL:     resolved,
		Headers: headers,
		FileID:  item.ID,
		Size:    item.Size,
		Hash:    rawString(raw, "etag"),
	}, nil
}

func (p *pan123ShareProvider) SaveShareItems(ctx context.Context, ref ShareRef, parentID string, items []ShareItem, dstDirID string) ([]string, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if p.cfg.AccessToken == "" {
		return nil, errors.New("123pan access_token is required")
	}
	results := make([]string, 0, len(items))
	for _, item := range items {
		if item.IsDir {
			return nil, errors.Errorf("123pan save directory is not supported yet: %s", item.Name)
		}
		raw := shareItemRawMap(item)
		etag := rawString(raw, "etag")
		name := firstNonEmpty(rawString(raw, "file_name"), item.Name)
		size := rawInt64(raw, "size", item.Size)
		var resp pan123UploadRequestResp
		httpResp, err := p.request(ctx).
			SetHeader("authorization", "Bearer "+p.cfg.AccessToken).
			SetBody(map[string]any{
				"driveId":      0,
				"etag":         etag,
				"fileName":     name,
				"parentFileId": dstDirID,
				"size":         size,
				"type":         0,
				"duplicate":    2, // overwrite existing same-name file on retry
			}).
			Post(p.apiURL + "/file/upload_request")
		if err != nil {
			confirmed, probeErr := p.probeSavedItem(ctx, dstDirID, name, size, etag)
			if probeErr == nil && confirmed.FileID != "" {
				results = append(results, encodePan123ShareSaveResult(confirmed))
				continue
			}
			results = append(results, encodePan123ShareSaveResult(pan123ShareSaveResult{
				Status:  pan123ShareSaveStatusResultUnknown,
				Name:    name,
				Message: firstNonEmpty(err.Error(), "123pan save request failed before result could be confirmed"),
			}))
			continue
		}
		if err := decodePan123JSON(httpResp, &resp); err != nil {
			confirmed, probeErr := p.probeSavedItem(ctx, dstDirID, name, size, etag)
			if probeErr == nil && confirmed.FileID != "" {
				results = append(results, encodePan123ShareSaveResult(confirmed))
				continue
			}
			status := classifyPan123ShareSaveHTTPResult(httpResp)
			if status == "" {
				status = pan123ShareSaveStatusResultUnknown
			}
			results = append(results, encodePan123ShareSaveResult(pan123ShareSaveResult{
				Status:  status,
				Name:    name,
				Message: firstNonEmpty(err.Error(), "123pan save response could not be decoded"),
			}))
			continue
		}
		if resp.Code != 0 {
			results = append(results, encodePan123ShareSaveResult(pan123ShareSaveResult{
				Status:  classifyPan123ShareSaveAPIResult(resp.Code),
				Name:    name,
				Message: firstNonEmpty(strings.TrimSpace(resp.Message), "123pan save request failed"),
			}))
			continue
		}
		if resp.Data.Info.FileID > 0 {
			results = append(results, encodePan123ShareSaveResult(pan123ShareSaveResult{
				Status: pan123ShareSaveStatusConfirmed,
				Name:   name,
				FileID: strconv.FormatInt(resp.Data.Info.FileID, 10),
			}))
			continue
		}
		confirmed, probeErr := p.probeSavedItem(ctx, dstDirID, name, size, etag)
		if probeErr == nil && confirmed.FileID != "" {
			results = append(results, encodePan123ShareSaveResult(confirmed))
			continue
		}
		results = append(results, encodePan123ShareSaveResult(pan123ShareSaveResult{
			Status:  pan123ShareSaveStatusResultUnknown,
			Name:    name,
			Message: "123pan save request succeeded without a confirmed file id",
		}))
	}
	return results, nil
}

func (p *pan123ShareProvider) WaitSaveComplete(ctx context.Context, taskIDs []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, taskID := range taskIDs {
		result, ok := decodePan123ShareSaveResult(taskID)
		if !ok || result.Status == pan123ShareSaveStatusConfirmed {
			continue
		}
		return &pan123ShareSaveWaitError{result: result}
	}
	return nil
}

func (p *pan123ShareProvider) request(ctx context.Context) *resty.Request {
	if p.limiter != nil {
		// A canceled context is still passed to Resty below; ignoring the
		// limiter error here preserves the provider's existing error surface.
		_ = p.limiter.Wait(ctx)
	}
	return p.client.R().
		SetContext(ctx).
		SetHeader("origin", "https://yun.123pan.com").
		SetHeader("referer", "https://yun.123pan.com/").
		SetHeader("platform", "web").
		SetHeader("app-version", "3")
}

func resolvePan123ShareRedirect(ctx context.Context, client *resty.Client, rawURL string) (string, map[string]string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", nil, errors.New("invalid 123pan share download URL")
	}
	if params := u.Query().Get("params"); params != "" {
		decoded, err := base64.StdEncoding.DecodeString(params)
		if err != nil {
			return "", nil, errors.New("invalid 123pan share download parameters")
		}
		decodedURL, err := url.Parse(string(decoded))
		if err != nil {
			return "", nil, errors.New("invalid 123pan share download target")
		}
		u = decodedURL
	}
	transport := http.DefaultTransport
	if client != nil && client.GetClient().Transport != nil {
		transport = client.GetClient().Transport
	}
	httpClient := &http.Client{Transport: transport, CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", nil, err
	}
	request.Header.Set("Referer", "https://yun.123pan.com/")
	response, err := httpClient.Do(request)
	if err != nil {
		return "", nil, err
	}
	defer response.Body.Close()
	resolved := strings.TrimSpace(response.Header.Get("Location"))
	if resolved == "" && response.StatusCode >= 200 && response.StatusCode < 300 {
		payloadBytes, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		if readErr != nil {
			return "", nil, readErr
		}
		var payload struct {
			Data struct {
				RedirectURL string `json:"redirect_url"`
			} `json:"data"`
		}
		if json.Unmarshal(payloadBytes, &payload) == nil {
			resolved = strings.TrimSpace(payload.Data.RedirectURL)
		}
	}
	if resolved == "" && response.StatusCode >= 200 && response.StatusCode < 300 {
		resolved = u.String()
	}
	if resolved == "" {
		return "", nil, fmt.Errorf("123pan share download redirect failed with status %d", response.StatusCode)
	}
	return resolved, map[string]string{"Referer": "https://yun.123pan.com/"}, nil
}

func decodePan123JSON(resp *resty.Response, out any) error {
	if resp == nil {
		return errors.New("empty 123pan response")
	}
	if err := json.Unmarshal(resp.Body(), out); err != nil {
		return errors.WithMessage(err, "decode 123pan response")
	}
	return nil
}

func pan123Error(message string) error {
	if strings.TrimSpace(message) == "" {
		return errors.New("123pan request failed")
	}
	return errors.New(message)
}

func classifyPan123ShareSaveHTTPResult(resp *resty.Response) string {
	if resp == nil {
		return pan123ShareSaveStatusResultUnknown
	}
	switch status := resp.StatusCode(); {
	case status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= http.StatusInternalServerError:
		return pan123ShareSaveStatusRetryable
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return pan123ShareSaveStatusTerminal
	default:
		return ""
	}
}

func classifyPan123ShareSaveAPIResult(code int) string {
	switch code {
	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		return pan123ShareSaveStatusRetryable
	case http.StatusUnauthorized, http.StatusForbidden:
		return pan123ShareSaveStatusTerminal
	default:
		return pan123ShareSaveStatusTerminal
	}
}

func (p *pan123ShareProvider) probeSavedItem(ctx context.Context, parentID, name string, size int64, etag string) (pan123ShareSaveResult, error) {
	parentID = strings.TrimSpace(parentID)
	if parentID == "" {
		return pan123ShareSaveResult{}, errors.New("123pan probe destination folder id is empty")
	}
	page := 1
	for {
		var resp pan123ListResp
		httpResp, err := p.request(ctx).
			SetHeader("authorization", "Bearer "+p.cfg.AccessToken).
			SetQueryParams(map[string]string{
				"driveId":              "0",
				"limit":                "100",
				"next":                 "0",
				"orderBy":              "file_name",
				"orderDirection":       "asc",
				"parentFileId":         parentID,
				"trashed":              "false",
				"SearchData":           "",
				"Page":                 strconv.Itoa(page),
				"OnlyLookAbnormalFile": "0",
				"event":                "homeListFile",
				"operateType":          "4",
				"inDirectSpace":        "false",
			}).
			Get(p.apiURL + "/file/list/new")
		if err != nil {
			return pan123ShareSaveResult{}, err
		}
		if err := decodePan123JSON(httpResp, &resp); err != nil {
			return pan123ShareSaveResult{}, err
		}
		if resp.Code != 0 {
			return pan123ShareSaveResult{}, pan123Error(resp.Message)
		}
		for _, item := range resp.Data.InfoList {
			if item.FileID <= 0 || strings.TrimSpace(item.FileName) != name || item.Size != size {
				continue
			}
			if etag != "" && !strings.EqualFold(strings.TrimSpace(item.Etag), etag) {
				continue
			}
			return pan123ShareSaveResult{
				Status: pan123ShareSaveStatusConfirmed,
				Name:   name,
				FileID: strconv.FormatInt(item.FileID, 10),
			}, nil
		}
		if len(resp.Data.InfoList) == 0 || strings.TrimSpace(resp.Data.Next) == "-1" {
			break
		}
		page++
	}
	return pan123ShareSaveResult{}, errors.New("123pan save probe did not find the destination file")
}

type pan123ShareSaveResult struct {
	Status  string `json:"status"`
	Name    string `json:"name,omitempty"`
	FileID  string `json:"file_id,omitempty"`
	Message string `json:"message,omitempty"`
}

func encodePan123ShareSaveResult(result pan123ShareSaveResult) string {
	body, err := json.Marshal(result)
	if err != nil {
		return pan123ShareSaveResultTokenPrefix + base64.RawURLEncoding.EncodeToString([]byte(`{"status":"terminal","message":"encode 123pan save result"}`))
	}
	return pan123ShareSaveResultTokenPrefix + base64.RawURLEncoding.EncodeToString(body)
}

func decodePan123ShareSaveResult(taskID string) (pan123ShareSaveResult, bool) {
	taskID = strings.TrimSpace(taskID)
	if !strings.HasPrefix(taskID, pan123ShareSaveResultTokenPrefix) {
		return pan123ShareSaveResult{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(taskID, pan123ShareSaveResultTokenPrefix))
	if err != nil {
		return pan123ShareSaveResult{}, false
	}
	var result pan123ShareSaveResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return pan123ShareSaveResult{}, false
	}
	return result, true
}

type pan123ShareSaveWaitError struct {
	result pan123ShareSaveResult
}

func (e *pan123ShareSaveWaitError) Error() string {
	if e == nil {
		return ""
	}
	return firstNonEmpty(e.result.Message, fmt.Sprintf("123pan save %s: %s", e.result.Status, e.result.Name))
}

func (e *pan123ShareSaveWaitError) ClusterErrorCode() string {
	if e == nil {
		return ""
	}
	switch e.result.Status {
	case pan123ShareSaveStatusRetryable:
		return pan123ClusterErrorCodeShareSaveRetryable
	case pan123ShareSaveStatusResultUnknown:
		return pan123ClusterErrorCodeShareSaveResultUnknown
	default:
		return pan123ClusterErrorCodeShareSaveTerminal
	}
}

type pan123ListResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		InfoList []pan123File `json:"InfoList"`
		Next     string       `json:"Next"`
	} `json:"data"`
}

type pan123File struct {
	FileID   int64  `json:"FileId"`
	FileName string `json:"FileName"`
	Type     int    `json:"Type"`
	Size     int64  `json:"Size"`
	Etag     string `json:"Etag"`
	UpdateAt string `json:"UpdateAt"`
}

func (f pan123File) shareItem(parentID string) ShareItem {
	id := strconv.FormatInt(f.FileID, 10)
	isDir := f.Type == 1
	return ShareItem{
		ID:       id,
		ParentID: parentID,
		Name:     f.FileName,
		Size:     f.Size,
		Modified: parsePan123Time(f.UpdateAt),
		IsDir:    isDir,
		Raw: map[string]any{
			"file_id":   id,
			"etag":      f.Etag,
			"size":      f.Size,
			"file_name": f.FileName,
			"type":      f.Type,
		},
	}
}

type pan123UploadRequestResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Info struct {
			FileID int64 `json:"FileId"`
		} `json:"Info"`
	} `json:"data"`
}

func parsePan123Time(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed
	}
	if parsed, err := time.Parse("2006-01-02 15:04:05", value); err == nil {
		return parsed
	}
	return time.Time{}
}

func shareItemRawMap(item ShareItem) map[string]any {
	if raw, ok := item.Raw.(map[string]any); ok {
		return raw
	}
	return nil
}

func rawString(raw map[string]any, key string) string {
	if raw == nil {
		return ""
	}
	switch value := raw[key].(type) {
	case string:
		return strings.TrimSpace(value)
	case fmt.Stringer:
		return strings.TrimSpace(value.String())
	default:
		return ""
	}
}

func rawInt64(raw map[string]any, key string, fallback int64) int64 {
	if raw == nil {
		return fallback
	}
	switch value := raw[key].(type) {
	case int:
		return int64(value)
	case int64:
		return value
	case float64:
		return int64(value)
	case json.Number:
		parsed, err := value.Int64()
		if err == nil {
			return parsed
		}
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

var _ ShareSaver = (*pan123ShareProvider)(nil)
var _ ShareDirectDownloader = (*pan123ShareProvider)(nil)

package subscription

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	_115sy "github.com/OpenListTeam/OpenList/v4/internal/115sy"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
)

const pan115WebURL = "https://115cdn.com"

const (
	// 115 share APIs are sensitive to burst traffic. Keep one process-wide
	// schedule for all share/list/receive requests and retries.
	pan115RequestInterval    = time.Second
	pan115MaxRequestAttempts = 3
	pan115RetryBaseDelay     = time.Second
	pan115ConfirmationTries  = 3
	pan115ConfirmationDelay  = 500 * time.Millisecond
)

var pan115RequestLimiter = newPan115RateLimiter(pan115RequestInterval)

type pan115RateLimiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

func newPan115RateLimiter(interval time.Duration) *pan115RateLimiter {
	return &pan115RateLimiter{interval: interval}
}

func (l *pan115RateLimiter) wait(ctx context.Context) error {
	if l == nil || l.interval <= 0 {
		return nil
	}
	l.mu.Lock()
	now := time.Now()
	waitUntil := now
	if l.next.After(waitUntil) {
		waitUntil = l.next
	}
	l.next = waitUntil.Add(l.interval)
	l.mu.Unlock()

	if delay := time.Until(waitUntil); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}

const (
	pan115ClusterErrorCodeShareSaveCredentialsInvalid = "share_save_credentials_invalid"
	pan115ClusterErrorCodeShareSaveRateLimited        = "share_save_rate_limited"
	pan115ClusterErrorCodeShareSaveMethodNotAllowed   = "share_save_method_not_allowed"
	pan115ClusterErrorCodeShareSaveSourceInvalid      = "share_save_source_invalid"
	pan115ClusterErrorCodeShareSaveGatewayResponse    = "share_save_gateway_response"
	pan115ClusterErrorCodeShareSaveTransient          = "share_save_transient"
	pan115ClusterErrorCodeShareSaveResultUnknown      = "share_save_result_unknown"
)

type pan115ShareProvider struct {
	cfg            model.SubscriptionTelegramPanConfig
	webURL         string
	client         *resty.Client
	limiter        *pan115RateLimiter
	retryBaseDelay time.Duration
	confirmClient  *_115sy.Client
	confirmEnabled bool
}

func NewPan115ShareProvider(cfg model.SubscriptionTelegramPanConfig) ShareSaver {
	cfg = normalizeTelegramPanConfig(cfg)
	confirmEnabled := pan115ConfirmationEnabled()
	var confirmClient *_115sy.Client
	if cfg.Cookie != "" {
		confirmClient, _ = _115sy.NewClient(_115sy.ClientOptions{
			Cookie:       cfg.Cookie,
			LimitRate:    1,
			PageCooldown: pan115RequestInterval,
		})
	}
	return &pan115ShareProvider{
		cfg:            cfg,
		webURL:         pan115WebURL,
		client:         newShareHTTPClient(),
		limiter:        pan115RequestLimiter,
		retryBaseDelay: pan115RetryBaseDelay,
		confirmClient:  confirmClient,
		confirmEnabled: confirmEnabled,
	}
}

func pan115ConfirmationEnabled() bool {
	if db.GetDb() == nil {
		return false
	}
	cfg, err := GetConfig()
	return err == nil && cfg.ResultConfirmationEnabled
}

func (p *pan115ShareProvider) Name() ShareProviderName {
	return ShareProviderPan115
}

func (p *pan115ShareProvider) ParseURL(raw string) (ShareRef, error) {
	ref, err := ParseShareURL(raw)
	if err != nil {
		return ShareRef{}, err
	}
	if ref.Provider != ShareProviderPan115 {
		return ShareRef{}, fmt.Errorf("share URL provider = %s, want %s", ref.Provider, ShareProviderPan115)
	}
	return ref, nil
}

func (p *pan115ShareProvider) EnsureDir(ctx context.Context, path string) (string, error) {
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

func (p *pan115ShareProvider) ListShareChildren(ctx context.Context, ref ShareRef, parentID string) ([]ShareItem, error) {
	var resp pan115SnapResp
	httpResp, err := p.doRequest(ctx, func() (*resty.Response, error) {
		return p.client.R().
			SetContext(ctx).
			SetHeader("Referer", pan115ShareReferer(p.webURL, ref)).
			SetQueryParams(map[string]string{
				"share_code":   ref.ShareID,
				"receive_code": ref.Passcode,
				"cid":          parentID,
				"offset":       "0",
				"limit":        "50",
				"asc":          "0",
				"format":       "json",
			}).
			Get(p.webURL + "/webapi/share/snap")
	})
	if err != nil {
		return nil, err
	}
	if err := decodePan115JSON(httpResp, &resp); err != nil {
		return nil, err
	}
	if !resp.State {
		return nil, pan115Error(resp.Error)
	}
	items := make([]ShareItem, 0, len(resp.Data.List))
	for _, item := range resp.Data.List {
		items = append(items, item.shareItem(parentID))
	}
	return items, nil
}

func (p *pan115ShareProvider) SaveShareItems(ctx context.Context, ref ShareRef, parentID string, items []ShareItem, dstDirID string) ([]string, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if p.cfg.Cookie == "" {
		return nil, errors.New("115 cookie is required")
	}
	fileIDs := make([]string, 0, len(items))
	for _, item := range items {
		fileID := firstNonEmpty(shareItemToken(item), item.ID)
		if fileID != "" {
			fileIDs = append(fileIDs, fileID)
		}
	}
	if len(fileIDs) == 0 {
		return nil, errors.New("115 share item ids are empty")
	}
	var resp pan115ReceiveResp
	httpResp, err := p.doRequest(ctx, func() (*resty.Response, error) {
		return p.client.R().
			SetContext(ctx).
			SetHeader("Cookie", p.cfg.Cookie).
			SetHeader("Origin", p.webURL).
			SetHeader("Referer", pan115ShareReferer(p.webURL, ref)).
			SetFormData(map[string]string{
				"cid":          firstNonEmpty(dstDirID, "0"),
				"share_code":   ref.ShareID,
				"receive_code": ref.Passcode,
				"file_id":      strings.Join(fileIDs, ","),
			}).
			Post(p.webURL + "/webapi/share/receive")
	})
	if err != nil {
		return nil, err
	}
	if err := decodePan115JSON(httpResp, &resp); err != nil {
		return nil, err
	}
	if !resp.State {
		return nil, pan115Error(resp.Error)
	}
	operation := pan115SaveOperation{
		ShareID:       ref.ShareID,
		DestinationID: firstNonEmpty(dstDirID, "0"),
		Items:         make([]pan115SaveItem, 0, len(items)),
	}
	for _, item := range items {
		operation.Items = append(operation.Items, pan115SaveItem{Name: item.Name, Size: item.Size})
	}
	if !p.confirmEnabled {
		// Keep the legacy task marker for deployments that have not opted into
		// remote confirmation yet. It is still only an internal completion
		// token; the confirmation-enabled path carries the probe checkpoint.
		return []string{"pan115_sync_" + ref.ShareID}, nil
	}
	return []string{encodePan115SaveOperation(operation)}, nil
}

func (p *pan115ShareProvider) doRequest(ctx context.Context, request func() (*resty.Response, error)) (*resty.Response, error) {
	if p == nil || p.client == nil {
		return nil, errors.New("115 HTTP client is not initialized")
	}
	var lastErr error
	for attempt := 0; attempt < pan115MaxRequestAttempts; attempt++ {
		if err := p.limiter.wait(ctx); err != nil {
			return nil, err
		}
		response, err := request()
		if !pan115ShouldRetryResponse(ctx, response, err) {
			return response, err
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = errors.Errorf("115 request temporarily unavailable: status=%d", response.StatusCode())
		}
		if attempt+1 == pan115MaxRequestAttempts {
			if response != nil {
				return response, nil
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if lastErr != nil {
				return nil, &pan115ClusterError{message: lastErr.Error(), code: pan115ClusterErrorCodeShareSaveTransient}
			}
			return nil, lastErr
		}
		if err := waitPan115Retry(ctx, p.retryDelay(attempt, response)); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func pan115ShouldRetryResponse(ctx context.Context, response *resty.Response, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	if err != nil {
		return true
	}
	if response == nil {
		return true
	}
	status := response.StatusCode()
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= http.StatusInternalServerError
}

func (p *pan115ShareProvider) retryDelay(attempt int, response *resty.Response) time.Duration {
	if response != nil {
		if delay := parsePan115RetryAfter(response.Header().Get("Retry-After")); delay > 0 {
			return delay
		}
	}
	base := p.retryBaseDelay
	if base <= 0 {
		return 0
	}
	if attempt > 5 {
		attempt = 5
	}
	return base * time.Duration(1<<attempt)
}

func waitPan115Retry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func parsePan115RetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return 0
}

func (p *pan115ShareProvider) WaitSaveComplete(ctx context.Context, taskIDs []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !p.confirmEnabled {
		return nil
	}
	for _, taskID := range taskIDs {
		operation, ok := decodePan115SaveOperation(taskID)
		if !ok {
			return &pan115ClusterError{message: "115 save result is not confirmable", code: pan115ClusterErrorCodeShareSaveResultUnknown}
		}
		if err := p.confirmSavedOperation(ctx, operation); err != nil {
			return err
		}
	}
	return nil
}

func (p *pan115ShareProvider) confirmSavedOperation(ctx context.Context, operation pan115SaveOperation) error {
	if p == nil || p.confirmClient == nil {
		return &pan115ClusterError{message: "115 save confirmation client is unavailable", code: pan115ClusterErrorCodeShareSaveResultUnknown}
	}
	for attempt := 0; attempt < pan115ConfirmationTries; attempt++ {
		items, err := p.confirmClient.ListFiles(ctx, operation.DestinationID, _115sy.ListOptions{PageSize: 1150})
		if err != nil {
			return &pan115ClusterError{message: fmt.Sprintf("115 save confirmation request failed: %v", err), code: pan115ClusterErrorCodeShareSaveResultUnknown}
		}
		remaining := make([]pan115SaveItem, 0, len(operation.Items))
		for _, expected := range operation.Items {
			found := false
			for _, item := range items {
				if item.IsDir || strings.TrimSpace(item.Name) != strings.TrimSpace(expected.Name) || item.Size != expected.Size {
					continue
				}
				found = true
				break
			}
			if !found {
				remaining = append(remaining, expected)
			}
		}
		if len(remaining) == 0 {
			return nil
		}
		if attempt+1 < pan115ConfirmationTries {
			timer := time.NewTimer(pan115ConfirmationDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return &pan115ClusterError{message: fmt.Sprintf("115 save confirmation did not find all %d requested files", len(operation.Items)), code: pan115ClusterErrorCodeShareSaveResultUnknown}
}

type pan115SaveOperation struct {
	ShareID       string           `json:"share_id"`
	DestinationID string           `json:"destination_id"`
	Items         []pan115SaveItem `json:"items"`
}

type pan115SaveItem struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

const pan115SaveOperationPrefix = "pan115_result:"

func encodePan115SaveOperation(operation pan115SaveOperation) string {
	body, err := json.Marshal(operation)
	if err != nil {
		return pan115SaveOperationPrefix + "invalid"
	}
	return pan115SaveOperationPrefix + base64.RawURLEncoding.EncodeToString(body)
}

func decodePan115SaveOperation(value string) (pan115SaveOperation, bool) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, pan115SaveOperationPrefix) {
		return pan115SaveOperation{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, pan115SaveOperationPrefix))
	if err != nil {
		return pan115SaveOperation{}, false
	}
	var operation pan115SaveOperation
	if err := json.Unmarshal(payload, &operation); err != nil || strings.TrimSpace(operation.DestinationID) == "" || len(operation.Items) == 0 {
		return pan115SaveOperation{}, false
	}
	return operation, true
}

func pan115ShareReferer(baseURL string, ref ShareRef) string {
	return fmt.Sprintf("%s/s/%s?password=%s&", strings.TrimRight(baseURL, "/"), ref.ShareID, ref.Passcode)
}

func decodePan115JSON(resp *resty.Response, out any) error {
	if resp == nil {
		return errors.New("empty 115 response")
	}
	if err := json.Unmarshal(resp.Body(), out); err != nil {
		contentType := strings.TrimSpace(resp.Header().Get("Content-Type"))
		message := fmt.Sprintf(
			"decode 115 response: status=%d content-type=%s body_len=%d first_non_space=%q kind=%s",
			resp.StatusCode(),
			firstNonEmpty(contentType, "unknown"),
			len(resp.Body()),
			pan115FirstNonSpace(resp.Body()),
			pan115ResponseKind(contentType, resp.Body()),
		)
		code := classifyPan115HTTPResponse(resp)
		if code != "" {
			return &pan115ClusterError{message: fmt.Sprintf("%s: %v", message, err), code: code}
		}
		return errors.Wrapf(err,
			"decode 115 response: status=%d content-type=%s body_len=%d first_non_space=%q kind=%s",
			resp.StatusCode(),
			firstNonEmpty(contentType, "unknown"),
			len(resp.Body()),
			pan115FirstNonSpace(resp.Body()),
			pan115ResponseKind(contentType, resp.Body()),
		)
	}
	if code := classifyPan115HTTPResponse(resp); code != "" {
		return &pan115ClusterError{
			message: fmt.Sprintf("115 response rejected: status=%d content-type=%s kind=%s", resp.StatusCode(), firstNonEmpty(strings.TrimSpace(resp.Header().Get("Content-Type")), "unknown"), pan115ResponseKind(resp.Header().Get("Content-Type"), resp.Body())),
			code:    code,
		}
	}
	return nil
}

func pan115Error(message string) error {
	message = strings.TrimSpace(message)
	if message == "" {
		return errors.New("115 request failed")
	}
	if code := classifyPan115ClusterErrorCode(message); code != "" {
		return &pan115ClusterError{message: message, code: code}
	}
	return errors.New(message)
}

func classifyPan115ClusterErrorCode(message string) string {
	normalized := strings.ToLower(strings.TrimSpace(message))
	compact := strings.NewReplacer(" ", "", "\t", "", "\r", "", "\n", "").Replace(normalized)
	switch {
	case strings.Contains(normalized, "密钥错误"),
		strings.Contains(normalized, "签名无效"),
		strings.Contains(normalized, "invalid signature"),
		strings.Contains(normalized, "key error"),
		strings.Contains(compact, "refresh_token无效"),
		strings.Contains(compact, "refresh token无效"):
		return pan115ClusterErrorCodeShareSaveCredentialsInvalid
	case strings.Contains(normalized, "rate limit"),
		strings.Contains(normalized, "too many requests"),
		strings.Contains(normalized, "限流"),
		strings.Contains(normalized, "429"):
		return pan115ClusterErrorCodeShareSaveRateLimited
	case strings.Contains(normalized, "分享已失效"),
		strings.Contains(normalized, "分享已取消"),
		strings.Contains(normalized, "分享不存在"),
		strings.Contains(normalized, "share expired"),
		strings.Contains(normalized, "share invalid"),
		strings.Contains(normalized, "share canceled"),
		strings.Contains(normalized, "share cancelled"):
		return pan115ClusterErrorCodeShareSaveSourceInvalid
	default:
		return ""
	}
}

func classifyPan115HTTPResponse(resp *resty.Response) string {
	if resp == nil {
		return ""
	}
	contentType := strings.TrimSpace(resp.Header().Get("Content-Type"))
	kind := pan115ResponseKind(contentType, resp.Body())
	switch {
	case resp.StatusCode() == http.StatusMethodNotAllowed:
		return pan115ClusterErrorCodeShareSaveMethodNotAllowed
	case resp.StatusCode() == http.StatusUnauthorized || resp.StatusCode() == http.StatusForbidden:
		return pan115ClusterErrorCodeShareSaveCredentialsInvalid
	case resp.StatusCode() == http.StatusTooManyRequests:
		return pan115ClusterErrorCodeShareSaveRateLimited
	case resp.StatusCode() >= http.StatusInternalServerError:
		return pan115ClusterErrorCodeShareSaveGatewayResponse
	case kind == "html":
		return pan115ClusterErrorCodeShareSaveGatewayResponse
	default:
		return ""
	}
}

type pan115ClusterError struct {
	message string
	code    string
}

func (e *pan115ClusterError) Error() string {
	if e == nil {
		return ""
	}
	return e.message
}

func (e *pan115ClusterError) ClusterErrorCode() string {
	if e == nil {
		return ""
	}
	return e.code
}

type pan115SnapResp struct {
	State bool   `json:"state"`
	Error string `json:"error"`
	Data  struct {
		Count int          `json:"count"`
		List  []pan115File `json:"list"`
	} `json:"data"`
}

type pan115ID string

func (id *pan115ID) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" || trimmed == "null" {
		*id = ""
		return nil
	}
	if strings.HasPrefix(trimmed, `"`) {
		var value string
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		*id = pan115ID(strings.TrimSpace(value))
		return nil
	}
	var value json.Number
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*id = pan115ID(value.String())
	return nil
}

func (id pan115ID) String() string {
	return string(id)
}

type pan115File struct {
	FID      pan115ID `json:"fid"`
	CID      pan115ID `json:"cid"`
	Name     string   `json:"n"`
	Size     int64    `json:"s"`
	UpdateAt string   `json:"t"`
	Icon     string   `json:"ico"`
}

func (f pan115File) shareItem(parentID string) ShareItem {
	fid := f.FID.String()
	cid := f.CID.String()
	isDir := fid == ""
	id := fid
	if isDir {
		id = cid
	}
	if parentID == "" {
		parentID = cid
		if isDir {
			parentID = "0"
		}
	}
	return ShareItem{
		ID:       id,
		ParentID: parentID,
		Name:     f.Name,
		Size:     f.Size,
		Modified: parsePan115Time(f.UpdateAt),
		IsDir:    isDir,
		Raw: map[string]any{
			"share_fid_token": id,
		},
	}
}

type pan115ReceiveResp struct {
	State bool   `json:"state"`
	Error string `json:"error"`
}

func parsePan115Time(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.Unix(parsed, 0)
	}
	if parsed, err := time.Parse("2006-01-02 15:04:05", value); err == nil {
		return parsed
	}
	return time.Time{}
}

func pan115FirstNonSpace(body []byte) string {
	for _, b := range body {
		if b <= ' ' {
			continue
		}
		return string([]byte{b})
	}
	return ""
}

func pan115ResponseKind(contentType string, body []byte) string {
	if mediaType, _, err := mime.ParseMediaType(contentType); err == nil {
		switch mediaType {
		case "application/json", "text/json":
			return "json"
		case "text/html", "application/xhtml+xml":
			return "html"
		}
	}
	switch pan115FirstNonSpace(body) {
	case "{", "[":
		return "json"
	case "<":
		return "html"
	case "":
		return "empty"
	default:
		return "text"
	}
}

var _ ShareSaver = (*pan115ShareProvider)(nil)

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
	pan115DirectAppURL = "https://proapi.115.com/2.0/share/downurl"
	pan115DirectWebURL = "https://webapi.115.com/share/downurl"
)

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
	directAppURL   string
	directWebURL   string
	client         *resty.Client
	limiter        *pan115RateLimiter
	retryBaseDelay time.Duration
	receiveClient  *_115sy.Client
	confirmClient  *_115sy.Client
	directClient   *_115sy.Client
	confirmEnabled bool
}

func NewPan115ShareProvider(cfg model.SubscriptionTelegramPanConfig) ShareSaver {
	cfg = normalizeTelegramPanConfig(cfg)
	confirmEnabled := pan115ConfirmationEnabled()
	provider := &pan115ShareProvider{
		cfg:            cfg,
		webURL:         pan115WebURL,
		directAppURL:   pan115DirectAppURL,
		directWebURL:   pan115DirectWebURL,
		client:         newShareHTTPClient(),
		limiter:        pan115RequestLimiter,
		retryBaseDelay: pan115RetryBaseDelay,
		confirmEnabled: confirmEnabled,
	}
	if cfg.Cookie != "" {
		client, _ := _115sy.NewClient(_115sy.ClientOptions{
			Cookie:       cfg.Cookie,
			LimitRate:    1,
			PageCooldown: pan115RequestInterval,
			RequestGate:  provider.limiter.wait,
		})
		provider.confirmClient = client
		provider.directClient = client
		provider.receiveClient = client
	}
	return provider
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
	if p == nil || p.receiveClient == nil {
		return nil, &pan115ClusterError{
			message: "115 share snapshot client is unavailable",
			code:    pan115ClusterErrorCodeShareSaveResultUnknown,
		}
	}

	items, err := p.receiveClient.ShareChildren(ctx, _115sy.ShareURL{
		ShareCode:   ref.ShareID,
		ReceiveCode: ref.Passcode,
	}, parentID)
	if err != nil {
		return nil, pan115ReceiveError(err)
	}

	converted := make([]ShareItem, 0, len(items))
	for _, item := range items {
		itemParentID := firstNonEmpty(strings.TrimSpace(item.ParentID), "0")
		modified := time.Time{}
		if item.ModifyTime > 0 {
			modified = time.Unix(item.ModifyTime, 0)
		}
		converted = append(converted, ShareItem{
			ID:       item.ID,
			ParentID: itemParentID,
			Name:     item.Name,
			Size:     item.Size,
			Modified: modified,
			IsDir:    item.IsDir,
			Raw: map[string]any{
				"share_fid_token": item.ID,
			},
		})
	}
	return converted, nil
}

// GetShareDownloadURL uses the official-client-compatible app endpoint first,
// then the web endpoint when the app route is rejected with 405. The URL is
// short-lived and is returned only to the worker that performs the download.
func (p *pan115ShareProvider) GetShareDownloadURL(ctx context.Context, ref ShareRef, item ShareItem) (ShareDownloadLink, error) {
	if item.IsDir {
		return ShareDownloadLink{}, errors.New("115 share item is a directory")
	}
	if p == nil || strings.TrimSpace(p.cfg.Cookie) == "" {
		return ShareDownloadLink{}, &pan115ClusterError{message: "115 cookie is required", code: pan115ClusterErrorCodeShareSaveCredentialsInvalid}
	}
	fileID := firstNonEmpty(shareItemToken(item), item.ID)
	if fileID == "" {
		return ShareDownloadLink{}, errors.New("115 share item id is empty")
	}
	if p.directClient != nil && p.directAppURL == pan115DirectAppURL && p.directWebURL == pan115DirectWebURL {
		link, err := p.directClient.ShareDownloadURL(ctx, _115sy.ShareURL{
			ShareCode:   ref.ShareID,
			ReceiveCode: ref.Passcode,
			SourceURL:   ref.RawURL,
		}, fileID, _115sy.DefaultAndroidUA)
		if err != nil {
			return ShareDownloadLink{}, pan115DirectError(err)
		}
		return ShareDownloadLink{
			URL:     link.URL,
			Headers: headerValues(link.Header),
			FileID:  fileID,
			Size:    item.Size,
		}, nil
	}

	endpoints := []string{p.directAppURL, p.directWebURL}
	for index, endpoint := range endpoints {
		if strings.TrimSpace(endpoint) == "" {
			continue
		}
		var resp pan115DirectDownloadResp
		httpResp, err := p.doRequest(ctx, func() (*resty.Response, error) {
			return p.client.R().
				SetContext(ctx).
				SetHeader("Cookie", p.cfg.Cookie).
				SetHeader("Origin", p.webURL).
				SetHeader("Referer", pan115ShareReferer(p.webURL, ref)).
				SetHeader("User-Agent", _115sy.DefaultAndroidUA).
				SetQueryParams(map[string]string{
					"share_code":   ref.ShareID,
					"receive_code": ref.Passcode,
					"file_id":      fileID,
					"dl":           "1",
				}).
				Get(endpoint)
		})
		if err != nil {
			return ShareDownloadLink{}, err
		}
		if err := decodePan115JSON(httpResp, &resp); err != nil {
			if index == 0 && pan115ErrorCode(err) == pan115ClusterErrorCodeShareSaveMethodNotAllowed {
				continue
			}
			return ShareDownloadLink{}, err
		}
		if !resp.State {
			return ShareDownloadLink{}, pan115Error(resp.Error)
		}
		info := resp.Data
		if info.File != nil {
			info = *info.File
		}
		urlValue := decodePan115DirectURL(info.URL)
		if urlValue == "" {
			return ShareDownloadLink{}, errors.New("115 share direct download URL is empty (directory or unavailable)")
		}
		return ShareDownloadLink{
			URL:     urlValue,
			Headers: map[string]string{"User-Agent": _115sy.DefaultAndroidUA},
			FileID:  firstNonEmpty(info.FID.String(), fileID),
			Size:    firstPositiveInt64(info.Size, item.Size),
			Hash:    strings.TrimSpace(info.SHA1),
		}, nil
	}
	return ShareDownloadLink{}, errors.New("115 share direct download endpoints are unavailable")
}

func pan115DirectError(err error) error {
	if err == nil {
		return nil
	}
	var httpErr *_115sy.HTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return &pan115ClusterError{message: err.Error(), code: pan115ClusterErrorCodeShareSaveCredentialsInvalid}
		case http.StatusMethodNotAllowed:
			return &pan115ClusterError{message: err.Error(), code: pan115ClusterErrorCodeShareSaveMethodNotAllowed}
		case http.StatusTooManyRequests:
			return &pan115ClusterError{message: err.Error(), code: pan115ClusterErrorCodeShareSaveRateLimited}
		case http.StatusRequestTimeout:
			return &pan115ClusterError{message: err.Error(), code: pan115ClusterErrorCodeShareSaveTransient}
		default:
			if httpErr.StatusCode >= http.StatusInternalServerError {
				return &pan115ClusterError{message: err.Error(), code: pan115ClusterErrorCodeShareSaveGatewayResponse}
			}
		}
	}
	if code := classifyPan115ClusterErrorCode(err.Error()); code != "" {
		return &pan115ClusterError{message: err.Error(), code: code}
	}
	return &pan115ClusterError{message: err.Error(), code: pan115ClusterErrorCodeShareSaveTransient}
}

func pan115ReceiveError(err error) error {
	if err == nil {
		return nil
	}
	var httpErr *_115sy.HTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return &pan115ClusterError{message: err.Error(), code: pan115ClusterErrorCodeShareSaveCredentialsInvalid}
		case http.StatusMethodNotAllowed:
			return &pan115ClusterError{message: err.Error(), code: pan115ClusterErrorCodeShareSaveMethodNotAllowed}
		case http.StatusTooManyRequests:
			return &pan115ClusterError{message: err.Error(), code: pan115ClusterErrorCodeShareSaveRateLimited}
		case http.StatusRequestTimeout:
			return &pan115ClusterError{message: err.Error(), code: pan115ClusterErrorCodeShareSaveTransient}
		default:
			if httpErr.StatusCode >= http.StatusInternalServerError {
				return &pan115ClusterError{message: err.Error(), code: pan115ClusterErrorCodeShareSaveGatewayResponse}
			}
		}
	}
	var protocolErr *_115sy.ProtocolError
	if errors.As(err, &protocolErr) {
		// A successful HTTP response with an HTML or malformed body is
		// ambiguous for a POST: the cloud may have accepted the save before
		// the gateway rendered an error page. Do not blindly replay it.
		return &pan115ClusterError{message: err.Error(), code: pan115ClusterErrorCodeShareSaveResultUnknown}
	}
	var businessErr *_115sy.BusinessError
	if errors.As(err, &businessErr) {
		if classified := pan115Error(businessErr.Message); classified != nil {
			return classified
		}
	}
	var networkErr *_115sy.NetworkError
	if errors.As(err, &networkErr) {
		return &pan115ClusterError{message: err.Error(), code: pan115ClusterErrorCodeShareSaveResultUnknown}
	}
	if code := classifyPan115ClusterErrorCode(err.Error()); code != "" {
		return &pan115ClusterError{message: err.Error(), code: code}
	}
	return &pan115ClusterError{message: err.Error(), code: pan115ClusterErrorCodeShareSaveResultUnknown}
}

func pan115ErrorCode(err error) string {
	var coded interface{ ClusterErrorCode() string }
	if errors.As(err, &coded) {
		return coded.ClusterErrorCode()
	}
	return ""
}

func headerValues(header http.Header) map[string]string {
	if len(header) == 0 {
		return nil
	}
	values := make(map[string]string, len(header))
	for key, items := range header {
		if len(items) > 0 && strings.TrimSpace(key) != "" && strings.TrimSpace(items[0]) != "" {
			values[key] = items[0]
		}
	}
	return values
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
	if p.receiveClient == nil {
		return nil, &pan115ClusterError{message: "115 share receive client is unavailable", code: pan115ClusterErrorCodeShareSaveResultUnknown}
	}
	received, err := p.receiveClient.ReceiveShare(ctx, _115sy.ReceiveShareRequest{
		ShareCode:   ref.ShareID,
		ReceiveCode: ref.Passcode,
		TargetCID:   firstNonEmpty(dstDirID, "0"),
		FileID:      strings.Join(fileIDs, ","),
	})
	if err != nil {
		return nil, pan115ReceiveError(err)
	}
	if !received.State {
		return nil, pan115Error(received.Message)
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
		strings.Contains(normalized, "操作太频繁"),
		strings.Contains(normalized, "429"):
		return pan115ClusterErrorCodeShareSaveRateLimited
	case strings.Contains(normalized, "请求异常需要重试"),
		strings.Contains(normalized, "temporary unavailable"),
		strings.Contains(normalized, "temporarily unavailable"):
		return pan115ClusterErrorCodeShareSaveTransient
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

// isShareSourceInvalidError reports whether an error from share inspect or
// transfer indicates the share link is permanently invalid (cancelled, expired,
// removed, banned, or violates content policy). When this returns true the
// caller should clear any bound share so a fresh link can be discovered via
// HDHive, Telegram, or another source on the next subscription run.
func isShareSourceInvalidError(err error) bool {
	if err == nil {
		return false
	}
	normalized := strings.ToLower(strings.TrimSpace(err.Error()))
	if strings.Contains(normalized, "share_save_source_invalid") {
		return true
	}
	// Provider-specific permanent failure messages observed in production.
	permanentMarkers := []string{
		"分享已失效", "分享已取消", "分享不存在", "分享地址已失效",
		"好友已取消了分享", "分享者用户封禁链接查看受限",
		"文件涉及违规内容", "文件不存在",
		"share expired", "share invalid", "share canceled", "share cancelled",
		"share_link is cancelled", "sharelink.cancelled",
		"object not found", "path component not found",
	}
	for _, marker := range permanentMarkers {
		if strings.Contains(normalized, strings.ToLower(marker)) {
			return true
		}
	}
	return false
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

type pan115DirectDownloadInfo struct {
	FID  pan115ID                  `json:"fid"`
	Name string                    `json:"fn"`
	Size int64                     `json:"fs"`
	SHA1 string                    `json:"sha1"`
	URL  json.RawMessage           `json:"url"`
	File *pan115DirectDownloadInfo `json:"file"`
}

type pan115DirectDownloadResp struct {
	State bool                     `json:"state"`
	Error string                   `json:"error"`
	Data  pan115DirectDownloadInfo `json:"data"`
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

func decodePan115DirectURL(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var direct string
	if json.Unmarshal(raw, &direct) == nil {
		return strings.TrimSpace(direct)
	}
	var nested struct {
		URL string `json:"url"`
	}
	if json.Unmarshal(raw, &nested) == nil {
		return strings.TrimSpace(nested.URL)
	}
	return ""
}

func firstPositiveInt64(values ...int64) int64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
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
var _ ShareDirectDownloader = (*pan115ShareProvider)(nil)

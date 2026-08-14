package hdhive

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type SymediaConfig struct {
	BaseURL      string
	UserID       string
	ProxyUserKey string
	ProxySecret  string
	Timeout      time.Duration
}

type Status struct {
	Authorized bool
}

type ResourceDetails struct {
	FullURL       string
	AccessCode    string
	IsUnlocked    bool
	IsFreeForUser bool
	UnlockPoints  *int
}

type Resource struct {
	Slug            string
	ResourceURL     string
	Title           string
	PanType         string
	UnlockPoints    *int
	UnlockCount     *int
	IsUnlocked      bool
	IsOfficial      bool
	VideoResolution []string
	Source          []string
	Remark          string
	ShareSize       string
	ValidateStatus  string
}

type UnlockResult struct {
	FullURL      string
	AccessCode   string
	AlreadyOwned bool
	PointsSpent  *int
}

type Client interface {
	Status(context.Context) (Status, error)
	Share(context.Context, string) (ResourceDetails, error)
	Unlock(context.Context, string) (UnlockResult, error)
}

type Error struct {
	Code            string
	Message         string
	HTTPStatus      int
	RetryAfter      time.Duration
	ServerErrorCode string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Code != "" {
		return e.Code
	}
	return "HDHive request failed"
}

type SymediaClient struct {
	baseURL      string
	userID       string
	proxyUserKey string
	proxySecret  []byte
	timeout      time.Duration
	httpClient   *http.Client
	randomNonce  func() (string, error)

	mu         sync.Mutex
	sessionID  string
	sessionKey []byte
	sequence   int64
}

func NewSymediaClient(cfg SymediaConfig, httpClient *http.Client) (*SymediaClient, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if !strings.HasPrefix(strings.ToLower(baseURL), "http://") && !strings.HasPrefix(strings.ToLower(baseURL), "https://") {
		return nil, &Error{Code: "HDHIVE_SYMEDIA_CONFIG_INVALID", Message: "Symedia base URL must be HTTP(S)"}
	}
	if strings.TrimSpace(cfg.UserID) == "" || strings.TrimSpace(cfg.ProxyUserKey) == "" || strings.TrimSpace(cfg.ProxySecret) == "" {
		return nil, &Error{Code: "HDHIVE_SYMEDIA_CONFIG_INVALID", Message: "Symedia proxy credentials are incomplete"}
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: timeout}
	}
	return &SymediaClient{
		baseURL:      baseURL,
		userID:       strings.TrimSpace(cfg.UserID),
		proxyUserKey: strings.TrimSpace(cfg.ProxyUserKey),
		proxySecret:  []byte(cfg.ProxySecret),
		timeout:      timeout,
		httpClient:   httpClient,
		randomNonce:  randomNonce,
	}, nil
}

func (c *SymediaClient) Status(ctx context.Context) (Status, error) {
	payload, err := c.signedRequest(ctx, http.MethodGet, "/api/v1/users/"+urlPathEscape(c.userID)+"/status", nil)
	if err != nil {
		return Status{}, err
	}
	data := responseData(payload)
	return Status{Authorized: boolValue(data, "authorized")}, nil
}

func (c *SymediaClient) Search(ctx context.Context, mediaType string, tmdbID int64) ([]Resource, error) {
	mediaType = strings.ToLower(strings.TrimSpace(mediaType))
	if mediaType != "tv" && mediaType != "movie" {
		return nil, &Error{Code: "HDHIVE_SYMEDIA_SEARCH_INVALID", Message: "HDHive media type must be tv or movie"}
	}
	if tmdbID <= 0 {
		return nil, &Error{Code: "HDHIVE_SYMEDIA_SEARCH_INVALID", Message: "HDHive search requires a positive TMDB ID"}
	}
	requestPath := "/api/v1/open/" + urlPathEscape(c.userID) + "/resources/" + urlPathEscape(mediaType) + "/" + strconv.FormatInt(tmdbID, 10)
	payload, err := c.signedRequest(ctx, http.MethodGet, requestPath, nil)
	if err != nil {
		return nil, err
	}
	items := responseItems(payload)
	resources := make([]Resource, 0, len(items))
	for _, item := range items {
		resources = append(resources, resourceFromMap(item))
	}
	return resources, nil
}

func (c *SymediaClient) Share(ctx context.Context, slug string) (ResourceDetails, error) {
	slug = normalizeSlug(slug)
	if slug == "" {
		return ResourceDetails{}, &Error{Code: "INVALID_RESOURCE", Message: "HDHive slug is invalid"}
	}
	payload, err := c.signedRequest(ctx, http.MethodGet, "/api/v1/open/"+urlPathEscape(c.userID)+"/shares/"+urlPathEscape(slug), nil)
	if err != nil {
		return ResourceDetails{}, err
	}
	data := responseData(payload)
	return ResourceDetails{
		FullURL:       firstString(data, "full_url", "fullUrl", "url", "share_url", "shareUrl"),
		AccessCode:    firstString(data, "access_code", "accessCode", "password", "pwd"),
		IsUnlocked:    firstBool(data, "is_unlocked", "isUnlocked", "already_owned", "alreadyOwned", "is_free_for_user", "isFreeForUser"),
		IsFreeForUser: firstBool(data, "is_free_for_user", "isFreeForUser"),
		UnlockPoints:  numberPointer(data, "unlock_points", "unlockPoints", "actual_unlock_points"),
	}, nil
}

func (c *SymediaClient) Unlock(ctx context.Context, slug string) (UnlockResult, error) {
	slug = normalizeSlug(slug)
	if slug == "" {
		return UnlockResult{}, &Error{Code: "INVALID_RESOURCE", Message: "HDHive slug is invalid"}
	}
	body, err := json.Marshal(map[string]string{"slug": slug})
	if err != nil {
		return UnlockResult{}, err
	}
	payload, err := c.signedRequest(ctx, http.MethodPost, "/api/v1/open/"+urlPathEscape(c.userID)+"/resources/unlock", body)
	if err != nil {
		return UnlockResult{}, err
	}
	data := responseData(payload)
	fullURL := firstString(data, "full_url", "fullUrl", "url", "share_url", "shareUrl")
	if fullURL == "" {
		return UnlockResult{}, &Error{Code: "HDHIVE_SYMEDIA_UNLOCK_NO_LINK", Message: "Symedia unlock response did not contain a share URL"}
	}
	return UnlockResult{
		FullURL:      fullURL,
		AccessCode:   firstString(data, "access_code", "accessCode", "password", "pwd"),
		AlreadyOwned: firstBool(data, "already_owned", "alreadyOwned", "is_unlocked", "isUnlocked"),
		PointsSpent:  numberPointer(data, "points_spent", "pointsSpent", "unlock_points", "unlockPoints"),
	}, nil
}

func (c *SymediaClient) signedRequest(ctx context.Context, method, requestPath string, body []byte) (map[string]any, error) {
	payload, err := c.doSignedRequest(ctx, method, requestPath, body)
	if err != nil && isProxyAuthError(err) {
		c.clearSession()
		payload, err = c.doSignedRequest(ctx, method, requestPath, body)
	}
	return payload, err
}

func (c *SymediaClient) doSignedRequest(ctx context.Context, method, requestPath string, body []byte) (map[string]any, error) {
	if err := c.ensureSession(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	sequence := c.sequence + 1
	bodyHash := sha256.Sum256(body)
	canonical := strings.Join([]string{method, signaturePath(requestPath), c.sessionID, strconv.FormatInt(sequence, 10), hex.EncodeToString(bodyHash[:]), c.proxyUserKey}, "\n")
	signature := hmacHex(c.sessionKey, []byte(canonical))
	c.sequence = sequence
	sessionID := c.sessionID
	c.mu.Unlock()

	headers := http.Header{
		"Accept":              []string{"application/json"},
		"X-Proxy-Session":     []string{sessionID},
		"X-Proxy-Sequence":    []string{strconv.FormatInt(sequence, 10)},
		"X-Proxy-Body-SHA256": []string{hex.EncodeToString(bodyHash[:])},
		"X-Proxy-User-Key":    []string{c.proxyUserKey},
		"X-Proxy-Signature":   []string{signature},
	}
	if len(body) > 0 {
		headers.Set("Content-Type", "application/json")
	}
	return c.request(ctx, method, requestPath, body, headers)
}

func (c *SymediaClient) clearSession() {
	c.mu.Lock()
	c.sessionID = ""
	c.sessionKey = nil
	c.sequence = 0
	c.mu.Unlock()
}

func signaturePath(requestPath string) string {
	parsed, err := url.Parse(requestPath)
	if err != nil {
		return requestPath
	}
	if parsed.RawQuery == "" {
		return parsed.Path
	}
	return parsed.Path + "?" + parsed.RawQuery
}

func isProxyAuthError(err error) bool {
	var proxyErr *Error
	if !errors.As(err, &proxyErr) {
		return false
	}
	if proxyErr.HTTPStatus == http.StatusUnauthorized {
		return true
	}
	if proxyErr.HTTPStatus != http.StatusForbidden {
		return false
	}
	return strings.Contains(proxyErr.Message, "密钥错误或签名无效") || strings.Contains(proxyErr.Message, "缺少必要请求头")
}

func (c *SymediaClient) ensureSession(ctx context.Context) error {
	c.mu.Lock()
	if c.sessionID != "" && len(c.sessionKey) > 0 {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	nonce, err := c.randomNonce()
	if err != nil {
		return err
	}
	proof := hmacHex(c.proxySecret, []byte("hdhive-openproxy-proof\nclient\n"+nonce))
	body, err := json.Marshal(map[string]string{"client_nonce": nonce, "client_proof": proof})
	if err != nil {
		return err
	}
	payload, err := c.request(ctx, http.MethodPost, "/api/v1/auth/session", body, http.Header{
		"Accept":       []string{"application/json"},
		"Content-Type": []string{"application/json"},
	})
	if err != nil {
		return err
	}
	data := responseData(payload)
	serverNonce := firstString(data, "server_nonce", "serverNonce")
	sessionID := firstString(data, "session_id", "sessionId")
	serverProof := firstString(data, "server_proof", "serverProof")
	if serverNonce == "" || sessionID == "" || serverProof == "" {
		return &Error{Code: "HDHIVE_SYMEDIA_SESSION_INVALID", Message: "Symedia handshake response is incomplete"}
	}
	expected := hmacHex(c.proxySecret, []byte("hdhive-openproxy-proof\nserver\n"+serverNonce))
	if !hmacEqual(expected, serverProof) {
		return &Error{Code: "HDHIVE_SYMEDIA_SESSION_INVALID", Message: "Symedia server proof verification failed"}
	}
	prk := hmacBytes([]byte("hdhive-openproxy-session:"+nonce+":"+serverNonce), c.proxySecret)
	sessionKey := hmacBytes(prk, []byte("hdhive-openproxy-session-key\x01"))

	c.mu.Lock()
	if c.sessionID == "" || len(c.sessionKey) == 0 {
		c.sessionID = sessionID
		c.sessionKey = sessionKey
		c.sequence = 0
	}
	c.mu.Unlock()
	return nil
}

func (c *SymediaClient) request(ctx context.Context, method, requestPath string, body []byte, headers http.Header) (map[string]any, error) {
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, method, c.baseURL+requestPath, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header = headers
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, &Error{Code: "HDHIVE_SYMEDIA_REQUEST_FAILED", Message: "Symedia proxy request failed"}
	}
	defer resp.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if readErr != nil {
		return nil, readErr
	}
	var payload map[string]any
	if len(bytes.TrimSpace(raw)) == 0 {
		payload = map[string]any{}
	} else if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, &Error{Code: "HDHIVE_SYMEDIA_INVALID_RESPONSE", Message: "Symedia proxy returned invalid JSON", HTTPStatus: resp.StatusCode}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || boolValue(payload, "success") == false && hasKey(payload, "success") {
		code := firstString(payload, "code", "error_code", "error")
		if resp.StatusCode == http.StatusTooManyRequests {
			code = "HDHIVE_SYMEDIA_RATE_LIMITED"
		}
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			code = "HDHIVE_SYMEDIA_AUTH_FAILED"
		}
		if code == "" {
			code = "HDHIVE_SYMEDIA_HTTP_" + strconv.Itoa(resp.StatusCode)
		}
		return nil, &Error{Code: code, Message: firstString(payload, "message", "description", "detail", "error"), HTTPStatus: resp.StatusCode, RetryAfter: retryAfter(resp, payload), ServerErrorCode: firstString(payload, "code", "error_code")}
	}
	return payload, nil
}

func randomNonce() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func hmacBytes(key, message []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(message)
	return h.Sum(nil)
}

func hmacHex(key, message []byte) string { return hex.EncodeToString(hmacBytes(key, message)) }

func hmacEqual(expected, actual string) bool {
	left, err1 := hex.DecodeString(expected)
	right, err2 := hex.DecodeString(strings.TrimSpace(actual))
	return err1 == nil && err2 == nil && len(left) > 0 && hmac.Equal(left, right)
}

func responseData(payload map[string]any) map[string]any {
	if data, ok := payload["data"].(map[string]any); ok {
		return data
	}
	return payload
}

func responseItems(payload map[string]any) []map[string]any {
	var raw []any
	appendItems := func(value any) {
		items, ok := value.([]any)
		if ok {
			raw = append(raw, items...)
		}
	}
	if data, ok := payload["data"]; ok {
		switch typed := data.(type) {
		case []any:
			appendItems(typed)
		case map[string]any:
			for _, key := range []string{"items", "resources"} {
				if value, exists := typed[key]; exists {
					appendItems(value)
				}
			}
		}
	}
	if len(raw) == 0 {
		for _, key := range []string{"items", "resources"} {
			if value, ok := payload[key]; ok {
				appendItems(value)
			}
		}
	}
	items := make([]map[string]any, 0, len(raw))
	for _, value := range raw {
		if item, ok := value.(map[string]any); ok {
			items = append(items, item)
		}
	}
	return items
}

func resourceFromMap(values map[string]any) Resource {
	return Resource{
		Slug:            firstString(values, "slug", "resource_slug", "resourceSlug"),
		ResourceURL:     firstString(values, "resource_url", "resourceUrl", "url"),
		Title:           firstString(values, "title", "name"),
		PanType:         strings.ToLower(firstString(values, "pan_type", "panType", "cloud_type", "cloudType")),
		UnlockPoints:    numberPointer(values, "unlock_points", "unlockPoints"),
		UnlockCount:     numberPointer(values, "unlock_count", "unlockCount"),
		IsUnlocked:      firstBool(values, "is_unlocked", "isUnlocked", "already_owned", "alreadyOwned"),
		IsOfficial:      firstBool(values, "is_official", "isOfficial"),
		VideoResolution: stringSlice(values, "video_resolution", "videoResolution", "resolutions"),
		Source:          stringSlice(values, "source", "sources"),
		Remark:          firstString(values, "remark", "description"),
		ShareSize:       firstString(values, "share_size", "shareSize", "size"),
		ValidateStatus:  firstString(values, "validate_status", "validateStatus"),
	}
}

func firstString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := values[key]; ok {
			if text := strings.TrimSpace(fmt.Sprint(value)); text != "" && text != "<nil>" {
				return text
			}
		}
	}
	return ""
}

func stringSlice(values map[string]any, keys ...string) []string {
	for _, key := range keys {
		value, ok := values[key]
		if !ok {
			continue
		}
		var result []string
		switch typed := value.(type) {
		case []any:
			for _, item := range typed {
				if text := strings.TrimSpace(fmt.Sprint(item)); text != "" && text != "<nil>" {
					result = append(result, text)
				}
			}
		case []string:
			for _, item := range typed {
				if text := strings.TrimSpace(item); text != "" {
					result = append(result, text)
				}
			}
		}
		if len(result) > 0 {
			return result
		}
	}
	return nil
}

func firstBool(values map[string]any, keys ...string) bool {
	for _, key := range keys {
		if value, ok := values[key]; ok {
			switch typed := value.(type) {
			case bool:
				if typed {
					return true
				}
			case string:
				parsed, err := strconv.ParseBool(strings.TrimSpace(typed))
				if err == nil && parsed {
					return true
				}
			case float64:
				if typed != 0 {
					return true
				}
			}
		}
	}
	return false
}

func boolValue(values map[string]any, key string) bool { return firstBool(values, key) }

func hasKey(values map[string]any, key string) bool { _, ok := values[key]; return ok }

func numberPointer(values map[string]any, keys ...string) *int {
	for _, key := range keys {
		value, ok := values[key]
		if !ok {
			continue
		}
		var number int
		switch typed := value.(type) {
		case float64:
			number = int(typed)
		case int:
			number = typed
		case string:
			parsed, err := strconv.Atoi(strings.TrimSpace(typed))
			if err != nil {
				continue
			}
			number = parsed
		default:
			continue
		}
		return &number
	}
	return nil
}

func retryAfter(resp *http.Response, payload map[string]any) time.Duration {
	value := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if value == "" {
		value = firstString(payload, "retry_after_seconds")
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func urlPathEscape(value string) string { return url.PathEscape(value) }

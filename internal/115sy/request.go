package _115sy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type responseEnvelope struct {
	State   *responseState  `json:"state"`
	Errno   flexibleInt     `json:"errno"`
	Error   string          `json:"error"`
	Message string          `json:"message"`
	Msg     string          `json:"msg"`
	Data    json.RawMessage `json:"data"`
	Count   flexibleInt     `json:"count"`
	Total   flexibleInt     `json:"total"`
	Offset  flexibleInt     `json:"offset"`
	Limit   flexibleInt     `json:"limit"`
	Next    flexibleInt     `json:"next_offset"`
	HasMore *flexibleBool   `json:"has_more"`
}

type responseState bool

func (s *responseState) UnmarshalJSON(data []byte) error {
	value := strings.TrimSpace(string(data))
	switch value {
	case "true", "1", `"1"`:
		*s = true
		return nil
	case "false", "0", `"0"`, "null", "":
		*s = false
		return nil
	default:
		return fmt.Errorf("invalid response state %q", value)
	}
}

var pageRequestGates sync.Map

func (c *Client) doJSON(ctx context.Context, operation Operation, profile Profile, method, endpoint string, query neturl.Values, payload any, out any) error {
	body, err := marshalJSONBody(payload)
	if err != nil {
		return err
	}
	return c.do(ctx, operation, profile, method, endpoint, query, body, "application/json", out)
}

func (c *Client) doForm(ctx context.Context, operation Operation, profile Profile, method, endpoint string, query neturl.Values, form neturl.Values, out any) error {
	var body []byte
	if form != nil {
		body = []byte(form.Encode())
	}
	return c.do(ctx, operation, profile, method, endpoint, query, body, "application/x-www-form-urlencoded", out)
}

func marshalJSONBody(payload any) ([]byte, error) {
	if payload == nil {
		return nil, nil
	}
	return json.Marshal(payload)
}

func (c *Client) do(ctx context.Context, operation Operation, profile Profile, method, endpoint string, query neturl.Values, body []byte, contentType string, out any) error {
	policy := policyForOperation(operation)
	currentProfile := profile
	if currentProfile == "" {
		currentProfile = policy.Primary
	}

	fallbackTried := false
	attempt := 0
	for {
		attempt++
		if c.requestGate != nil {
			if err := c.requestGate(ctx); err != nil {
				return wrapContextError(err, endpoint, currentProfile)
			}
		}
		if err := waitAccountLimiter(ctx, c.accountLimiter); err != nil {
			return wrapContextError(err, endpoint, currentProfile)
		}

		releasePageGate, err := c.acquirePageGate(ctx, policy)
		if err != nil {
			return wrapContextError(err, endpoint, currentProfile)
		}

		if policy.PageCooldown {
			if err := c.pageLimiter.WaitCooldown(ctx); err != nil {
				releasePageGate()
				return wrapContextError(err, endpoint, currentProfile)
			}
		}

		requestEndpoint := endpointForProfile(operation, currentProfile, endpoint)
		req, err := c.newRequestWithContext(ctx, currentProfile, method, requestEndpoint, query, body, contentType)
		if err != nil {
			releasePageGate()
			return err
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			releasePageGate()
			if contextErr := normalizedContextError(ctx, err); contextErr != nil {
				return wrapContextError(contextErr, endpoint, currentProfile)
			}
			if operationIsIdempotent(operation) && attempt < maxRequestAttempts {
				if retryErr := waitRequestRetry(ctx, attempt, 0); retryErr != nil {
					return retryErr
				}
				continue
			}
			return &NetworkError{
				Kind:     KindNetwork,
				Method:   method,
				Endpoint: endpoint,
				Profile:  currentProfile,
				Err:      sanitizeRequestError(err),
			}
		}

		respBody, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		fallbackEligible := false
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			fallbackEligible = shouldFallbackHTTP(policy, currentProfile, resp.StatusCode)
		}
		if policy.PageCooldown && readErr == nil && closeErr == nil && !fallbackEligible {
			c.pageLimiter.MarkCompleted()
		}
		releasePageGate()

		if readErr != nil {
			if operationIsIdempotent(operation) && attempt < maxRequestAttempts {
				if retryErr := waitRequestRetry(ctx, attempt, 0); retryErr != nil {
					return retryErr
				}
				continue
			}
			return &NetworkError{
				Kind:     KindNetwork,
				Method:   method,
				Endpoint: endpoint,
				Profile:  currentProfile,
				Err:      sanitizeRequestError(readErr),
			}
		}
		if closeErr != nil {
			if operationIsIdempotent(operation) && attempt < maxRequestAttempts {
				if retryErr := waitRequestRetry(ctx, attempt, 0); retryErr != nil {
					return retryErr
				}
				continue
			}
			return &NetworkError{
				Kind:     KindNetwork,
				Method:   method,
				Endpoint: endpoint,
				Profile:  currentProfile,
				Err:      sanitizeRequestError(closeErr),
			}
		}

		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			meta := ResponseMeta{
				StatusCode: resp.StatusCode, ContentType: resp.Header.Get("Content-Type"),
				BodyKind: bodyKind(respBody, resp.Header.Get("Content-Type")), BodyLength: len(respBody),
				RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")), Endpoint: endpoint, Profile: currentProfile,
			}
			if !fallbackTried && shouldFallbackHTTP(policy, currentProfile, resp.StatusCode) {
				currentProfile = policy.Fallback
				fallbackTried = true
				attempt = 0
				continue
			}
			httpErr := &HTTPError{
				Kind:       KindHTTP,
				StatusCode: resp.StatusCode,
				Endpoint:   endpoint,
				Profile:    currentProfile,
				Meta:       meta,
			}
			switch resp.StatusCode {
			case http.StatusUnauthorized, http.StatusForbidden:
				httpErr.Disposition = RetryDispositionReauthorize
			case http.StatusMethodNotAllowed:
				// A 405 is only a profile fallback when a fallback was
				// actually available. Once both profiles have been tried it
				// is terminal and must not enter a retry loop.
				if fallbackTried {
					httpErr.Disposition = RetryDispositionFallbackProfile
				} else {
					httpErr.Disposition = RetryDispositionTerminal
				}
			case http.StatusBadRequest, http.StatusNotFound:
				httpErr.Disposition = RetryDispositionTerminal
			}
			if shouldRetryHTTP(resp.StatusCode) {
				if operationIsIdempotent(operation) {
					httpErr.Disposition = RetryDispositionRetryAfter
				} else {
					httpErr.Disposition = RetryDispositionResultUnknown
				}
			}
			if operationIsIdempotent(operation) && shouldRetryHTTP(resp.StatusCode) && attempt < maxRequestAttempts {
				if retryErr := waitRequestRetry(ctx, attempt, meta.RetryAfter); retryErr != nil {
					return retryErr
				}
				continue
			}
			return httpErr
		}

		var envelope responseEnvelope
		if len(respBody) > 0 {
			trimmedBody := bytes.TrimSpace(respBody)
			if len(trimmedBody) > 0 && trimmedBody[0] == '[' {
				envelope.State = new(responseState)
				*envelope.State = true
				envelope.Data = append(json.RawMessage(nil), trimmedBody...)
			} else if err := json.Unmarshal(respBody, &envelope); err != nil {
				meta := ResponseMeta{
					StatusCode:  resp.StatusCode,
					ContentType: resp.Header.Get("Content-Type"),
					BodyKind:    bodyKind(respBody, resp.Header.Get("Content-Type")),
					BodyLength:  len(respBody),
					Endpoint:    endpoint,
					Profile:     currentProfile,
				}
				return &ProtocolError{
					Endpoint: endpoint,
					Message:  fmt.Sprintf("invalid %s response body (%d bytes)", meta.BodyKind, meta.BodyLength),
					Meta:     &meta,
				}
			}
		}

		errno := int(envelope.Errno.value)
		if errno != 0 || (envelope.State != nil && !*envelope.State) {
			return &BusinessError{
				Kind:     classifyBusinessError(errno, envelope.Error, envelope.Message, envelope.Msg),
				Errno:    errno,
				Message:  sanitizeRequestText(firstNonEmpty(envelope.Error, envelope.Message, envelope.Msg)),
				Endpoint: endpoint,
				Profile:  currentProfile,
			}
		}

		if out == nil {
			return nil
		}
		if fullEnvelope, ok := out.(*responseEnvelope); ok {
			*fullEnvelope = envelope
			return nil
		}
		if len(envelope.Data) > 0 && string(envelope.Data) != "null" {
			return json.Unmarshal(envelope.Data, out)
		}
		if len(respBody) == 0 {
			return nil
		}
		return json.Unmarshal(respBody, out)
	}
}

func endpointForProfile(operation Operation, profile Profile, endpoint string) string {
	switch operation {
	case OperationShareSnapshot:
		if profile == ProfileAndroid && endpoint == EndpointShareSnapshot {
			return EndpointShareSnapshotApp
		}
		if profile == ProfileWeb && endpoint == EndpointShareSnapshotApp {
			return EndpointShareSnapshot
		}
	case OperationFileList:
		if profile == ProfileWeb && endpoint == EndpointFileList {
			return EndpointFileListWeb
		}
		if profile == ProfileWeb && endpoint == EndpointCategory {
			return EndpointCategoryWeb
		}
	case OperationShareReceive:
		if profile == ProfileAndroid && endpoint == EndpointShareReceive {
			return EndpointShareReceiveApp
		}
		if profile == ProfileWeb && endpoint == EndpointShareReceiveApp {
			return EndpointShareReceive
		}
		if profile == ProfileAndroid && endpoint == EndpointShareSend {
			return EndpointShareSendApp
		}
		if profile == ProfileWeb && endpoint == EndpointShareSendApp {
			return EndpointShareSend
		}
	}
	return endpoint
}

func waitAccountLimiter(ctx context.Context, limiter *accountLimiter) error {
	if limiter == nil {
		return nil
	}
	if err := limiter.Wait(ctx); err != nil {
		if contextErr := normalizedContextError(ctx, err); contextErr != nil {
			return contextErr
		}
		return err
	}
	return nil
}

func normalizedContextError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if errorsIsContext(err) {
		return err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if strings.Contains(err.Error(), "would exceed context deadline") {
		return context.DeadlineExceeded
	}
	return nil
}

func shouldFallbackProfile(policy operationPolicy, profile Profile) bool {
	return policy.Fallback != "" && policy.Fallback != profile
}

func shouldFallbackHTTP(policy operationPolicy, profile Profile, statusCode int) bool {
	if !shouldFallbackProfile(policy, profile) {
		return false
	}
	return policy.FallbackHTTP405 && statusCode == http.StatusMethodNotAllowed
}

const (
	maxRequestAttempts = 3
	requestRetryBase   = 250 * time.Millisecond
)

func operationIsIdempotent(operation Operation) bool {
	switch operation {
	case OperationUserInfo, OperationFileList, OperationShareSnapshot, OperationShareDownloadURL, OperationDownloadURL,
		OperationQRCodeToken, OperationQRCodeStatus:
		return true
	default:
		return false
	}
}

func shouldRetryHTTP(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= http.StatusInternalServerError
}

func waitRequestRetry(ctx context.Context, attempt int, retryAfter time.Duration) error {
	delay := retryAfter
	if delay <= 0 {
		delay = time.Duration(attempt) * requestRetryBase
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

func parseRetryAfter(value string) time.Duration {
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

func bodyKind(body []byte, contentType string) string {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return "empty"
	}
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	if strings.Contains(contentType, "html") || strings.HasPrefix(trimmed, "<") {
		return "html"
	}
	if strings.Contains(contentType, "json") || strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		return "json"
	}
	if strings.HasPrefix(contentType, "text/") {
		return "text"
	}
	return "binary"
}

func (c *Client) acquirePageGate(ctx context.Context, policy operationPolicy) (func(), error) {
	if !policy.PageCooldown || c.pageLimiter == nil || c.pageLimiter.cooldown <= 0 {
		return func() {}, nil
	}

	gate := pageGateForLimiter(c.pageLimiter)
	select {
	case gate <- struct{}{}:
		return func() {
			<-gate
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func pageGateForLimiter(limiter *pageLimiter) chan struct{} {
	gate, _ := pageRequestGates.LoadOrStore(limiter, make(chan struct{}, 1))
	return gate.(chan struct{})
}

func classifyBusinessError(errno int, values ...string) ErrorKind {
	if kind := classifyBusinessKind(errno); kind != KindBusiness {
		return kind
	}

	message := strings.ToLower(strings.TrimSpace(firstNonEmpty(values...)))
	switch {
	case strings.Contains(message, "风控"), strings.Contains(message, "risk"):
		return KindRisk
	case strings.Contains(message, "参数"), strings.Contains(message, "param"), strings.Contains(message, "invalid"):
		return KindParam
	default:
		return KindBusiness
	}
}

func (c *Client) newRequestWithContext(ctx context.Context, profile Profile, method, endpoint string, query neturl.Values, body []byte, contentType string) (*http.Request, error) {
	targetURL, err := c.resolveURL(profile, endpoint, query)
	if err != nil {
		return nil, err
	}

	var reader io.Reader
	if len(body) > 0 {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, targetURL, reader)
	if err != nil {
		return nil, err
	}
	c.applyHeaders(req, profile, contentType, len(body) > 0)
	return req, nil
}

func (c *Client) resolveURL(profile Profile, endpoint string, query neturl.Values) (string, error) {
	base, err := neturl.Parse(c.baseURL(profile))
	if err != nil {
		return "", err
	}
	ref, err := neturl.Parse(endpoint)
	if err != nil {
		return "", err
	}

	resolved := base.ResolveReference(ref)
	if len(query) > 0 {
		values := resolved.Query()
		for key, items := range query {
			for _, item := range items {
				values.Add(key, item)
			}
		}
		resolved.RawQuery = values.Encode()
	}
	return resolved.String(), nil
}

func (c *Client) applyHeaders(req *http.Request, profile Profile, contentType string, hasBody bool) {
	req.Header.Set("Accept", "application/json")
	if hasBody && contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	userAgent := c.userAgent
	if userAgent == "" {
		userAgent = defaultUserAgent(profile)
	}
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}

	if profile == ProfileWeb {
		base := c.baseURL(ProfileWeb)
		req.Header.Set("Origin", base)
		req.Header.Set("Referer", strings.TrimRight(base, "/")+"/")
		return
	}
	if profile == ProfileChrome {
		req.Header.Set("Referer", strings.TrimRight(c.baseURL(ProfileChrome), "/"))
		return
	}
	if profile == ProfileQRCode || profile == ProfilePassport {
		return
	}

	req.Header.Set("app", string(ProfileAndroid))
	req.Header.Set("appversion", c.appVersion)
}

func defaultUserAgent(profile Profile) string {
	if profile == ProfileAndroid {
		return DefaultAndroidUA
	}
	return DefaultWebUserAgent
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func sanitizeRequestError(err error) error {
	if err == nil {
		return nil
	}
	return simpleSanitizedError{message: sanitizeRequestText(err.Error())}
}

func sanitizeRequestText(value string) string {
	redacted := sanitizeMessage(value)
	for _, key := range []string{"access_token=", "refresh_token=", "token=", "authorization:", "authorization="} {
		redacted = redactAllOccurrences(redacted, key)
	}
	return redacted
}

type simpleSanitizedError struct {
	message string
}

func (e simpleSanitizedError) Error() string {
	return e.message
}

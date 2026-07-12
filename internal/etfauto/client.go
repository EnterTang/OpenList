package etfauto

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type TargetClient struct {
	baseURL    string
	apiToken   string
	publicAPI  bool
	httpClient *http.Client
}

type CreateSubscriptionPayload struct {
	TMDBID       int64  `json:"tmdb_id"`
	MediaType    string `json:"media_type"`
	ShareURL     string `json:"share_url"`
	AccessCode   string `json:"access_code,omitempty"`
	ShareType    string `json:"share_type"`
	SeasonStart  int    `json:"season_start,omitempty"`
	EpisodeStart int    `json:"episode_start,omitempty"`
}

type TargetTaskResult struct {
	SubscriptionID int64  `json:"subscription_id"`
	TaskID         string `json:"task_id"`
	Type           string `json:"type"`
	Status         string `json:"status"`
	RawJSON        string `json:"raw_json"`
}

type DeliveryUncertainError struct{ Err error }

func (e *DeliveryUncertainError) Error() string {
	return "target delivery outcome is unknown: " + e.Err.Error()
}
func (e *DeliveryUncertainError) Unwrap() error { return e.Err }

func IsDeliveryUncertain(err error) bool {
	var target *DeliveryUncertainError
	return errors.As(err, &target)
}

func NewTargetClient(baseURL, apiToken string, httpClient *http.Client, timeout time.Duration) *TargetClient {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	apiToken = strings.TrimSpace(apiToken)
	if httpClient == nil {
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		httpClient = &http.Client{Timeout: timeout}
	}
	return &TargetClient{
		baseURL:    baseURL,
		apiToken:   apiToken,
		publicAPI:  apiToken != "",
		httpClient: httpClient,
	}
}

func (c *TargetClient) CreateSubscription(ctx context.Context, payload CreateSubscriptionPayload, idempotencyKey ...string) (*TargetTaskResult, error) {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(payload); err != nil {
		return nil, err
	}
	path := "/subscriptions"
	if c != nil && c.publicAPI {
		path = "/subscriptions/manual"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint(path), &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	setIdempotencyKey(req, idempotencyKey)
	raw, err := c.do(req)
	if err != nil {
		return nil, err
	}
	return parseTargetTaskResult(raw)
}

func (c *TargetClient) CheckSubscription(ctx context.Context, subscriptionID int64, idempotencyKey ...string) (*TargetTaskResult, error) {
	if subscriptionID <= 0 {
		return nil, fmt.Errorf("subscription id is required")
	}
	id := strconv.FormatInt(subscriptionID, 10)
	if c != nil && c.publicAPI {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/subscriptions/"+id+"/update"), nil)
		if err != nil {
			return nil, err
		}
		setIdempotencyKey(req, idempotencyKey)
		raw, err := c.do(req)
		if err != nil {
			return nil, err
		}
		return parseTargetTaskResult(raw)
	}
	preflightReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint("/subscriptions/"+id), nil)
	if err != nil {
		return nil, err
	}
	if _, err := c.do(preflightReq); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/subscriptions/"+id+"/check"), nil)
	if err != nil {
		return nil, err
	}
	setIdempotencyKey(req, idempotencyKey)
	raw, err := c.do(req)
	if err != nil {
		return nil, err
	}
	return parseTargetTaskResult(raw)
}

func setIdempotencyKey(req *http.Request, values []string) {
	if req == nil || len(values) == 0 {
		return
	}
	if key := strings.TrimSpace(values[0]); key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
}

func (c *TargetClient) endpoint(suffix string) string {
	return strings.TrimRight(c.baseURL, "/") + "/" + strings.TrimLeft(suffix, "/")
}

func (c *TargetClient) do(req *http.Request) ([]byte, error) {
	if c == nil || c.httpClient == nil {
		return nil, fmt.Errorf("target client is not initialized")
	}
	if token := strings.TrimSpace(c.apiToken); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, &DeliveryUncertainError{Err: err}
	}
	defer resp.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if readErr != nil {
		return nil, &DeliveryUncertainError{Err: readErr}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("target service returned %d: %s", resp.StatusCode, string(raw))
		if resp.StatusCode >= 500 {
			return nil, &DeliveryUncertainError{Err: err}
		}
		return nil, err
	}
	return raw, nil
}

func parseTargetTaskResult(raw []byte) (*TargetTaskResult, error) {
	var decoded struct {
		SubscriptionID int64 `json:"subscription_id"`
		Subscription   struct {
			ID int64 `json:"id"`
		} `json:"subscription"`
		TaskID     string `json:"task_id"`
		TaskStatus string `json:"task_status"`
		Type       string `json:"type"`
		Status     string `json:"status"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, err
	}
	subscriptionID := decoded.SubscriptionID
	if subscriptionID <= 0 {
		subscriptionID = decoded.Subscription.ID
	}
	status := strings.TrimSpace(decoded.TaskStatus)
	if status == "" {
		status = strings.TrimSpace(decoded.Status)
	}
	if subscriptionID <= 0 && decoded.TaskID == "" {
		return nil, fmt.Errorf("target service returned unexpected response: %s", string(raw))
	}
	return &TargetTaskResult{
		SubscriptionID: subscriptionID,
		TaskID:         decoded.TaskID,
		Type:           decoded.Type,
		Status:         status,
		RawJSON:        string(raw),
	}, nil
}

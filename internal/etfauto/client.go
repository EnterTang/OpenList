package etfauto

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type TargetClient struct {
	baseURL    string
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

func NewTargetClient(baseURL string, httpClient *http.Client, timeout time.Duration) *TargetClient {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if httpClient == nil {
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		httpClient = &http.Client{Timeout: timeout}
	}
	return &TargetClient{baseURL: baseURL, httpClient: httpClient}
}

func (c *TargetClient) CreateSubscription(ctx context.Context, payload CreateSubscriptionPayload) (*TargetTaskResult, error) {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(payload); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/subscriptions"), &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	raw, err := c.do(req)
	if err != nil {
		return nil, err
	}
	return parseTargetTaskResult(raw)
}

func (c *TargetClient) CheckSubscription(ctx context.Context, subscriptionID int64) (*TargetTaskResult, error) {
	if subscriptionID <= 0 {
		return nil, fmt.Errorf("subscription id is required")
	}
	preflightReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint("/subscriptions/"+strconv.FormatInt(subscriptionID, 10)), nil)
	if err != nil {
		return nil, err
	}
	if _, err := c.do(preflightReq); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/subscriptions/"+strconv.FormatInt(subscriptionID, 10)+"/check"), nil)
	if err != nil {
		return nil, err
	}
	raw, err := c.do(req)
	if err != nil {
		return nil, err
	}
	return parseTargetTaskResult(raw)
}

func (c *TargetClient) endpoint(suffix string) string {
	return strings.TrimRight(c.baseURL, "/") + "/" + strings.TrimLeft(suffix, "/")
}

func (c *TargetClient) do(req *http.Request) ([]byte, error) {
	if c == nil || c.httpClient == nil {
		return nil, fmt.Errorf("target client is not initialized")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("target service returned %d: %s", resp.StatusCode, string(raw))
	}
	return raw, nil
}

func parseTargetTaskResult(raw []byte) (*TargetTaskResult, error) {
	var decoded struct {
		Subscription struct {
			ID int64 `json:"id"`
		} `json:"subscription"`
		TaskID string `json:"task_id"`
		Type   string `json:"type"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, err
	}
	return &TargetTaskResult{
		SubscriptionID: decoded.Subscription.ID,
		TaskID:         decoded.TaskID,
		Type:           decoded.Type,
		Status:         decoded.Status,
		RawJSON:        string(raw),
	}, nil
}

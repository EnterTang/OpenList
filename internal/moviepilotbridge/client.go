package moviepilotbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/google/uuid"
)

const BridgeIntentPath = "/api/v1/plugin/OpenListBridge/intent"

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type Client struct {
	HTTPClient HTTPDoer
	Resolve    SecretResolver
	Now        func() time.Time
}

func (c *Client) SubmitIntent(ctx context.Context, bridge model.MoviePilotBridgeInstance, payload DownloadIntentRequest) error {
	if c == nil || c.HTTPClient == nil {
		return errors.New("moviepilot bridge HTTP client is required")
	}
	if c.Resolve == nil {
		return errors.New("moviepilot bridge secret resolver is not configured")
	}
	return c.postJSON(ctx, bridge, BridgeIntentPath, payload.RequestID, payload, nil)
}

func (c *Client) SearchResources(ctx context.Context, bridge model.MoviePilotBridgeInstance, payload ResourceSearchRequest) ([]ResourceSearchResult, error) {
	if strings.TrimSpace(payload.RequestID) == "" {
		payload.RequestID = uuid.NewString()
	}
	var response ResourceSearchResponse
	if err := c.postJSON(ctx, bridge, BridgeSearchPath, payload.RequestID, payload, &response); err != nil {
		return nil, err
	}
	for _, result := range response.Results {
		if err := validateOpaqueResourceRef(result.ResourceRef); err != nil {
			return nil, fmt.Errorf("MoviePilot Bridge search returned an invalid resource reference: %w", err)
		}
	}
	return response.Results, nil
}

func (c *Client) ControlTorrent(ctx context.Context, bridge model.MoviePilotBridgeInstance, payload TorrentControlRequest) error {
	payload.Action = strings.ToLower(strings.TrimSpace(payload.Action))
	payload.TorrentHash = strings.ToLower(strings.TrimSpace(payload.TorrentHash))
	if err := payload.Validate(); err != nil {
		return err
	}
	return c.postJSON(ctx, bridge, BridgeControlPath, payload.RequestID, payload, nil)
}

func (c *Client) postJSON(ctx context.Context, bridge model.MoviePilotBridgeInstance, endpoint, requestID string, payload any, responsePayload any) error {
	if c == nil || c.HTTPClient == nil {
		return errors.New("moviepilot bridge HTTP client is required")
	}
	if c.Resolve == nil {
		return errors.New("moviepilot bridge secret resolver is not configured")
	}
	baseURL, err := validateBridgeURL(bridge.BaseURL)
	if err != nil {
		return err
	}
	key, err := c.Resolve(ctx, bridge.SecretRef)
	if err != nil {
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal bridge request: %w", err)
	}
	if err := validateNoForbiddenBridgeFields(body); err != nil {
		return err
	}
	now := time.Now().UTC()
	if c.Now != nil {
		now = c.Now().UTC()
	}
	signed := SignRequest{
		Version: SignatureVersionV1, InstanceID: bridge.ID, Method: http.MethodPost,
		Path: endpoint, Timestamp: now, Nonce: uuid.NewString(), Body: body,
	}
	headers, err := signed.Headers(key)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+endpoint, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	request.Header = headers
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-OpenList-Request-ID", requestID)
	response, err := c.HTTPClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	bodyResponse, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if readErr != nil {
		return readErr
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("moviepilot bridge returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(bodyResponse)))
	}
	if responsePayload != nil && len(bodyResponse) > 0 {
		if err := json.Unmarshal(bodyResponse, responsePayload); err != nil {
			return fmt.Errorf("decode moviepilot bridge response: %w", err)
		}
	}
	return nil
}

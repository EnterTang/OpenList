package moviepilotbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/google/uuid"
)

const BridgeIntentPath = "/api/v1/openlist/intent"

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
		return fmt.Errorf("marshal bridge intent: %w", err)
	}
	now := time.Now().UTC()
	if c.Now != nil {
		now = c.Now().UTC()
	}
	signed := SignRequest{
		Version: SignatureVersionV1, InstanceID: bridge.ID, Method: http.MethodPost,
		Path: BridgeIntentPath, Timestamp: now, Nonce: uuid.NewString(), Body: body,
	}
	headers, err := signed.Headers(key)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+BridgeIntentPath, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	request.Header = headers
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-OpenList-Request-ID", payload.RequestID)
	response, err := c.HTTPClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("moviepilot bridge returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(detail)))
	}
	return nil
}

func bridgeURLPath(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return "", errors.New("moviepilot bridge URL must use HTTPS")
	}
	return strings.TrimRight(u.String(), "/") + BridgeIntentPath, nil
}

package moviepilotbridge

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

type captureBridgeHTTPClient struct {
	request *http.Request
	body    []byte
}

func (c *captureBridgeHTTPClient) Do(request *http.Request) (*http.Response, error) {
	c.request = request
	var err error
	c.body, err = io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
}

func TestSubmitIntentPersistsOutboxBeforeSendingSignedBody(t *testing.T) {
	database := newBridgeClientDatabase(t)
	key := []byte("client-signing-key-that-is-long-enough")
	bridge := model.MoviePilotBridgeInstance{
		ID: "mp-client", Name: "client", BaseURL: "https://moviepilot.example/base", SecretRef: "secret", Enabled: true,
	}
	if err := database.Create(&bridge).Error; err != nil {
		t.Fatalf("create bridge: %v", err)
	}
	httpClient := &captureBridgeHTTPClient{}
	now := time.Unix(1700000000, 0).UTC()
	service := NewService(database, func(context.Context, string) ([]byte, error) { return key, nil }, httpClient)
	service.now = func() time.Time { return now }
	intent := &model.MoviePilotDownloadIntent{
		ID: "intent-client", RequestID: "request-client", BridgeInstanceID: bridge.ID,
		MediaSource: "tmdb", MediaID: "123", ResourceRef: "resource-1", TorrentFingerprint: "fingerprint-1",
	}
	payload := DownloadIntentRequest{
		RequestID:        "request-client",
		Media:            MediaIdentity{MediaSource: "tmdb", MediaID: "123", MediaType: "tv", Season: 1, Episode: 2},
		Torrent:          TorrentResource{ResourceRef: "resource-1", SelectedFingerprint: "fingerprint-1"},
		DownloaderPolicy: DownloaderPolicy{Mode: "moviepilot_select"},
	}
	if err := service.SubmitIntent(context.Background(), bridge.ID, intent, payload); err != nil {
		t.Fatalf("submit intent: %v", err)
	}
	if httpClient.request == nil {
		t.Fatal("bridge HTTP request was not sent")
	}
	if got, want := httpClient.request.URL.String(), "https://moviepilot.example/base/api/v1/openlist/intent"; got != want {
		t.Fatalf("bridge URL = %q, want %q", got, want)
	}
	if got := httpClient.request.Header.Get("X-OpenList-Request-ID"); got != payload.RequestID {
		t.Fatalf("request id header = %q", got)
	}
	signed := SignRequest{
		Version: SignatureVersionV1, InstanceID: bridge.ID, Method: http.MethodPost,
		Path: BridgeIntentPath, Timestamp: now, Nonce: httpClient.request.Header.Get(HeaderNonce), Body: httpClient.body,
	}
	expected, err := signed.Signature(key)
	if err != nil {
		t.Fatalf("signature: %v", err)
	}
	if got := httpClient.request.Header.Get(HeaderSignature); got != expected {
		t.Fatalf("signature = %q, want %q", got, expected)
	}
	var outbox model.MoviePilotBridgeOutbox
	if err := database.Where("request_id = ?", payload.RequestID).First(&outbox).Error; err != nil {
		t.Fatalf("load outbox: %v", err)
	}
	if outbox.Status != "sent" || outbox.PayloadJSON == "" {
		t.Fatalf("outbox = %+v", outbox)
	}
}

func TestClientRejectsNonHTTPSBridgeURL(t *testing.T) {
	client := &Client{HTTPClient: &captureBridgeHTTPClient{}, Resolve: func(context.Context, string) ([]byte, error) {
		return []byte("key"), nil
	}}
	err := client.SubmitIntent(context.Background(), model.MoviePilotBridgeInstance{
		ID: "mp", BaseURL: "http://moviepilot.example", SecretRef: "secret",
	}, DownloadIntentRequest{RequestID: "request", Media: MediaIdentity{MediaSource: "tmdb", MediaID: "1"}, Torrent: TorrentResource{ResourceRef: "r"}, DownloaderPolicy: DownloaderPolicy{Mode: "moviepilot_select"}})
	if err == nil || !strings.Contains(err.Error(), "must use HTTPS") {
		t.Fatalf("non-HTTPS error = %v", err)
	}
}

func newBridgeClientDatabase(t *testing.T) *gorm.DB {
	t.Helper()
	database, err := gorm.Open(sqlite.Open("file:moviepilot_bridge_client_test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqlDB, err := database.DB()
	if err != nil {
		t.Fatalf("get sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := database.AutoMigrate(&model.MoviePilotBridgeInstance{}, &model.MoviePilotDownloadIntent{}, &model.MoviePilotBridgeOutbox{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return database
}

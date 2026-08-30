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
	if err := database.Create(&model.MoviePilotTorrentBinding{
		ID: "binding-client", IntentID: intent.ID, BridgeInstanceID: bridge.ID,
		DownloaderAlias: "qb-hk", WorkerNodeID: "worker-1", QBClientID: "qb-client",
		TorrentHash: strings.Repeat("a", 40), ContentPath: "/downloads/Show", Status: model.MoviePilotTorrentStatusBound,
	}).Error; err != nil {
		t.Fatalf("create binding: %v", err)
	}
	if err := service.SubmitIntent(context.Background(), bridge.ID, &model.MoviePilotDownloadIntent{
		ID: "intent-client-retry", RequestID: intent.RequestID, BridgeInstanceID: bridge.ID,
		MediaSource: intent.MediaSource, MediaID: intent.MediaID, ResourceRef: intent.ResourceRef,
		TorrentFingerprint: intent.TorrentFingerprint,
	}, payload); err != nil {
		t.Fatalf("submit idempotent retry: %v", err)
	}
	if httpClient.request == nil {
		t.Fatal("bridge HTTP request was not sent")
	}
	var outboxCount int64
	if err := database.Model(&model.MoviePilotBridgeOutbox{}).Where("request_id = ?", payload.RequestID).Count(&outboxCount).Error; err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxCount != 1 {
		t.Fatalf("outbox count = %d, want one idempotent row", outboxCount)
	}
	if got, want := httpClient.request.URL.String(), "https://moviepilot.example/base/api/v1/plugin/OpenListBridge/intent"; got != want {
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

func TestSubmitIntentReconcilesSentOrphanIntent(t *testing.T) {
	database := newBridgeClientDatabase(t)
	key := []byte("client-signing-key-that-is-long-enough")
	bridge := model.MoviePilotBridgeInstance{
		ID: "mp-reconcile", Name: "reconcile", BaseURL: "https://moviepilot.example", SecretRef: "secret", Enabled: true,
	}
	if err := database.Create(&bridge).Error; err != nil {
		t.Fatalf("create bridge: %v", err)
	}
	httpClient := &captureBridgeHTTPClient{}
	service := NewService(database, func(context.Context, string) ([]byte, error) { return key, nil }, httpClient)
	intent := &model.MoviePilotDownloadIntent{
		ID: "intent-reconcile", RequestID: "request-reconcile", BridgeInstanceID: bridge.ID,
		MediaSource: "tmdb", MediaID: "123", ResourceRef: "resource-reconcile", TorrentFingerprint: "fingerprint-reconcile",
	}
	payload := DownloadIntentRequest{
		RequestID:        intent.RequestID,
		Media:            MediaIdentity{MediaSource: "tmdb", MediaID: "123"},
		Torrent:          TorrentResource{ResourceRef: intent.ResourceRef, SelectedFingerprint: intent.TorrentFingerprint},
		DownloaderPolicy: DownloaderPolicy{Mode: "moviepilot_select"},
	}
	if err := service.SubmitIntent(context.Background(), bridge.ID, intent, payload); err != nil {
		t.Fatalf("initial submit: %v", err)
	}
	if err := service.SubmitIntent(context.Background(), bridge.ID, intent, payload); err != nil {
		t.Fatalf("orphan reconcile: %v", err)
	}
	if got, want := httpClient.request.URL.Path, "/api/v1/plugin/OpenListBridge/intent/request-reconcile/reconcile"; got != want {
		t.Fatalf("reconcile path = %q, want %q", got, want)
	}
	if got, want := string(httpClient.body), "{}"; got != want {
		t.Fatalf("reconcile body = %q, want %q", got, want)
	}
}

func TestSubmitIntentRejectsRequestIDReuseWithDifferentPayload(t *testing.T) {
	database := newBridgeClientDatabase(t)
	bridge := model.MoviePilotBridgeInstance{
		ID: "mp-payload-idempotency", Name: "payload", BaseURL: "https://moviepilot.example",
		SecretRef: "secret", Enabled: true,
	}
	if err := database.Create(&bridge).Error; err != nil {
		t.Fatal(err)
	}
	service := NewService(database, func(context.Context, string) ([]byte, error) {
		return []byte("client-signing-key-that-is-long-enough"), nil
	}, &captureBridgeHTTPClient{})
	intent := &model.MoviePilotDownloadIntent{
		ID: "intent-payload-idempotency", RequestID: "request-payload-idempotency",
		BridgeInstanceID: bridge.ID, MediaSource: "tmdb", MediaID: "123",
		ResourceRef: "resource-1", TorrentFingerprint: "fingerprint-1",
	}
	payload := DownloadIntentRequest{
		RequestID:        intent.RequestID,
		Media:            MediaIdentity{MediaSource: "tmdb", MediaID: "123"},
		Torrent:          TorrentResource{ResourceRef: intent.ResourceRef, SelectedFingerprint: intent.TorrentFingerprint, Title: "first title"},
		DownloaderPolicy: DownloaderPolicy{Mode: "moviepilot_select"},
	}
	if err := service.SubmitIntent(context.Background(), bridge.ID, intent, payload); err != nil {
		t.Fatal(err)
	}
	payload.Torrent.Title = "different title"
	if err := service.SubmitIntent(context.Background(), bridge.ID, intent, payload); err == nil || !strings.Contains(err.Error(), "different payload") {
		t.Fatalf("request ID payload reuse error = %v", err)
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

func TestClientRejectsBridgeURLUserInfo(t *testing.T) {
	client := &Client{HTTPClient: &captureBridgeHTTPClient{}, Resolve: func(context.Context, string) ([]byte, error) {
		return []byte("key-that-is-long-enough"), nil
	}}
	err := client.ControlTorrent(context.Background(), model.MoviePilotBridgeInstance{
		ID: "mp", BaseURL: "https://admin:secret@moviepilot.example", SecretRef: "secret",
	}, TorrentControlRequest{RequestID: "request", Downloader: "qb", TorrentHash: strings.Repeat("a", 40), Action: "pause"})
	if err == nil || !strings.Contains(err.Error(), "userinfo") {
		t.Fatalf("bridge URL userinfo error = %v", err)
	}
}

func TestClientSendsSignedExactTorrentControl(t *testing.T) {
	httpClient := &captureBridgeHTTPClient{}
	key := []byte("client-signing-key-that-is-long-enough")
	now := time.Unix(1700000000, 0).UTC()
	client := &Client{
		HTTPClient: httpClient, Resolve: func(context.Context, string) ([]byte, error) { return key, nil },
		Now: func() time.Time { return now },
	}
	bridge := model.MoviePilotBridgeInstance{ID: "mp-control", BaseURL: "https://moviepilot.example/base", SecretRef: "secret"}
	payload := TorrentControlRequest{RequestID: "request-control", Downloader: "qb-hk", TorrentHash: strings.Repeat("a", 40), Action: "pause", Reason: "worker_offline"}
	if err := client.ControlTorrent(context.Background(), bridge, payload); err != nil {
		t.Fatal(err)
	}
	if got, want := httpClient.request.URL.String(), "https://moviepilot.example/base/api/v1/plugin/OpenListBridge/control"; got != want {
		t.Fatalf("control URL = %q, want %q", got, want)
	}
	signed := SignRequest{
		Version: SignatureVersionV1, InstanceID: bridge.ID, Method: http.MethodPost,
		Path: BridgeControlPath, Timestamp: now, Nonce: httpClient.request.Header.Get(HeaderNonce), Body: httpClient.body,
	}
	wantSignature, err := signed.Signature(key)
	if err != nil {
		t.Fatal(err)
	}
	if got := httpClient.request.Header.Get(HeaderSignature); got != wantSignature {
		t.Fatalf("control signature = %q, want %q", got, wantSignature)
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
	if err := database.AutoMigrate(&model.MoviePilotBridgeInstance{}, &model.MoviePilotDownloadIntent{}, &model.MoviePilotTorrentBinding{}, &model.MoviePilotBridgeOutbox{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return database
}

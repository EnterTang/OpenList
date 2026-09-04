package moviepilotbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

type retryBridgeHTTPClient struct {
	calls  int
	nonces []string
}

type cancelledIntentBridgeHTTPClient struct {
	paths []string
}

func (c *cancelledIntentBridgeHTTPClient) Do(request *http.Request) (*http.Response, error) {
	c.paths = append(c.paths, request.URL.Path)
	_, _ = io.Copy(io.Discard, request.Body)
	if strings.HasSuffix(request.URL.Path, "/reconcile") {
		return &http.Response{
			StatusCode: http.StatusConflict,
			Body:       io.NopCloser(strings.NewReader(`{"error":"failed or cancelled intent cannot be reconciled"}`)),
			Header:     make(http.Header),
		}, nil
	}
	return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
}

func (c *retryBridgeHTTPClient) Do(request *http.Request) (*http.Response, error) {
	c.calls++
	c.nonces = append(c.nonces, request.Header.Get(HeaderNonce))
	_, _ = io.Copy(io.Discard, request.Body)
	if c.calls == 1 {
		return nil, errors.New("bridge temporarily unavailable")
	}
	return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
}

func TestProcessPendingOutboxRetriesFailedIntentWithFreshSignature(t *testing.T) {
	database := newBridgeClientDatabase(t)
	bridge := model.MoviePilotBridgeInstance{ID: "mp-retry", Name: "retry", BaseURL: "https://moviepilot.example", SecretRef: "secret", Enabled: true}
	if err := database.Create(&bridge).Error; err != nil {
		t.Fatalf("create bridge: %v", err)
	}
	current := time.Unix(1700000000, 0).UTC()
	httpClient := &retryBridgeHTTPClient{}
	service := NewService(database, func(context.Context, string) ([]byte, error) {
		return []byte("retry-signing-key-that-is-long-enough"), nil
	}, httpClient)
	service.now = func() time.Time { return current }
	intent := &model.MoviePilotDownloadIntent{
		ID: "intent-retry", RequestID: "request-retry", BridgeInstanceID: bridge.ID,
		MediaSource: "tmdb", MediaID: "123", ResourceRef: "resource-retry", TorrentFingerprint: "fingerprint-retry",
	}
	payload := DownloadIntentRequest{
		RequestID:        intent.RequestID,
		Media:            MediaIdentity{MediaSource: "tmdb", MediaID: "123", MediaType: "tv", Season: 1, Episode: 1},
		Torrent:          TorrentResource{ResourceRef: intent.ResourceRef, SelectedFingerprint: intent.TorrentFingerprint},
		DownloaderPolicy: DownloaderPolicy{Mode: "moviepilot_select"},
	}
	if err := service.SubmitIntent(context.Background(), bridge.ID, intent, payload); err == nil {
		t.Fatal("first submission unexpectedly succeeded")
	}
	var outbox model.MoviePilotBridgeOutbox
	if err := database.First(&outbox, "request_id = ?", intent.RequestID).Error; err != nil {
		t.Fatalf("load failed outbox: %v", err)
	}
	if outbox.Status != "failed" || outbox.AttemptCount != 1 || !outbox.AvailableAt.After(current) {
		t.Fatalf("failed outbox = %#v", outbox)
	}
	current = outbox.AvailableAt.Add(time.Second)
	processed, err := service.ProcessPendingOutbox(context.Background(), 10)
	if err != nil {
		t.Fatalf("retry pending outbox: %v", err)
	}
	if processed != 1 || httpClient.calls != 2 {
		t.Fatalf("retry result processed=%d calls=%d", processed, httpClient.calls)
	}
	if err := database.First(&outbox, "request_id = ?", intent.RequestID).Error; err != nil {
		t.Fatalf("reload outbox: %v", err)
	}
	if outbox.Status != "sent" || outbox.AttemptCount != 2 || outbox.LastError != "" {
		t.Fatalf("sent outbox = %#v", outbox)
	}
	if len(httpClient.nonces) != 2 || httpClient.nonces[0] == httpClient.nonces[1] {
		t.Fatalf("retry signatures reused nonce: %#v", httpClient.nonces)
	}
}

func TestReconcileSentIntentBindingsRepairsOrphanWithoutSubscriptionRerun(t *testing.T) {
	database := newBridgeClientDatabase(t)
	bridge := model.MoviePilotBridgeInstance{ID: "mp-orphan", Name: "orphan", BaseURL: "https://moviepilot.example", SecretRef: "secret", Enabled: true}
	if err := database.Create(&bridge).Error; err != nil {
		t.Fatalf("create bridge: %v", err)
	}
	now := time.Unix(1700000000, 0).UTC()
	intent := model.MoviePilotDownloadIntent{
		ID: "intent-orphan", RequestID: "request-orphan", BridgeInstanceID: bridge.ID,
		SubscriptionID: 1, MediaSource: "tmdb", MediaID: "123", ResourceRef: "resource-orphan",
		TorrentFingerprint: "fingerprint-orphan", Status: model.MoviePilotIntentStatusAccepted,
	}
	if err := database.Create(&intent).Error; err != nil {
		t.Fatalf("create intent: %v", err)
	}
	if err := database.Create(&model.MoviePilotBridgeOutbox{
		ID: "outbox-orphan", BridgeID: bridge.ID, RequestID: intent.RequestID, EventID: "event-orphan",
		PayloadJSON: "{}", Status: "sent", CreatedAt: now.Add(-2 * time.Minute), UpdatedAt: now.Add(-2 * time.Minute), AvailableAt: now.Add(-2 * time.Minute),
	}).Error; err != nil {
		t.Fatalf("create sent outbox: %v", err)
	}
	httpClient := &captureBridgeHTTPClient{}
	service := NewService(database, func(context.Context, string) ([]byte, error) {
		return []byte("orphan-reconcile-signing-key"), nil
	}, httpClient)
	service.now = func() time.Time { return now }
	processed, err := service.ReconcileSentIntentBindings(context.Background(), 10)
	if err != nil {
		t.Fatalf("reconcile sent intent: %v", err)
	}
	if processed != 1 {
		t.Fatalf("reconciled rows = %d, want 1", processed)
	}
	if httpClient.request == nil || httpClient.request.URL.Path != "/api/v1/plugin/OpenListBridge/intent/request-orphan/reconcile" {
		t.Fatalf("reconcile request = %#v", httpClient.request)
	}
	var outbox model.MoviePilotBridgeOutbox
	if err := database.First(&outbox, "id = ?", "outbox-orphan").Error; err != nil {
		t.Fatalf("reload sent outbox: %v", err)
	}
	if !outbox.AvailableAt.After(now) || outbox.LastError != "" {
		t.Fatalf("outbox after successful reconcile = %#v", outbox)
	}
}

func TestSubmitIntentRetriesBridgeCancelledIntent(t *testing.T) {
	database := newBridgeClientDatabase(t)
	bridge := model.MoviePilotBridgeInstance{ID: "mp-cancelled", Name: "cancelled", BaseURL: "https://moviepilot.example", SecretRef: "secret", Enabled: true}
	if err := database.Create(&bridge).Error; err != nil {
		t.Fatalf("create bridge: %v", err)
	}
	now := time.Unix(1700000000, 0).UTC()
	intent := &model.MoviePilotDownloadIntent{
		ID: "intent-cancelled", RequestID: "request-cancelled", BridgeInstanceID: bridge.ID,
		MediaSource: "tmdb", MediaID: "123", ResourceRef: "resource-cancelled", TorrentFingerprint: "fingerprint-cancelled",
		Status: model.MoviePilotIntentStatusPending,
	}
	payload := DownloadIntentRequest{
		RequestID:        intent.RequestID,
		Media:            MediaIdentity{MediaSource: "tmdb", MediaID: "123"},
		Torrent:          TorrentResource{ResourceRef: intent.ResourceRef, SelectedFingerprint: intent.TorrentFingerprint},
		DownloaderPolicy: DownloaderPolicy{Mode: DownloaderPolicyMoviePilotSelect},
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if err := database.Create(intent).Error; err != nil {
		t.Fatalf("create intent: %v", err)
	}
	if err := database.Create(&model.MoviePilotBridgeOutbox{
		ID: "outbox-cancelled", BridgeID: bridge.ID, RequestID: intent.RequestID, EventID: "event-cancelled",
		PayloadJSON: string(payloadJSON), Status: "sent", CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute), AvailableAt: now.Add(-time.Minute),
	}).Error; err != nil {
		t.Fatalf("create outbox: %v", err)
	}
	httpClient := &cancelledIntentBridgeHTTPClient{}
	service := NewService(database, func(context.Context, string) ([]byte, error) {
		return []byte("cancelled-intent-signing-key"), nil
	}, httpClient)
	service.now = func() time.Time { return now }
	if err := service.SubmitIntent(context.Background(), bridge.ID, intent, payload); err != nil {
		t.Fatalf("retry cancelled intent: %v", err)
	}
	if len(httpClient.paths) != 2 || !strings.HasSuffix(httpClient.paths[0], "/reconcile") || httpClient.paths[1] != BridgeIntentPath {
		t.Fatalf("bridge retry paths = %#v", httpClient.paths)
	}
	var outbox model.MoviePilotBridgeOutbox
	if err := database.First(&outbox, "id = ?", "outbox-cancelled").Error; err != nil {
		t.Fatalf("reload outbox: %v", err)
	}
	if outbox.Status != "sent" || outbox.LastError != "" {
		t.Fatalf("outbox after cancelled retry = %#v", outbox)
	}
}

func TestReconcileSentIntentBindingsRequeuesBridgeCancelledIntent(t *testing.T) {
	database := newBridgeClientDatabase(t)
	bridge := model.MoviePilotBridgeInstance{ID: "mp-cancelled-reconcile", Name: "cancelled-reconcile", BaseURL: "https://moviepilot.example", SecretRef: "secret", Enabled: true}
	if err := database.Create(&bridge).Error; err != nil {
		t.Fatalf("create bridge: %v", err)
	}
	now := time.Unix(1700000000, 0).UTC()
	intent := model.MoviePilotDownloadIntent{
		ID: "intent-cancelled-reconcile", RequestID: "request-cancelled-reconcile", BridgeInstanceID: bridge.ID,
		MediaSource: "tmdb", MediaID: "123", ResourceRef: "resource-cancelled-reconcile", TorrentFingerprint: "fingerprint-cancelled-reconcile",
		Status: model.MoviePilotIntentStatusAccepted,
	}
	payload := DownloadIntentRequest{
		RequestID:        intent.RequestID,
		Media:            MediaIdentity{MediaSource: "tmdb", MediaID: "123"},
		Torrent:          TorrentResource{ResourceRef: intent.ResourceRef, SelectedFingerprint: intent.TorrentFingerprint},
		DownloaderPolicy: DownloaderPolicy{Mode: DownloaderPolicyMoviePilotSelect},
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if err := database.Create(&intent).Error; err != nil {
		t.Fatalf("create intent: %v", err)
	}
	if err := database.Create(&model.MoviePilotBridgeOutbox{
		ID: "outbox-cancelled-reconcile", BridgeID: bridge.ID, RequestID: intent.RequestID, EventID: "event-cancelled-reconcile",
		PayloadJSON: string(payloadJSON), Status: "sent", CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute), AvailableAt: now.Add(-time.Minute),
	}).Error; err != nil {
		t.Fatalf("create outbox: %v", err)
	}
	httpClient := &cancelledIntentBridgeHTTPClient{}
	service := NewService(database, func(context.Context, string) ([]byte, error) {
		return []byte("cancelled-reconcile-signing-key"), nil
	}, httpClient)
	service.now = func() time.Time { return now }
	processed, err := service.ReconcileSentIntentBindings(context.Background(), 10)
	if err != nil || processed != 1 {
		t.Fatalf("reconcile cancelled intent = processed %d err %v", processed, err)
	}
	var outbox model.MoviePilotBridgeOutbox
	if err := database.First(&outbox, "id = ?", "outbox-cancelled-reconcile").Error; err != nil {
		t.Fatalf("reload outbox: %v", err)
	}
	if outbox.Status != "pending" || !outbox.AvailableAt.Equal(now) {
		t.Fatalf("requeued outbox = %#v", outbox)
	}
	processed, err = service.ProcessPendingOutbox(context.Background(), 10)
	if err != nil || processed != 1 {
		t.Fatalf("process requeued intent = processed %d err %v", processed, err)
	}
	if len(httpClient.paths) != 2 || httpClient.paths[1] != BridgeIntentPath {
		t.Fatalf("bridge retry paths = %#v", httpClient.paths)
	}
}

func TestProcessPendingEventsMarksHandlerFailureForRetry(t *testing.T) {
	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqlDB, err := database.DB()
	if err != nil {
		t.Fatalf("get sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := database.AutoMigrate(&model.MoviePilotBridgeInbox{}); err != nil {
		t.Fatalf("migrate inbox: %v", err)
	}
	payload, err := json.Marshal(BridgeEvent{EventID: "event-retry", RequestID: "request-retry", Type: EventTorrentStateChanged})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	if err := database.Create(&model.MoviePilotBridgeInbox{
		EventID: "event-retry", BridgeID: "bridge-1", RequestID: "request-retry", EventType: EventTorrentStateChanged,
		PayloadJSON: string(payload), Status: "received", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}).Error; err != nil {
		t.Fatalf("create inbox event: %v", err)
	}
	wantErr := errors.New("coordinator temporarily unavailable")
	service := NewService(database, nil, nil)
	service.SetEventHandler(func(context.Context, string, BridgeEvent) error { return wantErr })
	if processed, err := service.ProcessPendingEvents(context.Background(), 10); !errors.Is(err, wantErr) || processed != 0 {
		t.Fatalf("process pending = processed %d err %v", processed, err)
	}
	var inbox model.MoviePilotBridgeInbox
	if err := database.First(&inbox, "event_id = ?", "event-retry").Error; err != nil {
		t.Fatalf("load inbox event: %v", err)
	}
	if inbox.Status != "failed" || inbox.AttemptCount != 1 || inbox.LastError != wantErr.Error() {
		t.Fatalf("inbox after handler failure = %#v", inbox)
	}
}

func TestProcessPendingEventsReclaimsStaleProcessingEvent(t *testing.T) {
	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqlDB, err := database.DB()
	if err != nil {
		t.Fatalf("get sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := database.AutoMigrate(&model.MoviePilotBridgeInbox{}); err != nil {
		t.Fatalf("migrate inbox: %v", err)
	}
	now := time.Unix(1700000000, 0).UTC()
	payload, err := json.Marshal(BridgeEvent{EventID: "event-stale", RequestID: "request-stale", Type: EventTorrentStateChanged})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	if err := database.Create(&model.MoviePilotBridgeInbox{
		EventID: "event-stale", BridgeID: "bridge-1", RequestID: "request-stale", EventType: EventTorrentStateChanged,
		PayloadJSON: string(payload), Status: "processing", CreatedAt: now.Add(-20 * time.Minute), UpdatedAt: now.Add(-10 * time.Minute),
	}).Error; err != nil {
		t.Fatalf("create stale inbox event: %v", err)
	}
	service := NewService(database, nil, nil)
	service.now = func() time.Time { return now }
	service.SetEventHandler(func(context.Context, string, BridgeEvent) error { return nil })
	if processed, err := service.ProcessPendingEvents(context.Background(), 10); err != nil || processed != 1 {
		t.Fatalf("process stale pending = processed %d err %v", processed, err)
	}
	var inbox model.MoviePilotBridgeInbox
	if err := database.First(&inbox, "event_id = ?", "event-stale").Error; err != nil {
		t.Fatalf("load stale inbox event: %v", err)
	}
	if inbox.Status != "processed" || inbox.AttemptCount != 1 || inbox.ProcessedAt == nil {
		t.Fatalf("stale inbox after reclaim = %#v", inbox)
	}
}

func TestConsumeBridgeEventRejectsEventIDPayloadCollision(t *testing.T) {
	database, _, key, now := newVerifierTest(t)
	bridge := model.MoviePilotBridgeInstance{ID: "mp-event-collision", Name: "collision", BaseURL: "https://moviepilot.example", SecretRef: "secret", Enabled: true}
	if err := database.Create(&bridge).Error; err != nil {
		t.Fatal(err)
	}
	service := NewService(database, func(context.Context, string) ([]byte, error) { return key, nil }, nil)
	service.now = func() time.Time { return now }
	path := "/api/v1/cluster/moviepilot/events"
	consume := func(event BridgeEvent, nonce string) error {
		body, err := json.Marshal(event)
		if err != nil {
			return err
		}
		headers, err := (SignRequest{
			Version: SignatureVersionV1, InstanceID: bridge.ID, Method: http.MethodPost,
			Path: path, Timestamp: now, Nonce: nonce, Body: body,
		}).Headers(key)
		if err != nil {
			return err
		}
		_, err = service.ConsumeBridgeEvent(context.Background(), headers, http.MethodPost, path, body, event)
		return err
	}
	first := BridgeEvent{EventID: "event-collision", RequestID: "request-1", Type: EventIntentAccepted, OccurredAt: now}
	if err := consume(first, "nonce-collision-first"); err != nil {
		t.Fatal(err)
	}
	changed := first
	changed.OccurredAt = changed.OccurredAt.Add(time.Minute)
	if err := consume(changed, "nonce-collision-changed"); err == nil || !strings.Contains(err.Error(), "different Bridge event") {
		t.Fatalf("event ID payload collision error = %v", err)
	}
}

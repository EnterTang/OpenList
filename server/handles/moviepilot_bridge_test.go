package handles

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/moviepilotbridge"
	"github.com/OpenListTeam/OpenList/v4/server/middlewares"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestMoviePilotBridgeEventHandlerRejectsInvalidSignature(t *testing.T) {
	service, bridge, key := newMoviePilotBridgeHandlerService(t)
	engine := moviePilotBridgeTestEngine(service)
	body := bridgeEventBody(t, "event-invalid")
	signed := signBridgeHandlerBody(bridge.ID, "/events", body, key, time.Now().UTC(), "nonce-invalid")
	signed.Set(moviepilotbridge.HeaderSignature, strings.Repeat("0", 64))
	request := newBridgeHandlerRequest(http.MethodPost, "/events", body, signed)
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("invalid signature status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestMoviePilotBridgeEventHandlerRejectsExpiredTimestamp(t *testing.T) {
	service, bridge, key := newMoviePilotBridgeHandlerService(t)
	engine := moviePilotBridgeTestEngine(service)
	body := bridgeEventBody(t, "event-expired")
	signed := signBridgeHandlerBody(bridge.ID, "/events", body, key, time.Now().UTC().Add(-10*time.Minute), "nonce-expired")
	request := newBridgeHandlerRequest(http.MethodPost, "/events", body, signed)
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expired timestamp status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestMoviePilotBridgeEventHandlerStoresAndDeduplicatesEvent(t *testing.T) {
	service, bridge, key := newMoviePilotBridgeHandlerService(t)
	engine := moviePilotBridgeTestEngine(service)
	body := bridgeEventBody(t, "event-duplicate")
	firstHeaders := signBridgeHandlerBody(bridge.ID, "/events", body, key, time.Now().UTC(), "nonce-first")
	first := httptest.NewRecorder()
	engine.ServeHTTP(first, newBridgeHandlerRequest(http.MethodPost, "/events", body, firstHeaders))
	if first.Code != http.StatusOK || !strings.Contains(first.Body.String(), `"stored":true`) {
		t.Fatalf("first event response = %d %s", first.Code, first.Body.String())
	}
	secondHeaders := signBridgeHandlerBody(bridge.ID, "/events", body, key, time.Now().UTC(), "nonce-second")
	second := httptest.NewRecorder()
	engine.ServeHTTP(second, newBridgeHandlerRequest(http.MethodPost, "/events", body, secondHeaders))
	if second.Code != http.StatusOK || !strings.Contains(second.Body.String(), `"duplicate":true`) {
		t.Fatalf("duplicate event response = %d %s", second.Code, second.Body.String())
	}
}

func TestMoviePilotBridgeEventHandlerRejectsEventIDPayloadCollision(t *testing.T) {
	service, bridge, key := newMoviePilotBridgeHandlerService(t)
	engine := moviePilotBridgeTestEngine(service)
	firstBody := bridgeEventBody(t, "event-payload-collision")
	firstHeaders := signBridgeHandlerBody(bridge.ID, "/events", firstBody, key, time.Now().UTC(), "nonce-payload-first")
	first := httptest.NewRecorder()
	engine.ServeHTTP(first, newBridgeHandlerRequest(http.MethodPost, "/events", firstBody, firstHeaders))
	if first.Code != http.StatusOK {
		t.Fatalf("first event response = %d %s", first.Code, first.Body.String())
	}
	changedBody, err := json.Marshal(moviepilotbridge.BridgeEvent{
		EventID: "event-payload-collision", RequestID: "request-1", Type: moviepilotbridge.EventIntentAccepted,
		OccurredAt: time.Now().UTC().Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	changedHeaders := signBridgeHandlerBody(bridge.ID, "/events", changedBody, key, time.Now().UTC(), "nonce-payload-changed")
	changed := httptest.NewRecorder()
	engine.ServeHTTP(changed, newBridgeHandlerRequest(http.MethodPost, "/events", changedBody, changedHeaders))
	if changed.Code != http.StatusBadRequest || !strings.Contains(changed.Body.String(), "different Bridge event") {
		t.Fatalf("event ID collision response = %d %s", changed.Code, changed.Body.String())
	}
}

func moviePilotBridgeTestEngine(service *moviepilotbridge.Service) *gin.Engine {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.POST("/events", middlewares.MoviePilotBridgeAuth(service), ConsumeMoviePilotBridgeEvent(service))
	return engine
}

func newMoviePilotBridgeHandlerService(t *testing.T) (*moviepilotbridge.Service, model.MoviePilotBridgeInstance, []byte) {
	t.Helper()
	database, err := gorm.Open(sqlite.Open("file:moviepilot_bridge_handler_"+strings.ReplaceAll(t.Name(), "/", "_")+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqlDB, err := database.DB()
	if err != nil {
		t.Fatalf("get sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := database.AutoMigrate(&model.MoviePilotBridgeInstance{}, &model.MoviePilotBridgeNonce{}, &model.MoviePilotBridgeInbox{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	key := []byte("bridge-handler-signing-key")
	bridge := model.MoviePilotBridgeInstance{ID: "mp-handler", Name: "handler", BaseURL: "https://moviepilot.example", SecretRef: "secret", Enabled: true}
	if err := database.Create(&bridge).Error; err != nil {
		t.Fatalf("create bridge: %v", err)
	}
	service := moviepilotbridge.NewService(database, func(context.Context, string) ([]byte, error) { return key, nil }, http.DefaultClient)
	return service, bridge, key
}

func bridgeEventBody(t *testing.T, eventID string) []byte {
	t.Helper()
	body, err := json.Marshal(moviepilotbridge.BridgeEvent{
		EventID: eventID, RequestID: "request-1", Type: moviepilotbridge.EventIntentAccepted,
		OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return body
}

func signBridgeHandlerBody(instanceID, path string, body, key []byte, timestamp time.Time, nonce string) http.Header {
	signed := moviepilotbridge.SignRequest{
		Version: moviepilotbridge.SignatureVersionV1, InstanceID: instanceID, Method: http.MethodPost,
		Path: path, Timestamp: timestamp, Nonce: nonce, Body: body,
	}
	headers, err := signed.Headers(key)
	if err != nil {
		panic(err)
	}
	return headers
}

func newBridgeHandlerRequest(method, path string, body []byte, headers http.Header) *http.Request {
	request := httptest.NewRequest(method, path, strings.NewReader(string(body)))
	request.Header = headers
	request.TLS = &tls.ConnectionState{}
	return request
}

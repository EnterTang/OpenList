package moviepilotbridge

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestVerifyRequestRejectsReplayedNonce(t *testing.T) {
	database, verifier, key, now := newVerifierTest(t)
	if err := database.Create(&model.MoviePilotBridgeInstance{
		ID: "mp-main", Name: "main", BaseURL: "https://moviepilot.example", SecretRef: "secret", Enabled: true,
	}).Error; err != nil {
		t.Fatalf("create bridge: %v", err)
	}
	body := []byte(`{"event_id":"e-1"}`)
	signed := SignRequest{
		Version: SignatureVersionV1, InstanceID: "mp-main", Method: http.MethodPost,
		Path: "/api/v1/cluster/moviepilot/events", Timestamp: now, Nonce: "nonce-1", Body: body,
	}
	headers, err := signed.Headers(key)
	if err != nil {
		t.Fatalf("sign request: %v", err)
	}
	if err := verifier.Verify(context.Background(), headers, http.MethodPost, signed.Path, body); err != nil {
		t.Fatalf("verify first request: %v", err)
	}
	if err := verifier.Verify(context.Background(), headers, http.MethodPost, signed.Path, body); err == nil || err.Error() != "bridge nonce has already been used" {
		t.Fatalf("verify replay error = %v", err)
	}
}

func TestVerifyRequestRejectsExpiredTimestamp(t *testing.T) {
	database, verifier, key, now := newVerifierTest(t)
	if err := database.Create(&model.MoviePilotBridgeInstance{
		ID: "mp-main", Name: "main", BaseURL: "https://moviepilot.example", SecretRef: "secret", Enabled: true,
	}).Error; err != nil {
		t.Fatalf("create bridge: %v", err)
	}
	body := []byte(`{"event_id":"e-1"}`)
	signed := SignRequest{
		Version: SignatureVersionV1, InstanceID: "mp-main", Method: http.MethodPost,
		Path: "/api/v1/cluster/moviepilot/events", Timestamp: now.Add(-DefaultMaxClockSkew - time.Second), Nonce: "nonce-expired", Body: body,
	}
	headers, err := signed.Headers(key)
	if err != nil {
		t.Fatalf("sign request: %v", err)
	}
	if err := verifier.Verify(context.Background(), headers, http.MethodPost, signed.Path, body); err == nil || err.Error() != "bridge timestamp is expired" {
		t.Fatalf("verify expired error = %v", err)
	}
}

func TestVerifyRequestPrunesExpiredNonceRows(t *testing.T) {
	database, verifier, key, now := newVerifierTest(t)
	if err := database.Create(&model.MoviePilotBridgeInstance{
		ID: "mp-main", Name: "main", BaseURL: "https://moviepilot.example", SecretRef: "secret", Enabled: true,
	}).Error; err != nil {
		t.Fatalf("create bridge: %v", err)
	}
	if err := database.Create(&model.MoviePilotBridgeNonce{
		ID: "old-nonce", BridgeID: "mp-main", Nonce: "old", CreatedAt: now.Add(-time.Hour), UsedAt: now.Add(-time.Hour),
	}).Error; err != nil {
		t.Fatalf("create old nonce: %v", err)
	}
	body := []byte(`{"event_id":"e-new"}`)
	signed := SignRequest{
		Version: SignatureVersionV1, InstanceID: "mp-main", Method: http.MethodPost,
		Path: "/api/v1/cluster/moviepilot/events", Timestamp: now, Nonce: "nonce-new", Body: body,
	}
	headers, err := signed.Headers(key)
	if err != nil {
		t.Fatalf("sign request: %v", err)
	}

	if err := verifier.Verify(context.Background(), headers, http.MethodPost, signed.Path, body); err != nil {
		t.Fatalf("verify request: %v", err)
	}

	var oldCount int64
	if err := database.Model(&model.MoviePilotBridgeNonce{}).Where("id = ?", "old-nonce").Count(&oldCount).Error; err != nil {
		t.Fatalf("count old nonce: %v", err)
	}
	if oldCount != 0 {
		t.Fatalf("expired nonce count = %d, want zero", oldCount)
	}
}

func TestSignRequestUsesRawBodyHashAndMethod(t *testing.T) {
	request := SignRequest{
		Version: SignatureVersionV1, InstanceID: "mp-main", Method: "post",
		Path: "/api/v1/openlist/intent?retry=1", Timestamp: time.Unix(1700000000, 0).UTC(), Nonce: "nonce", Body: []byte("body"),
	}
	canonical, err := request.Canonical()
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}
	if !strings.Contains(string(canonical), "\nPOST\n/api/v1/openlist/intent?retry=1\n") {
		t.Fatalf("canonical request = %q", canonical)
	}
}

func newVerifierTest(t *testing.T) (*gorm.DB, *Verifier, []byte, time.Time) {
	t.Helper()
	database, err := gorm.Open(sqlite.Open("file:moviepilot_bridge_signing_test?mode=memory&cache=shared"), &gorm.Config{})
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
	key := []byte("bridge-signing-key-that-is-long-enough")
	now := time.Unix(1700000000, 0).UTC()
	verifier := &Verifier{
		database:     database,
		resolve:      func(context.Context, string) ([]byte, error) { return key, nil },
		now:          func() time.Time { return now },
		maxClockSkew: DefaultMaxClockSkew,
	}
	return database, verifier, key, now
}

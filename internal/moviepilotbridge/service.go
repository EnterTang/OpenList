package moviepilotbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SecretResolver func(context.Context, string) ([]byte, error)

type Service struct {
	database     *gorm.DB
	resolve      SecretResolver
	httpClient   HTTPDoer
	now          func() time.Time
	maxClockSkew time.Duration
}

func NewService(database *gorm.DB, resolve SecretResolver, httpClient HTTPDoer) *Service {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Service{
		database:     database,
		resolve:      resolve,
		httpClient:   httpClient,
		now:          func() time.Time { return time.Now().UTC() },
		maxClockSkew: DefaultMaxClockSkew,
	}
}

func (s *Service) Verifier() *Verifier {
	return &Verifier{
		database:     s.database,
		resolve:      s.resolve,
		now:          s.now,
		maxClockSkew: s.maxClockSkew,
	}
}

func (s *Service) ListInstances(ctx context.Context) ([]model.MoviePilotBridgeInstance, error) {
	if s == nil || s.database == nil {
		return nil, errors.New("moviepilot bridge database is required")
	}
	var items []model.MoviePilotBridgeInstance
	err := s.database.WithContext(ctx).Order("name ASC, id ASC").Find(&items).Error
	return items, err
}

func (s *Service) GetInstance(ctx context.Context, id string) (*model.MoviePilotBridgeInstance, error) {
	if s == nil || s.database == nil {
		return nil, errors.New("moviepilot bridge database is required")
	}
	var item model.MoviePilotBridgeInstance
	if err := s.database.WithContext(ctx).First(&item, "id = ?", strings.TrimSpace(id)).Error; err != nil {
		return nil, err
	}
	return &item, nil
}

type InstanceUpsertRequest struct {
	ID                string
	Name              string
	BaseURL           string
	SecretRef         string
	SecretFingerprint string
	Enabled           bool
}

func (s *Service) UpsertInstance(ctx context.Context, req InstanceUpsertRequest) (*model.MoviePilotBridgeInstance, error) {
	if s == nil || s.database == nil {
		return nil, errors.New("moviepilot bridge database is required")
	}
	if strings.TrimSpace(req.Name) == "" || strings.TrimSpace(req.SecretRef) == "" {
		return nil, errors.New("bridge name and secret reference are required")
	}
	baseURL, err := validateBridgeURL(req.BaseURL)
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(req.ID)
	if id == "" {
		id = uuid.NewString()
	}
	now := time.Now().UTC()
	item := &model.MoviePilotBridgeInstance{}
	err = s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existing model.MoviePilotBridgeInstance
		lookup := tx.First(&existing, "id = ?", id).Error
		if lookup != nil && !errors.Is(lookup, gorm.ErrRecordNotFound) {
			return lookup
		}
		createdAt := now
		if lookup == nil {
			createdAt = existing.CreatedAt
		}
		*item = model.MoviePilotBridgeInstance{
			ID: id, CreatedAt: createdAt, UpdatedAt: now,
			Name: strings.TrimSpace(req.Name), BaseURL: baseURL,
			SecretRef: strings.TrimSpace(req.SecretRef), SecretFingerprint: strings.TrimSpace(req.SecretFingerprint),
			Enabled: req.Enabled,
		}
		return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, DoUpdates: clause.AssignmentColumns([]string{
			"updated_at", "name", "base_url", "secret_ref", "secret_fingerprint", "enabled",
		})}).Create(item).Error
	})
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Service) DisableInstance(ctx context.Context, id string) error {
	if s == nil || s.database == nil {
		return errors.New("moviepilot bridge database is required")
	}
	result := s.database.WithContext(ctx).Model(&model.MoviePilotBridgeInstance{}).Where("id = ?", strings.TrimSpace(id)).Updates(map[string]interface{}{
		"enabled":    false,
		"updated_at": time.Now().UTC(),
	})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func (s *Service) SearchResources(ctx context.Context, bridgeID string, payload ResourceSearchRequest) ([]ResourceSearchResult, error) {
	if s == nil || s.database == nil {
		return nil, errors.New("moviepilot bridge database is required")
	}
	var bridge model.MoviePilotBridgeInstance
	if err := s.database.WithContext(ctx).First(&bridge, "id = ? AND enabled = ?", strings.TrimSpace(bridgeID), true).Error; err != nil {
		return nil, err
	}
	client := &Client{HTTPClient: s.httpClient, Resolve: s.resolve, Now: s.now}
	return client.SearchResources(ctx, bridge, payload)
}

func (s *Service) SubmitIntent(ctx context.Context, bridgeID string, intent *model.MoviePilotDownloadIntent, payload DownloadIntentRequest) error {
	if s == nil || s.database == nil {
		return errors.New("moviepilot bridge database is required")
	}
	if intent == nil {
		return errors.New("download intent is required")
	}
	var bridge model.MoviePilotBridgeInstance
	if err := s.database.WithContext(ctx).First(&bridge, "id = ? AND enabled = ?", strings.TrimSpace(bridgeID), true).Error; err != nil {
		return err
	}
	if strings.TrimSpace(intent.BridgeInstanceID) == "" {
		intent.BridgeInstanceID = bridge.ID
	} else if intent.BridgeInstanceID != bridge.ID {
		return errors.New("download intent bridge does not match the selected instance")
	}
	if payload.RequestID == "" {
		payload.RequestID = intent.RequestID
	}
	if payload.RequestID != intent.RequestID {
		return errors.New("download intent request id does not match payload")
	}
	if err := payload.Validate(); err != nil {
		return err
	}
	if err := db.CreateIntentTx(ctx, s.database, intent); err != nil {
		return err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal bridge intent: %w", err)
	}
	var outbox model.MoviePilotBridgeOutbox
	lookup := s.database.WithContext(ctx).Where("bridge_id = ? AND request_id = ?", bridge.ID, intent.RequestID).Order("created_at DESC").First(&outbox).Error
	if lookup == nil && outbox.Status == "sent" {
		return nil
	}
	now := time.Now().UTC()
	if lookup != nil && !errors.Is(lookup, gorm.ErrRecordNotFound) {
		return lookup
	}
	if errors.Is(lookup, gorm.ErrRecordNotFound) {
		outbox = model.MoviePilotBridgeOutbox{
			ID: uuid.NewString(), CreatedAt: now, BridgeID: bridge.ID, RequestID: intent.RequestID, EventID: uuid.NewString(),
		}
	}
	outbox.UpdatedAt = now
	outbox.PayloadJSON = string(raw)
	outbox.Status = "pending"
	outbox.AvailableAt = now
	outbox.LastError = ""
	if err := s.database.WithContext(ctx).Save(&outbox).Error; err != nil {
		return err
	}
	client := &Client{HTTPClient: s.httpClient, Resolve: s.resolve, Now: s.now}
	if err := client.SubmitIntent(ctx, bridge, payload); err != nil {
		_ = s.database.WithContext(ctx).Model(&outbox).Updates(map[string]interface{}{"status": "failed", "last_error": err.Error(), "attempt_count": gorm.Expr("attempt_count + 1")}).Error
		return err
	}
	return s.database.WithContext(ctx).Model(&outbox).Updates(map[string]interface{}{"status": "sent", "attempt_count": gorm.Expr("attempt_count + 1")}).Error
}

type EventConsumeResult struct {
	Duplicate bool
	Stored    bool
}

func (s *Service) ConsumeBridgeEvent(ctx context.Context, headers http.Header, method, path string, body []byte, event BridgeEvent) (EventConsumeResult, error) {
	if s == nil || s.database == nil {
		return EventConsumeResult{}, errors.New("moviepilot bridge database is required")
	}
	if err := event.Validate(); err != nil {
		return EventConsumeResult{}, err
	}
	verifier := s.Verifier()
	bridge, signed, err := verifier.verifySignatureOnly(ctx, headers, method, path, body)
	if err != nil {
		return EventConsumeResult{}, err
	}
	result := EventConsumeResult{}
	err = s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := insertNonce(tx, bridge.ID, signed.Nonce, signed.Timestamp, s.now()); err != nil {
			return err
		}
		var existing model.MoviePilotBridgeInbox
		lookup := tx.First(&existing, "event_id = ?", event.EventID).Error
		if lookup == nil {
			if existing.BridgeID != bridge.ID || existing.RequestID != event.RequestID || existing.EventType != event.Type {
				return errors.New("event id already belongs to a different Bridge event")
			}
			result.Duplicate = true
			return nil
		}
		if !errors.Is(lookup, gorm.ErrRecordNotFound) {
			return lookup
		}
		raw, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			return marshalErr
		}
		now := s.now()
		inbox := &model.MoviePilotBridgeInbox{
			EventID: event.EventID, CreatedAt: now, UpdatedAt: now,
			BridgeID: bridge.ID, RequestID: event.RequestID, EventType: event.Type,
			PayloadJSON: string(raw), Status: "received",
		}
		if err := tx.Create(inbox).Error; err != nil {
			return err
		}
		result.Stored = true
		return nil
	})
	return result, err
}

func validateBridgeURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	u, err := url.Parse(trimmed)
	if err != nil || !strings.EqualFold(u.Scheme, "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("moviepilot bridge URL must use HTTPS")
	}
	u.User = nil
	return strings.TrimRight(u.String(), "/"), nil
}

type Verifier struct {
	database     *gorm.DB
	resolve      SecretResolver
	now          func() time.Time
	maxClockSkew time.Duration
}

func (v *Verifier) Verify(ctx context.Context, headers http.Header, method, path string, body []byte) error {
	_, _, err := v.verify(ctx, headers, method, path, body)
	return err
}

func (v *Verifier) VerifySignature(ctx context.Context, headers http.Header, method, path string, body []byte) (*model.MoviePilotBridgeInstance, error) {
	bridge, _, err := v.verifySignatureOnly(ctx, headers, method, path, body)
	return bridge, err
}

func (v *Verifier) verify(ctx context.Context, headers http.Header, method, path string, body []byte) (*model.MoviePilotBridgeInstance, SignRequest, error) {
	bridge, signed, err := v.verifySignatureOnly(ctx, headers, method, path, body)
	if err != nil {
		return nil, SignRequest{}, err
	}
	if v.database == nil {
		return nil, SignRequest{}, errors.New("moviepilot bridge database is required")
	}
	now := v.clock()
	err = v.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return insertNonce(tx, bridge.ID, signed.Nonce, signed.Timestamp, now)
	})
	return bridge, signed, err
}

func (v *Verifier) verifySignatureOnly(ctx context.Context, headers http.Header, method, path string, body []byte) (*model.MoviePilotBridgeInstance, SignRequest, error) {
	if v == nil || v.database == nil {
		return nil, SignRequest{}, errors.New("moviepilot bridge database is required")
	}
	request := VerifyRequest{Headers: headers, Method: method, Path: path, Body: body, Now: v.clock()}
	signed, err := parseSignedRequest(request)
	if err != nil {
		return nil, SignRequest{}, err
	}
	if request.Now.IsZero() {
		request.Now = time.Now().UTC()
	}
	maxSkew := v.maxClockSkew
	if maxSkew <= 0 {
		maxSkew = DefaultMaxClockSkew
	}
	if delta := request.Now.Sub(signed.Timestamp); delta > maxSkew || delta < -maxSkew {
		return nil, SignRequest{}, errors.New("bridge timestamp is expired")
	}
	var bridge model.MoviePilotBridgeInstance
	if err := v.database.WithContext(ctx).First(&bridge, "id = ? AND enabled = ?", signed.InstanceID, true).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, SignRequest{}, errors.New("unknown or disabled bridge instance")
		}
		return nil, SignRequest{}, err
	}
	if v.resolve == nil {
		return nil, SignRequest{}, errors.New("moviepilot bridge secret resolver is not configured")
	}
	key, err := v.resolve(ctx, bridge.SecretRef)
	if err != nil {
		return nil, SignRequest{}, err
	}
	if _, err := verifySignature(request, key); err != nil {
		return nil, SignRequest{}, err
	}
	return &bridge, signed, nil
}

func (v *Verifier) clock() time.Time {
	if v != nil && v.now != nil {
		return v.now().UTC()
	}
	return time.Now().UTC()
}

func insertNonce(tx *gorm.DB, bridgeID, nonce string, signedAt, now time.Time) error {
	var existing model.MoviePilotBridgeNonce
	err := tx.Where("bridge_id = ? AND nonce = ?", bridgeID, nonce).First(&existing).Error
	if err == nil {
		return errors.New("bridge nonce has already been used")
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	return tx.Create(&model.MoviePilotBridgeNonce{
		ID: uuid.NewString(), CreatedAt: signedAt, BridgeID: bridgeID, Nonce: nonce, UsedAt: now,
	}).Error
}

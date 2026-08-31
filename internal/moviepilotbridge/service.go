package moviepilotbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SecretResolver func(context.Context, string) ([]byte, error)
type EventHandler func(context.Context, string, BridgeEvent) error

type Service struct {
	database      *gorm.DB
	resolve       SecretResolver
	httpClient    HTTPDoer
	now           func() time.Time
	maxClockSkew  time.Duration
	handlerMu     sync.RWMutex
	handler       EventHandler
	processorOnce sync.Once
}

const (
	// Give the Bridge callback outbox time to deliver the normal torrent.bound
	// event before asking it to replay the same binding. This avoids generating
	// duplicate callbacks during the healthy path while still recovering a
	// Coordinator restart or a lost callback without a manual subscription run.
	moviePilotOrphanReconcileMinAge = 30 * time.Second
	moviePilotOrphanReconcileRetry  = time.Minute
	moviePilotOrphanReconcileAfter  = 5 * time.Minute
)

// SetEventHandler installs the durable inbox consumer. The handler is invoked
// only after an event has been committed to the inbox, so a failed consumer
// can be retried without asking MoviePilot to resend the event.
func (s *Service) SetEventHandler(handler EventHandler) {
	if s == nil {
		return
	}
	s.handlerMu.Lock()
	s.handler = handler
	s.handlerMu.Unlock()
}

func (s *Service) eventHandler() EventHandler {
	if s == nil {
		return nil
	}
	s.handlerMu.RLock()
	defer s.handlerMu.RUnlock()
	return s.handler
}

// StartEventProcessor runs the durable inbound-event and outbound-intent retry
// loops. It is safe to call more than once; only the first call starts a
// processor for this service.
func (s *Service) StartEventProcessor(ctx context.Context) {
	if s == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.processorOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			_, _ = s.ProcessPendingOutbox(ctx, 20)
			_, _ = s.ReconcileSentIntentBindings(ctx, 20)
			_, _ = s.ProcessPendingEvents(ctx, 20)
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					_, _ = s.ProcessPendingOutbox(ctx, 20)
					_, _ = s.ReconcileSentIntentBindings(ctx, 20)
					_, _ = s.ProcessPendingEvents(ctx, 20)
				}
			}
		}()
	})
}

// ReconcileSentIntentBindings repairs the gap between a Bridge accepting an
// intent and Coordinator persisting its torrent binding. The normal callback
// path remains at-least-once, but this sweep makes recovery independent of a
// later subscription scan or scheduler tick.
func (s *Service) ReconcileSentIntentBindings(ctx context.Context, limit int) (int, error) {
	if s == nil || s.database == nil {
		return 0, errors.New("moviepilot bridge database is required")
	}
	if limit <= 0 {
		limit = 20
	}
	now := s.nowUTC()
	var rows []model.MoviePilotBridgeOutbox
	if err := s.database.WithContext(ctx).
		Where("status = ? AND available_at <= ? AND updated_at <= ?", "sent", now, now.Add(-moviePilotOrphanReconcileMinAge)).
		Order("updated_at ASC, id ASC").Limit(limit).Find(&rows).Error; err != nil {
		return 0, err
	}
	processed := 0
	var firstErr error
	for i := range rows {
		row := &rows[i]
		var intent model.MoviePilotDownloadIntent
		if err := s.database.WithContext(ctx).Where("bridge_instance_id = ? AND request_id = ?", row.BridgeID, row.RequestID).First(&intent).Error; err != nil {
			if updateErr := s.deferSentIntentReconcile(ctx, row.ID, now, moviePilotOrphanReconcileAfter, err); updateErr != nil {
				return processed, updateErr
			}
			continue
		}
		var bindingCount int64
		if err := s.database.WithContext(ctx).Model(&model.MoviePilotTorrentBinding{}).Where("intent_id = ?", intent.ID).Count(&bindingCount).Error; err != nil {
			return processed, err
		}
		if bindingCount > 0 || intent.Status == model.MoviePilotIntentStatusFailed || intent.Status == model.MoviePilotIntentStatusCancelled {
			continue
		}
		var bridge model.MoviePilotBridgeInstance
		if err := s.database.WithContext(ctx).First(&bridge, "id = ? AND enabled = ?", row.BridgeID, true).Error; err != nil {
			if updateErr := s.deferSentIntentReconcile(ctx, row.ID, now, moviePilotOrphanReconcileAfter, err); updateErr != nil {
				return processed, updateErr
			}
			continue
		}
		client := &Client{HTTPClient: s.httpClient, Resolve: s.resolve, Now: s.now}
		reconcileErr := client.ReconcileIntent(ctx, bridge, intent.RequestID)
		processed++
		if reconcileErr != nil {
			if firstErr == nil {
				firstErr = reconcileErr
			}
			if updateErr := s.deferSentIntentReconcile(ctx, row.ID, now, moviePilotOrphanReconcileRetry, reconcileErr); updateErr != nil {
				return processed, updateErr
			}
			continue
		}
		if err := s.deferSentIntentReconcile(ctx, row.ID, now, moviePilotOrphanReconcileAfter, nil); err != nil {
			return processed, err
		}
	}
	return processed, firstErr
}

func (s *Service) deferSentIntentReconcile(ctx context.Context, outboxID string, now time.Time, delay time.Duration, reconcileErr error) error {
	lastError := ""
	if reconcileErr != nil {
		lastError = reconcileErr.Error()
	}
	return s.database.WithContext(ctx).Model(&model.MoviePilotBridgeOutbox{}).Where("id = ? AND status = ?", outboxID, "sent").Updates(map[string]any{
		"available_at": now.Add(delay), "last_error": lastError, "updated_at": now,
	}).Error
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

// ListEnabledInstanceIDs returns the deterministic Bridge order used by
// automatic MoviePilot subscription searches. The secret itself is resolved
// only when a search request is sent.
func (s *Service) ListEnabledInstanceIDs(ctx context.Context) ([]string, error) {
	items, err := s.ListInstances(ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		if item.Enabled && strings.TrimSpace(item.ID) != "" {
			ids = append(ids, strings.TrimSpace(item.ID))
		}
	}
	return ids, nil
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

func (s *Service) PauseTorrent(ctx context.Context, bridgeID, requestID, downloader, torrentHash, reason string) error {
	return s.controlTorrent(ctx, bridgeID, requestID, downloader, torrentHash, "pause", reason)
}

func (s *Service) ResumeTorrent(ctx context.Context, bridgeID, requestID, downloader, torrentHash, reason string) error {
	return s.controlTorrent(ctx, bridgeID, requestID, downloader, torrentHash, "resume", reason)
}

func (s *Service) controlTorrent(ctx context.Context, bridgeID, requestID, downloader, torrentHash, action, reason string) error {
	if s == nil || s.database == nil {
		return errors.New("moviepilot bridge database is required")
	}
	var bridge model.MoviePilotBridgeInstance
	if err := s.database.WithContext(ctx).First(&bridge, "id = ? AND enabled = ?", strings.TrimSpace(bridgeID), true).Error; err != nil {
		return err
	}
	client := &Client{HTTPClient: s.httpClient, Resolve: s.resolve, Now: s.now}
	return client.ControlTorrent(ctx, bridge, TorrentControlRequest{
		RequestID: strings.TrimSpace(requestID), Downloader: strings.TrimSpace(downloader),
		TorrentHash: strings.ToLower(strings.TrimSpace(torrentHash)), Action: action, Reason: strings.TrimSpace(reason),
	})
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
	now := s.nowUTC()
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
	if lookup == nil {
		if outbox.PayloadJSON != string(raw) {
			return fmt.Errorf("request id %q already belongs to a different payload", intent.RequestID)
		}
		if outbox.Status == "sending" {
			return nil
		}
		if outbox.Status == "sent" {
			var binding model.MoviePilotTorrentBinding
			bindingErr := s.database.WithContext(ctx).Where("intent_id = ?", intent.ID).First(&binding).Error
			if bindingErr == nil {
				return nil
			}
			if !errors.Is(bindingErr, gorm.ErrRecordNotFound) {
				return bindingErr
			}
			// The Bridge may have accepted the download while Coordinator's
			// bound-event handler was unavailable. Ask it to replay the exact
			// persisted binding instead of creating another qB download.
			client := &Client{HTTPClient: s.httpClient, Resolve: s.resolve, Now: s.now}
			return client.ReconcileIntent(ctx, bridge, intent.RequestID)
		}
	}
	if lookup != nil && !errors.Is(lookup, gorm.ErrRecordNotFound) {
		return lookup
	}
	now := s.nowUTC()
	if errors.Is(lookup, gorm.ErrRecordNotFound) {
		candidate := model.MoviePilotBridgeOutbox{
			ID: uuid.NewString(), CreatedAt: now, BridgeID: bridge.ID, RequestID: intent.RequestID, EventID: uuid.NewString(),
			UpdatedAt: now, PayloadJSON: string(raw), Status: "pending", AvailableAt: now,
		}
		if err := s.database.WithContext(ctx).Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "bridge_id"}, {Name: "request_id"}},
			DoNothing: true,
		}).Create(&candidate).Error; err != nil {
			return err
		}
		if err := s.database.WithContext(ctx).Where("bridge_id = ? AND request_id = ?", bridge.ID, intent.RequestID).First(&outbox).Error; err != nil {
			return err
		}
		if outbox.PayloadJSON != string(raw) {
			return fmt.Errorf("request id %q already belongs to a different payload", intent.RequestID)
		}
		if outbox.Status == "sending" {
			return nil
		}
		if outbox.Status == "sent" {
			var binding model.MoviePilotTorrentBinding
			bindingErr := s.database.WithContext(ctx).Where("intent_id = ?", intent.ID).First(&binding).Error
			if bindingErr == nil {
				return nil
			}
			if !errors.Is(bindingErr, gorm.ErrRecordNotFound) {
				return bindingErr
			}
			client := &Client{HTTPClient: s.httpClient, Resolve: s.resolve, Now: s.now}
			return client.ReconcileIntent(ctx, bridge, intent.RequestID)
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
	claimed, err := s.claimOutbox(ctx, &outbox)
	if err != nil || !claimed {
		return err
	}
	if err := s.sendOutbox(ctx, bridge, outbox); err != nil {
		if updateErr := s.markOutboxFailed(ctx, outbox, err); updateErr != nil {
			return errors.Join(err, updateErr)
		}
		return err
	}
	return s.markOutboxSent(ctx, outbox)
}

// ProcessPendingOutbox retries durable download intents in creation order.
// Transport failures are persisted with exponential backoff and do not stop
// later rows from being attempted.
func (s *Service) ProcessPendingOutbox(ctx context.Context, limit int) (int, error) {
	if s == nil || s.database == nil {
		return 0, errors.New("moviepilot bridge database is required")
	}
	if limit <= 0 {
		limit = 20
	}
	now := s.nowUTC()
	if err := s.database.WithContext(ctx).Model(&model.MoviePilotBridgeOutbox{}).
		Where("status = ? AND updated_at < ?", "sending", now.Add(-5*time.Minute)).
		Updates(map[string]interface{}{"status": "failed", "last_error": "intent delivery lease expired", "available_at": now, "updated_at": now}).Error; err != nil {
		return 0, err
	}
	var rows []model.MoviePilotBridgeOutbox
	if err := s.database.WithContext(ctx).
		Where("status IN ? AND available_at <= ?", []string{"pending", "failed"}, now).
		Order("created_at ASC, id ASC").Limit(limit).Find(&rows).Error; err != nil {
		return 0, err
	}
	processed := 0
	for i := range rows {
		claimed, err := s.claimOutbox(ctx, &rows[i])
		if err != nil {
			return processed, err
		}
		if !claimed {
			continue
		}
		var bridge model.MoviePilotBridgeInstance
		if err := s.database.WithContext(ctx).First(&bridge, "id = ? AND enabled = ?", rows[i].BridgeID, true).Error; err != nil {
			if updateErr := s.markOutboxFailed(ctx, rows[i], err); updateErr != nil {
				return processed, errors.Join(err, updateErr)
			}
			continue
		}
		if err := s.sendOutbox(ctx, bridge, rows[i]); err != nil {
			if updateErr := s.markOutboxFailed(ctx, rows[i], err); updateErr != nil {
				return processed, errors.Join(err, updateErr)
			}
			continue
		}
		if err := s.markOutboxSent(ctx, rows[i]); err != nil {
			return processed, err
		}
		processed++
	}
	return processed, nil
}

func (s *Service) claimOutbox(ctx context.Context, outbox *model.MoviePilotBridgeOutbox) (bool, error) {
	now := s.nowUTC()
	result := s.database.WithContext(ctx).Model(&model.MoviePilotBridgeOutbox{}).
		Where("id = ? AND status IN ?", outbox.ID, []string{"pending", "failed"}).
		Updates(map[string]interface{}{"status": "sending", "updated_at": now})
	return result.RowsAffected == 1, result.Error
}

func (s *Service) sendOutbox(ctx context.Context, bridge model.MoviePilotBridgeInstance, outbox model.MoviePilotBridgeOutbox) error {
	var payload DownloadIntentRequest
	if err := json.Unmarshal([]byte(outbox.PayloadJSON), &payload); err != nil {
		return fmt.Errorf("decode bridge intent outbox %s: %w", outbox.ID, err)
	}
	client := &Client{HTTPClient: s.httpClient, Resolve: s.resolve, Now: s.now}
	return client.SubmitIntent(ctx, bridge, payload)
}

func (s *Service) markOutboxFailed(ctx context.Context, outbox model.MoviePilotBridgeOutbox, deliveryErr error) error {
	now := s.nowUTC()
	attempt := outbox.AttemptCount + 1
	return s.database.WithContext(ctx).Model(&model.MoviePilotBridgeOutbox{}).Where("id = ? AND status = ?", outbox.ID, "sending").Updates(map[string]interface{}{
		"status": "failed", "last_error": deliveryErr.Error(), "attempt_count": attempt,
		"available_at": now.Add(moviePilotOutboxRetryDelay(attempt)), "updated_at": now,
	}).Error
}

func (s *Service) markOutboxSent(ctx context.Context, outbox model.MoviePilotBridgeOutbox) error {
	now := s.nowUTC()
	return s.database.WithContext(ctx).Model(&model.MoviePilotBridgeOutbox{}).Where("id = ? AND status = ?", outbox.ID, "sending").Updates(map[string]interface{}{
		"status": "sent", "last_error": "", "attempt_count": outbox.AttemptCount + 1, "updated_at": now,
	}).Error
}

func moviePilotOutboxRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 7 {
		attempt = 7
	}
	delay := 10 * time.Second * time.Duration(1<<(attempt-1))
	if delay > 15*time.Minute {
		return 15 * time.Minute
	}
	return delay
}

func (s *Service) nowUTC() time.Time {
	if s != nil && s.now != nil {
		return s.now().UTC()
	}
	return time.Now().UTC()
}

type EventConsumeResult struct {
	Duplicate bool
	Stored    bool
}

func (s *Service) ConsumeBridgeEvent(ctx context.Context, headers http.Header, method, path string, body []byte, event BridgeEvent) (EventConsumeResult, error) {
	if s == nil || s.database == nil {
		return EventConsumeResult{}, errors.New("moviepilot bridge database is required")
	}
	if err := validateNoForbiddenBridgeFields(body); err != nil {
		return EventConsumeResult{}, err
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
		raw, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			return marshalErr
		}
		var existing model.MoviePilotBridgeInbox
		lookup := tx.First(&existing, "event_id = ?", event.EventID).Error
		if lookup == nil {
			if existing.BridgeID != bridge.ID || existing.RequestID != event.RequestID || existing.EventType != event.Type || existing.PayloadJSON != string(raw) {
				return errors.New("event id already belongs to a different Bridge event")
			}
			result.Duplicate = true
			return nil
		}
		if !errors.Is(lookup, gorm.ErrRecordNotFound) {
			return lookup
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

// ProcessPendingEvents drains the signed event inbox in creation order. Event
// delivery is deliberately at-least-once; the coordinator handlers are
// idempotent and the inbox row remains available for a later retry.
func (s *Service) ProcessPendingEvents(ctx context.Context, limit int) (int, error) {
	if s == nil || s.database == nil {
		return 0, errors.New("moviepilot bridge database is required")
	}
	handler := s.eventHandler()
	if handler == nil {
		return 0, errors.New("moviepilot bridge event handler is not configured")
	}
	if limit <= 0 {
		limit = 20
	}
	now := s.now()
	processingLease := now.Add(-5 * time.Minute)
	if err := s.database.WithContext(ctx).Model(&model.MoviePilotBridgeInbox{}).
		Where("status = ? AND updated_at < ?", "processing", processingLease).
		Updates(map[string]interface{}{"status": "failed", "last_error": "event processing lease expired", "updated_at": now}).Error; err != nil {
		return 0, err
	}
	var inboxes []model.MoviePilotBridgeInbox
	if err := s.database.WithContext(ctx).
		Where("status IN ?", []string{"received", "failed"}).
		Order("created_at ASC, event_id ASC").Limit(limit).Find(&inboxes).Error; err != nil {
		return 0, err
	}
	processed := 0
	var firstHandlerErr error
	for i := range inboxes {
		claimed := s.database.WithContext(ctx).Model(&model.MoviePilotBridgeInbox{}).
			Where("event_id = ? AND status IN ?", inboxes[i].EventID, []string{"received", "failed"}).
			Updates(map[string]interface{}{"status": "processing", "attempt_count": gorm.Expr("attempt_count + 1"), "updated_at": s.now()})
		if claimed.Error != nil {
			return processed, claimed.Error
		}
		if claimed.RowsAffected == 0 {
			continue
		}
		var event BridgeEvent
		if err := json.Unmarshal([]byte(inboxes[i].PayloadJSON), &event); err != nil {
			if updateErr := s.database.WithContext(ctx).Model(&model.MoviePilotBridgeInbox{}).Where("event_id = ?", inboxes[i].EventID).Updates(map[string]interface{}{
				"status": "failed", "last_error": err.Error(), "updated_at": s.now(),
			}).Error; updateErr != nil {
				return processed, updateErr
			}
			if firstHandlerErr == nil {
				firstHandlerErr = err
			}
			continue
		}
		err := handler(ctx, inboxes[i].BridgeID, event)
		if err != nil {
			if updateErr := s.database.WithContext(ctx).Model(&model.MoviePilotBridgeInbox{}).Where("event_id = ?", inboxes[i].EventID).Updates(map[string]interface{}{
				"status": "failed", "last_error": err.Error(), "updated_at": s.now(),
			}).Error; updateErr != nil {
				return processed, updateErr
			}
			if firstHandlerErr == nil {
				firstHandlerErr = err
			}
			continue
		}
		now := s.now()
		if err := s.database.WithContext(ctx).Model(&model.MoviePilotBridgeInbox{}).Where("event_id = ? AND status = ?", inboxes[i].EventID, "processing").Updates(map[string]interface{}{
			"status": "processed", "processed_at": now, "last_error": "", "updated_at": now,
		}).Error; err != nil {
			return processed, err
		}
		processed++
	}
	return processed, firstHandlerErr
}

func validateBridgeURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	u, err := url.Parse(trimmed)
	if err != nil || !strings.EqualFold(u.Scheme, "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("moviepilot bridge URL must use HTTPS")
	}
	if u.User != nil {
		return "", errors.New("moviepilot bridge URL must not contain userinfo")
	}
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
	if err := tx.Where("used_at < ?", now.Add(-BridgeNonceRetention)).Delete(&model.MoviePilotBridgeNonce{}).Error; err != nil {
		return fmt.Errorf("prune expired bridge nonces: %w", err)
	}
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

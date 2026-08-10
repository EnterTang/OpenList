package db

import (
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const subscriptionTelegramEventClaimTimeout = 5 * time.Minute

func EnqueueSubscriptionTelegramEvent(event *model.SubscriptionTelegramEvent) (*model.SubscriptionTelegramEvent, bool, error) {
	if event == nil {
		return nil, false, errors.New("subscription Telegram event is nil")
	}
	if event.Status == "" {
		event.Status = model.SubscriptionTelegramEventStatusPending
	}
	if event.AvailableAt.IsZero() {
		event.AvailableAt = time.Now().UTC()
	}
	created := false
	err := db.Transaction(func(tx *gorm.DB) error {
		result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(event)
		if result.Error != nil {
			return result.Error
		}
		created = result.RowsAffected > 0
		if !created {
			return tx.Where("subscription_id = ? AND channel = ? AND message_id = ?", event.SubscriptionID, event.Channel, event.MessageID).First(event).Error
		}
		return nil
	})
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	return event, created, nil
}

func ClaimSubscriptionTelegramEvents(limit int, now time.Time) ([]model.SubscriptionTelegramEvent, error) {
	if limit <= 0 {
		limit = 20
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	staleBefore := now.Add(-subscriptionTelegramEventClaimTimeout)
	claimable := []string{model.SubscriptionTelegramEventStatusPending, model.SubscriptionTelegramEventStatusRetryWait}
	var pending []model.SubscriptionTelegramEvent
	if err := db.Where("(status IN ? AND available_at <= ?) OR (status = ? AND updated_at <= ?)", claimable, now, model.SubscriptionTelegramEventStatusProcessing, staleBefore).
		Order("available_at ASC, id ASC").Limit(limit).Find(&pending).Error; err != nil {
		return nil, errors.WithStack(err)
	}
	claimed := make([]model.SubscriptionTelegramEvent, 0, len(pending))
	for _, event := range pending {
		result := db.Model(&model.SubscriptionTelegramEvent{}).
			Where("id = ? AND ((status IN ? AND available_at <= ?) OR (status = ? AND updated_at <= ?))", event.ID, claimable, now, model.SubscriptionTelegramEventStatusProcessing, staleBefore).
			Updates(map[string]any{"status": model.SubscriptionTelegramEventStatusProcessing, "attempts": event.Attempts + 1, "updated_at": now})
		if result.Error != nil {
			return claimed, errors.WithStack(result.Error)
		}
		if result.RowsAffected == 0 {
			continue
		}
		event.Status = model.SubscriptionTelegramEventStatusProcessing
		event.Attempts++
		claimed = append(claimed, event)
	}
	return claimed, nil
}

func CompleteSubscriptionTelegramEvent(id uint, observationKey string, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return errors.WithStack(db.Model(&model.SubscriptionTelegramEvent{}).Where("id = ?", id).Updates(map[string]any{
		"status": model.SubscriptionTelegramEventStatusProcessed, "processed_at": now, "observation_key": observationKey,
		"last_error": "", "updated_at": now,
	}).Error)
}

func RetrySubscriptionTelegramEvent(id uint, cause error, availableAt time.Time) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	return errors.WithStack(db.Model(&model.SubscriptionTelegramEvent{}).Where("id = ?", id).Updates(map[string]any{
		"status": model.SubscriptionTelegramEventStatusRetryWait, "available_at": availableAt.UTC(), "last_error": message,
		"updated_at": time.Now().UTC(),
	}).Error)
}

func DeadLetterSubscriptionTelegramEvent(id uint, cause error) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	return errors.WithStack(db.Model(&model.SubscriptionTelegramEvent{}).Where("id = ?", id).Updates(map[string]any{
		"status": model.SubscriptionTelegramEventStatusDeadLetter, "last_error": message, "updated_at": time.Now().UTC(),
	}).Error)
}

func CreateSubscriptionRealtimeCandidate(candidate *model.SubscriptionRealtimeCandidate) (*model.SubscriptionRealtimeCandidate, bool, error) {
	if candidate == nil {
		return nil, false, errors.New("subscription realtime candidate is nil")
	}
	if candidate.Status == "" {
		candidate.Status = model.SubscriptionRealtimeCandidateStatusPending
	}
	created := false
	err := db.Transaction(func(tx *gorm.DB) error {
		result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(candidate)
		if result.Error != nil {
			return result.Error
		}
		created = result.RowsAffected > 0
		if !created {
			return tx.Where("subscription_id = ? AND slot_key = ? AND source_key = ? AND file_hash = ?", candidate.SubscriptionID, candidate.SlotKey, candidate.SourceKey, candidate.FileHash).First(candidate).Error
		}
		return nil
	})
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	return candidate, created, nil
}

func ListReadySubscriptionRealtimeCandidates(now time.Time, limit int) ([]model.SubscriptionRealtimeCandidate, error) {
	if limit <= 0 {
		limit = 100
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var items []model.SubscriptionRealtimeCandidate
	err := db.Where("status = ? AND ready_at <= ?", model.SubscriptionRealtimeCandidateStatusPending, now).
		Order("ready_at ASC, id ASC").Limit(limit).Find(&items).Error
	return items, errors.WithStack(err)
}

func ListSubscriptionRealtimeCandidates(subscriptionID uint, slotKey string) ([]model.SubscriptionRealtimeCandidate, error) {
	var items []model.SubscriptionRealtimeCandidate
	err := db.Where("subscription_id = ? AND slot_key = ? AND status = ?", subscriptionID, slotKey, model.SubscriptionRealtimeCandidateStatusPending).
		Order("created_at ASC, id ASC").Find(&items).Error
	return items, errors.WithStack(err)
}

func UpdateSubscriptionRealtimeCandidateStatus(ids []uint, status, lastError string) error {
	if len(ids) == 0 {
		return nil
	}
	return errors.WithStack(db.Model(&model.SubscriptionRealtimeCandidate{}).Where("id IN ?", ids).Updates(map[string]any{
		"status": status, "last_error": lastError, "updated_at": time.Now().UTC(),
	}).Error)
}

func ListSubscriptionTelegramEventsBySubscriptionIDs(subscriptionIDs []uint) ([]model.SubscriptionTelegramEvent, error) {
	if len(subscriptionIDs) == 0 {
		return []model.SubscriptionTelegramEvent{}, nil
	}
	var items []model.SubscriptionTelegramEvent
	err := db.Where("subscription_id IN ?", subscriptionIDs).Order("created_at DESC, id DESC").Find(&items).Error
	return items, errors.WithStack(err)
}

// ListLatestSubscriptionTelegramEventsBySubscriptionIDs avoids loading the
// complete event history when the caller only needs the card's latest event.
func ListLatestSubscriptionTelegramEventsBySubscriptionIDs(subscriptionIDs []uint) ([]model.SubscriptionTelegramEvent, error) {
	if len(subscriptionIDs) == 0 {
		return []model.SubscriptionTelegramEvent{}, nil
	}
	uniqueSubscriptionIDs := make([]uint, 0, len(subscriptionIDs))
	seen := make(map[uint]struct{}, len(subscriptionIDs))
	for _, subscriptionID := range subscriptionIDs {
		if _, ok := seen[subscriptionID]; ok {
			continue
		}
		seen[subscriptionID] = struct{}{}
		uniqueSubscriptionIDs = append(uniqueSubscriptionIDs, subscriptionID)
	}

	table := modelTableName("SubscriptionTelegramEvent")
	requestedSubscriptions := requestedSubscriptionIDsQuery(uniqueSubscriptionIDs)
	latest := "latest_events"

	var items []model.SubscriptionTelegramEvent
	err := db.Table(table+" AS current_events").
		Select("current_events.*").
		Joins("JOIN (?) AS requested_subscriptions ON requested_subscriptions.subscription_id = current_events.subscription_id", requestedSubscriptions).
		Where("current_events.id = (SELECT " + latest + ".id FROM " + table + " AS " + latest + " WHERE " + latest + ".subscription_id = requested_subscriptions.subscription_id ORDER BY " + latest + ".created_at DESC, " + latest + ".id DESC LIMIT 1)").
		Find(&items).Error
	return items, errors.WithStack(err)
}

func requestedSubscriptionIDsQuery(subscriptionIDs []uint) *gorm.DB {
	var query strings.Builder
	query.WriteString("SELECT ? AS subscription_id")
	args := make([]any, 0, len(subscriptionIDs))
	for i, subscriptionID := range subscriptionIDs {
		if i > 0 {
			query.WriteString(" UNION ALL SELECT ?")
		}
		args = append(args, subscriptionID)
	}
	return db.Raw(query.String(), args...)
}

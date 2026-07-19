package db

import (
	"context"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

func CreateExternalSubscriptionRequest(ctx context.Context, request *model.ExternalSubscriptionRequest, subscription *model.Subscription) error {
	if request == nil {
		return errors.New("external subscription request is nil")
	}
	if subscription == nil {
		return errors.New("subscription is nil")
	}
	return errors.WithStack(db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if subscription.LastStatus == "" {
			subscription.LastStatus = model.SubscriptionStatusIdle
		}
		if err := tx.Create(subscription).Error; err != nil {
			return err
		}
		request.SubscriptionID = subscription.ID
		return tx.Create(request).Error
	}))
}

func GetExternalSubscriptionRequest(ctx context.Context, id uint) (*model.ExternalSubscriptionRequest, error) {
	var request model.ExternalSubscriptionRequest
	if err := db.WithContext(ctx).First(&request, id).Error; err != nil {
		return nil, errors.WithStack(err)
	}
	return &request, nil
}

func GetExternalSubscriptionRequestByIdempotencyKey(ctx context.Context, key string) (*model.ExternalSubscriptionRequest, error) {
	var request model.ExternalSubscriptionRequest
	if err := db.WithContext(ctx).Where(columnName("idempotency_key")+" = ?", key).First(&request).Error; err != nil {
		return nil, errors.WithStack(err)
	}
	return &request, nil
}

func GetExternalSubscriptionRequestByLookupKey(ctx context.Context, key string) (*model.ExternalSubscriptionRequest, error) {
	var request model.ExternalSubscriptionRequest
	if err := db.WithContext(ctx).Where(columnName("lookup_key")+" = ?", key).First(&request).Error; err != nil {
		return nil, errors.WithStack(err)
	}
	return &request, nil
}

func UpdateExternalSubscriptionRequestState(ctx context.Context, id uint, status, message, progressJSON, seasonsJSON, lastError string, lastRunAt *time.Time) error {
	updates := map[string]any{
		"last_status":  status,
		"last_message": message,
		"last_error":   lastError,
	}
	if progressJSON != "" {
		updates["progress_json"] = progressJSON
	}
	if seasonsJSON != "" {
		updates["seasons_json"] = seasonsJSON
	}
	if lastRunAt != nil {
		updates["last_run_at"] = lastRunAt
	}
	return errors.WithStack(db.WithContext(ctx).Model(&model.ExternalSubscriptionRequest{}).Where(columnName("id")+" = ?", id).Updates(updates).Error)
}

func UpdateExternalSubscriptionResponseJSON(ctx context.Context, id uint, responseJSON string) error {
	return errors.WithStack(db.WithContext(ctx).Model(&model.ExternalSubscriptionRequest{}).
		Where(columnName("id")+" = ?", id).
		Update("response_json", responseJSON).Error)
}

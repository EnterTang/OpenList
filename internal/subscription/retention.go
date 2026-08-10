package subscription

import (
	"context"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"gorm.io/gorm"
)

const (
	defaultRetentionInterval  = time.Hour
	defaultRetentionBatchSize = 1000
	defaultEventTerminalAge   = 7 * 24 * time.Hour
	defaultInboxProcessedAge  = 14 * 24 * time.Hour
	minimumRetentionInterval  = time.Minute
	minimumRetentionBatchSize = 1
)

// RetentionOptions controls one bounded retention run. Now is injectable so
// age-boundary behavior can be tested without depending on wall-clock time.
type RetentionOptions struct {
	DryRun            bool
	BatchSize         int
	EventTerminalAge  time.Duration
	InboxProcessedAge time.Duration
	Now               time.Time
}

// RetentionReport describes one retention run without exposing row payloads.
type RetentionReport struct {
	EventEligible int64
	EventDeleted  int64
	InboxEligible int64
	InboxDeleted  int64
}

func RunSubscriptionRetention(ctx context.Context, options RetentionOptions) (RetentionReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return RetentionReport{}, err
	}
	options = normalizeRetentionOptions(options)
	database := db.GetDb()
	if database == nil {
		return RetentionReport{}, gorm.ErrInvalidDB
	}

	eventEligible, eventDeleted, err := retainSubscriptionEvents(ctx, database, options)
	if err != nil {
		return RetentionReport{}, err
	}
	inboxEligible, inboxDeleted, err := retainClusterInbox(ctx, database, options)
	if err != nil {
		return RetentionReport{}, err
	}
	return RetentionReport{
		EventEligible: eventEligible,
		EventDeleted:  eventDeleted,
		InboxEligible: inboxEligible,
		InboxDeleted:  inboxDeleted,
	}, nil
}

func normalizeRetentionOptions(options RetentionOptions) RetentionOptions {
	if options.BatchSize < minimumRetentionBatchSize {
		options.BatchSize = defaultRetentionBatchSize
	}
	if options.EventTerminalAge <= 0 {
		options.EventTerminalAge = defaultEventTerminalAge
	}
	if options.InboxProcessedAge <= 0 {
		options.InboxProcessedAge = defaultInboxProcessedAge
	}
	if options.Now.IsZero() {
		options.Now = time.Now().UTC()
	}
	return options
}

func retainSubscriptionEvents(ctx context.Context, database *gorm.DB, options RetentionOptions) (int64, int64, error) {
	cutoff := options.Now.Add(-options.EventTerminalAge)
	query := database.WithContext(ctx).
		Model(&model.SubscriptionTelegramEvent{}).
		Where("status IN ?", []string{
			model.SubscriptionTelegramEventStatusProcessed,
			model.SubscriptionTelegramEventStatusDeadLetter,
		}).
		Where("(processed_at IS NOT NULL AND processed_at < ?) OR (processed_at IS NULL AND updated_at < ?)", cutoff, cutoff)
	return retainRows[uint](ctx, query, options, &model.SubscriptionTelegramEvent{})
}

func retainClusterInbox(ctx context.Context, database *gorm.DB, options RetentionOptions) (int64, int64, error) {
	cutoff := options.Now.Add(-options.InboxProcessedAge)
	query := database.WithContext(ctx).
		Model(&model.ClusterInbox{}).
		Where("status = ? AND processed_at IS NOT NULL AND processed_at < ?", model.ClusterMessageStatusProcessed, cutoff)
	return retainRows[string](ctx, query, options, &model.ClusterInbox{})
}

func retainRows[T any](ctx context.Context, query *gorm.DB, options RetentionOptions, modelValue any) (int64, int64, error) {
	if options.DryRun {
		var eligible int64
		if err := query.Count(&eligible).Error; err != nil {
			return 0, 0, err
		}
		return eligible, 0, nil
	}

	var eligible, deleted int64
	for {
		if err := ctx.Err(); err != nil {
			return eligible, deleted, err
		}
		var ids []T
		if err := query.Select("id").Order("id ASC").Limit(options.BatchSize).Find(&ids).Error; err != nil {
			return eligible, deleted, err
		}
		if len(ids) == 0 {
			break
		}
		eligible += int64(len(ids))
		result := query.Session(&gorm.Session{NewDB: true}).Where("id IN ?", ids).Delete(modelValue)
		if result.Error != nil {
			return eligible, deleted, result.Error
		}
		deleted += result.RowsAffected
		if len(ids) < options.BatchSize {
			break
		}
	}
	return eligible, deleted, nil
}

var retentionSchedulerOnce sync.Once

// StartRetentionScheduler starts the coordinator-owned cleanup loop. The
// scheduler is deliberately separate from request handling so retention never
// delays the subscription list endpoint.
func StartRetentionScheduler() {
	retentionSchedulerOnce.Do(func() {
		if !conf.Conf.SubscriptionRetention.Enabled {
			return
		}
		interval := time.Duration(conf.Conf.SubscriptionRetention.IntervalMinutes) * time.Minute
		if interval < minimumRetentionInterval {
			interval = defaultRetentionInterval
		}
		options := RetentionOptions{
			BatchSize:         conf.Conf.SubscriptionRetention.BatchSize,
			EventTerminalAge:  time.Duration(conf.Conf.SubscriptionRetention.EventTerminalDays) * 24 * time.Hour,
			InboxProcessedAge: time.Duration(conf.Conf.SubscriptionRetention.InboxProcessedDays) * 24 * time.Hour,
		}
		go func() {
			run := func() {
				report, err := RunSubscriptionRetention(context.Background(), options)
				if err != nil {
					utils.Log.Errorf("subscription retention failed: %v", err)
					return
				}
				if report.EventDeleted > 0 || report.InboxDeleted > 0 {
					utils.Log.Infof("subscription retention deleted events=%d inbox=%d", report.EventDeleted, report.InboxDeleted)
				}
			}
			run()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for range ticker.C {
				run()
			}
		}()
	})
}

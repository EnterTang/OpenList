package etfauto

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	mobileshare "github.com/OpenListTeam/OpenList/v4/internal/mobile_share"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type ShareProvider interface {
	CreateOrReuseShare(ctx context.Context, root *model.ETFMediaRoot) (*model.MobileShareRecord, error)
}

type RunnerOptions struct {
	ShareProvider ShareProvider
	HTTPClient    *http.Client
	Timeout       time.Duration
	MaxRetries    int
	RetryDelay    time.Duration
	Limit         int
	Now           time.Time
}

type defaultShareProvider struct{}

func (defaultShareProvider) CreateOrReuseShare(ctx context.Context, root *model.ETFMediaRoot) (*model.MobileShareRecord, error) {
	rootPath := root.MediaRootPath
	if root != nil && root.ActualMediaRootPath != "" {
		rootPath = root.ActualMediaRootPath
	}
	storage, actualPath, err := op.GetStorageAndActualPath(rootPath)
	if err != nil {
		return nil, err
	}
	result, err := mobileshare.CreateOrReuseShareByPath(ctx, storage, actualPath, root.SharePeriodUnit, false)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("mobile share result is nil")
	}
	return result.Record, nil
}

func RunPendingJobs(ctx context.Context, opts RunnerOptions) (int, error) {
	opts = normalizeRunnerOptions(opts)
	database := db.GetDb()
	if database == nil {
		return 0, errors.New("database is not initialized")
	}
	var jobs []model.ETFSubscriptionJob
	if err := database.WithContext(ctx).
		Where("status = ? OR (status = ? AND (next_retry_at IS NULL OR next_retry_at <= ?))",
			model.ETFSubscriptionJobStatusPending, model.ETFSubscriptionJobStatusFailed, opts.Now).
		Order("updated_at ASC").
		Limit(opts.Limit).
		Find(&jobs).Error; err != nil {
		return 0, errors.WithStack(err)
	}
	processed := 0
	for i := range jobs {
		if err := runJob(ctx, &jobs[i], opts); err != nil {
			return processed, err
		}
		processed++
	}
	return processed, nil
}

func runJob(ctx context.Context, job *model.ETFSubscriptionJob, opts RunnerOptions) error {
	switch job.Type {
	case model.ETFSubscriptionJobTypeCreate:
		return runCreateSubscriptionJob(ctx, job, opts)
	case model.ETFSubscriptionJobTypeManualCheck:
		return runManualCheckJob(ctx, job, opts)
	default:
		return markJobFailed(ctx, job.ID, fmt.Errorf("unsupported etf subscription job type %q", job.Type), opts)
	}
}

func runCreateSubscriptionJob(ctx context.Context, job *model.ETFSubscriptionJob, opts RunnerOptions) error {
	root, err := getMediaRoot(ctx, job.MediaRootID)
	if err != nil {
		return markJobFailed(ctx, job.ID, err, opts)
	}
	share, err := opts.ShareProvider.CreateOrReuseShare(ctx, root)
	if err != nil {
		return markJobFailed(ctx, job.ID, err, opts)
	}
	if share == nil || share.ShareURL == "" {
		return markJobFailed(ctx, job.ID, errors.New("share provider returned empty share url"), opts)
	}
	if err := updateJobShare(ctx, job.ID, root.ID, share); err != nil {
		return err
	}
	client := NewTargetClient(job.TargetBaseURL, job.TargetAPIToken, opts.HTTPClient, opts.Timeout)
	result, err := client.CreateSubscription(ctx, CreateSubscriptionPayload{
		TMDBID:       root.TMDBID,
		MediaType:    root.MediaType,
		ShareURL:     share.ShareURL,
		AccessCode:   share.ExtractCode,
		ShareType:    normalizeShareType(job.ShareType),
		SeasonStart:  defaultSeasonStart(root.MediaType),
		EpisodeStart: defaultEpisodeStart(root.MediaType),
	}, job.JobKey)
	if err != nil {
		if IsDeliveryUncertain(err) {
			if job.TargetSupportsIdempotency {
				return markJobFailed(ctx, job.ID, err, opts)
			}
			return markJobUnknown(ctx, job.ID, err)
		}
		return markJobFailed(ctx, job.ID, err, opts)
	}
	return MarkCreateSubscriptionSucceeded(ctx, job.ID, CreateSubscriptionResult{
		SubscriptionID: result.SubscriptionID,
		TaskID:         result.TaskID,
		Fingerprint:    job.Fingerprint,
		ResponseJSON:   result.RawJSON,
	})
}

func runManualCheckJob(ctx context.Context, job *model.ETFSubscriptionJob, opts RunnerOptions) error {
	root, err := getMediaRoot(ctx, job.MediaRootID)
	if err != nil {
		return markJobFailed(ctx, job.ID, err, opts)
	}
	subscriptionID := job.TargetSubscriptionID
	if subscriptionID <= 0 {
		subscriptionID = root.TargetSubscriptionID
	}
	client := NewTargetClient(job.TargetBaseURL, job.TargetAPIToken, opts.HTTPClient, opts.Timeout)
	result, err := client.CheckSubscription(ctx, subscriptionID, job.JobKey)
	if err != nil {
		if IsDeliveryUncertain(err) {
			if job.TargetSupportsIdempotency {
				return markJobFailed(ctx, job.ID, err, opts)
			}
			return markJobUnknown(ctx, job.ID, err)
		}
		return markJobFailed(ctx, job.ID, err, opts)
	}
	return markManualCheckSucceeded(ctx, job.ID, result)
}

func markJobUnknown(ctx context.Context, jobID uint, cause error) error {
	if cause == nil {
		cause = errors.New("target delivery outcome is unknown")
	}
	return db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job model.ETFSubscriptionJob
		if err := tx.First(&job, jobID).Error; err != nil {
			return err
		}
		job.Attempts++
		job.Status = model.ETFSubscriptionJobStatusUnknown
		job.NextRetryAt = nil
		job.LastError = cause.Error()
		if err := tx.Save(&job).Error; err != nil {
			return err
		}
		return updateClusterJobNotificationStatus(tx, job.ClusterJobIDsJSON, model.ClusterNotificationStatusUnknown)
	})
}

func updateJobShare(ctx context.Context, jobID, mediaRootID uint, share *model.MobileShareRecord) error {
	database := db.GetDb()
	return database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job model.ETFSubscriptionJob
		if err := tx.First(&job, jobID).Error; err != nil {
			return err
		}
		var root model.ETFMediaRoot
		if err := tx.First(&root, mediaRootID).Error; err != nil {
			return err
		}
		job.MobileShareRecordID = share.ID
		job.ShareURL = share.ShareURL
		job.AccessCode = share.ExtractCode
		root.MobileShareRecordID = share.ID
		root.ShareURL = share.ShareURL
		root.AccessCode = share.ExtractCode
		if strings.TrimSpace(share.SourcePath) != "" {
			root.ActualMediaRootPath = strings.TrimSpace(share.SourcePath)
		}
		if err := tx.Save(&job).Error; err != nil {
			return err
		}
		return tx.Save(&root).Error
	})
}

func markManualCheckSucceeded(ctx context.Context, jobID uint, result *TargetTaskResult) error {
	database := db.GetDb()
	return database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job model.ETFSubscriptionJob
		if err := tx.First(&job, jobID).Error; err != nil {
			return err
		}
		var root model.ETFMediaRoot
		if err := tx.First(&root, job.MediaRootID).Error; err != nil {
			return err
		}
		job.Status = model.ETFSubscriptionJobStatusSucceeded
		job.TargetTaskID = result.TaskID
		job.ResponseJSON = result.RawJSON
		job.LastError = ""
		root.LastCheckTaskID = result.TaskID
		root.LastNotifiedFingerprint = job.Fingerprint
		root.CurrentFingerprint = job.Fingerprint
		root.PendingChangeCount = 0
		root.DirtySince = nil
		root.Status = model.ETFMediaRootStatusSubscribed
		root.LastError = ""
		if err := tx.Save(&job).Error; err != nil {
			return err
		}
		if err := tx.Save(&root).Error; err != nil {
			return err
		}
		return updateClusterJobNotificationStatus(tx, job.ClusterJobIDsJSON, model.ClusterNotificationStatusSucceeded)
	})
}

func markJobFailed(ctx context.Context, jobID uint, cause error, opts RunnerOptions) error {
	if cause == nil {
		cause = errors.New("unknown error")
	}
	database := db.GetDb()
	return database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job model.ETFSubscriptionJob
		if err := tx.First(&job, jobID).Error; err != nil {
			return err
		}
		job.Attempts++
		job.LastError = cause.Error()
		status := model.ETFSubscriptionJobStatusFailed
		if opts.MaxRetries > 0 && job.Attempts >= opts.MaxRetries {
			status = model.ETFSubscriptionJobStatusDeadLetter
		}
		job.Status = status
		if status == model.ETFSubscriptionJobStatusFailed {
			next := opts.Now.Add(opts.RetryDelay)
			job.NextRetryAt = &next
		}
		if err := tx.Save(&job).Error; err != nil {
			return err
		}
		clusterStatus := model.ClusterNotificationStatusPending
		if status == model.ETFSubscriptionJobStatusDeadLetter {
			clusterStatus = model.ClusterNotificationStatusFailed
		}
		return updateClusterJobNotificationStatus(tx, job.ClusterJobIDsJSON, clusterStatus)
	})
}

func normalizeRunnerOptions(opts RunnerOptions) RunnerOptions {
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.MaxRetries <= 0 {
		opts.MaxRetries = 5
	}
	if opts.RetryDelay <= 0 {
		opts.RetryDelay = 30 * time.Second
	}
	if opts.Limit <= 0 {
		opts.Limit = 20
	}
	if opts.ShareProvider == nil {
		opts.ShareProvider = defaultShareProvider{}
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	return opts
}

func defaultSeasonStart(mediaType string) int {
	if mediaType == "tv" {
		return 1
	}
	return 0
}

func defaultEpisodeStart(mediaType string) int {
	if mediaType == "tv" {
		return 1
	}
	return 0
}

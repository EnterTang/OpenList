package etfauto

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Config struct {
	Enabled                   bool
	TargetBaseURL             string
	TargetAPIToken            string
	TargetSupportsIdempotency bool
	QuietWindow               time.Duration
	SharePeriodUnit           int
	ShareType                 string
}

type ArchiveEvent struct {
	Record           *model.ETFArchiveRecord
	ClusterJobID     string
	MediaRootFileID  string
	MediaRootPath    string
	MediaRootCreated bool
	OccurredAt       time.Time
}

type JobFilter struct {
	Type   string
	Status string
}

type MediaRootFilter struct {
	Keyword string
	Status  string
	Page    int
	PerPage int
}

type CreateSubscriptionResult struct {
	SubscriptionID int64
	TaskID         string
	Fingerprint    string
	ResponseJSON   string
}

type ManualCheckStatus string

const (
	ManualCheckQueued          ManualCheckStatus = "queued"
	ManualCheckAlreadyQueued   ManualCheckStatus = "already_queued"
	ManualCheckNoChange        ManualCheckStatus = "no_change"
	ManualCheckNoSubscription  ManualCheckStatus = "no_subscription"
	ManualCheckBatchCollecting ManualCheckStatus = "batch_collecting"
)

type ManualCheckResult struct {
	Status ManualCheckStatus         `json:"status"`
	Job    *model.ETFSubscriptionJob `json:"job,omitempty"`
}

func RecordArchiveEvent(ctx context.Context, event ArchiveEvent, cfg Config) (*model.ETFMediaRootBatch, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if event.Record == nil {
		return nil, errors.New("etf archive record is nil")
	}
	if event.Record.Status != model.ETFArchiveStatusArchived && event.Record.Status != model.ETFArchiveStatusCorrected {
		return nil, nil
	}
	if !event.Record.TMDBMatched || event.Record.TMDBID <= 0 || strings.TrimSpace(event.Record.MediaType) == "" {
		return nil, nil
	}
	if strings.TrimSpace(event.MediaRootPath) == "" {
		return nil, errors.New("media root path is empty")
	}
	database := db.GetDb()
	if database == nil {
		return nil, errors.New("database is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now()
	}
	cfg = normalizeConfig(cfg)
	if err := db.CreateETFArchiveRecord(event.Record); err != nil {
		return nil, err
	}
	root, rootCreated, err := upsertMediaRoot(ctx, event, cfg)
	if err != nil {
		return nil, err
	}
	event.MediaRootCreated = event.MediaRootCreated || rootCreated
	return upsertCollectingBatch(ctx, root, event, cfg)
}

func CloseDueBatches(ctx context.Context, now time.Time) (int, error) {
	if now.IsZero() {
		now = time.Now()
	}
	database := db.GetDb()
	if database == nil {
		return 0, errors.New("database is not initialized")
	}
	var batches []model.ETFMediaRootBatch
	if err := database.WithContext(ctx).
		Where("status = ? AND quiet_until <= ?", model.ETFMediaRootBatchStatusCollecting, now).
		Order("quiet_until ASC").
		Find(&batches).Error; err != nil {
		return 0, errors.WithStack(err)
	}
	closed := 0
	for _, batch := range batches {
		if err := closeBatch(ctx, &batch); err != nil {
			return closed, err
		}
		closed++
	}
	return closed, nil
}

func ComputeMediaRootFingerprint(ctx context.Context, mediaRootID uint) (string, error) {
	root, err := getMediaRoot(ctx, mediaRootID)
	if err != nil {
		return "", err
	}
	database := db.GetDb()
	if database == nil {
		return "", errors.New("database is not initialized")
	}
	rootPath := strings.TrimSpace(root.ActualMediaRootPath)
	if rootPath == "" {
		rootPath = root.MediaRootPath
	}
	prefix := strings.TrimRight(rootPath, "/") + "/%"
	var records []model.ETFArchiveRecord
	if err := database.WithContext(ctx).
		Where("storage_mount_path = ? AND media_type = ? AND tmdb_id = ? AND archive_etf_path LIKE ? AND status IN ?",
			root.StorageMountPath, root.MediaType, root.TMDBID, prefix, []string{model.ETFArchiveStatusArchived, model.ETFArchiveStatusCorrected}).
		Find(&records).Error; err != nil {
		return "", errors.WithStack(err)
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].ArchiveETFPath < records[j].ArchiveETFPath
	})
	hasher := sha256.New()
	for _, record := range records {
		line := fmt.Sprintf("%s|%s|%d|%d|%d\n",
			strings.TrimSpace(record.ArchiveETFPath),
			strings.ToUpper(strings.TrimSpace(record.SourceSHA256)),
			record.SourceSize,
			record.Season,
			record.Episode,
		)
		_, _ = hasher.Write([]byte(line))
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func MarkCreateSubscriptionSucceeded(ctx context.Context, jobID uint, result CreateSubscriptionResult) error {
	database := db.GetDb()
	if database == nil {
		return errors.New("database is not initialized")
	}
	return database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job model.ETFSubscriptionJob
		if err := tx.First(&job, jobID).Error; err != nil {
			return err
		}
		var root model.ETFMediaRoot
		if err := tx.First(&root, job.MediaRootID).Error; err != nil {
			return err
		}
		if result.Fingerprint == "" {
			result.Fingerprint = job.Fingerprint
		}
		job.Status = model.ETFSubscriptionJobStatusSucceeded
		job.TargetSubscriptionID = result.SubscriptionID
		job.TargetTaskID = strings.TrimSpace(result.TaskID)
		job.Fingerprint = result.Fingerprint
		job.ResponseJSON = result.ResponseJSON
		job.LastError = ""
		root.TargetSubscriptionID = result.SubscriptionID
		root.LastCreateTaskID = strings.TrimSpace(result.TaskID)
		root.CurrentFingerprint = result.Fingerprint
		root.LastNotifiedFingerprint = result.Fingerprint
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

func RequestManualCheck(ctx context.Context, mediaRootID uint, now time.Time) (*ManualCheckResult, error) {
	if now.IsZero() {
		now = time.Now()
	}
	root, err := getMediaRoot(ctx, mediaRootID)
	if err != nil {
		return nil, err
	}
	if root.TargetSubscriptionID <= 0 {
		return &ManualCheckResult{Status: ManualCheckNoSubscription}, nil
	}
	collecting, err := hasCollectingBatch(ctx, mediaRootID)
	if err != nil {
		return nil, err
	}
	if collecting {
		return &ManualCheckResult{Status: ManualCheckBatchCollecting}, nil
	}
	fingerprint, err := ComputeMediaRootFingerprint(ctx, mediaRootID)
	if err != nil {
		return nil, err
	}
	if fingerprint == root.LastNotifiedFingerprint {
		return &ManualCheckResult{Status: ManualCheckNoChange}, nil
	}
	database := db.GetDb()
	if database == nil {
		return nil, errors.New("database is not initialized")
	}
	jobKey := "check:" + root.RootKey + ":" + fingerprint
	var existing model.ETFSubscriptionJob
	err = database.WithContext(ctx).Where("job_key = ?", jobKey).First(&existing).Error
	if err == nil {
		return &ManualCheckResult{Status: ManualCheckAlreadyQueued, Job: &existing}, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, errors.WithStack(err)
	}
	job := &model.ETFSubscriptionJob{
		JobKey:                    jobKey,
		MediaRootID:               root.ID,
		Type:                      model.ETFSubscriptionJobTypeManualCheck,
		Status:                    model.ETFSubscriptionJobStatusPending,
		TargetBaseURL:             root.TargetBaseURL,
		TargetAPIToken:            root.TargetAPIToken,
		TargetSupportsIdempotency: root.TargetSupportsIdempotency,
		ShareType:                 normalizeShareType(root.ShareType),
		TargetSubscriptionID:      root.TargetSubscriptionID,
		Fingerprint:               fingerprint,
	}
	if err := database.WithContext(ctx).Create(job).Error; err != nil {
		return nil, errors.WithStack(err)
	}
	return &ManualCheckResult{Status: ManualCheckQueued, Job: job}, nil
}

func ListJobs(ctx context.Context, filter JobFilter) ([]model.ETFSubscriptionJob, error) {
	database := db.GetDb()
	if database == nil {
		return nil, errors.New("database is not initialized")
	}
	query := database.WithContext(ctx).Model(&model.ETFSubscriptionJob{})
	if filter.Type != "" {
		query = query.Where("type = ?", filter.Type)
	}
	if filter.Status != "" {
		query = query.Where("status = ?", filter.Status)
	}
	var jobs []model.ETFSubscriptionJob
	if err := query.Order("id ASC").Find(&jobs).Error; err != nil {
		return nil, errors.WithStack(err)
	}
	return jobs, nil
}

func RetryUnknownJob(ctx context.Context, jobID uint) error {
	if jobID == 0 {
		return errors.New("ETF notification job id is required")
	}
	return db.GetDb().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job model.ETFSubscriptionJob
		if err := tx.First(&job, "id = ? AND status = ?", jobID, model.ETFSubscriptionJobStatusUnknown).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errors.New("ETF notification job is not in unknown state")
			}
			return err
		}
		if err := tx.Model(&job).Updates(map[string]any{"status": model.ETFSubscriptionJobStatusPending, "next_retry_at": nil, "last_error": ""}).Error; err != nil {
			return err
		}
		return updateClusterJobNotificationStatus(tx, job.ClusterJobIDsJSON, model.ClusterNotificationStatusPending)
	})
}

func ListMediaRoots(ctx context.Context, filter MediaRootFilter) ([]model.ETFMediaRoot, int64, error) {
	database := db.GetDb()
	if database == nil {
		return nil, 0, errors.New("database is not initialized")
	}
	query := database.WithContext(ctx).Model(&model.ETFMediaRoot{})
	if keyword := strings.TrimSpace(filter.Keyword); keyword != "" {
		like := "%" + keyword + "%"
		query = query.Where("media_root_path LIKE ? OR tmdb_name LIKE ? OR share_url LIKE ?", like, like, like)
	}
	if status := strings.TrimSpace(filter.Status); status != "" {
		query = query.Where("status = ?", status)
	}
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, errors.WithStack(err)
	}
	page := filter.Page
	if page < 1 {
		page = 1
	}
	perPage := filter.PerPage
	if perPage < 1 {
		perPage = 20
	}
	var roots []model.ETFMediaRoot
	if err := query.Order("updated_at DESC").Offset((page - 1) * perPage).Limit(perPage).Find(&roots).Error; err != nil {
		return nil, 0, errors.WithStack(err)
	}
	return roots, total, nil
}

func closeBatch(ctx context.Context, batch *model.ETFMediaRootBatch) error {
	database := db.GetDb()
	if database == nil {
		return errors.New("database is not initialized")
	}
	return database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var root model.ETFMediaRoot
		if err := tx.First(&root, batch.MediaRootID).Error; err != nil {
			return err
		}
		fingerprint, err := ComputeMediaRootFingerprint(ctx, batch.MediaRootID)
		if err != nil {
			return err
		}
		batch.Status = model.ETFMediaRootBatchStatusClosed
		batch.FingerprintAfterBatch = fingerprint
		root.CurrentFingerprint = fingerprint
		if batch.MediaRootCreated {
			job := &model.ETFSubscriptionJob{
				JobKey:                    "create:" + root.RootKey,
				MediaRootID:               root.ID,
				BatchID:                   batch.ID,
				Type:                      model.ETFSubscriptionJobTypeCreate,
				Status:                    model.ETFSubscriptionJobStatusPending,
				TargetBaseURL:             root.TargetBaseURL,
				TargetAPIToken:            root.TargetAPIToken,
				TargetSupportsIdempotency: root.TargetSupportsIdempotency,
				ShareType:                 normalizeShareType(root.ShareType),
				Fingerprint:               fingerprint,
				ClusterJobIDsJSON:         batch.ClusterJobIDsJSON,
			}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(job).Error; err != nil {
				return err
			}
		} else {
			dirtySince := batch.FirstEventAt
			root.DirtySince = &dirtySince
			root.PendingChangeCount += batch.ETFCount
			root.Status = model.ETFMediaRootStatusDirty
			if root.TargetSubscriptionID > 0 && fingerprint != root.LastNotifiedFingerprint {
				job := &model.ETFSubscriptionJob{
					JobKey:                    "check:" + root.RootKey + ":" + fingerprint,
					MediaRootID:               root.ID,
					BatchID:                   batch.ID,
					Type:                      model.ETFSubscriptionJobTypeManualCheck,
					Status:                    model.ETFSubscriptionJobStatusPending,
					TargetBaseURL:             root.TargetBaseURL,
					TargetAPIToken:            root.TargetAPIToken,
					TargetSupportsIdempotency: root.TargetSupportsIdempotency,
					ShareType:                 normalizeShareType(root.ShareType),
					TargetSubscriptionID:      root.TargetSubscriptionID,
					Fingerprint:               fingerprint,
					ClusterJobIDsJSON:         batch.ClusterJobIDsJSON,
				}
				if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(job).Error; err != nil {
					return err
				}
			}
		}
		if err := tx.Save(batch).Error; err != nil {
			return err
		}
		return tx.Save(&root).Error
	})
}

func upsertMediaRoot(ctx context.Context, event ArchiveEvent, cfg Config) (*model.ETFMediaRoot, bool, error) {
	database := db.GetDb()
	record := event.Record
	rootKey := MediaRootKey(record.StorageMountPath, event.MediaRootPath, record.MediaType, record.TMDBID)
	rootCreated := false
	var existing model.ETFMediaRoot
	err := database.WithContext(ctx).Where("root_key = ?", rootKey).Select("id").First(&existing).Error
	if err == nil {
		rootCreated = false
	} else if errors.Is(err, gorm.ErrRecordNotFound) {
		rootCreated = true
	} else {
		return nil, false, errors.WithStack(err)
	}
	root := &model.ETFMediaRoot{
		RootKey:                   rootKey,
		StorageID:                 record.StorageID,
		StorageMountPath:          utils.FixAndCleanPath(record.StorageMountPath),
		DriveID:                   utils.FixAndCleanPath(record.StorageMountPath),
		MediaRootFileID:           strings.TrimSpace(event.MediaRootFileID),
		MediaRootPath:             normalizeFullPath(event.MediaRootPath),
		ActualMediaRootPath:       normalizeFullPath(event.MediaRootPath),
		MediaType:                 strings.TrimSpace(record.MediaType),
		TMDBID:                    record.TMDBID,
		TMDBName:                  record.TMDBName,
		TMDBYear:                  record.TMDBYear,
		Category:                  record.Category,
		TargetBaseURL:             strings.TrimRight(strings.TrimSpace(cfg.TargetBaseURL), "/"),
		TargetAPIToken:            strings.TrimSpace(cfg.TargetAPIToken),
		TargetSupportsIdempotency: cfg.TargetSupportsIdempotency,
		ShareType:                 normalizeShareType(cfg.ShareType),
		SharePeriodUnit:           cfg.SharePeriodUnit,
		Status:                    model.ETFMediaRootStatusCollecting,
	}
	if err := database.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "root_key"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"storage_id":                  root.StorageID,
			"storage_mount_path":          root.StorageMountPath,
			"drive_id":                    root.DriveID,
			"media_root_file_id":          root.MediaRootFileID,
			"media_root_path":             root.MediaRootPath,
			"media_type":                  root.MediaType,
			"tmdb_id":                     root.TMDBID,
			"tmdb_name":                   root.TMDBName,
			"tmdb_year":                   root.TMDBYear,
			"category":                    root.Category,
			"target_base_url":             root.TargetBaseURL,
			"target_api_token":            root.TargetAPIToken,
			"target_supports_idempotency": root.TargetSupportsIdempotency,
			"share_type":                  root.ShareType,
			"share_period_unit":           root.SharePeriodUnit,
			"updated_at":                  time.Now(),
		}),
	}).Create(root).Error; err != nil {
		return nil, false, errors.WithStack(err)
	}
	var saved model.ETFMediaRoot
	if err := database.WithContext(ctx).Where("root_key = ?", rootKey).First(&saved).Error; err != nil {
		return nil, false, errors.WithStack(err)
	}
	return &saved, rootCreated, nil
}

func upsertCollectingBatch(ctx context.Context, root *model.ETFMediaRoot, event ArchiveEvent, cfg Config) (*model.ETFMediaRootBatch, error) {
	database := db.GetDb()
	var batch model.ETFMediaRootBatch
	err := database.WithContext(ctx).
		Where("media_root_id = ? AND status = ?", root.ID, model.ETFMediaRootBatchStatusCollecting).
		First(&batch).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, errors.WithStack(err)
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		batch = model.ETFMediaRootBatch{
			BatchKey:         fmt.Sprintf("%s:%d", root.RootKey, event.OccurredAt.UnixNano()),
			MediaRootID:      root.ID,
			Status:           model.ETFMediaRootBatchStatusCollecting,
			Reason:           batchReason(event.MediaRootCreated),
			FirstEventAt:     event.OccurredAt,
			MediaRootCreated: event.MediaRootCreated,
		}
	}
	batch.ETFCount++
	batch.LastEventAt = event.OccurredAt
	batch.QuietUntil = event.OccurredAt.Add(cfg.QuietWindow)
	batch.MediaRootCreated = batch.MediaRootCreated || event.MediaRootCreated
	clusterJobIDs, err := mergeClusterJobIDs(batch.ClusterJobIDsJSON, event.ClusterJobID)
	if err != nil {
		return nil, err
	}
	batch.ClusterJobIDsJSON = clusterJobIDs
	if batch.MediaRootCreated {
		batch.Reason = model.ETFMediaRootBatchReasonInitialCreate
	}
	if batch.ID == 0 {
		if err := database.WithContext(ctx).Create(&batch).Error; err != nil {
			return nil, errors.WithStack(err)
		}
		return &batch, nil
	}
	if err := database.WithContext(ctx).Save(&batch).Error; err != nil {
		return nil, errors.WithStack(err)
	}
	return &batch, nil
}

func mergeClusterJobIDs(raw, id string) (string, error) {
	ids, err := decodeClusterJobIDs(raw)
	if err != nil {
		return "", err
	}
	id = strings.TrimSpace(id)
	if id != "" {
		ids = append(ids, id)
	}
	seen := make(map[string]struct{}, len(ids))
	unique := make([]string, 0, len(ids))
	for _, candidate := range ids {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		unique = append(unique, candidate)
	}
	if len(unique) == 0 {
		return "", nil
	}
	sort.Strings(unique)
	encoded, err := json.Marshal(unique)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return string(encoded), nil
}

func decodeClusterJobIDs(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var ids []string
	if err := json.Unmarshal([]byte(raw), &ids); err != nil {
		return nil, errors.Wrap(err, "decode linked cluster job IDs")
	}
	return ids, nil
}

func updateClusterJobNotificationStatus(tx *gorm.DB, raw, status string) error {
	ids, err := decodeClusterJobIDs(raw)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	return tx.Model(&model.ClusterJob{}).Where("id IN ?", ids).Update("notification_status", status).Error
}

func getMediaRoot(ctx context.Context, id uint) (*model.ETFMediaRoot, error) {
	database := db.GetDb()
	if database == nil {
		return nil, errors.New("database is not initialized")
	}
	var root model.ETFMediaRoot
	if err := database.WithContext(ctx).First(&root, id).Error; err != nil {
		return nil, errors.WithStack(err)
	}
	return &root, nil
}

func hasCollectingBatch(ctx context.Context, mediaRootID uint) (bool, error) {
	database := db.GetDb()
	var count int64
	err := database.WithContext(ctx).Model(&model.ETFMediaRootBatch{}).
		Where("media_root_id = ? AND status = ?", mediaRootID, model.ETFMediaRootBatchStatusCollecting).
		Count(&count).Error
	return count > 0, errors.WithStack(err)
}

func MediaRootKey(storageMountPath, mediaRootPath, mediaType string, tmdbID int64) string {
	sum := sha1.Sum([]byte(strings.Join([]string{
		normalizeFullPath(storageMountPath),
		normalizeFullPath(mediaRootPath),
		strings.ToLower(strings.TrimSpace(mediaType)),
		fmt.Sprintf("%d", tmdbID),
	}, "|")))
	return hex.EncodeToString(sum[:])
}

func normalizeConfig(cfg Config) Config {
	if cfg.QuietWindow <= 0 {
		cfg.QuietWindow = 30 * time.Second
	}
	cfg.TargetBaseURL = strings.TrimRight(strings.TrimSpace(cfg.TargetBaseURL), "/")
	cfg.TargetAPIToken = strings.TrimSpace(cfg.TargetAPIToken)
	cfg.ShareType = normalizeShareType(cfg.ShareType)
	if cfg.SharePeriodUnit <= 0 {
		cfg.SharePeriodUnit = 1
	}
	return cfg
}

func normalizeShareType(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "regular") {
		return "regular"
	}
	return "etf"
}

func batchReason(created bool) string {
	if created {
		return model.ETFMediaRootBatchReasonInitialCreate
	}
	return model.ETFMediaRootBatchReasonContentChanged
}

func normalizeFullPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "/"
	}
	return path.Clean("/" + strings.TrimLeft(value, "/"))
}

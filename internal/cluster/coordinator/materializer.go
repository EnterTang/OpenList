package coordinator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/etfauto"
	"github.com/OpenListTeam/OpenList/v4/internal/etfmeta"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"gorm.io/gorm"
)

var seasonFolderPattern = regexp.MustCompile(`(?i)^season\s+0*[1-9]\d*$`)

func (s *Service) ProcessPendingManifests(ctx context.Context, limit int) (int, error) {
	materializeETF := strings.TrimSpace(conf.Conf.Cluster.ETFRootPath) != ""
	if limit <= 0 {
		limit = 20
	}
	var manifests []model.ClusterUploadManifest
	if err := s.db.WithContext(ctx).
		Where("status IN ? AND consumed_at IS NULL", []string{model.ClusterUploadManifestStatusAccepted, model.ClusterUploadManifestStatusAdopted}).
		Order("received_at ASC").Limit(limit).Find(&manifests).Error; err != nil {
		return 0, err
	}
	processed := 0
	for i := range manifests {
		var err error
		if materializeETF {
			err = s.materializeManifest(ctx, &manifests[i])
		} else {
			err = s.completeManifestMaterialization(ctx, manifests[i].ID, manifests[i].JobID, model.ClusterNotificationStatusNotRequired, time.Now().UTC())
		}
		if err != nil {
			_ = s.db.WithContext(ctx).Model(&model.ClusterUploadManifest{}).Where("id = ?", manifests[i].ID).Update("last_error", err.Error()).Error
			continue
		}
		processed++
	}
	return processed, nil
}

func (s *Service) materializeManifest(ctx context.Context, manifest *model.ClusterUploadManifest) error {
	if manifest == nil {
		return errors.New("cluster upload manifest is nil")
	}
	root, notification, err := resolveClusterMaterializationSettings(conf.Conf.Cluster.ETFRootPath)
	if err != nil {
		return err
	}
	relativeArchiveDir, err := safeRelativeArchiveDirectory(manifest.LogicalTargetPath)
	if err != nil {
		return err
	}
	relativeMediaRoot, err := safeRelativeMediaRoot(manifest.LogicalTargetPath)
	if err != nil {
		return err
	}
	dstDir := path.Join(root, relativeArchiveDir)
	mediaRootPath := path.Join(root, relativeMediaRoot)
	if err := fs.MakeDir(ctx, dstDir); err != nil {
		return fmt.Errorf("create cluster ETF directory: %w", err)
	}
	info := &etfmeta.Info{
		Name:       manifest.Name,
		Size:       manifest.Size,
		SHA256:     strings.ToUpper(manifest.SHA256),
		CreateTime: manifest.ReceivedAt.UTC().Format(time.RFC3339),
	}
	content, err := etfmeta.Encode(info)
	if err != nil {
		return err
	}
	etfName := etfmeta.FileName(manifest.Name)
	archivePath := path.Join(dstDir, etfName)
	storage, _, err := op.GetStorageAndActualPath(dstDir)
	if err != nil {
		return fmt.Errorf("resolve cluster ETF storage: %w", err)
	}
	record := &model.ETFArchiveRecord{
		StorageID:        storage.GetStorage().ID,
		StorageMountPath: storage.GetStorage().MountPath,
		SourceName:       manifest.Name,
		SourcePath:       manifest.LogicalTargetPath,
		LocalETFPath:     archivePath,
		ArchiveETFPath:   archivePath,
		ArchiveRoot:      root,
		ArchiveEnabled:   true,
		TMDBMatched:      manifest.TMDBID > 0,
		TMDBID:           manifest.TMDBID,
		TMDBName:         manifest.TMDBName,
		MediaType:        manifest.MediaType,
		Season:           manifest.Season,
		Episode:          manifest.Episode,
		SourceSize:       manifest.Size,
		SourceSHA256:     strings.ToUpper(manifest.SHA256),
		Status:           model.ETFArchiveStatusArchived,
	}
	existing, writeETF, err := s.prepareArchiveRecord(ctx, record)
	if err != nil {
		return err
	}
	if writeETF {
		file := &stream.FileStream{
			Ctx: ctx,
			Obj: &model.Object{
				Name:     etfName,
				Size:     int64(len(content)),
				Modified: time.Now(),
			},
			Reader:   bytes.NewReader(content),
			Mimetype: "application/octet-stream",
		}
		if err := fs.PutDirectly(ctx, dstDir, file, true); err != nil {
			return fmt.Errorf("write cluster ETF: %w", err)
		}
		if err := s.persistArchiveRecord(ctx, existing); err != nil {
			return err
		}
	}
	record = existing
	mediaRootStorage, actualMediaRootDir, err := op.GetStorageAndActualPath(mediaRootPath)
	if err != nil {
		return fmt.Errorf("resolve cluster ETF media root: %w", err)
	}
	rootObj, err := op.Get(ctx, mediaRootStorage, actualMediaRootDir)
	if err != nil {
		return fmt.Errorf("read cluster ETF media root: %w", err)
	}
	_, err = etfauto.RecordArchiveEvent(ctx, etfauto.ArchiveEvent{
		Record:          record,
		ClusterJobID:    manifest.JobID,
		MediaRootFileID: rootObj.GetID(),
		MediaRootPath:   mediaRootPath,
		OccurredAt:      time.Now().UTC(),
	}, notification)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	return s.completeManifestMaterialization(ctx, manifest.ID, manifest.JobID, clusterTargetNotificationStatus(notification.TargetBaseURL), now)
}

func resolveClusterMaterializationSettings(configuredRoot string) (string, etfauto.Config, error) {
	root := path.Clean(strings.TrimSpace(configuredRoot))
	if root == "." || root == "/" {
		return "", etfauto.Config{}, errors.New("cluster ETF root path must select a storage directory")
	}
	storage, _, err := op.GetStorageAndActualPath(root)
	if err != nil {
		return "", etfauto.Config{}, fmt.Errorf("resolve cluster ETF root storage: %w", err)
	}
	settings := driver.ETFArchiveSettings{}
	if provider, ok := storage.(driver.ETFArchiveSettingsProvider); ok {
		settings = provider.ETFArchiveSettings()
	}
	notification := etfauto.Config{
		Enabled:                   strings.TrimSpace(conf.Conf.Cluster.TargetBaseURL) != "",
		TargetBaseURL:             strings.TrimRight(strings.TrimSpace(conf.Conf.Cluster.TargetBaseURL), "/"),
		TargetAPIToken:            strings.TrimSpace(conf.Conf.Cluster.TargetAPIToken),
		TargetSupportsIdempotency: conf.Conf.Cluster.TargetSupportsIdempotency,
		QuietWindow:               time.Duration(conf.Conf.Cluster.QuietWindowSecond) * time.Second,
		SharePeriodUnit:           conf.Conf.Cluster.SharePeriodUnit,
		ShareType:                 conf.Conf.Cluster.ShareType,
		ShareNotificationEnabled:  true,
	}
	root, notification = mergeClusterMaterializationSettings(root, storage.GetStorage().MountPath, notification, settings)
	return root, notification, nil
}

func mergeClusterMaterializationSettings(root, mountPath string, notification etfauto.Config, settings driver.ETFArchiveSettings) (string, etfauto.Config) {
	root = path.Clean(strings.TrimSpace(root))
	mountPath = path.Clean(strings.TrimSpace(mountPath))
	if root == mountPath && strings.TrimSpace(settings.RelativeRoot) != "" {
		root = path.Join(root, settings.RelativeRoot)
	}
	if notification.TargetBaseURL == "" && settings.AutoSubscriptionEnabled && strings.TrimSpace(settings.TargetBaseURL) != "" {
		notification = etfauto.Config{
			Enabled:                   true,
			TargetBaseURL:             strings.TrimRight(strings.TrimSpace(settings.TargetBaseURL), "/"),
			TargetAPIToken:            strings.TrimSpace(settings.TargetAPIToken),
			TargetSupportsIdempotency: settings.TargetSupportsIdempotency,
			QuietWindow:               time.Duration(settings.QuietWindowSeconds) * time.Second,
			SharePeriodUnit:           settings.SharePeriodUnit,
			ShareType:                 settings.ShareType,
			ShareNotificationEnabled:  settings.AutoSubscriptionEnabled,
			DirectImportEnabled:       settings.DirectImportEnabled,
		}
	}
	return root, notification
}

func (s *Service) prepareArchiveRecord(ctx context.Context, candidate *model.ETFArchiveRecord) (*model.ETFArchiveRecord, bool, error) {
	if candidate == nil {
		return nil, false, errors.New("ETF archive record is nil")
	}
	candidate.StorageMountPath = strings.TrimSpace(candidate.StorageMountPath)
	candidate.ArchiveETFPath = strings.TrimSpace(candidate.ArchiveETFPath)
	candidate.SourceSHA256 = strings.ToUpper(strings.TrimSpace(candidate.SourceSHA256))
	var existing model.ETFArchiveRecord
	err := s.db.WithContext(ctx).Where(
		"storage_mount_path = ? AND archive_etf_path = ?",
		candidate.StorageMountPath,
		candidate.ArchiveETFPath,
	).First(&existing).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return candidate, true, nil
	}
	if err != nil {
		return nil, false, err
	}
	if strings.EqualFold(existing.SourceSHA256, candidate.SourceSHA256) {
		return &existing, false, nil
	}
	mergeArchiveRecord(&existing, candidate)
	return &existing, true, nil
}

func mergeArchiveRecord(existing, candidate *model.ETFArchiveRecord) {
	existing.StorageID = candidate.StorageID
	existing.StorageMountPath = candidate.StorageMountPath
	existing.SourceName = candidate.SourceName
	existing.SourcePath = candidate.SourcePath
	existing.LocalETFPath = candidate.LocalETFPath
	existing.ArchiveETFPath = candidate.ArchiveETFPath
	existing.ArchiveRoot = candidate.ArchiveRoot
	existing.ArchiveEnabled = candidate.ArchiveEnabled
	existing.TMDBMatched = candidate.TMDBMatched
	existing.TMDBID = candidate.TMDBID
	existing.MediaType = candidate.MediaType
	existing.Season = candidate.Season
	existing.Episode = candidate.Episode
	existing.SourceSize = candidate.SourceSize
	existing.SourceSHA256 = candidate.SourceSHA256
	existing.Status = candidate.Status
	existing.Error = candidate.Error
}

func (s *Service) persistArchiveRecord(ctx context.Context, record *model.ETFArchiveRecord) error {
	if record.ID == 0 {
		return s.db.WithContext(ctx).Create(record).Error
	}
	return s.db.WithContext(ctx).Save(record).Error
}

func (s *Service) completeManifestMaterialization(ctx context.Context, manifestID, jobID, notificationStatus string, now time.Time) error {
	var completed model.ClusterJob
	var completedItem model.SubscriptionItem
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&model.ClusterUploadManifest{}).Where("id = ?", manifestID).Updates(map[string]any{
			"status":      model.ClusterUploadManifestStatusConsumed,
			"consumed_at": now,
			"last_error":  "",
		}).Error; err != nil {
			return err
		}
		if err := tx.Model(&model.ClusterJobStage{}).Where("job_id = ? AND name = ?", jobID, model.ClusterStageETFMaterializing).Updates(map[string]any{
			"status":      model.ClusterStageStatusSucceeded,
			"finished_at": now,
		}).Error; err != nil {
			return err
		}
		if err := tx.Model(&model.ClusterJob{}).Where("id = ?", jobID).Updates(map[string]any{
			"status":              model.ClusterJobStatusSucceeded,
			"finished_at":         now,
			"notification_status": notificationStatus,
		}).Error; err != nil {
			return err
		}
		if err := tx.Model(&model.MoviePilotDeliveryFile{}).Where("cluster_job_id = ?", jobID).Updates(map[string]any{
			"status": model.MoviePilotDeliveryStatusMaterialized, "manifest_id": manifestID, "upload_progress": 1,
			"finished_at": now, "last_error_code": "", "last_error": "",
		}).Error; err != nil && !isOptionalMoviePilotTableError(err) {
			return err
		}
		if err := tx.Select("parent_job_id", "subscription_id", "subscription_item_id").First(&completed, "id = ?", jobID).Error; err != nil {
			return err
		}
		if completed.SubscriptionItemID != 0 {
			itemResult := tx.Where("id = ? AND cluster_job_id = ?", completed.SubscriptionItemID, jobID).First(&completedItem)
			if itemResult.Error != nil && !errors.Is(itemResult.Error, gorm.ErrRecordNotFound) {
				return itemResult.Error
			}
			if itemResult.RowsAffected == 0 {
				return reconcileParentJobTx(tx, completed.ParentJobID, now)
			}
			itemStatus := model.SubscriptionItemStatusTransferred
			if notificationStatus != model.ClusterNotificationStatusNotRequired && notificationStatus != model.ClusterNotificationStatusSucceeded {
				itemStatus = model.SubscriptionItemStatusNotifying
			}
			if err := tx.Model(&model.SubscriptionItem{}).
				Where("id = ? AND cluster_job_id = ?", completed.SubscriptionItemID, jobID).
				Updates(map[string]any{"status": itemStatus, "last_error": ""}).Error; err != nil {
				return err
			}
			if err := tx.Model(&model.SubscriptionEpisodeSource{}).
				Where("source_item_id = ? AND file_hash = ?", completedItem.ID, completedItem.FileHash).
				Updates(map[string]any{"status": itemStatus, "updated_at": now}).Error; err != nil {
				return err
			}
		}
		return reconcileParentJobTx(tx, completed.ParentJobID, now)
	})
	if err != nil {
		return err
	}
	return nil
}

func clusterTargetNotificationStatus(targetBaseURL string) string {
	if strings.TrimSpace(targetBaseURL) == "" {
		return model.ClusterNotificationStatusNotRequired
	}
	return model.ClusterNotificationStatusPending
}

func safeRelativeMediaRoot(logicalTargetPath string) (string, error) {
	dir, err := safeRelativeArchiveDirectory(logicalTargetPath)
	if err != nil {
		return "", err
	}
	if seasonFolderPattern.MatchString(path.Base(dir)) {
		dir = path.Dir(dir)
	}
	if dir == "" || dir == "." || strings.HasPrefix(dir, "../") {
		return "", errors.New("logical target path has no safe media root")
	}
	return dir, nil
}

func safeRelativeArchiveDirectory(logicalTargetPath string) (string, error) {
	cleaned := path.Clean("/" + strings.TrimSpace(logicalTargetPath))
	if cleaned == "/" || cleaned == "." {
		return "", errors.New("logical target path is empty")
	}
	dir := strings.TrimPrefix(path.Dir(cleaned), "/")
	if dir == "" || dir == "." || strings.HasPrefix(dir, "../") {
		return "", errors.New("logical target path has no safe archive directory")
	}
	return dir, nil
}

package coordinator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/etfauto"
	"github.com/OpenListTeam/OpenList/v4/internal/etfmeta"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"gorm.io/gorm"
)

func (s *Service) ProcessPendingManifests(ctx context.Context, limit int) (int, error) {
	if strings.TrimSpace(conf.Conf.Cluster.ETFRootPath) == "" {
		return 0, nil
	}
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
		if err := s.materializeManifest(ctx, &manifests[i]); err != nil {
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
	root := strings.TrimSpace(conf.Conf.Cluster.ETFRootPath)
	relativeRoot, err := safeRelativeMediaRoot(manifest.LogicalTargetPath)
	if err != nil {
		return err
	}
	dstDir := path.Join(root, relativeRoot)
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
	storage, actualDir, err := op.GetStorageAndActualPath(dstDir)
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
	rootObj, err := op.Get(ctx, storage, actualDir)
	if err != nil {
		return fmt.Errorf("read cluster ETF directory: %w", err)
	}
	_, err = etfauto.RecordArchiveEvent(ctx, etfauto.ArchiveEvent{
		Record:          record,
		ClusterJobID:    manifest.JobID,
		MediaRootFileID: rootObj.GetID(),
		MediaRootPath:   dstDir,
		OccurredAt:      time.Now().UTC(),
	}, etfauto.Config{
		Enabled:                   clusterTargetNotificationStatus(conf.Conf.Cluster.TargetBaseURL) == model.ClusterNotificationStatusPending,
		TargetBaseURL:             conf.Conf.Cluster.TargetBaseURL,
		TargetAPIToken:            conf.Conf.Cluster.TargetAPIToken,
		TargetSupportsIdempotency: conf.Conf.Cluster.TargetSupportsIdempotency,
		QuietWindow:               time.Duration(conf.Conf.Cluster.QuietWindowSecond) * time.Second,
		SharePeriodUnit:           conf.Conf.Cluster.SharePeriodUnit,
		ShareType:                 conf.Conf.Cluster.ShareType,
	})
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	return s.completeManifestMaterialization(ctx, manifest.ID, manifest.JobID, clusterTargetNotificationStatus(conf.Conf.Cluster.TargetBaseURL), now)
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
		if err := tx.Select("parent_job_id", "subscription_id", "subscription_item_id").First(&completed, "id = ?", jobID).Error; err != nil {
			return err
		}
		if completed.SubscriptionItemID != 0 {
			if err := tx.Model(&model.SubscriptionItem{}).
				Where("id = ? AND cluster_job_id = ?", completed.SubscriptionItemID, jobID).
				Updates(map[string]any{"status": model.SubscriptionItemStatusTransferred, "last_error": ""}).Error; err != nil {
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
	cleaned := path.Clean("/" + strings.TrimSpace(logicalTargetPath))
	if cleaned == "/" || cleaned == "." {
		return "", errors.New("logical target path is empty")
	}
	dir := strings.TrimPrefix(path.Dir(cleaned), "/")
	if dir == "" || dir == "." || strings.HasPrefix(dir, "../") {
		return "", errors.New("logical target path has no safe media root")
	}
	return dir, nil
}

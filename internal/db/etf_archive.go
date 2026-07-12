package db

import (
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type ETFArchiveRecordFilter struct {
	Keyword     string
	TMDBID      int64
	TMDBMatched *bool
	Page        int
	PerPage     int
}

func CreateETFArchiveRecord(record *model.ETFArchiveRecord) error {
	if record == nil {
		return errors.New("etf archive record is nil")
	}
	record.SourceSHA256 = strings.ToUpper(strings.TrimSpace(record.SourceSHA256))
	record.ArchiveETFPath = strings.TrimSpace(record.ArchiveETFPath)
	if existing, err := FindETFArchiveRecordByFingerprint(record.StorageMountPath, record.SourceSHA256, record.ArchiveETFPath); err == nil {
		record.ID = existing.ID
		record.CreatedAt = existing.CreatedAt
		return errors.WithStack(db.Save(record).Error)
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return errors.WithStack(err)
	}
	return errors.WithStack(db.Create(record).Error)
}

func UpdateETFArchiveRecord(record *model.ETFArchiveRecord) error {
	return errors.WithStack(db.Save(record).Error)
}

func GetETFMediaRootByRootKey(rootKey string) (*model.ETFMediaRoot, error) {
	var root model.ETFMediaRoot
	err := db.Where(columnName("root_key")+" = ?", strings.TrimSpace(rootKey)).First(&root).Error
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &root, nil
}

func FindETFMediaRootByPath(storageMountPath, mediaRootPath string) (*model.ETFMediaRoot, error) {
	storageMountPath = strings.TrimSpace(storageMountPath)
	mediaRootPath = strings.TrimSpace(mediaRootPath)
	var root model.ETFMediaRoot
	err := db.Where(
		columnName("storage_mount_path")+" = ? AND ("+columnName("media_root_path")+" = ? OR "+columnName("actual_media_root_path")+" = ?)",
		storageMountPath,
		mediaRootPath,
		mediaRootPath,
	).First(&root).Error
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &root, nil
}

func UpdateETFMediaRoot(root *model.ETFMediaRoot) error {
	if root == nil {
		return errors.New("etf media root is nil")
	}
	return errors.WithStack(db.Save(root).Error)
}

func UpdateETFArchivePathsByPrefix(storageMountPath, oldPrefix, newPrefix string) error {
	storageMountPath = strings.TrimSpace(storageMountPath)
	oldPrefix = strings.TrimSpace(oldPrefix)
	newPrefix = strings.TrimSpace(newPrefix)
	if storageMountPath == "" || oldPrefix == "" || newPrefix == "" || oldPrefix == newPrefix {
		return nil
	}
	var records []model.ETFArchiveRecord
	if err := db.Where(
		columnName("storage_mount_path")+" = ? AND "+columnName("archive_etf_path")+" LIKE ?",
		storageMountPath,
		oldPrefix+"/%",
	).Find(&records).Error; err != nil {
		return errors.WithStack(err)
	}
	for i := range records {
		records[i].ArchiveETFPath = newPrefix + strings.TrimPrefix(records[i].ArchiveETFPath, oldPrefix)
		if err := db.Save(&records[i]).Error; err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func UpdateETFArchivePath(storageMountPath, oldPath, newPath string) error {
	storageMountPath = strings.TrimSpace(storageMountPath)
	oldPath = strings.TrimSpace(oldPath)
	newPath = strings.TrimSpace(newPath)
	if storageMountPath == "" || oldPath == "" || newPath == "" || oldPath == newPath {
		return nil
	}
	return errors.WithStack(
		db.Model(&model.ETFArchiveRecord{}).
			Where(columnName("storage_mount_path")+" = ? AND "+columnName("archive_etf_path")+" = ?", storageMountPath, oldPath).
			Update(columnName("archive_etf_path"), newPath).Error,
	)
}

func GetETFArchiveRecordByID(id uint) (*model.ETFArchiveRecord, error) {
	var record model.ETFArchiveRecord
	if err := db.First(&record, id).Error; err != nil {
		return nil, errors.WithStack(err)
	}
	return &record, nil
}

func FindETFArchiveRecordByFingerprint(storageMountPath, sourceSHA256, archiveETFPath string) (*model.ETFArchiveRecord, error) {
	sourceSHA256 = strings.ToUpper(strings.TrimSpace(sourceSHA256))
	archiveETFPath = strings.TrimSpace(archiveETFPath)
	if sourceSHA256 == "" || archiveETFPath == "" {
		return nil, gorm.ErrRecordNotFound
	}
	var record model.ETFArchiveRecord
	err := db.Where(
		columnName("storage_mount_path")+" = ? AND "+columnName("source_sha256")+" = ? AND "+columnName("archive_etf_path")+" = ?",
		strings.TrimSpace(storageMountPath), sourceSHA256, archiveETFPath,
	).First(&record).Error
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &record, nil
}

func ListETFArchiveRecords(filter ETFArchiveRecordFilter) ([]model.ETFArchiveRecord, int64, error) {
	query := db.Model(&model.ETFArchiveRecord{})
	if keyword := strings.TrimSpace(filter.Keyword); keyword != "" {
		like := "%" + keyword + "%"
		query = query.Where(
			columnName("source_name")+" LIKE ? OR "+columnName("tmdb_name")+" LIKE ? OR "+columnName("local_etf_path")+" LIKE ? OR "+columnName("archive_etf_path")+" LIKE ?",
			like, like, like, like,
		)
	}
	if filter.TMDBID > 0 {
		query = query.Where(columnName("tmdb_id")+" = ?", filter.TMDBID)
	}
	if filter.TMDBMatched != nil {
		query = query.Where(columnName("tmdb_matched")+" = ?", *filter.TMDBMatched)
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
	var records []model.ETFArchiveRecord
	if err := query.Order(columnName("updated_at") + " DESC").Offset((page - 1) * perPage).Limit(perPage).Find(&records).Error; err != nil {
		return nil, 0, errors.WithStack(err)
	}
	return records, total, nil
}

package db

import (
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type MobileShareRecordFilter struct {
	Keyword          string
	StorageMountPath string
	SourceType       string
	IsValid          *bool
	Page             int
	PerPage          int
}

func GetMobileShareRecordBySource(driveID, sourceFileID string) (*model.MobileShareRecord, error) {
	driveID = strings.TrimSpace(driveID)
	sourceFileID = strings.TrimSpace(sourceFileID)
	if driveID == "" || sourceFileID == "" {
		return nil, nil
	}
	var record model.MobileShareRecord
	err := db.Where(columnName("drive_id")+" = ? AND "+columnName("source_file_id")+" = ?", driveID, sourceFileID).First(&record).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &record, nil
}

func GetMobileShareRecordByID(id uint) (*model.MobileShareRecord, error) {
	var record model.MobileShareRecord
	if err := db.First(&record, id).Error; err != nil {
		return nil, errors.WithStack(err)
	}
	return &record, nil
}

func UpsertMobileShareRecord(record *model.MobileShareRecord) (*model.MobileShareRecord, error) {
	if record == nil {
		return nil, errors.New("mobile share record is nil")
	}
	if record.DriveID == "" || record.SourceFileID == "" {
		return nil, errors.New("drive_id and source_file_id are required")
	}
	err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "drive_id"}, {Name: "source_file_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"storage_id",
			"storage_mount_path",
			"source_path",
			"source_name",
			"source_type",
			"period_unit",
			"link_id",
			"share_url",
			"extract_code",
			"is_valid",
			"last_error",
			"updated_at",
		}),
	}).Create(record).Error
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return GetMobileShareRecordBySource(record.DriveID, record.SourceFileID)
}

func UpdateMobileShareRecord(record *model.MobileShareRecord) error {
	return errors.WithStack(db.Save(record).Error)
}

func ListMobileShareRecords(filter MobileShareRecordFilter) ([]model.MobileShareRecord, int64, error) {
	query := db.Model(&model.MobileShareRecord{})
	if keyword := strings.TrimSpace(filter.Keyword); keyword != "" {
		like := "%" + keyword + "%"
		query = query.Where(
			columnName("source_name")+" LIKE ? OR "+columnName("source_path")+" LIKE ? OR "+columnName("share_url")+" LIKE ? OR "+columnName("link_id")+" LIKE ?",
			like, like, like, like,
		)
	}
	if mountPath := strings.TrimSpace(filter.StorageMountPath); mountPath != "" {
		query = query.Where(columnName("storage_mount_path")+" = ?", mountPath)
	}
	if sourceType := strings.TrimSpace(filter.SourceType); sourceType != "" {
		query = query.Where(columnName("source_type")+" = ?", sourceType)
	}
	if filter.IsValid != nil {
		query = query.Where(columnName("is_valid")+" = ?", *filter.IsValid)
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
	var records []model.MobileShareRecord
	if err := query.Order(columnName("updated_at") + " DESC").Offset((page - 1) * perPage).Limit(perPage).Find(&records).Error; err != nil {
		return nil, 0, errors.WithStack(err)
	}
	return records, total, nil
}

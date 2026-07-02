package db

import (
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
)

type ETFArchiveRecordFilter struct {
	Keyword     string
	TMDBID      int64
	TMDBMatched *bool
	Page        int
	PerPage     int
}

func CreateETFArchiveRecord(record *model.ETFArchiveRecord) error {
	return errors.WithStack(db.Create(record).Error)
}

func UpdateETFArchiveRecord(record *model.ETFArchiveRecord) error {
	return errors.WithStack(db.Save(record).Error)
}

func GetETFArchiveRecordByID(id uint) (*model.ETFArchiveRecord, error) {
	var record model.ETFArchiveRecord
	if err := db.First(&record, id).Error; err != nil {
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

package db

import (
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SubscriptionFilter struct {
	Keyword    string
	SourceType string
	Active     *bool
	Page       int
	PerPage    int
}

type SubscriptionRunFilter struct {
	SubscriptionID uint
	Status         string
	Page           int
	PerPage        int
}

func CreateSubscription(item *model.Subscription) error {
	if item.LastStatus == "" {
		item.LastStatus = model.SubscriptionStatusIdle
	}
	return errors.WithStack(db.Create(item).Error)
}

func UpdateSubscription(item *model.Subscription) error {
	return errors.WithStack(db.Save(item).Error)
}

func DeleteSubscription(id uint) error {
	return errors.WithStack(db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where(columnName("subscription_id")+" = ?", id).Delete(&model.SubscriptionItem{}).Error; err != nil {
			return err
		}
		if err := tx.Where(columnName("subscription_id")+" = ?", id).Delete(&model.SubscriptionRun{}).Error; err != nil {
			return err
		}
		return tx.Delete(&model.Subscription{}, id).Error
	}))
}

func GetSubscriptionByID(id uint) (*model.Subscription, error) {
	var item model.Subscription
	if err := db.First(&item, id).Error; err != nil {
		return nil, errors.WithStack(err)
	}
	return &item, nil
}

func ListSubscriptions(filter SubscriptionFilter) ([]model.Subscription, int64, error) {
	query := db.Model(&model.Subscription{})
	if keyword := strings.TrimSpace(filter.Keyword); keyword != "" {
		like := "%" + keyword + "%"
		query = query.Where(
			columnName("name")+" LIKE ? OR "+columnName("tmdb_name")+" LIKE ? OR "+columnName("target_root")+" LIKE ?",
			like, like, like,
		)
	}
	if sourceType := strings.TrimSpace(filter.SourceType); sourceType != "" {
		query = query.Where(columnName("source_type")+" = ?", sourceType)
	}
	if filter.Active != nil {
		query = query.Where(columnName("active")+" = ?", *filter.Active)
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
	var items []model.Subscription
	if err := query.Order(columnName("updated_at") + " DESC").Offset((page - 1) * perPage).Limit(perPage).Find(&items).Error; err != nil {
		return nil, 0, errors.WithStack(err)
	}
	return items, total, nil
}

func ListActiveSubscriptions() ([]model.Subscription, error) {
	var items []model.Subscription
	err := db.Where(columnName("active")+" = ?", true).Find(&items).Error
	return items, errors.WithStack(err)
}

func UpsertSubscriptionItem(item *model.SubscriptionItem) (*model.SubscriptionItem, bool, error) {
	if item == nil {
		return nil, false, errors.New("subscription item is nil")
	}
	var existing model.SubscriptionItem
	err := db.Where(columnName("subscription_id")+" = ? AND "+columnName("source_key")+" = ?", item.SubscriptionID, item.SourceKey).First(&existing).Error
	isNew := false
	if errors.Is(err, gorm.ErrRecordNotFound) {
		isNew = true
	} else if err != nil {
		return nil, false, errors.WithStack(err)
	}
	if isNew {
		if item.Status == "" {
			item.Status = model.SubscriptionItemStatusPending
		}
	} else if existing.FileHash != "" && existing.FileHash != item.FileHash {
		item.Status = model.SubscriptionItemStatusPending
	} else {
		if item.Status == "" || item.Status == model.SubscriptionItemStatusPending {
			item.Status = existing.Status
			item.LastError = existing.LastError
		}
	}
	err = db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "subscription_id"}, {Name: "source_key"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"source_url",
			"source_path",
			"file_id",
			"file_path",
			"file_name",
			"file_size",
			"file_hash",
			"season",
			"episode",
			"target_dir",
			"target_name",
			"target_path",
			"status",
			"last_seen_at",
			"last_error",
			"updated_at",
		}),
	}).Create(item).Error
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	saved, err := GetSubscriptionItem(item.SubscriptionID, item.SourceKey)
	return saved, isNew, err
}

func GetSubscriptionItem(subscriptionID uint, sourceKey string) (*model.SubscriptionItem, error) {
	var item model.SubscriptionItem
	err := db.Where(columnName("subscription_id")+" = ? AND "+columnName("source_key")+" = ?", subscriptionID, sourceKey).First(&item).Error
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &item, nil
}

func ListSubscriptionItems(subscriptionID uint) ([]model.SubscriptionItem, error) {
	var items []model.SubscriptionItem
	err := db.Where(columnName("subscription_id")+" = ?", subscriptionID).
		Order(columnName("season") + ", " + columnName("episode") + ", " + columnName("file_path")).
		Find(&items).Error
	return items, errors.WithStack(err)
}

func CreateSubscriptionRun(run *model.SubscriptionRun) error {
	return errors.WithStack(db.Create(run).Error)
}

func UpdateSubscriptionRun(run *model.SubscriptionRun) error {
	return errors.WithStack(db.Save(run).Error)
}

func ListSubscriptionRuns(filter SubscriptionRunFilter) ([]model.SubscriptionRun, int64, error) {
	query := db.Model(&model.SubscriptionRun{})
	if filter.SubscriptionID > 0 {
		query = query.Where(columnName("subscription_id")+" = ?", filter.SubscriptionID)
	}
	if status := strings.TrimSpace(filter.Status); status != "" {
		query = query.Where(columnName("status")+" = ?", status)
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
	var items []model.SubscriptionRun
	err := query.Order(columnName("started_at") + " DESC").
		Offset((page - 1) * perPage).
		Limit(perPage).
		Find(&items).Error
	return items, total, errors.WithStack(err)
}

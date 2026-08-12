package db

import (
	"errors"

	log "github.com/sirupsen/logrus"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"gorm.io/gorm"
)

var db *gorm.DB

func Init(d *gorm.DB) {
	db = d
	if err := NormalizeSubscriptionStateVersion(db); err != nil {
		log.Fatalf("failed normalize subscription state version: %s", err.Error())
	}
	err := AutoMigrate(new(model.Storage), new(model.User), new(model.Meta), new(model.SettingItem), new(model.SearchNode), new(model.TaskItem), new(model.SSHPublicKey), new(model.SharingDB), new(model.ETFArchiveRecord), new(model.MobileShareRecord), new(model.ETFMediaRoot), new(model.ETFMediaRootBatch), new(model.ETFSubscriptionJob), new(model.Subscription), new(model.ExternalSubscriptionRequest), new(model.SubscriptionItem), new(model.SubscriptionEpisodeSource), new(model.SubscriptionRun), new(model.SubscriptionTelegramEvent), new(model.SubscriptionRealtimeCandidate), new(model.ClusterNode), new(model.ClusterNodeSession), new(model.ClusterNodeInventory), new(model.ClusterCoordinatorLease), new(model.ClusterSecret), new(model.ClusterNodeDesiredConfig), new(model.ClusterStorageProfile), new(model.ClusterControlAudit), new(model.ClusterWorkerObservedState), new(model.ClusterJob), new(model.ClusterJobAttempt), new(model.ClusterJobStage), new(model.ClusterUploadManifest), new(model.ClusterShareInspectManifest), new(model.ClusterOutbox), new(model.ClusterInbox))
	if err != nil {
		log.Fatalf("failed migrate database: %s", err.Error())
	}
	if err := NormalizeSubscriptionStateVersion(db); err != nil {
		log.Fatalf("failed normalize migrated subscription state version: %s", err.Error())
	}
}

// NormalizeSubscriptionStateVersion repairs databases created before the
// optimistic-concurrency column became non-null. Reconciliation treats a
// missing version as zero, but the durable schema must be repaired at startup
// so subsequent writes cannot keep producing false version conflicts.
func NormalizeSubscriptionStateVersion(database *gorm.DB) error {
	if database == nil {
		return errors.New("database is required")
	}
	statement := &gorm.Statement{DB: database}
	if err := statement.Parse(&model.SubscriptionItem{}); err != nil {
		return err
	}
	if statement.Schema == nil || !database.Migrator().HasTable(statement.Schema.Table) {
		return nil
	}
	if !database.Migrator().HasColumn(&model.SubscriptionItem{}, "state_version") {
		return nil
	}
	return database.Table(statement.Schema.Table).
		Where("state_version IS NULL").
		Update("state_version", uint64(0)).Error
}

func AutoMigrate(dst ...interface{}) error {
	var err error
	if conf.Conf.Database.Type == "mysql" {
		err = db.Set("gorm:table_options", "ENGINE=InnoDB CHARSET=utf8mb4").AutoMigrate(dst...)
	} else {
		err = db.AutoMigrate(dst...)
	}
	return err
}

func GetDb() *gorm.DB {
	return db
}

// UseConnection replaces the active GORM handle without running migrations.
// It is intended for bounded maintenance transactions that must reuse package
// services while guaranteeing that a dry-run cannot alter the schema.
func UseConnection(d *gorm.DB) {
	db = d
}

func Close() {
	log.Info("closing db")
	sqlDB, err := db.DB()
	if err != nil {
		log.Errorf("failed to get db: %s", err.Error())
		return
	}
	err = sqlDB.Close()
	if err != nil {
		log.Errorf("failed to close db: %s", err.Error())
		return
	}
}

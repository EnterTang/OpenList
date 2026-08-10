package db

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func TestMigrateSQLiteToPostgresCopiesAndValidatesSelectedTables(t *testing.T) {
	source, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "source.db")), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: "x_"},
	})
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	target, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "target.db")), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: "x_"},
	})
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	if err := source.AutoMigrate(&model.Subscription{}, &model.SubscriptionTelegramEvent{}); err != nil {
		t.Fatalf("migrate source: %v", err)
	}
	if err := target.AutoMigrate(&model.Subscription{}, &model.SubscriptionTelegramEvent{}); err != nil {
		t.Fatalf("migrate target: %v", err)
	}
	subscriptions := []model.Subscription{
		{Name: "Migration One", TMDBName: "Migration One"},
		{Name: "Migration Two", TMDBName: "Migration Two"},
	}
	if err := source.Create(&subscriptions).Error; err != nil {
		t.Fatalf("seed subscriptions: %v", err)
	}
	events := []model.SubscriptionTelegramEvent{
		{SubscriptionID: subscriptions[0].ID, Channel: "source", MessageID: "migration-1"},
		{SubscriptionID: subscriptions[1].ID, Channel: "source", MessageID: "migration-2"},
	}
	if err := source.Create(&events).Error; err != nil {
		t.Fatalf("seed events: %v", err)
	}

	report, err := MigrateSQLiteToPostgres(context.Background(), source, target, MigrationOptions{
		TablePrefix: "x_",
		TableNames:  []string{"x_subscriptions", "x_subscription_telegram_events"},
		BatchSize:   1,
		SampleSize:  10,
	})
	if err != nil {
		t.Fatalf("migrate selected tables: %v", err)
	}
	if len(report.Tables) != 2 || report.Tables[0].SourceRows == 0 || report.Tables[1].TargetRows != 2 {
		t.Fatalf("migration report = %#v", report)
	}
	if report.Tables[0].SourceSampleHash != report.Tables[0].TargetSampleHash || report.Tables[1].SourceSampleHash != report.Tables[1].TargetSampleHash {
		t.Fatalf("sample hashes differ: %#v", report.Tables)
	}

	second, err := MigrateSQLiteToPostgres(context.Background(), source, target, MigrationOptions{
		TablePrefix: "x_",
		TableNames:  []string{"x_subscriptions", "x_subscription_telegram_events"},
		BatchSize:   2,
		SampleSize:  10,
	})
	if err != nil {
		t.Fatalf("resume migration: %v", err)
	}
	if second.Tables[0].TargetRows != 2 || second.Tables[1].TargetRows != 2 {
		t.Fatalf("resume duplicated rows: %#v", second.Tables)
	}
}

func TestMigrationDryRunDoesNotWriteTarget(t *testing.T) {
	source, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "source.db")), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: "x_"},
	})
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	target, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "target.db")), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: "x_"},
	})
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	if err := source.AutoMigrate(&model.Subscription{}); err != nil {
		t.Fatalf("migrate source: %v", err)
	}
	if err := source.Create(&model.Subscription{Name: "Dry run", TMDBName: "Dry run"}).Error; err != nil {
		t.Fatalf("seed source: %v", err)
	}
	report, err := MigrateSQLiteToPostgres(context.Background(), source, target, MigrationOptions{
		TablePrefix: "x_",
		TableNames:  []string{"x_subscriptions"},
		DryRun:      true,
	})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if len(report.Tables) != 1 || report.Tables[0].SourceRows != 1 {
		t.Fatalf("dry-run report = %#v", report)
	}
	if target.Migrator().HasTable("x_subscriptions") {
		t.Fatal("dry run created the target table")
	}
}

func TestMigrationValidationReportsPayloadMismatch(t *testing.T) {
	source, target := openMigrationTestDatabases(t)
	if err := source.AutoMigrate(&model.Subscription{}); err != nil {
		t.Fatalf("migrate source: %v", err)
	}
	if err := target.AutoMigrate(&model.Subscription{}); err != nil {
		t.Fatalf("migrate target: %v", err)
	}
	if err := source.Create(&model.Subscription{Name: "Original", TMDBName: "Original"}).Error; err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if _, err := MigrateSQLiteToPostgres(context.Background(), source, target, MigrationOptions{
		TablePrefix: "x_",
		TableNames:  []string{"x_subscriptions"},
		SampleSize:  10,
	}); err != nil {
		t.Fatalf("initial migration: %v", err)
	}
	if err := target.Model(&model.Subscription{}).Where("id = ?", 1).Update("name", "Tampered").Error; err != nil {
		t.Fatalf("tamper target: %v", err)
	}
	_, err := MigrateSQLiteToPostgres(context.Background(), source, target, MigrationOptions{
		TablePrefix:  "x_",
		TableNames:   []string{"x_subscriptions"},
		SampleSize:   10,
		ValidateOnly: true,
	})
	if err == nil || !strings.Contains(err.Error(), "x_subscriptions") {
		t.Fatalf("validation error = %v, want table-specific mismatch", err)
	}
}

func TestMigrationTableSpecsIncludeOrdering(t *testing.T) {
	specs, err := migrationTableSpecs("x_")
	if err != nil {
		t.Fatalf("build migration table specs: %v", err)
	}
	if len(specs) < 20 {
		t.Fatalf("migration table specs = %d, want complete model inventory", len(specs))
	}
	for _, spec := range specs {
		if spec.Table == "" || len(spec.OrderColumns) == 0 || spec.NewModel == nil {
			t.Fatalf("incomplete migration spec = %#v", spec)
		}
	}
}

func openMigrationTestDatabases(t *testing.T) (*gorm.DB, *gorm.DB) {
	t.Helper()
	source, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "source.db")), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: "x_"},
	})
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	target, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "target.db")), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: "x_"},
	})
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	return source, target
}

package bootstrap

import (
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestOpenSQLiteAppliesProductionPragmas(t *testing.T) {
	database := openSQLiteTestDB(t, filepath.Join(t.TempDir(), "openlist bootstrap.db"))

	var journalMode string
	if err := database.Raw("PRAGMA journal_mode").Scan(&journalMode).Error; err != nil {
		t.Fatalf("read journal mode: %v", err)
	}
	if journalMode != "wal" {
		t.Errorf("journal mode = %q, want %q", journalMode, "wal")
	}

	var busyTimeout int
	if err := database.Raw("PRAGMA busy_timeout").Scan(&busyTimeout).Error; err != nil {
		t.Fatalf("read busy timeout: %v", err)
	}
	if busyTimeout != 5000 {
		t.Errorf("busy timeout = %d, want %d", busyTimeout, 5000)
	}
}

func TestOpenSQLitePreservesPlainTargetQuery(t *testing.T) {
	target := filepath.Join(t.TempDir(), "shared.db") + "?mode=memory&cache=shared"
	first := openSQLiteTestDB(t, target)
	second := openSQLiteTestDB(t, target)

	var journalMode string
	if err := first.Raw("PRAGMA journal_mode").Scan(&journalMode).Error; err != nil {
		t.Fatalf("read journal mode: %v", err)
	}
	if journalMode != "memory" {
		t.Errorf("journal mode = %q, want %q", journalMode, "memory")
	}

	if err := first.Exec("CREATE TABLE shared_values (value INTEGER)").Error; err != nil {
		t.Fatalf("create shared table: %v", err)
	}
	if err := first.Exec("INSERT INTO shared_values (value) VALUES (1)").Error; err != nil {
		t.Fatalf("insert shared value: %v", err)
	}
	var count int
	if err := second.Raw("SELECT COUNT(*) FROM shared_values").Scan(&count).Error; err != nil {
		t.Fatalf("read shared table through second connection: %v", err)
	}
	if count != 1 {
		t.Errorf("shared row count = %d, want %d", count, 1)
	}

	var busyTimeout int
	if err := second.Raw("PRAGMA busy_timeout").Scan(&busyTimeout).Error; err != nil {
		t.Fatalf("read busy timeout: %v", err)
	}
	if busyTimeout != 5000 {
		t.Errorf("busy timeout = %d, want %d", busyTimeout, 5000)
	}
}

func TestOpenSQLitePreservesHashInFilename(t *testing.T) {
	target := filepath.Join(t.TempDir(), "openlist#bootstrap.db")
	database := openSQLiteTestDB(t, target)

	if err := database.Exec("CREATE TABLE hash_path_probe (value INTEGER)").Error; err != nil {
		t.Fatalf("create table in hash-named database: %v", err)
	}
	var databases []struct {
		Name string
		File string
	}
	if err := database.Raw("PRAGMA database_list").Scan(&databases).Error; err != nil {
		t.Fatalf("read SQLite database list: %v", err)
	}
	for _, entry := range databases {
		if entry.Name == "main" {
			if got, want := filepath.Base(entry.File), filepath.Base(target); got != want {
				t.Errorf("database basename = %q, want %q", got, want)
			}
			return
		}
	}
	t.Fatal("main SQLite database not found")
}

func TestOpenSQLiteCanonicalizesUppercaseFileScheme(t *testing.T) {
	database := openSQLiteTestDB(t, "FILE:uppercase-query?mode=memory&cache=shared")

	var journalMode string
	if err := database.Raw("PRAGMA journal_mode").Scan(&journalMode).Error; err != nil {
		t.Fatalf("read journal mode: %v", err)
	}
	if journalMode != "memory" {
		t.Errorf("journal mode = %q, want %q", journalMode, "memory")
	}
}

func TestSQLiteDSNPreservesWindowsDrivePathAndHashes(t *testing.T) {
	dsn := sqliteDSN(`C:\OpenList\database#snapshot.db?mode=memory&label=alpha#beta`, url.Values{
		"_pragma": {"busy_timeout(5000)"},
	})
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse SQLite DSN: %v", err)
	}
	if parsed.Path != "/C:/OpenList/database#snapshot.db" {
		t.Errorf("path = %q, want %q", parsed.Path, "/C:/OpenList/database#snapshot.db")
	}
	if parsed.Query().Get("mode") != "memory" {
		t.Errorf("mode = %q, want %q", parsed.Query().Get("mode"), "memory")
	}
	if parsed.Query().Get("label") != "alpha#beta" {
		t.Errorf("label = %q, want %q", parsed.Query().Get("label"), "alpha#beta")
	}
	if parsed.Query().Get("_pragma") != "busy_timeout(5000)" {
		t.Errorf("driver option = %q, want %q", parsed.Query().Get("_pragma"), "busy_timeout(5000)")
	}
	if parsed.Fragment != "" {
		t.Errorf("fragment = %q, want empty", parsed.Fragment)
	}
}

func TestSQLiteDSNFoldsExplicitFileHashIntoPath(t *testing.T) {
	dsn := sqliteDSN("FILE:/tmp/database#snapshot.db?mode=memory&label=alpha#beta", url.Values{
		"_pragma": {"busy_timeout(5000)"},
	})
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse SQLite DSN: %v", err)
	}
	if parsed.Scheme != "file" {
		t.Errorf("scheme = %q, want %q", parsed.Scheme, "file")
	}
	if parsed.Path != "/tmp/database#snapshot.db" {
		t.Errorf("path = %q, want %q", parsed.Path, "/tmp/database#snapshot.db")
	}
	if parsed.Query().Get("mode") != "memory" {
		t.Errorf("mode = %q, want %q", parsed.Query().Get("mode"), "memory")
	}
	if parsed.Query().Get("label") != "alpha#beta" {
		t.Errorf("label = %q, want %q", parsed.Query().Get("label"), "alpha#beta")
	}
	if parsed.Query().Get("_pragma") != "busy_timeout(5000)" {
		t.Errorf("driver option = %q, want %q", parsed.Query().Get("_pragma"), "busy_timeout(5000)")
	}
	if parsed.Fragment != "" {
		t.Errorf("fragment = %q, want empty", parsed.Fragment)
	}
}

func TestOpenSQLiteStartsTransactionsWithImmediateLock(t *testing.T) {
	database := openSQLiteTestDB(t, filepath.Join(t.TempDir(), "immediate-lock.db"))
	type counter struct {
		ID    uint `gorm:"primaryKey"`
		Value int
	}
	if err := database.AutoMigrate(&counter{}); err != nil {
		t.Fatalf("migrate counter: %v", err)
	}
	if err := database.Create(&counter{ID: 1}).Error; err != nil {
		t.Fatalf("create counter: %v", err)
	}

	transactionStarted := make(chan struct{})
	releaseTransaction := make(chan struct{})
	transactionDone := make(chan error, 1)
	go func() {
		transactionDone <- database.Transaction(func(tx *gorm.DB) error {
			var item counter
			if err := tx.First(&item, 1).Error; err != nil {
				return err
			}
			close(transactionStarted)
			<-releaseTransaction
			return tx.Model(&item).Update("value", gorm.Expr("value + 1")).Error
		})
	}()

	select {
	case <-transactionStarted:
	case <-time.After(time.Second):
		t.Fatal("transaction did not start")
	}

	competingWriterDone := make(chan error, 1)
	competingWriterLaunched := make(chan struct{})
	go func() {
		close(competingWriterLaunched)
		competingWriterDone <- database.Model(&counter{}).Where("id = ?", 1).
			Update("value", gorm.Expr("value + 1")).Error
	}()
	<-competingWriterLaunched

	select {
	case err := <-competingWriterDone:
		close(releaseTransaction)
		<-transactionDone
		t.Fatalf("competing writer completed before transaction release: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseTransaction)
	if err := <-transactionDone; err != nil {
		t.Fatalf("complete first transaction: %v", err)
	}
	if err := <-competingWriterDone; err != nil {
		t.Fatalf("complete competing writer: %v", err)
	}

	var item counter
	if err := database.First(&item, 1).Error; err != nil {
		t.Fatalf("read counter: %v", err)
	}
	if item.Value != 2 {
		t.Fatalf("counter value = %d, want 2", item.Value)
	}
}

func openSQLiteTestDB(t *testing.T, target string) *gorm.DB {
	t.Helper()
	database, err := gorm.Open(openSQLite(target), &gorm.Config{})
	if err != nil {
		t.Fatalf("open SQLite database: %v", err)
	}
	sqlDB, err := database.DB()
	if err != nil {
		t.Fatalf("get SQLite database handle: %v", err)
	}
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			t.Errorf("close SQLite database: %v", err)
		}
	})
	return database
}

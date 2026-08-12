package bootstrap

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/cmd/flags"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"gorm.io/gorm"
)

func TestOpenSQLiteConfiguresWALAndBusyTimeout(t *testing.T) {
	previousConf := conf.Conf
	previousDev := flags.Dev
	t.Cleanup(func() {
		conf.Conf = previousConf
		flags.Dev = previousDev
	})

	flags.Dev = false
	conf.Conf = &conf.Config{
		Database: conf.Database{
			Type:   "sqlite3",
			DBFile: filepath.Join(t.TempDir(), "openlist.db"),
		},
	}

	InitDB()
	t.Cleanup(db.Close)

	var journalMode string
	if err := db.GetDb().Raw("PRAGMA journal_mode").Scan(&journalMode).Error; err != nil {
		t.Fatalf("query journal mode: %v", err)
	}
	if journalMode != "wal" {
		t.Fatalf("journal mode = %q, want %q", journalMode, "wal")
	}

	var busyTimeout int
	if err := db.GetDb().Raw("PRAGMA busy_timeout").Scan(&busyTimeout).Error; err != nil {
		t.Fatalf("query busy timeout: %v", err)
	}
	if busyTimeout != 5000 {
		t.Fatalf("busy timeout = %d, want %d", busyTimeout, 5000)
	}
}

func TestOpenSQLiteUsesImmediateTransactions(t *testing.T) {
	previousConf := conf.Conf
	previousDev := flags.Dev
	t.Cleanup(func() {
		conf.Conf = previousConf
		flags.Dev = previousDev
	})

	flags.Dev = false
	conf.Conf = &conf.Config{
		Database: conf.Database{
			Type:   "sqlite3",
			DBFile: filepath.Join(t.TempDir(), "openlist.db"),
		},
	}

	InitDB()
	t.Cleanup(db.Close)
	database := db.GetDb()
	sqlDB, err := database.DB()
	if err != nil {
		t.Fatalf("get sql database: %v", err)
	}
	sqlDB.SetMaxOpenConns(2)
	if err := database.Exec("CREATE TABLE txlock_probe (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)").Error; err != nil {
		t.Fatalf("create transaction probe: %v", err)
	}
	if err := database.Exec("INSERT INTO txlock_probe (id, value) VALUES (1, 1)").Error; err != nil {
		t.Fatalf("seed transaction probe: %v", err)
	}

	writerDone := make(chan error, 1)
	writerFinishedInsideTransaction := false
	err = database.Transaction(func(tx *gorm.DB) error {
		var value int
		if err := tx.Raw("SELECT value FROM txlock_probe WHERE id = 1").Scan(&value).Error; err != nil {
			return err
		}
		go func() {
			writerDone <- database.Exec("UPDATE txlock_probe SET value = value + 1 WHERE id = 1").Error
		}()
		select {
		case err := <-writerDone:
			writerFinishedInsideTransaction = true
			if err != nil {
				return err
			}
		case <-time.After(200 * time.Millisecond):
		}
		return tx.Exec("UPDATE txlock_probe SET value = value + 1 WHERE id = 1").Error
	})
	if err != nil {
		t.Fatalf("write after concurrent transaction: %v", err)
	}
	if !writerFinishedInsideTransaction {
		select {
		case err := <-writerDone:
			if err != nil {
				t.Fatalf("concurrent writer: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent writer did not resume after transaction commit")
		}
	}

	var value int
	if err := database.Raw("SELECT value FROM txlock_probe WHERE id = 1").Scan(&value).Error; err != nil {
		t.Fatalf("read transaction probe: %v", err)
	}
	if value != 3 {
		t.Fatalf("transaction probe value = %d, want 3", value)
	}
}

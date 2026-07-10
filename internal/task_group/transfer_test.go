package task_group

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/OpenListTeam/OpenList/v4/drivers/local"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestVerifyAndRemoveAcceptsGeneratedETFAsDestination(t *testing.T) {
	setupTaskGroupTestDB(t)

	srcRoot := filepath.Join(t.TempDir(), "src")
	dstRoot := filepath.Join(t.TempDir(), "dst")
	if err := os.MkdirAll(srcRoot, 0o755); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dstRoot, "Season 2"), 0o755); err != nil {
		t.Fatalf("mkdir dst: %v", err)
	}
	srcFile := filepath.Join(srcRoot, "Movie.mkv")
	if err := os.WriteFile(srcFile, []byte("source"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dstRoot, "Season 2", "Movie.mkv.etf"), []byte("etf"), 0o644); err != nil {
		t.Fatalf("write etf: %v", err)
	}

	srcMount := "/" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()) + "_src"
	dstMount := "/" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()) + "_dst"
	if _, err := op.CreateStorage(context.Background(), model.Storage{
		Driver:    "Local",
		MountPath: srcMount,
		Addition:  fmt.Sprintf(`{"root_folder_path":%q}`, srcRoot),
	}); err != nil {
		t.Fatalf("create src storage: %v", err)
	}
	if _, err := op.CreateStorage(context.Background(), model.Storage{
		Driver:    "Local",
		MountPath: dstMount,
		Addition:  fmt.Sprintf(`{"root_folder_path":%q}`, dstRoot),
	}); err != nil {
		t.Fatalf("create dst storage: %v", err)
	}

	srcStorage, srcActualPath, err := op.GetStorageAndActualPath(srcMount + "/Movie.mkv")
	if err != nil {
		t.Fatalf("get src storage: %v", err)
	}
	dstStorage, dstActualPath, err := op.GetStorageAndActualPath(dstMount + "/Season 2")
	if err != nil {
		t.Fatalf("get dst storage: %v", err)
	}
	if err := verifyAndRemove(context.Background(), srcStorage, dstStorage, srcActualPath, dstActualPath); err != nil {
		t.Fatalf("verify and remove: %v", err)
	}
	if _, err := os.Stat(srcFile); !os.IsNotExist(err) {
		t.Fatalf("source file still exists or stat failed with unexpected error: %v", err)
	}
}

func setupTaskGroupTestDB(t *testing.T) {
	t.Helper()
	dsn := "file:" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()) + "?mode=memory&cache=shared"
	database, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	conf.Conf = conf.DefaultConfig("data")
	db.Init(database)
	op.SettingCacheUpdate()
	t.Cleanup(func() {
		op.SettingCacheUpdate()
		sqlDB, err := database.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})
}

package mobileshare

import (
	"context"
	"fmt"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

type fakeCreator struct {
	link  *model.MobileShareLink
	calls int
}

func (f *fakeCreator) CreateMobileShare(ctx context.Context, obj model.Obj, args model.MobileShareCreateArgs) (*model.MobileShareLink, error) {
	f.calls++
	return f.link, nil
}

func setupMobileShareServiceDB(t *testing.T) {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	database, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	conf.Conf = conf.DefaultConfig("data")
	db.Init(database)
	t.Cleanup(func() {
		sqlDB, err := database.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})
}

func TestCreateOrReuseShareReturnsExistingValidRecord(t *testing.T) {
	setupMobileShareServiceDB(t)
	existing, err := db.UpsertMobileShareRecord(&model.MobileShareRecord{
		StorageID:        1,
		StorageMountPath: "/139_60t",
		DriveID:          "/139_60t",
		SourceFileID:     "folder-media-root",
		SourcePath:       "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		SourceName:       "婚姻攻略 (2024) {tmdb-260868}",
		SourceType:       "folder",
		PeriodUnit:       1,
		LinkID:           "old",
		ShareURL:         "https://yun.139.com/w/i/old",
		IsValid:          true,
	})
	if err != nil {
		t.Fatalf("insert existing share: %v", err)
	}
	creator := &fakeCreator{link: &model.MobileShareLink{LinkID: "new", ShareURL: "https://yun.139.com/w/i/new"}}

	result, err := CreateOrReuseShare(context.Background(), creator, ShareRequest{
		StorageID:        1,
		StorageMountPath: "/139_60t",
		Object:           &model.Object{ID: "folder-media-root", Name: "婚姻攻略 (2024) {tmdb-260868}", IsFolder: true},
		SourcePath:       "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		PeriodUnit:       1,
	})
	if err != nil {
		t.Fatalf("create or reuse share: %v", err)
	}
	if creator.calls != 0 {
		t.Fatalf("creator calls = %d, want 0", creator.calls)
	}
	if result.Record.ID != existing.ID || !result.Existing || result.Created {
		t.Fatalf("result = %#v, want existing record %d", result, existing.ID)
	}
}

func TestCreateOrReuseShareCreatesAndPersistsFolderShare(t *testing.T) {
	setupMobileShareServiceDB(t)
	creator := &fakeCreator{link: &model.MobileShareLink{LinkID: "new", ShareURL: "https://yun.139.com/w/i/new", ExtractCode: "abcd"}}

	result, err := CreateOrReuseShare(context.Background(), creator, ShareRequest{
		StorageID:        1,
		StorageMountPath: "/139_60t",
		Object:           &model.Object{ID: "folder-media-root", Name: "婚姻攻略 (2024) {tmdb-260868}", IsFolder: true},
		SourcePath:       "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}",
		PeriodUnit:       3,
	})
	if err != nil {
		t.Fatalf("create or reuse share: %v", err)
	}
	if creator.calls != 1 {
		t.Fatalf("creator calls = %d, want 1", creator.calls)
	}
	if result.Record.ShareURL != "https://yun.139.com/w/i/new" || result.Record.ExtractCode != "abcd" || result.Record.SourceType != "folder" || result.Record.PeriodUnit != 3 {
		t.Fatalf("record = %#v, want persisted new folder share", result.Record)
	}
}

func TestCreateOrReuseSharePersistsRenamedSourceMetadata(t *testing.T) {
	setupMobileShareServiceDB(t)
	creator := &fakeCreator{link: &model.MobileShareLink{
		LinkID:      "new",
		ShareURL:    "https://yun.139.com/w/i/new",
		ExtractCode: "abcd",
		SourcePath:  "/139_60t/ETF管理/tv/国产剧/Guilt (2026) {tmdb-1}",
		SourceName:  "Guilt (2026) {tmdb-1}",
	}}

	result, err := CreateOrReuseShare(context.Background(), creator, ShareRequest{
		StorageID:        1,
		StorageMountPath: "/139_60t",
		Object:           &model.Object{ID: "folder-media-root", Name: "非分之罪 (2026) {tmdb-1}", IsFolder: true},
		SourcePath:       "/139_60t/ETF管理/tv/国产剧/非分之罪 (2026) {tmdb-1}",
		PeriodUnit:       1,
	})
	if err != nil {
		t.Fatalf("create or reuse share: %v", err)
	}
	if result.Record.SourcePath != "/139_60t/ETF管理/tv/国产剧/Guilt (2026) {tmdb-1}" {
		t.Fatalf("record.SourcePath = %q, want renamed path", result.Record.SourcePath)
	}
	if result.Record.SourceName != "Guilt (2026) {tmdb-1}" {
		t.Fatalf("record.SourceName = %q, want renamed name", result.Record.SourceName)
	}
}

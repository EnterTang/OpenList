package db

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func setupETFArchiveDB(t *testing.T) {
	t.Helper()
	database, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	conf.Conf = conf.DefaultConfig("data")
	Init(database)
	t.Cleanup(func() {
		sqlDB, err := database.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})
}

func TestListETFArchiveRecordsFilters(t *testing.T) {
	setupETFArchiveDB(t)
	records := []*model.ETFArchiveRecord{
		{
			SourceName:       "婚姻攻略.S01E01.mp4",
			StorageMountPath: "/139_60t",
			LocalETFPath:     "/139_60t/转存中转/婚姻攻略.S01E01.mp4.etf",
			ArchiveETFPath:   "/139_60t/ETF管理/tv/国产剧/婚姻攻略 (2024) {tmdb-260868}/Season 01/婚姻攻略.S01E01.mp4.etf",
			TMDBMatched:      true,
			TMDBID:           260868,
			TMDBName:         "婚姻攻略",
			Status:           model.ETFArchiveStatusArchived,
		},
		{
			SourceName:       "Unknown.Movie.mp4",
			StorageMountPath: "/139_60t",
			LocalETFPath:     "/139_60t/转存中转/Unknown.Movie.mp4.etf",
			TMDBMatched:      false,
			Status:           model.ETFArchiveStatusFailed,
		},
		{
			SourceName:       "Other.Show.S01E01.mp4",
			StorageMountPath: "/139_60t",
			LocalETFPath:     "/139_60t/转存中转/Other.Show.S01E01.mp4.etf",
			TMDBMatched:      true,
			TMDBID:           123,
			TMDBName:         "Other Show",
			Status:           model.ETFArchiveStatusArchived,
		},
	}
	for _, record := range records {
		if err := CreateETFArchiveRecord(record); err != nil {
			t.Fatalf("create record: %v", err)
		}
	}

	matched := true
	got, total, err := ListETFArchiveRecords(ETFArchiveRecordFilter{TMDBMatched: &matched, Page: 1, PerPage: 20})
	if err != nil {
		t.Fatalf("list matched records: %v", err)
	}
	if total != 2 || len(got) != 2 {
		t.Fatalf("matched total/len = %d/%d, want 2/2", total, len(got))
	}

	got, total, err = ListETFArchiveRecords(ETFArchiveRecordFilter{Keyword: "婚姻", Page: 1, PerPage: 20})
	if err != nil {
		t.Fatalf("list keyword records: %v", err)
	}
	if total != 1 || len(got) != 1 || got[0].TMDBID != 260868 {
		t.Fatalf("keyword result = total %d records %#v, want tmdb 260868", total, got)
	}

	got, total, err = ListETFArchiveRecords(ETFArchiveRecordFilter{TMDBID: 123, Page: 1, PerPage: 20})
	if err != nil {
		t.Fatalf("list tmdb records: %v", err)
	}
	if total != 1 || len(got) != 1 || got[0].SourceName != "Other.Show.S01E01.mp4" {
		t.Fatalf("tmdb result = total %d records %#v, want Other.Show.S01E01.mp4", total, got)
	}
}

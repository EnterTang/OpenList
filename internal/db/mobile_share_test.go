package db

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestUpsertMobileShareRecordKeepsUniqueSource(t *testing.T) {
	setupETFArchiveDB(t)

	first, err := UpsertMobileShareRecord(&model.MobileShareRecord{
		StorageMountPath: "/139_60t",
		DriveID:          "/139_60t",
		SourceFileID:     "source-id",
		SourcePath:       "/139_60t/Movie.mkv",
		SourceName:       "Movie.mkv",
		SourceType:       "file",
		PeriodUnit:       1,
		LinkID:           "link-1",
		ShareURL:         "https://share.example/1",
		ExtractCode:      "1111",
		IsValid:          true,
	})
	if err != nil {
		t.Fatalf("upsert first mobile share: %v", err)
	}

	second, err := UpsertMobileShareRecord(&model.MobileShareRecord{
		StorageMountPath: "/139_60t",
		DriveID:          "/139_60t",
		SourceFileID:     "source-id",
		SourcePath:       "/139_60t/Movie-renamed.mkv",
		SourceName:       "Movie-renamed.mkv",
		SourceType:       "file",
		PeriodUnit:       1,
		LinkID:           "link-2",
		ShareURL:         "https://share.example/2",
		ExtractCode:      "2222",
		IsValid:          false,
		LastError:        "expired",
	})
	if err != nil {
		t.Fatalf("upsert second mobile share: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("record id changed: first=%d second=%d", first.ID, second.ID)
	}
	if second.LinkID != "link-2" || second.SourceName != "Movie-renamed.mkv" || second.IsValid {
		t.Fatalf("second record not updated: %#v", second)
	}

	valid := false
	records, total, err := ListMobileShareRecords(MobileShareRecordFilter{
		Keyword: "renamed",
		IsValid: &valid,
		Page:    1,
		PerPage: 20,
	})
	if err != nil {
		t.Fatalf("list mobile shares: %v", err)
	}
	if total != 1 || len(records) != 1 || records[0].LinkID != "link-2" {
		t.Fatalf("filtered records = total %d %#v, want link-2", total, records)
	}
}

package plugin

import (
	"testing"
)

func TestApplyUploadNameAndExpectedSize(t *testing.T) {
	opts := ProcessOptions{AntiHash: true, ISORename: true, Whitelist: "mkv,mp4"}
	if got := ApplyUploadName("Show.S01E01.mkv", opts); got != "Show.S01E01.mkv.iso" {
		t.Fatalf("ApplyUploadName = %q", got)
	}
	if got := ApplyUploadName("Show.S01E01.mkv.iso", opts); got != "Show.S01E01.mkv.iso" {
		t.Fatalf("already iso ApplyUploadName = %q", got)
	}
	if got := ApplyUploadName("notes.txt", opts); got != "notes.txt" {
		t.Fatalf("non-whitelist ApplyUploadName = %q", got)
	}
	if got := ExpectedUploadSize(100, "a.mkv", opts); got != 100+TrailerSize {
		t.Fatalf("ExpectedUploadSize = %d", got)
	}
	if got := ExpectedUploadSize(100, "a.txt", opts); got != 100 {
		t.Fatalf("ExpectedUploadSize skip = %d", got)
	}
	if !ShouldProcessUpload("a.mkv", opts) {
		t.Fatal("mkv should process")
	}
	if ShouldProcessUpload("a.txt", opts) {
		t.Fatal("txt should not process")
	}
}

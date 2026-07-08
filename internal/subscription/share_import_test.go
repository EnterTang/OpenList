package subscription

import (
	"strings"
	"testing"
)

func TestParsePan123CommonPathFastLinkSupportsEmptyCommonPath(t *testing.T) {
	raw := "123FLCPV2$%69Y8N4KosSpjpcVCReGVzy#3531063629#达顿牧场 (2026) {tmdbid-299167}/Season 1/达顿牧场.S01E02.2026.1080p.Amazon Prime.WEB-DL.H.264.DDP 5.1-Ocat.mkv"
	files, issues, err := parsePan123CommonPathFastLink(raw)
	if err != nil {
		t.Fatalf("parse common-path fastlink: %v", err)
	}
	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want none", issues)
	}
	if len(files) != 1 {
		t.Fatalf("len(files) = %d, want 1", len(files))
	}
	if files[0].Path != "达顿牧场 (2026) {tmdbid-299167}/Season 1/达顿牧场.S01E02.2026.1080p.Amazon Prime.WEB-DL.H.264.DDP 5.1-Ocat.mkv" {
		t.Fatalf("path = %q", files[0].Path)
	}
	if len(files[0].Etag) != 32 {
		t.Fatalf("etag = %q, want 32-char hex", files[0].Etag)
	}
}

func TestParsePan123CommonPathFastLinkJoinsNestedCommonPath(t *testing.T) {
	raw := "123FLCPV2$4k普码电影/CCTV-6绝版蓝光高清电影大绝版18部/15-17、《大决战》三部曲顶级版/%4zGrzyjXOlXRd5rDXr9r3v#11973776568#1、《大决战》三部曲CCTV6顶级源码/3、《大决战》平津战役.CCTV6顶级.mkv"
	files, issues, err := parsePan123CommonPathFastLink(raw)
	if err != nil {
		t.Fatalf("parse nested common path: %v", err)
	}
	if len(issues) != 0 || len(files) != 1 {
		t.Fatalf("files/issues = %#v %#v", files, issues)
	}
	want := "4k普码电影/CCTV-6绝版蓝光高清电影大绝版18部/15-17、《大决战》三部曲顶级版/1、《大决战》三部曲CCTV6顶级源码/3、《大决战》平津战役.CCTV6顶级.mkv"
	if files[0].Path != want {
		t.Fatalf("path = %q, want %q", files[0].Path, want)
	}
}

func TestParsePan123FastLinkJSONSupportsOfficialAndLooseShapes(t *testing.T) {
	cases := []string{
		`{"commonPath":"Season 1/","usesBase62EtagsInExport":true,"files":[{"etag":"69Y8N4KosSpjpcVCReGVzy","size":3531063629,"path":"Episode 02.mkv"}]}`,
		`{"files":[{"etag":"bc18e4ea5fb89ec5778d1f38c9772f5f","size":"1024","path":"Movie.mkv"}]}`,
		`[{"etag":"bc18e4ea5fb89ec5778d1f38c9772f5f","size":1024,"path":"Movie.mkv"}]`,
	}
	for _, raw := range cases {
		files, issues, err := parsePan123FastLinkJSON(raw)
		if err != nil {
			t.Fatalf("parse JSON %s: %v", raw, err)
		}
		if len(issues) != 0 || len(files) != 1 {
			t.Fatalf("files/issues = %#v %#v", files, issues)
		}
		if len(files[0].Etag) != 32 || files[0].Name == "" {
			t.Fatalf("file = %#v", files[0])
		}
	}
}

func TestParseManualImportTextRejectsTraversalAndCollectsIssues(t *testing.T) {
	raw := strings.Join([]string{
		"123FSLinkV2$bc18e4ea5fb89ec5778d1f38c9772f5f#1024#Movie.mkv",
		"123FLCPV2$root/%bc18e4ea5fb89ec5778d1f38c9772f5f#2048#../escape.mkv",
	}, "\n")
	files, issues, err := parseManualImportText(raw)
	if err != nil {
		t.Fatalf("parse mixed imports: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("len(files) = %d, want 1", len(files))
	}
	if len(issues) != 1 || !strings.Contains(issues[0].Reason, "path") {
		t.Fatalf("issues = %#v, want path issue", issues)
	}
}

func TestParseManualImportTextSupportsWholeJSONDocument(t *testing.T) {
	raw := `{"commonPath":"Season 1/","usesBase62EtagsInExport":true,"files":[{"etag":"69Y8N4KosSpjpcVCReGVzy","size":3531063629,"path":"Episode 02.mkv"}]}`
	files, issues, err := parseManualImportText(raw)
	if err != nil {
		t.Fatalf("parse full JSON import: %v", err)
	}
	if len(issues) != 0 || len(files) != 1 {
		t.Fatalf("files/issues = %#v %#v", files, issues)
	}
	if files[0].Path != "Season 1/Episode 02.mkv" {
		t.Fatalf("path = %q, want Season 1/Episode 02.mkv", files[0].Path)
	}
}

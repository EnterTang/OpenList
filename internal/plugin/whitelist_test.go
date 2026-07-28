package plugin

import (
	"testing"
)

func TestParseWhitelistAndMatch(t *testing.T) {
	wl := ParseWhitelist(" mkv, MP4 ,,avi ")
	if ExtensionAllowed("a.mkv", wl) != true {
		t.Fatal("mkv should match")
	}
	if ExtensionAllowed("a.MP4", wl) != true {
		t.Fatal("MP4 should match case-insensitively")
	}
	if ExtensionAllowed("a.txt", wl) != false {
		t.Fatal("txt should not match")
	}
	if ExtensionAllowed("a.mkv", ParseWhitelist("")) != false {
		t.Fatal("empty whitelist must deny all")
	}
}

func TestIsTempIncompleteName(t *testing.T) {
	for _, name := range []string{"a.mkv.tmp", "a.part", "a.aria2", "a.!qB", "a.crdownload"} {
		if !IsTempIncompleteName(name) {
			t.Fatalf("%s should be temp", name)
		}
	}
	if IsTempIncompleteName("a.mkv") {
		t.Fatal("a.mkv should not be temp")
	}
}

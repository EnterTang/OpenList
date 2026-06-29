package etfmeta

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestEncodeDecodeNormalizesSHA256(t *testing.T) {
	info := Info{Name: "Movie.mkv", Size: 2048, SHA256: strings.Repeat("a", 64)}

	encoded, err := Encode(&info)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}

	if decoded.SHA256 != strings.Repeat("A", 64) {
		t.Fatalf("SHA256 = %q, want uppercase A hash", decoded.SHA256)
	}
	if decoded.Name != info.Name || decoded.Size != info.Size {
		t.Fatalf("decoded info = %+v, want name and size from input", decoded)
	}
}

func TestDecodeRejectsInvalidSHA256(t *testing.T) {
	payload := `{"name":"Movie.mkv","size":2048,"sha256":"not-a-sha"}`
	line := base64.StdEncoding.EncodeToString([]byte(payload))

	if _, err := Decode([]byte(line)); err == nil {
		t.Fatal("Decode returned nil error for invalid SHA256")
	}
}

func TestFileNameAndResolveRestoreName(t *testing.T) {
	if got := FileName("Movie.mkv"); got != "Movie.mkv.etf" {
		t.Fatalf("FileName = %q, want Movie.mkv.etf", got)
	}
	if !IsName("Movie.mkv.etf") {
		t.Fatal("IsName returned false for .etf file")
	}
	if IsName("Movie.mkv") {
		t.Fatal("IsName returned true for non-.etf file")
	}

	name, err := ResolveRestoreName("Ignored.etf", &Info{Name: "Movie.mkv", Size: 2048, SHA256: strings.Repeat("A", 64)})
	if err != nil {
		t.Fatalf("ResolveRestoreName returned error: %v", err)
	}
	if name != "Movie.mkv" {
		t.Fatalf("restore name = %q, want Movie.mkv", name)
	}
}

func TestDecodeFileUsesFirstValidEntry(t *testing.T) {
	first, err := Encode(&Info{Name: "Movie.mkv", Size: 2048, SHA256: strings.Repeat("a", 64)})
	if err != nil {
		t.Fatalf("Encode first: %v", err)
	}
	second, err := Encode(&Info{Name: "Other.mkv", Size: 4096, SHA256: strings.Repeat("b", 64)})
	if err != nil {
		t.Fatalf("Encode second: %v", err)
	}
	data := append(first, '\n')
	data = append(data, second...)

	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}
	if decoded.Name != "Movie.mkv" {
		t.Fatalf("decoded name = %q, want first valid entry", decoded.Name)
	}
}

package worker

import "testing"

func TestProviderNameTreats115CD2As115(t *testing.T) {
	if got := providerName("115 CD2"); got != "pan115" {
		t.Fatalf("providerName(115 CD2) = %q, want pan115", got)
	}
}

func TestNormalizeControlKeyTreats115CD2As115(t *testing.T) {
	if got := normalizeControlKey("115 CD2"); got != "pan115" {
		t.Fatalf("normalizeControlKey(115 CD2) = %q, want pan115", got)
	}
}

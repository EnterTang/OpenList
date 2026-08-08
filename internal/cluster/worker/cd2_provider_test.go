package worker

import "testing"

func TestProviderNameTreats115CD2As115(t *testing.T) {
	if got := providerName("115 CD2"); got != "pan115" {
		t.Fatalf("providerName(115 CD2) = %q, want pan115", got)
	}
}

func TestProviderNameTreats115SYAliasesAs115(t *testing.T) {
	for _, driver := range []string{"115_sy", "115 sy"} {
		t.Run(driver, func(t *testing.T) {
			if got := providerName(driver); got != "pan115" {
				t.Fatalf("providerName(%s) = %q, want pan115", driver, got)
			}
		})
	}
}

func TestNormalizeControlKeyTreats115CD2As115(t *testing.T) {
	if got := normalizeControlKey("115 CD2"); got != "pan115" {
		t.Fatalf("normalizeControlKey(115 CD2) = %q, want pan115", got)
	}
}

func TestNormalizeControlKeyTreats115SYAliasesAs115(t *testing.T) {
	for _, provider := range []string{"115_sy", "115 sy"} {
		t.Run(provider, func(t *testing.T) {
			if got := normalizeControlKey(provider); got != "pan115" {
				t.Fatalf("normalizeControlKey(%s) = %q, want pan115", provider, got)
			}
		})
	}
}

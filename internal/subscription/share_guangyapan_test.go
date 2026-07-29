package subscription

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestParseGuangYaPanShareURL(t *testing.T) {
	t.Parallel()

	ref, err := ParseShareURL("https://www.guangyapan.com/s/1908913489489252407_adyLT48EdLN_2AYh?code=abcd")
	if err != nil {
		t.Fatalf("ParseShareURL: %v", err)
	}
	if ref.Provider != ShareProviderGuangYaPan {
		t.Fatalf("provider = %s", ref.Provider)
	}
	if ref.ShareID != "1908913489489252407_adyLT48EdLN_2AYh" {
		t.Fatalf("shareID = %s", ref.ShareID)
	}
	if ref.Passcode != "abcd" {
		t.Fatalf("passcode = %s", ref.Passcode)
	}
}

func TestNormalizeTransferPriorityIncludesGuangYaPan(t *testing.T) {
	t.Parallel()

	got := normalizeTransferPriority([]string{"guangyapan"})
	if len(got) == 0 || got[0] != "guangyapan" {
		t.Fatalf("priority = %#v", got)
	}
	found := false
	for _, name := range got {
		if name == "guangyapan" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("guangyapan missing from %#v", got)
	}
}

func TestGuangYaPanConfigWithStorageFallbackPrefersStorageToken(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	if err := db.CreateStorage(&model.Storage{
		MountPath: "/guangya",
		Driver:    "GuangYaPan",
		Addition:  `{"access_token":" live-guangya-access ","refresh_token":" live-guangya-refresh "}`,
		Status:    "work",
	}); err != nil {
		t.Fatalf("create guangyapan storage: %v", err)
	}

	cfg := guangyapanConfigWithStorageFallback(model.SubscriptionTelegramPanConfig{
		AccessToken:  "stale-manual-access",
		RefreshToken: "stale-manual-refresh",
	})
	if got, want := cfg.AccessToken, "live-guangya-access"; got != want {
		t.Fatalf("access token = %q, want %q", got, want)
	}
	if got, want := cfg.RefreshToken, "live-guangya-refresh"; got != want {
		t.Fatalf("refresh token = %q, want %q", got, want)
	}

	accessConfigured, refreshConfigured := GuangYaPanStorageCredentialsConfigured()
	if !accessConfigured || !refreshConfigured {
		t.Fatalf("storage credentials configured = (%v, %v), want true/true", accessConfigured, refreshConfigured)
	}
}

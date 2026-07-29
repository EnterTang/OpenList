package subscription

import "testing"

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

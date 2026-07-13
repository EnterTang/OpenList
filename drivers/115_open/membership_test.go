package _115_open

import "testing"

func TestNormalize115OpenMembershipTier(t *testing.T) {
	for input, want := range map[string]string{
		"超级会员":  "svip",
		"VIP会员": "vip",
		"":      "",
	} {
		if got := normalize115OpenMembershipTier(input); got != want {
			t.Fatalf("normalize tier %q = %q, want %q", input, got, want)
		}
	}
}

func TestClusterMembershipTierUsesRuntimeUserInfoUnlessConfigured(t *testing.T) {
	driver := &Open115{runtimeMembershipTier: "vip"}
	if got := driver.ClusterMembershipTier(); got != "vip" {
		t.Fatalf("runtime tier = %q, want vip", got)
	}
	driver.Addition.MembershipTier = "svip"
	if got := driver.ClusterMembershipTier(); got != "svip" {
		t.Fatalf("configured tier = %q, want svip", got)
	}
}

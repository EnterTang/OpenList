package _115

import "testing"

func TestClusterMembershipTierUsesRuntimeUserInfoUnlessConfigured(t *testing.T) {
	driver := &Pan115{runtimeMembershipTier: "vip"}
	if got := driver.ClusterMembershipTier(); got != "vip" {
		t.Fatalf("runtime tier = %q, want vip", got)
	}
	driver.Addition.MembershipTier = "ordinary"
	if got := driver.ClusterMembershipTier(); got != "ordinary" {
		t.Fatalf("configured tier = %q, want ordinary", got)
	}
}

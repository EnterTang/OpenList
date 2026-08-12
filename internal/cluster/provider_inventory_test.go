package cluster

import "testing"

func TestMobile139MaxSingleUploadBytes(t *testing.T) {
	tests := map[string]int64{
		"ordinary": 5 << 30,
		"白银":       8 << 30,
		"gold":     20 << 30,
		"钻石":       500 << 30,
		"unknown":  0,
	}
	for tier, want := range tests {
		if got := mobile139MaxSingleUploadBytes(tier); got != want {
			t.Fatalf("limit(%q) = %d, want %d", tier, got, want)
		}
	}
}

func TestMembershipWeightOrdersKnownTiers(t *testing.T) {
	if !(membershipWeight("diamond") > membershipWeight("gold") &&
		membershipWeight("gold") > membershipWeight("silver") &&
		membershipWeight("silver") > membershipWeight("ordinary")) {
		t.Fatal("membership weights are not strictly ordered")
	}
}

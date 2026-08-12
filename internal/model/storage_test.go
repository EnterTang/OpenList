package model

import (
	"encoding/json"
	"testing"
)

func TestStorageDetailsMarshalJSONWithoutMembership(t *testing.T) {
	details := StorageDetails{DiskUsage: DiskUsage{TotalSpace: 100, UsedSpace: 40}}

	got, err := json.Marshal(details)
	if err != nil {
		t.Fatalf("marshal storage details: %v", err)
	}
	const want = `{"free_space":60,"total_space":100,"used_space":40}`
	if string(got) != want {
		t.Fatalf("storage details JSON = %s, want %s", got, want)
	}
}

func TestStorageDetailsMarshalJSONWithMembership(t *testing.T) {
	details := StorageDetails{
		DiskUsage: DiskUsage{TotalSpace: 100, UsedSpace: 40},
		Membership: &MembershipDetails{
			Tier:       "vip",
			Status:     "active",
			ExpireDate: "2040-01-31",
		},
	}

	got, err := json.Marshal(details)
	if err != nil {
		t.Fatalf("marshal storage details: %v", err)
	}
	const want = `{"free_space":60,"membership":{"tier":"vip","status":"active","expire_date":"2040-01-31"},"total_space":100,"used_space":40}`
	if string(got) != want {
		t.Fatalf("storage details JSON = %s, want %s", got, want)
	}
}

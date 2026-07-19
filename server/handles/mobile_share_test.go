package handles

import (
	"errors"
	"reflect"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestMobileShareDeleteRecordIDsDeduplicates(t *testing.T) {
	got := mobileShareDeleteRecordIDs(deleteMobileShareReq{
		ID:  2,
		IDs: []uint{0, 2, 3, 3, 4},
	})
	want := []uint{2, 3, 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ids = %#v, want %#v", got, want)
	}
}

// Regression: ISSUE-005 — remotely cancelled shares remained locally valid.
// Found by /qa on 2026-07-18
// Report: .gstack/qa-reports/qa-report-oplistetf-entertang-work-2026-07-18.md
func TestMobileShareRemoteInvalidError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "missing external link", err: errors.New("外链不存在"), want: true},
		{name: "cancelled by owner", err: errors.New("外链被分享者取消"), want: true},
		{name: "transient upstream failure", err: errors.New("upstream timeout"), want: false},
		{name: "nil", err: nil, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mobileShareRemoteInvalidError(tt.err); got != tt.want {
				t.Fatalf("mobileShareRemoteInvalidError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestMobileShareRecordLinkIDsDeduplicates(t *testing.T) {
	got := mobileShareRecordLinkIDs([]*model.MobileShareRecord{
		{LinkID: " link-1 "},
		{LinkID: ""},
		{LinkID: "link-1"},
		{LinkID: "link-2"},
	})
	want := []string{"link-1", "link-2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("link IDs = %#v, want %#v", got, want)
	}
}

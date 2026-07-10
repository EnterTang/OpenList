package handles

import (
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

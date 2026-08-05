package subscription

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestStorageProviderNameTreats115CD2As115(t *testing.T) {
	if got := storageProviderName("115 CD2"); got != "pan115" {
		t.Fatalf("storageProviderName(115 CD2) = %q, want pan115", got)
	}
}

func TestProviderSupportsDownloadFor115CD2(t *testing.T) {
	if !providerSupportsDownload("pan115", "115 CD2") {
		t.Fatal("115 CD2 should support pan115 downloads")
	}
}

func TestProviderAccountCandidateDoesNotAdvertise115CD2ShareSave(t *testing.T) {
	candidate := providerAccountCandidateFromStorage(t.Context(), model.Storage{
		ID:        1,
		MountPath: "/115-cd2",
		Driver:    "115 CD2",
		Status:    "work",
	})
	if candidate.SupportsShareSave {
		t.Fatalf("115 CD2 account unexpectedly advertises share-save support: %#v", candidate)
	}
}

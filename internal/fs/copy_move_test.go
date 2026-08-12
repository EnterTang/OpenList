package fs

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/resultqueue"
	"github.com/OpenListTeam/OpenList/v4/internal/task_group"
)

func TestAliyunTransferSourceDriverNames(t *testing.T) {
	tests := []struct {
		driver string
		want   bool
	}{
		{driver: "Aliyundrive", want: true},
		{driver: "AliyundriveOpen", want: true},
		{driver: "AliyundriveShare", want: true},
		{driver: "Local", want: false},
		{driver: "139Yun", want: false},
		{driver: "Pan123", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.driver, func(t *testing.T) {
			if got := isAliyunTransferSourceDriver(tt.driver); got != tt.want {
				t.Fatalf("isAliyunTransferSourceDriver(%q) = %v, want %v", tt.driver, got, tt.want)
			}
		})
	}
}

func TestFileTransferTaskRestoresClusterBindingAfterTaskManagerResetsContext(t *testing.T) {
	manifest := protocol.UploadETFManifest{
		MediaItemID: "media-1", Name: "episode.mkv", RemotePath: "/mobile/upload/episode.mkv",
		RemoteFileID: "remote-1", HashSource: "mobile_provider_response", SHA256: strings.Repeat("A", 64), Size: 42,
	}
	binding := task_group.ClusterTransferBinding{
		UploadManifest: &manifest,
		AdditionalCleanupTargets: []resultqueue.CleanupTarget{{
			OpenListPath: "/123/staging/episode.mkv", StorageMountPath: "/123", RemoteFileID: "source-1", Name: "episode.mkv", ExactFile: true,
		}},
		FinalizePayload: &task_group.TransferFinalizePayload{TargetDir: "/mobile/upload", FileName: "episode.mkv", TargetName: "Show.S01E01.mkv"},
	}
	task := &FileTransferTask{ClusterBinding: &binding}
	raw, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal persisted task: %v", err)
	}
	var restored FileTransferTask
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatalf("unmarshal persisted task: %v", err)
	}
	restored.SetCtx(context.Background())
	gotManifest, ok := task_group.UploadManifestFromContext(restored.Ctx())
	if !ok {
		t.Fatal("cluster upload manifest was not restored into task context")
	}
	if gotManifest.MediaItemID != "media-1" {
		t.Fatalf("manifest media item = %q", gotManifest.MediaItemID)
	}
	gotPayload, ok := task_group.TransferFinalizePayloadFromContext(restored.Ctx())
	if !ok || gotPayload.TargetName != "Show.S01E01.mkv" {
		t.Fatalf("finalize payload = %#v, %v", gotPayload, ok)
	}
}

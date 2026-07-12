package worker

import (
	"context"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/stretchr/testify/require"
)

func TestExecuteShareInspectReturnsManifestAsJobResultPayload(t *testing.T) {
	original := inspectShareTree
	t.Cleanup(func() { inspectShareTree = original })
	inspectShareTree = func(_ context.Context, share protocol.ShareTaskContext, version string) (protocol.ShareInspectManifest, error) {
		return protocol.ShareInspectManifest{
			Version: version, Share: share, ObjectHash: "sealed",
			Objects:     []protocol.SourceObject{{Provider: "aliyun_drive", SourceFileID: "f1", SourceRelativePath: "S01E01.mkv", Size: 42}},
			InspectedAt: time.Unix(1, 0).UTC(),
		}, nil
	}

	service := New(nil, nil)
	result, err := service.executeShareInspect(context.Background(), protocol.JobOffer{TaskContext: protocol.TaskContext{
		SealedManifestVersion: "share-inspect/v1",
		Share:                 protocol.ShareTaskContext{Provider: "aliyun_drive", URL: "https://www.alipan.com/s/example"},
	}})
	require.NoError(t, err)
	require.Equal(t, "share-inspect/v1", result["version"])
	require.Equal(t, "sealed", result["object_hash"])
	objects := result["objects"].([]any)
	require.Len(t, objects, 1)
}

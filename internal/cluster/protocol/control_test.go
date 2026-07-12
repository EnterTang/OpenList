package protocol

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigApplySupportsLegacyConfigJSON(t *testing.T) {
	desired := WorkerDesiredConfig{
		ProviderTempRoots:   map[string]string{"aliyundrive": "/ali/temp"},
		TargetBindings:      map[string]TargetBinding{"mobile": {MountPath: "/mobile"}},
		DownloadConcurrency: 2,
	}
	hash, err := HashWorkerDesiredConfig(desired)
	require.NoError(t, err)
	raw, err := json.Marshal(desired)
	require.NoError(t, err)
	decoded, err := (ConfigApply{Revision: 1, DesiredHash: hash, ConfigJSON: string(raw)}).DecodeDesiredConfig()
	require.NoError(t, err)
	require.Equal(t, desired, decoded)
}

func TestConfigApplyRejectsHashMismatch(t *testing.T) {
	desired := WorkerDesiredConfig{UploadConcurrency: 2}
	_, err := (ConfigApply{Revision: 1, DesiredHash: "not-the-hash", DesiredConfig: &desired}).DecodeDesiredConfig()
	require.ErrorContains(t, err, "hash mismatch")
}

func TestStorageApplyAADBindsNodeAndRevision(t *testing.T) {
	apply := StorageApply{Revision: 4, DesiredHash: "hash", Driver: "139Yun", MountPath: "/mobile"}
	require.NotEqual(t, string(StorageApplyAAD("worker-a", apply)), string(StorageApplyAAD("worker-b", apply)))
	changed := apply
	changed.Revision++
	require.NotEqual(t, string(StorageApplyAAD("worker-a", apply)), string(StorageApplyAAD("worker-a", changed)))
}

func TestWorkerDesiredConfigRejectsUnsafePaths(t *testing.T) {
	require.Error(t, (WorkerDesiredConfig{ProviderTempRoots: map[string]string{"aliyun": "relative/path"}}).Validate())
	require.Error(t, (WorkerDesiredConfig{TargetBindings: map[string]TargetBinding{"mobile": {MountPath: "/"}}}).Validate())
}

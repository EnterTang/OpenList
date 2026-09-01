package protocol

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkerDesiredConfigRejectsRouteWithoutPathMapping(t *testing.T) {
	cfg := WorkerDesiredConfig{
		QBClients:        []QBClientConfig{{ID: "qb-a", WebUIURL: "http://127.0.0.1:8080"}},
		MoviePilotRoutes: []MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
	}
	if err := cfg.Validate(); err == nil || err.Error() != `qB client "qb-a" requires at least one path mapping` {
		t.Fatalf("validation error = %v", err)
	}
}

func TestWorkerDesiredConfigRejectsCaseInsensitiveQBClientAliasCollision(t *testing.T) {
	cfg := WorkerDesiredConfig{
		QBClients: []QBClientConfig{
			{ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-a", PathMappings: []QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}}},
			{ID: "QB-A", WebUIURL: "http://127.0.0.1:8081", SecretRef: "secret-b", PathMappings: []QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/other"}}},
		},
	}
	if err := cfg.Validate(); err == nil || err.Error() != `qB client "QB-A" is duplicated` {
		t.Fatalf("Validate() error = %v, want case-insensitive alias collision", err)
	}
}

func TestWorkerDesiredConfigAcceptsContainerNetworkQBWebUI(t *testing.T) {
	cfg := WorkerDesiredConfig{QBClients: []QBClientConfig{{
		ID: "qb-a", WebUIURL: "http://qbittorrent:8080", SecretRef: "secret-a",
		PathMappings: []QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}},
	}}}
	require.NoError(t, cfg.Validate())

	for _, rawURL := range []string{
		"ftp://qbittorrent:8080",
		"http://user:password@qbittorrent:8080",
		"http://qbittorrent:8080?token=secret",
		"http://qbittorrent:8080/#fragment",
	} {
		cfg.QBClients[0].WebUIURL = rawURL
		require.Error(t, cfg.Validate(), "webui_url=%s", rawURL)
	}
}

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

func TestWorkerDesiredConfigAcceptsWindowsWorkerPaths(t *testing.T) {
	cfg := WorkerDesiredConfig{
		QBClients: []QBClientConfig{{
			ID: "qb-win", WebUIURL: "http://127.0.0.1:8383", SecretRef: "secret-qb-win",
			PathMappings: []QBPathMapping{{QBPath: `C:\Downloads`, WorkerPath: `F:\downloads`}},
		}},
		Staging: StagingConfig{Root: `F:\downloads\staging`},
	}
	require.NoError(t, cfg.Validate())
}

func TestWorkerDesiredConfigValidatesMoviePilotStagingWatermarks(t *testing.T) {
	base := WorkerDesiredConfig{Staging: StagingConfig{Root: "/srv/staging"}}
	for _, staging := range []StagingConfig{
		{Root: "/srv/staging", PauseDownloadLowWatermarkBytes: 100},
		{Root: "/srv/staging", ResumeDownloadHighWatermarkBytes: 200},
		{Root: "/srv/staging", PauseDownloadLowWatermarkBytes: 200, ResumeDownloadHighWatermarkBytes: 100},
		{Root: "/srv/staging", DownloadDiskLowWatermarkGB: 2, DownloadDiskHighWatermarkGB: 2},
		{Root: "/srv/staging", SafetyReserveBytes: -1},
	} {
		base.Staging = staging
		require.Error(t, base.Validate(), "staging=%+v", staging)
	}
	base.Staging = StagingConfig{Root: "/srv/staging", SafetyReserveBytes: 50, PauseDownloadLowWatermarkBytes: 100, ResumeDownloadHighWatermarkBytes: 200}
	require.NoError(t, base.Validate())
}

func TestWorkerDesiredConfigAcceptsDownloadDiskWatermarksInGB(t *testing.T) {
	config := WorkerDesiredConfig{Staging: StagingConfig{DownloadDiskLowWatermarkGB: 20, DownloadDiskHighWatermarkGB: 40}}
	if err := config.Validate(); err != nil {
		t.Fatalf("valid download disk watermarks rejected: %v", err)
	}
	config.Staging.DownloadDiskHighWatermarkGB = 20
	if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "high watermark") {
		t.Fatalf("invalid download disk watermarks error = %v", err)
	}
}

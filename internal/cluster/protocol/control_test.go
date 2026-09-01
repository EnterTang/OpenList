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
		{Root: "/srv/staging", StagingPauseDownloadWatermarkGB: 100},
		{Root: "/srv/staging", StagingResumeDownloadWatermarkGB: 200},
		{Root: "/srv/staging", StagingPauseDownloadWatermarkGB: 200, StagingResumeDownloadWatermarkGB: 100},
		{Root: "/srv/staging", DownloadDiskPauseWatermarkGB: 2, DownloadDiskResumeWatermarkGB: 2},
		{Root: "/srv/staging", StagingSafetyReserveGB: -1},
	} {
		base.Staging = staging
		require.Error(t, base.Validate(), "staging=%+v", staging)
	}
	base.Staging = StagingConfig{Root: "/srv/staging", StagingSafetyReserveGB: 50, StagingPauseDownloadWatermarkGB: 100, StagingResumeDownloadWatermarkGB: 200}
	require.NoError(t, base.Validate())
}

func TestWorkerDesiredConfigAcceptsDownloadDiskWatermarksInGB(t *testing.T) {
	config := WorkerDesiredConfig{Staging: StagingConfig{DownloadDiskPauseWatermarkGB: 20, DownloadDiskResumeWatermarkGB: 40}}
	if err := config.Validate(); err != nil {
		t.Fatalf("valid download disk watermarks rejected: %v", err)
	}
	config.Staging.DownloadDiskResumeWatermarkGB = 20
	if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "high watermark") {
		t.Fatalf("invalid download disk watermarks error = %v", err)
	}
}

func TestStagingConfigMigratesLegacyByteFieldsToGB(t *testing.T) {
	var config StagingConfig
	require.NoError(t, json.Unmarshal([]byte(`{
		"max_file_bytes": 161061273600,
		"safety_reserve_bytes": 85899345920,
		"pause_download_low_watermark_bytes": 408021893120,
		"resume_download_high_watermark_bytes": 461708984320,
		"download_disk_low_watermark_gb": 20,
		"download_disk_high_watermark_gb": 40
	}`), &config))
	require.Equal(t, int64(150), config.StagingMaxFileSizeGB)
	require.Equal(t, int64(80), config.StagingSafetyReserveGB)
	require.Equal(t, int64(380), config.StagingPauseDownloadWatermarkGB)
	require.Equal(t, int64(430), config.StagingResumeDownloadWatermarkGB)
	require.Equal(t, int64(20), config.DownloadDiskPauseWatermarkGB)
	require.Equal(t, int64(40), config.DownloadDiskResumeWatermarkGB)

	raw, err := json.Marshal(config)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "_bytes")
	require.Contains(t, string(raw), "staging_safety_reserve_gb")
}

package worker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/secure"
	internaldb "github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/qbittorrent"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type fakeStorageOperator struct {
	storage *model.Storage
	created model.Storage
	updated model.Storage
	err     error
}

func (f *fakeStorageOperator) FindByMountPath(string) (*model.Storage, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.storage, nil
}
func (f *fakeStorageOperator) Create(_ context.Context, storage model.Storage) (uint, error) {
	f.created = storage
	return 42, f.err
}
func (f *fakeStorageOperator) Update(_ context.Context, storage model.Storage) error {
	f.updated = storage
	return f.err
}

func TestConfigApplyUpdatesObservedStateAndBindings(t *testing.T) {
	sender := make(channelSender, 1)
	service := New(&fakeResultQueue{}, sender)
	desired := protocol.WorkerDesiredConfig{
		ProviderTempRoots:   map[string]string{"AliyunDrive": "/ali/cluster-temp"},
		TargetBindings:      map[string]protocol.TargetBinding{"mobile-primary": {MountPath: "/mobile-a", MaxConcurrency: 2}},
		DownloadConcurrency: 3, UploadConcurrency: 2,
	}
	hash, err := protocol.HashWorkerDesiredConfig(desired)
	require.NoError(t, err)
	message, err := protocol.NewEnvelope(protocol.MessageConfigApply, protocol.ConfigApply{Revision: 7, DesiredHash: hash, DesiredConfig: &desired})
	require.NoError(t, err)

	require.NoError(t, service.HandleMessage(context.Background(), nil, *message))
	response := <-sender
	require.Equal(t, protocol.MessageConfigObserved, response.Type)
	require.Equal(t, message.MessageID, response.CorrelationID)
	observed, err := protocol.DecodePayload[protocol.ConfigObserved](response)
	require.NoError(t, err)
	require.Equal(t, "applied", observed.Status)
	require.Equal(t, uint64(7), observed.Revision)
	require.Equal(t, "/ali/cluster-temp", service.providerTempRoot("aliyundrive"))
	root, name, _ := service.resolveTargetBinding("MOBILE-PRIMARY")
	require.Equal(t, "/mobile-a", root)
	require.Equal(t, "mobile_primary", name)
	_, revision := service.ControlIdentity()
	require.Equal(t, uint64(7), revision)
}

func TestConfigApplyDecryptsQBSecretWithoutPersistingPlaintext(t *testing.T) {
	keys, err := secure.GenerateKeyPair()
	require.NoError(t, err)
	sender := make(channelSender, 1)
	service := New(&fakeResultQueue{}, sender)
	service.ConfigureControlPlane("worker-qb", keys, nil)
	desired := protocol.WorkerDesiredConfig{QBClients: []protocol.QBClientConfig{{
		ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-qb-a",
		PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}},
	}}}
	hash, err := protocol.HashWorkerDesiredConfig(desired)
	require.NoError(t, err)
	apply := protocol.ConfigApply{Revision: 8, DesiredHash: hash, DesiredConfig: &desired}
	apply.QBSecretEnvelopes = map[string]string{"qb-a": ""}
	apply.QBSecretEnvelopes["qb-a"], err = secure.SealJSON(keys.PublicKey(), map[string]any{"username": "qb-user", "password": "qb-password"}, protocol.QBSecretApplyAAD("worker-qb", apply, "qb-a"))
	require.NoError(t, err)
	message, err := protocol.NewEnvelope(protocol.MessageConfigApply, apply)
	require.NoError(t, err)
	require.NoError(t, service.HandleMessage(context.Background(), nil, *message))
	response := <-sender
	observed, err := protocol.DecodePayload[protocol.ConfigObserved](response)
	require.NoError(t, err)
	require.Equal(t, "applied", observed.Status)
	service.mu.Lock()
	secret := service.qbSecrets["qb-a"]
	service.mu.Unlock()
	require.Equal(t, "qb-user", secret["username"])
	require.NotContains(t, string(response.Payload), "qb-password")
}

func TestConfigApplyResumesDisconnectPausedMoviePilotTorrent(t *testing.T) {
	keys, err := secure.GenerateKeyPair()
	require.NoError(t, err)
	sender := make(channelSender, 1)
	service := New(&fakeResultQueue{}, sender)
	service.ConfigureControlPlane("worker-qb", keys, nil)
	hash := strings.Repeat("b", 40)
	client := &fakeQBClient{info: qbittorrent.TorrentInfo{Hash: hash, Progress: .5, AmountLeft: 100}}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) { return client, nil }
	torrent := protocol.TorrentTaskContext{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a", TorrentHash: hash}
	service.moviePilotTorrents[moviePilotTorrentRegistryKey(&torrent)] = moviePilotTorrentRegistryEntry{Torrent: torrent, PausedByDisconnect: true}
	desired := protocol.WorkerDesiredConfig{
		QBClients: []protocol.QBClientConfig{{
			ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-qb-a",
			PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}},
		}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
	}
	desiredHash, err := protocol.HashWorkerDesiredConfig(desired)
	require.NoError(t, err)
	apply := protocol.ConfigApply{Revision: 9, DesiredHash: desiredHash, DesiredConfig: &desired}
	apply.QBSecretEnvelopes = map[string]string{"qb-a": ""}
	apply.QBSecretEnvelopes["qb-a"], err = secure.SealJSON(keys.PublicKey(), map[string]any{"username": "qb-user", "password": "qb-password"}, protocol.QBSecretApplyAAD("worker-qb", apply, "qb-a"))
	require.NoError(t, err)
	message, err := protocol.NewEnvelope(protocol.MessageConfigApply, apply)
	require.NoError(t, err)
	require.NoError(t, service.HandleMessage(context.Background(), nil, *message))
	<-sender
	require.Equal(t, []string{hash}, client.started)
}

func TestConfigureControlPlaneRestoresMoviePilotTorrentRegistry(t *testing.T) {
	database, err := gorm.Open(sqlite.Open("file:worker_moviepilot_registry_restore?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, database.AutoMigrate(&model.ClusterWorkerObservedState{}))
	previous := internaldb.GetDb()
	internaldb.UseConnection(database)
	t.Cleanup(func() {
		internaldb.UseConnection(previous)
		sqlDB, dbErr := database.DB()
		if dbErr == nil {
			_ = sqlDB.Close()
		}
	})
	torrent := protocol.TorrentTaskContext{BridgeInstanceID: "mp-main", QBClientID: "qb-a", TorrentHash: strings.Repeat("a", 40)}
	raw, err := json.Marshal(moviePilotTorrentRegistryEntry{Torrent: torrent, PausedByDisconnect: true})
	require.NoError(t, err)
	require.NoError(t, database.Create(&model.ClusterWorkerObservedState{
		ID: moviePilotTorrentRegistryID(&torrent), ResourceType: moviePilotTorrentRegistryType,
		ResourceKey: moviePilotTorrentRegistryKey(&torrent), Hash: torrent.TorrentHash, PayloadJSON: string(raw),
	}).Error)

	service := New(nil, nil)
	service.ConfigureControlPlane("worker-1", nil, nil)
	service.mu.Lock()
	restoredEntry, restored := service.moviePilotTorrents[moviePilotTorrentRegistryKey(&torrent)]
	service.mu.Unlock()
	require.True(t, restored)
	require.True(t, restoredEntry.PausedByDisconnect)
}

func TestStorageApplyDecryptsCredentialsWithoutEchoingThem(t *testing.T) {
	keys, err := secure.GenerateKeyPair()
	require.NoError(t, err)
	sender := make(channelSender, 1)
	operator := &fakeStorageOperator{}
	service := New(&fakeResultQueue{}, sender)
	service.ConfigureControlPlane("worker-1", keys, operator)
	apply := protocol.StorageApply{
		Revision: 9, DesiredHash: "desired-storage-hash", Driver: "139Yun", MountPath: "/mobile-a",
		Operation: "create", Parameters: map[string]any{"root_folder_id": "root"},
	}
	apply.SecretEnvelope, err = secure.SealJSON(keys.PublicKey(), map[string]any{"refresh_token": "highly-secret"}, protocol.StorageApplyAAD("worker-1", apply))
	require.NoError(t, err)
	message, err := protocol.NewEnvelope(protocol.MessageStorageApply, apply)
	require.NoError(t, err)

	require.NoError(t, service.HandleMessage(context.Background(), nil, *message))
	response := <-sender
	require.Equal(t, protocol.MessageStorageApplyResult, response.Type)
	result, err := protocol.DecodePayload[protocol.StorageApplyResult](response)
	require.NoError(t, err)
	require.Equal(t, "applied", result.Status)
	require.NotContains(t, string(response.Payload), "highly-secret")
	var addition map[string]any
	require.NoError(t, json.Unmarshal([]byte(operator.created.Addition), &addition))
	require.Equal(t, "highly-secret", addition["refresh_token"])
	require.Equal(t, "root", addition["root_folder_id"])
	identity, revision := service.ControlIdentity()
	require.Equal(t, keys.KeyID(), identity.KeyID)
	require.Equal(t, uint64(9), revision)
}

func TestStorageApplyRejectsEnvelopeForDifferentNode(t *testing.T) {
	keys, err := secure.GenerateKeyPair()
	require.NoError(t, err)
	sender := make(channelSender, 1)
	operator := &fakeStorageOperator{}
	service := New(&fakeResultQueue{}, sender)
	service.ConfigureControlPlane("worker-1", keys, operator)
	apply := protocol.StorageApply{Revision: 2, DesiredHash: "hash", Driver: "139Yun", MountPath: "/mobile"}
	apply.SecretEnvelope, err = secure.SealJSON(keys.PublicKey(), map[string]any{"token": "secret"}, protocol.StorageApplyAAD("worker-2", apply))
	require.NoError(t, err)
	message, err := protocol.NewEnvelope(protocol.MessageStorageApply, apply)
	require.NoError(t, err)

	require.NoError(t, service.HandleMessage(context.Background(), nil, *message))
	response := <-sender
	result, err := protocol.DecodePayload[protocol.StorageApplyResult](response)
	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	require.Equal(t, "secret_decryption_failed", result.ErrorCode)
	require.Empty(t, operator.created.MountPath)
	require.NotContains(t, string(response.Payload), `"token"`)
}

func TestLimitGateHonorsUpdatedLimit(t *testing.T) {
	gate := newLimitGate(1)
	release, err := gate.Acquire(context.Background())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = gate.Acquire(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	gate.SetLimit(2)
	releaseSecond, err := gate.Acquire(context.Background())
	require.NoError(t, err)
	releaseSecond()
	release()
}

func TestLimitGateTryAcquireDoesNotBlockWhenFull(t *testing.T) {
	gate := newLimitGate(1)
	release, ok := gate.TryAcquire()
	require.True(t, ok)

	secondRelease, ok := gate.TryAcquire()
	require.False(t, ok)
	require.Nil(t, secondRelease)

	release()
	secondRelease, ok = gate.TryAcquire()
	require.True(t, ok)
	require.NotNil(t, secondRelease)
	secondRelease()
}

func TestStorageFailureDoesNotExposeOperatorError(t *testing.T) {
	keys, err := secure.GenerateKeyPair()
	require.NoError(t, err)
	sender := make(channelSender, 1)
	service := New(&fakeResultQueue{}, sender)
	service.ConfigureControlPlane("worker-1", keys, &fakeStorageOperator{err: errors.New("token=plain-secret")})
	apply := protocol.StorageApply{Revision: 1, DesiredHash: "hash", Driver: "139Yun", MountPath: "/mobile"}
	apply.SecretEnvelope, err = secure.SealJSON(keys.PublicKey(), map[string]any{}, protocol.StorageApplyAAD("worker-1", apply))
	require.NoError(t, err)
	message, err := protocol.NewEnvelope(protocol.MessageStorageApply, apply)
	require.NoError(t, err)
	require.NoError(t, service.HandleMessage(context.Background(), nil, *message))
	response := <-sender
	require.NotContains(t, string(response.Payload), "plain-secret")
}

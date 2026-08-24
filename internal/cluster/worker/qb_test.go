package worker

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/qbittorrent"
)

type fakeQBClient struct {
	info                qbittorrent.TorrentInfo
	getErr              error
	files               []qbittorrent.FileInfo
	started             []string
	stopped             []string
	deleted             []string
	deleteFiles         []bool
	deletedMakesMissing bool
}

func (f *fakeQBClient) AddFromLink(string, string, string) error { return nil }
func (f *fakeQBClient) GetInfo(string) (qbittorrent.TorrentInfo, error) {
	return f.info, nil
}
func (f *fakeQBClient) GetFiles(string) ([]qbittorrent.FileInfo, error) { return f.files, nil }
func (f *fakeQBClient) GetTorrentByHash(context.Context, string) (qbittorrent.TorrentInfo, error) {
	if f.deletedMakesMissing && len(f.deleted) > 0 {
		return qbittorrent.TorrentInfo{}, qbittorrent.NewInfoNotFoundError(f.info.Hash)
	}
	return f.info, f.getErr
}
func (f *fakeQBClient) GetFilesByHash(context.Context, string) ([]qbittorrent.FileInfo, error) {
	return f.files, nil
}
func (f *fakeQBClient) StartByHash(_ context.Context, hash string) error {
	f.started = append(f.started, hash)
	return nil
}
func (f *fakeQBClient) StopByHash(_ context.Context, hash string) error {
	f.stopped = append(f.stopped, hash)
	return nil
}
func (f *fakeQBClient) DeleteByHash(_ context.Context, hash string, deleteFiles bool) error {
	f.deleted = append(f.deleted, hash)
	f.deleteFiles = append(f.deleteFiles, deleteFiles)
	return nil
}
func (f *fakeQBClient) Delete(string, bool) error { return nil }

func TestDiscoverTorrentFilesResolvesMultiFileTorrentToWorkerPaths(t *testing.T) {
	hash := strings.Repeat("a", 40)
	service := New(nil, make(channelSender, 10))
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients: []protocol.QBClientConfig{{
			ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-a",
			PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}},
		}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
		Staging:          protocol.StagingConfig{ExtensionWhitelist: []string{".mkv"}},
	}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) {
		return &fakeQBClient{
			info:  qbittorrent.TorrentInfo{Hash: hash, Progress: 1, AmountLeft: 0, ContentPath: "/downloads/Show"},
			files: []qbittorrent.FileInfo{{Name: "Season 01/E01.mkv", Size: 100, Progress: 1}, {Name: "Season 01/E02.mkv", Size: 101, Progress: 1}, {Name: "sample.nfo", Size: 20, Progress: 1}},
		}, nil
	}

	files, err := service.DiscoverTorrentFiles(context.Background(), &protocol.TorrentTaskContext{
		BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a", TorrentHash: hash,
	})
	if err != nil {
		t.Fatalf("discover torrent files: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("discovered %d files, want 2: %#v", len(files), files)
	}
	if files[0].WorkerPath != "/srv/downloads/Show/Season 01/E01.mkv" || files[1].WorkerPath != "/srv/downloads/Show/Season 01/E02.mkv" {
		t.Fatalf("unexpected Worker paths: %#v", files)
	}
}

func TestNewWorkerUsesStoredQBSecretByDefault(t *testing.T) {
	service := New(nil, nil)
	if service.qbClientFactory != nil {
		t.Fatal("default qB client factory bypasses Worker-local credential resolution")
	}
	webUIURL, err := workerQBWebUIURLWithSecret(protocol.QBClientConfig{WebUIURL: "http://127.0.0.1:8080"}, map[string]any{
		"username": "alice", "password": "correct-horse",
	})
	if err != nil {
		t.Fatalf("build qB authenticated URL: %v", err)
	}
	parsed, err := url.Parse(webUIURL)
	if err != nil {
		t.Fatalf("parse qB authenticated URL: %v", err)
	}
	password, _ := parsed.User.Password()
	if parsed.User.Username() != "alice" || password != "correct-horse" {
		t.Fatalf("qB credentials = %q/%q", parsed.User.Username(), password)
	}
}

func TestDiscoverTorrentClientRejectsMissingLocalCredentials(t *testing.T) {
	service := New(nil, nil)
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients: []protocol.QBClientConfig{{
			ID: "qb-a", WebUIURL: "http://127.0.0.1:1", SecretRef: "secret-a",
			PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}},
		}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
	}

	_, _, _, err := service.discoverTorrentClient(&protocol.TorrentTaskContext{
		BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a", TorrentHash: strings.Repeat("a", 40),
	})
	if err == nil || !strings.Contains(err.Error(), "credentials are unavailable") {
		t.Fatalf("missing credentials error = %v", err)
	}
	if service.qbHealth["qb-a"] != "unhealthy" {
		t.Fatalf("qB health = %q, want unhealthy", service.qbHealth["qb-a"])
	}
}

func TestWorkerQBWebUIURLWithSecretRequiresUsernameAndPassword(t *testing.T) {
	config := protocol.QBClientConfig{ID: "qb-credentials", WebUIURL: "http://qbittorrent:8080"}
	for name, secret := range map[string]map[string]any{
		"empty":            {},
		"missing username": {"password": "secret"},
		"missing password": {"username": "openlist"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := workerQBWebUIURLWithSecret(config, secret); err == nil || !strings.Contains(err.Error(), "username and password") {
				t.Fatalf("credential validation error = %v", err)
			}
		})
	}
}

func TestDiscoverTorrentFilesSelectsOneRelativeFile(t *testing.T) {
	hash := strings.Repeat("b", 40)
	service := New(nil, nil)
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients: []protocol.QBClientConfig{{
			ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-a",
			PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}},
		}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
		Staging:          protocol.StagingConfig{ExtensionWhitelist: []string{".mkv"}},
	}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) {
		return &fakeQBClient{
			info:  qbittorrent.TorrentInfo{Hash: hash, Progress: 1, ContentPath: "/downloads/Show"},
			files: []qbittorrent.FileInfo{{Name: "Season 01/E01.mkv", Size: 100, Progress: 1}, {Name: "Season 01/E02.mkv", Size: 101, Progress: 1}},
		}, nil
	}

	files, err := service.DiscoverTorrentFiles(context.Background(), &protocol.TorrentTaskContext{
		BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a", TorrentHash: hash, RelativePath: "Season 01/E02.mkv",
	})
	if err != nil {
		t.Fatalf("discover selected torrent file: %v", err)
	}
	if len(files) != 1 || files[0].Name != "Season 01/E02.mkv" {
		t.Fatalf("unexpected selected files: %#v", files)
	}
}

func TestDiscoverTorrentFilesRejectsIncompleteTorrent(t *testing.T) {
	service := New(nil, nil)
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients:        []protocol.QBClientConfig{{ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-a", PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}}}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
	}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) {
		return &fakeQBClient{info: qbittorrent.TorrentInfo{Progress: .5, AmountLeft: 100}}, nil
	}
	_, err := service.DiscoverTorrentFiles(context.Background(), &protocol.TorrentTaskContext{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a", TorrentHash: strings.Repeat("c", 40)})
	if err == nil || !strings.Contains(err.Error(), "is not complete") {
		t.Fatalf("expected incomplete torrent error, got %v", err)
	}
}

func TestExecuteTorrentObserveReportsQBProgressAndAuthoritativeState(t *testing.T) {
	hash := strings.Repeat("9", 40)
	sender := make(channelSender, 10)
	client := &fakeQBClient{
		info: qbittorrent.TorrentInfo{
			Hash: hash, Progress: 1, Completed: 100, TotalSize: 100, AmountLeft: 0,
			ContentPath: "/downloads/Show", State: qbittorrent.UPLOADING, Ratio: 1.75, SeedingTime: 5400,
		},
		files: []qbittorrent.FileInfo{{Name: "Show.S01E01.mkv", Size: 100, Progress: 1}},
	}
	service := New(nil, sender)
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients: []protocol.QBClientConfig{{
			ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-a",
			PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}},
		}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
		Staging:          protocol.StagingConfig{ExtensionWhitelist: []string{".mkv"}},
	}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) { return client, nil }
	offer := protocol.JobOffer{
		AttemptRef: protocol.AttemptRef{JobID: "observe-progress", AttemptID: "attempt-progress", Generation: 1, LeaseToken: "lease"},
		JobType:    model.ClusterJobTypeTorrentObserve,
		TaskContext: protocol.TaskContext{Torrent: &protocol.TorrentTaskContext{
			BindingID: "binding-progress", BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a", TorrentHash: hash,
		}},
	}
	service.active[offer.JobID] = &activeTask{attempt: offer.AttemptRef, offer: offer}

	result, err := service.executeTorrentObserve(context.Background(), offer)
	if err != nil {
		t.Fatal(err)
	}
	if result["qb_state"] != string(qbittorrent.UPLOADING) || result["ratio"] != 1.75 || result["seeding_seconds"] != int64(5400) {
		t.Fatalf("observation result = %#v", result)
	}
	foundProgress := false
	for len(sender) > 0 {
		message := <-sender
		if message.Type != protocol.MessageJobProgress {
			continue
		}
		progress, decodeErr := protocol.DecodePayload[protocol.JobProgress](message)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		foundProgress = progress.CompletedBytes == 100 && progress.TotalBytes == 100 && progress.EventSeq == 1
	}
	if !foundProgress {
		t.Fatal("torrent observer did not report qB download progress")
	}
}

func TestExecuteTorrentRetentionDeletesBoundHashAndFiles(t *testing.T) {
	hash := strings.Repeat("d", 40)
	client := &fakeQBClient{info: qbittorrent.TorrentInfo{Hash: hash}, deletedMakesMissing: true}
	service := New(nil, make(channelSender, 10))
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients:        []protocol.QBClientConfig{{ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-a", PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}}}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
	}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) { return client, nil }
	result, err := service.executeTorrentRetention(context.Background(), protocol.JobOffer{
		JobType: model.ClusterJobTypeTorrentRetention,
		TaskContext: protocol.TaskContext{Torrent: &protocol.TorrentTaskContext{
			BindingID: "binding-1", WorkerNodeID: "worker-1", BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a", TorrentHash: hash, Action: "delete",
		}},
	})
	if err != nil {
		t.Fatalf("execute retention: %v", err)
	}
	if len(client.deleted) != 1 || client.deleted[0] != hash || len(client.deleteFiles) != 1 || !client.deleteFiles[0] {
		t.Fatalf("delete calls = %#v %#v", client.deleted, client.deleteFiles)
	}
	if len(client.stopped) != 1 || client.stopped[0] != hash {
		t.Fatalf("stop calls = %#v", client.stopped)
	}
	if result["action"] != "delete" {
		t.Fatalf("result = %#v", result)
	}
}

func TestExecuteTorrentRetentionInspectReturnsAuthoritativeQBStateWithoutMutation(t *testing.T) {
	hash := strings.Repeat("8", 40)
	client := &fakeQBClient{info: qbittorrent.TorrentInfo{
		Hash: hash, State: qbittorrent.STALLEDUP, Progress: 1, Ratio: 2.25, SeedingTime: 6400, Completed: 100, TotalSize: 100,
	}}
	service := New(nil, make(channelSender, 10))
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients:        []protocol.QBClientConfig{{ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-a"}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
	}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) { return client, nil }
	offer := torrentRetentionOffer(hash)
	offer.TaskContext.Torrent.Action = "inspect"
	result, err := service.executeTorrentRetention(context.Background(), offer)
	if err != nil {
		t.Fatal(err)
	}
	if result["action"] != "inspect" || result["qb_state"] != string(qbittorrent.STALLEDUP) || result["ratio"] != 2.25 || result["seeding_seconds"] != int64(6400) {
		t.Fatalf("inspect result = %#v", result)
	}
	if len(client.started) != 0 || len(client.stopped) != 0 || len(client.deleted) != 0 {
		t.Fatalf("inspect mutated qB: start=%#v stop=%#v delete=%#v", client.started, client.stopped, client.deleted)
	}
}

func TestExecuteTorrentRetentionTreatsAlreadyMissingHashAsDeleted(t *testing.T) {
	hash := strings.Repeat("4", 40)
	client := &fakeQBClient{getErr: qbittorrent.NewInfoNotFoundError(hash)}
	service := New(nil, make(channelSender, 10))
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients:        []protocol.QBClientConfig{{ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-a"}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
	}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) { return client, nil }
	result, err := service.executeTorrentRetention(context.Background(), torrentRetentionOffer(hash))
	if err != nil {
		t.Fatalf("execute missing retention: %v", err)
	}
	if len(client.stopped) != 0 || len(client.deleted) != 0 || result["action"] != "delete" {
		t.Fatalf("missing retention side effects stopped=%#v deleted=%#v result=%#v", client.stopped, client.deleted, result)
	}
}

func TestExecuteTorrentRetentionFailsWhileHashStillExists(t *testing.T) {
	hash := strings.Repeat("5", 40)
	client := &fakeQBClient{info: qbittorrent.TorrentInfo{Hash: hash}}
	service := New(nil, make(channelSender, 10))
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients:        []protocol.QBClientConfig{{ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-a"}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
	}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) { return client, nil }
	if _, err := service.executeTorrentRetention(context.Background(), torrentRetentionOffer(hash)); err == nil || !strings.Contains(err.Error(), "still exists") {
		t.Fatalf("retention confirmation error = %v", err)
	}
}

func torrentRetentionOffer(hash string) protocol.JobOffer {
	return protocol.JobOffer{
		JobType: model.ClusterJobTypeTorrentRetention,
		TaskContext: protocol.TaskContext{Torrent: &protocol.TorrentTaskContext{
			BindingID: "binding-1", WorkerNodeID: "worker-1", BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a", TorrentHash: hash, Action: "delete",
		}},
	}
}

func TestPauseMoviePilotTorrentsStopsActiveBoundTorrent(t *testing.T) {
	hash := strings.Repeat("e", 40)
	client := &fakeQBClient{}
	service := New(nil, nil)
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients:        []protocol.QBClientConfig{{ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-a", PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}}}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
	}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) { return client, nil }
	service.active["job-1"] = &activeTask{offer: protocol.JobOffer{TaskContext: protocol.TaskContext{Torrent: &protocol.TorrentTaskContext{
		BindingID: "binding-1", WorkerNodeID: "worker-1", BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a", TorrentHash: hash,
	}}}}
	service.PauseMoviePilotTorrents(context.Background())
	if len(client.stopped) != 1 || client.stopped[0] != hash {
		t.Fatalf("stop calls = %#v", client.stopped)
	}
}

func TestPauseMoviePilotTorrentsStopsRememberedTorrentAfterActiveTaskFinishes(t *testing.T) {
	hash := strings.Repeat("f", 40)
	client := &fakeQBClient{info: qbittorrent.TorrentInfo{Hash: hash, Progress: .5, AmountLeft: 100}}
	service := New(nil, nil)
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients:        []protocol.QBClientConfig{{ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-a", PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}}}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
	}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) { return client, nil }
	torrent := &protocol.TorrentTaskContext{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a", TorrentHash: hash}
	if err := service.rememberMoviePilotTorrent(context.Background(), torrent); err != nil {
		t.Fatalf("remember qB torrent: %v", err)
	}
	service.PauseMoviePilotTorrents(context.Background())
	if len(client.stopped) != 1 || client.stopped[0] != hash {
		t.Fatalf("stop calls = %#v", client.stopped)
	}
}

func TestOnTransportReconnectedResumesOnlyDisconnectPausedMoviePilotTorrent(t *testing.T) {
	hash := strings.Repeat("0", 40)
	client := &fakeQBClient{info: qbittorrent.TorrentInfo{Hash: hash, Progress: .5, AmountLeft: 100}}
	service := New(nil, nil)
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients:        []protocol.QBClientConfig{{ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-a", PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}}}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
	}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) { return client, nil }
	torrent := &protocol.TorrentTaskContext{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a", TorrentHash: hash}
	if err := service.rememberMoviePilotTorrent(context.Background(), torrent); err != nil {
		t.Fatalf("remember qB torrent: %v", err)
	}
	service.PauseMoviePilotTorrents(context.Background())
	service.OnTransportReconnected(context.Background())
	if len(client.started) != 1 || client.started[0] != hash {
		t.Fatalf("start calls = %#v", client.started)
	}
}

func TestResumeMoviePilotTorrentsForgetsMissingTorrent(t *testing.T) {
	hash := strings.Repeat("1", 40)
	client := &fakeQBClient{getErr: qbittorrent.NewInfoNotFoundError(hash)}
	service := New(nil, nil)
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients:        []protocol.QBClientConfig{{ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-a", PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}}}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
	}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) { return client, nil }
	torrent := protocol.TorrentTaskContext{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a", TorrentHash: hash}
	service.moviePilotTorrents[moviePilotTorrentRegistryKey(&torrent)] = moviePilotTorrentRegistryEntry{Torrent: torrent, PausedByDisconnect: true}

	service.ResumeMoviePilotTorrents(context.Background())

	if len(service.moviePilotTorrents) != 0 {
		t.Fatalf("missing torrent remained in registry: %#v", service.moviePilotTorrents)
	}
	if len(client.started) != 0 {
		t.Fatalf("start calls = %#v, want none", client.started)
	}
}

func TestReconcileMoviePilotStagingCapacityUsesLowHighWatermarks(t *testing.T) {
	hash := strings.Repeat("2", 40)
	client := &fakeQBClient{info: qbittorrent.TorrentInfo{Hash: hash, Progress: .5, AmountLeft: 100}}
	service := New(nil, nil)
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients:        []protocol.QBClientConfig{{ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-a", PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}}}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
		Staging: protocol.StagingConfig{
			Root: "/srv/staging", PauseDownloadLowWatermarkBytes: 100, ResumeDownloadHighWatermarkBytes: 200,
		},
	}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) { return client, nil }
	torrent := protocol.TorrentTaskContext{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a", TorrentHash: hash}
	service.moviePilotTorrents[moviePilotTorrentRegistryKey(&torrent)] = moviePilotTorrentRegistryEntry{Torrent: torrent}
	free := uint64(100)
	service.stagingFreeSpace = func(context.Context, string) (uint64, error) { return free, nil }

	service.ReconcileMoviePilotStagingCapacity(context.Background())
	if len(client.stopped) != 1 || client.stopped[0] != hash {
		t.Fatalf("stop calls = %#v", client.stopped)
	}
	entry := service.moviePilotTorrents[moviePilotTorrentRegistryKey(&torrent)]
	if !entry.PausedByCapacity {
		t.Fatalf("registry entry = %#v, want capacity pause", entry)
	}

	free = 200
	service.ReconcileMoviePilotStagingCapacity(context.Background())
	if len(client.started) != 1 || client.started[0] != hash {
		t.Fatalf("start calls = %#v", client.started)
	}
	entry = service.moviePilotTorrents[moviePilotTorrentRegistryKey(&torrent)]
	if entry.PausedByCapacity {
		t.Fatalf("registry entry = %#v, want capacity pause cleared", entry)
	}
}

func TestReconnectDoesNotBypassMoviePilotCapacityPause(t *testing.T) {
	hash := strings.Repeat("3", 40)
	client := &fakeQBClient{info: qbittorrent.TorrentInfo{Hash: hash, Progress: .5, AmountLeft: 100}}
	service := New(nil, nil)
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients:        []protocol.QBClientConfig{{ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-a", PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}}}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
		Staging:          protocol.StagingConfig{Root: "/srv/staging", PauseDownloadLowWatermarkBytes: 100, ResumeDownloadHighWatermarkBytes: 200},
	}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) { return client, nil }
	service.stagingFreeSpace = func(context.Context, string) (uint64, error) { return 150, nil }
	torrent := protocol.TorrentTaskContext{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a", TorrentHash: hash}
	service.moviePilotTorrents[moviePilotTorrentRegistryKey(&torrent)] = moviePilotTorrentRegistryEntry{Torrent: torrent, PausedByDisconnect: true, PausedByCapacity: true}

	service.OnTransportReconnected(context.Background())

	if len(client.started) != 0 {
		t.Fatalf("start calls = %#v, want no restart below high watermark", client.started)
	}
	entry := service.moviePilotTorrents[moviePilotTorrentRegistryKey(&torrent)]
	if entry.PausedByDisconnect || !entry.PausedByCapacity {
		t.Fatalf("registry entry = %#v, want capacity-only pause", entry)
	}
}

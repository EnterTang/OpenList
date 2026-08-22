package worker

import (
	"context"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/qbittorrent"
)

type fakeQBClient struct {
	info        qbittorrent.TorrentInfo
	files       []qbittorrent.FileInfo
	started     []string
	stopped     []string
	deleted     []string
	deleteFiles []bool
}

func (f *fakeQBClient) AddFromLink(string, string, string) error { return nil }
func (f *fakeQBClient) GetInfo(string) (qbittorrent.TorrentInfo, error) {
	return f.info, nil
}
func (f *fakeQBClient) GetFiles(string) ([]qbittorrent.FileInfo, error) { return f.files, nil }
func (f *fakeQBClient) GetTorrentByHash(context.Context, string) (qbittorrent.TorrentInfo, error) {
	return f.info, nil
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

func TestExecuteTorrentRetentionDeletesBoundHashAndFiles(t *testing.T) {
	hash := strings.Repeat("d", 40)
	client := &fakeQBClient{}
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
	if result["action"] != "delete" {
		t.Fatalf("result = %#v", result)
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

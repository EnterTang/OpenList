package worker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/qbittorrent"
)

func TestResolveQBPathUsesLongestPrefix(t *testing.T) {
	client := protocol.QBClientConfig{
		ID: "qb-a",
		PathMappings: []protocol.QBPathMapping{
			{QBPath: "/downloads", WorkerPath: "/srv/downloads"},
			{QBPath: "/downloads/tv", WorkerPath: "/srv/tv"},
		},
	}

	got, err := ResolveQBPath(client, "/downloads/tv/Show/S01E01.mkv")
	if err != nil {
		t.Fatalf("resolve qB path: %v", err)
	}
	if got != "/srv/tv/Show/S01E01.mkv" {
		t.Fatalf("resolved path = %q, want %q", got, "/srv/tv/Show/S01E01.mkv")
	}
}

func TestResolveQBPathAcceptsNativeWindowsQBPath(t *testing.T) {
	client := protocol.QBClientConfig{
		ID: "qb-windows",
		PathMappings: []protocol.QBPathMapping{{
			QBPath:     `C:\Downloads`,
			WorkerPath: "/srv/downloads",
		}},
	}

	got, err := ResolveQBPath(client, `C:\Downloads\Movies\film.mkv`)
	if err != nil {
		t.Fatalf("resolve Windows qB path: %v", err)
	}
	if got != "/srv/downloads/Movies/film.mkv" {
		t.Fatalf("resolved path = %q, want %q", got, "/srv/downloads/Movies/film.mkv")
	}
}

func TestResolveQBPathForWorkerPathUsesTheInverseMapping(t *testing.T) {
	client := protocol.QBClientConfig{PathMappings: []protocol.QBPathMapping{{
		QBPath: `F:\downloads`, WorkerPath: `/srv/downloads`,
	}}}

	got, err := ResolveQBPathForWorkerPath(client, `/srv/downloads/.openlist-staging`)
	if err != nil {
		t.Fatalf("resolve qB path from Worker path: %v", err)
	}
	if got != `F:/downloads/.openlist-staging` {
		t.Fatalf("qB staging path = %q, want %q", got, `F:/downloads/.openlist-staging`)
	}
}

func TestResolveQBPathRejectsUnmappedPath(t *testing.T) {
	_, err := ResolveQBPath(protocol.QBClientConfig{
		PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}},
	}, "/other/file.iso")
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("expected unmapped path error, got %v", err)
	}
}

func TestMoviePilotRouteInventoryIsRedacted(t *testing.T) {
	stagingRoot := t.TempDir()
	service := &Service{
		active:   make(map[string]*activeTask),
		qbHealth: map[string]string{"qb-a": "healthy"},
		desiredConfig: protocol.WorkerDesiredConfig{
			QBClients:        []protocol.QBClientConfig{{ID: "qb-a", WebUIURL: "http://127.0.0.1:8080", SecretRef: "secret-qb-a"}},
			MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
			Staging:          protocol.StagingConfig{Root: stagingRoot, MaxUploadConcurrency: 2},
		},
	}

	routes := service.moviePilotRouteInventory()
	if len(routes) != 1 || routes[0].QBClientID != "qb-a" || routes[0].QBHealth != "healthy" {
		t.Fatalf("unexpected route inventory: %#v", routes)
	}
	raw, err := json.Marshal(routes)
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(raw)
	for _, forbidden := range []string{"127.0.0.1:8080", "secret-qb-a", stagingRoot} {
		if strings.Contains(encoded, forbidden) {
			t.Fatalf("route inventory leaks %q: %s", forbidden, encoded)
		}
	}
}

func TestMoviePilotRouteInventoryReportsActiveStagingBytesAndObservedQBHealth(t *testing.T) {
	service := &Service{
		active: map[string]*activeTask{
			"job-1": {offer: protocol.JobOffer{JobType: model.ClusterJobTypeMediaTransfer, TaskContext: protocol.TaskContext{
				Torrent:       &protocol.TorrentTaskContext{QBClientID: "qb-a"},
				SourceObjects: []protocol.SourceObject{{Size: 42}},
			}}},
			"observe-1": {offer: protocol.JobOffer{JobType: model.ClusterJobTypeTorrentObserve, TaskContext: protocol.TaskContext{
				Torrent:       &protocol.TorrentTaskContext{QBClientID: "qb-a"},
				SourceObjects: []protocol.SourceObject{{Size: 99}},
			}}},
		},
		qbHealth: map[string]string{"qb-a": "unhealthy"},
		desiredConfig: protocol.WorkerDesiredConfig{
			QBClients:        []protocol.QBClientConfig{{ID: "qb-a"}},
			MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
		},
	}

	routes := service.moviePilotRouteInventory()

	if len(routes) != 1 || routes[0].QBHealth != "unhealthy" || routes[0].ActiveStagingBytes != 42 || routes[0].ActiveUploadSlots != 1 {
		t.Fatalf("unexpected route inventory: %#v", routes)
	}
}

func TestMoviePilotRouteInventoryReportsDownloadDiskCapacity(t *testing.T) {
	service := New(nil, nil)
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients:        []protocol.QBClientConfig{{ID: "qb-a", PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}}}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
		Staging:          protocol.StagingConfig{DownloadDiskPauseWatermarkGB: 10, DownloadDiskResumeWatermarkGB: 20},
	}
	service.downloadFreeSpace = func(context.Context, string) (uint64, error) { return 30 * 1024 * 1024 * 1024, nil }

	routes := service.moviePilotRouteInventory()
	if len(routes) != 1 || !routes[0].DownloadCapacityKnown || routes[0].DownloadFreeBytes != 30*1024*1024*1024 || routes[0].DownloadLowWatermarkBytes != 10*1024*1024*1024 {
		t.Fatalf("download capacity route inventory = %#v", routes)
	}
}

func TestMoviePilotRouteInventoryPrefersQBPathCapacity(t *testing.T) {
	service := New(nil, nil)
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients: []protocol.QBClientConfig{{
			ID: "qb-a", PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}},
		}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
	}
	service.downloadFreeSpace = func(context.Context, string) (uint64, error) { return 99 << 30, nil }
	client := &fakeQBFreeSpaceClient{freeSpace: 42 << 30}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) { return client, nil }

	routes := service.moviePilotRouteInventory()
	if len(routes) != 1 || routes[0].DownloadFreeBytes != 42<<30 || !routes[0].DownloadCapacityKnown {
		t.Fatalf("qB route capacity = %#v, want 42 GiB from qB API", routes)
	}
	if client.freeSpacePath != "/downloads" {
		t.Fatalf("qB capacity path = %q, want /downloads", client.freeSpacePath)
	}
}

func TestMoviePilotRouteInventoryUsesQBGlobalCapacityForOlderQB(t *testing.T) {
	service := New(nil, nil)
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients: []protocol.QBClientConfig{{
			ID: "qb-a", PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}},
		}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
	}
	service.downloadFreeSpace = func(context.Context, string) (uint64, error) { return 99 << 30, nil }
	client := &fakeQBFreeSpaceClient{
		freeSpaceErr:    qbittorrent.ErrFreeSpaceAtPathUnsupported,
		globalFreeSpace: 42 << 30,
	}
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) { return client, nil }

	routes := service.moviePilotRouteInventory()
	if len(routes) != 1 || routes[0].DownloadFreeBytes != 42<<30 || !routes[0].DownloadCapacityKnown {
		t.Fatalf("qB global route capacity = %#v, want 42 GiB", routes)
	}
}

func TestMoviePilotRouteInventoryUsesEffectiveDownloadConcurrencyWhenUnset(t *testing.T) {
	service := New(nil, nil)
	service.desiredConfig = protocol.WorkerDesiredConfig{
		QBClients:        []protocol.QBClientConfig{{ID: "qb-a"}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
	}
	service.downloadFreeSpace = func(context.Context, string) (uint64, error) { return 50 << 30, nil }

	routes := service.moviePilotRouteInventory()
	if len(routes) != 1 || routes[0].DownloadConcurrency != effectiveConcurrency(0) || routes[0].DownloadConcurrency <= 0 {
		t.Fatalf("download concurrency = %#v, want effective default", routes)
	}
}

func TestMoviePilotRouteInventoryReportsQBDownloadLoad(t *testing.T) {
	service := New(nil, nil)
	service.desiredConfig = protocol.WorkerDesiredConfig{
		DownloadConcurrency: 3,
		QBClients: []protocol.QBClientConfig{{
			ID:           "qb-a",
			PathMappings: []protocol.QBPathMapping{{QBPath: "/downloads", WorkerPath: "/srv/downloads"}},
		}},
		MoviePilotRoutes: []protocol.MoviePilotRoute{{
			BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a",
		}},
	}
	service.downloadFreeSpace = func(context.Context, string) (uint64, error) { return 50 << 30, nil }
	service.qbClientFactory = func(protocol.QBClientConfig) (qbittorrent.Client, error) {
		return &fakeQBClient{torrents: []qbittorrent.TorrentInfo{
			{Progress: 0.25, AmountLeft: 750, Size: 1000, Dlspeed: 100, State: qbittorrent.DOWNLOADING},
			{Progress: 0.5, AmountLeft: 500, Size: 1000, Dlspeed: 200, State: qbittorrent.PAUSEDDL},
			{Progress: 1, AmountLeft: 0, Size: 1000, State: qbittorrent.UPLOADING},
		}}, nil
	}

	routes := service.moviePilotRouteInventory()
	if len(routes) != 1 {
		t.Fatalf("routes = %#v", routes)
	}
	route := routes[0]
	if !route.DownloadLoadKnown || route.DownloadActiveCount != 1 || route.DownloadRemainingBytes != 750 || route.DownloadRateBytesPerSecond != 100 || route.DownloadConcurrency != 3 {
		t.Fatalf("download telemetry = %#v", route)
	}
}

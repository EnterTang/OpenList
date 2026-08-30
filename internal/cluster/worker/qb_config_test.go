package worker

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
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

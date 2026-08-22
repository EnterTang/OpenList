package worker

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/shirou/gopsutil/v4/disk"
)

const moviePilotDefaultUploadConcurrency = 2

// ResolveMoviePilotRoute resolves the configured MoviePilot downloader to the
// qB client that owns its files. Secrets are intentionally not returned here;
// qB client authentication is a Worker-local concern.
func (s *Service) ResolveMoviePilotRoute(bridgeInstanceID, downloader string) (protocol.MoviePilotRoute, protocol.QBClientConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	route, ok := s.desiredConfig.ResolveMoviePilotRoute(bridgeInstanceID, downloader)
	if !ok {
		return protocol.MoviePilotRoute{}, protocol.QBClientConfig{}, fmt.Errorf("MoviePilot route %q/%q is not configured", strings.TrimSpace(bridgeInstanceID), strings.TrimSpace(downloader))
	}
	client, ok := s.desiredConfig.QBClient(route.QBClientID)
	if !ok {
		return protocol.MoviePilotRoute{}, protocol.QBClientConfig{}, fmt.Errorf("MoviePilot route references unknown qB client %q", route.QBClientID)
	}
	return route, client, nil
}

// ResolveQBPath maps a path reported by qB to the corresponding path on this
// Worker. The most specific mapping wins, which allows a general download
// root plus a narrower override for one qB category.
func ResolveQBPath(client protocol.QBClientConfig, qbPath string) (string, error) {
	qbPath = path.Clean(strings.TrimSpace(qbPath))
	if qbPath == "." || !path.IsAbs(qbPath) {
		return "", errors.New("qB content path must be absolute")
	}
	best := -1
	var selected protocol.QBPathMapping
	for _, mapping := range client.PathMappings {
		source := path.Clean(strings.TrimSpace(mapping.QBPath))
		if source == "." || !path.IsAbs(source) {
			continue
		}
		if qbPath != source && !strings.HasPrefix(qbPath, source+"/") {
			continue
		}
		if len(source) > best {
			best = len(source)
			selected = mapping
		}
	}
	if best < 0 {
		return "", fmt.Errorf("qB path %q does not match any configured path mapping", qbPath)
	}
	workerRoot := path.Clean(strings.TrimSpace(selected.WorkerPath))
	if workerRoot == "." || !path.IsAbs(workerRoot) {
		return "", errors.New("qB worker path mapping must be absolute")
	}
	source := path.Clean(strings.TrimSpace(selected.QBPath))
	suffix := strings.TrimPrefix(qbPath, source)
	resolved := path.Clean(path.Join(workerRoot, suffix))
	if resolved != workerRoot && !strings.HasPrefix(resolved, workerRoot+"/") {
		return "", fmt.Errorf("qB path %q escapes worker mapping", qbPath)
	}
	return resolved, nil
}

func moviePilotUploadConcurrency(config protocol.StagingConfig) int {
	if config.MaxUploadConcurrency > 0 {
		return config.MaxUploadConcurrency
	}
	return moviePilotDefaultUploadConcurrency
}

func (s *Service) moviePilotRouteInventory() []protocol.MoviePilotRouteInventory {
	s.mu.Lock()
	config := cloneDesiredConfig(s.desiredConfig)
	active := make(map[string]int)
	for _, task := range s.active {
		if task == nil || task.offer.TaskContext.Torrent == nil {
			continue
		}
		key := strings.TrimSpace(task.offer.TaskContext.Torrent.QBClientID)
		if key != "" {
			active[key]++
		}
	}
	s.mu.Unlock()

	if len(config.MoviePilotRoutes) == 0 {
		return nil
	}
	freeBytes := int64(0)
	if root := strings.TrimSpace(config.Staging.Root); root != "" {
		if usage, err := disk.UsageWithContext(context.Background(), root); err == nil {
			freeBytes = clampUint64ToInt64(usage.Free)
		}
	}
	result := make([]protocol.MoviePilotRouteInventory, 0, len(config.MoviePilotRoutes))
	for _, route := range config.MoviePilotRoutes {
		result = append(result, protocol.MoviePilotRouteInventory{
			BridgeInstanceID:  strings.TrimSpace(route.BridgeInstanceID),
			Downloader:        strings.TrimSpace(route.Downloader),
			QBClientID:        strings.TrimSpace(route.QBClientID),
			StagingRootLabel:  "moviepilot-staging",
			StagingFreeBytes:  freeBytes,
			ActiveUploadSlots: active[strings.TrimSpace(route.QBClientID)],
			UploadConcurrency: moviePilotUploadConcurrency(config.Staging),
			QBHealth:          "configured",
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].BridgeInstanceID != result[j].BridgeInstanceID {
			return result[i].BridgeInstanceID < result[j].BridgeInstanceID
		}
		return result[i].Downloader < result[j].Downloader
	})
	return result
}

func clampUint64ToInt64(value uint64) int64 {
	const maxInt64 = uint64(^uint64(0) >> 1)
	if value > maxInt64 {
		return int64(maxInt64)
	}
	return int64(value)
}

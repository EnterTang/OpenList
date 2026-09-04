package worker

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/shirou/gopsutil/v4/disk"
)

const moviePilotDefaultUploadConcurrency = 2
const bytesPerGB int64 = 1024 * 1024 * 1024

func gigabytesToBytes(value int64) int64 {
	if value <= 0 {
		return value
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	if value > maxInt64/bytesPerGB {
		return maxInt64
	}
	return value * bytesPerGB
}

func stagingMaxFileBytes(config protocol.StagingConfig) int64 {
	if config.StagingMaxFileSizeGB <= 0 {
		return maxQBStagingFileBytes
	}
	return gigabytesToBytes(config.StagingMaxFileSizeGB)
}

func stagingSafetyReserveBytes(config protocol.StagingConfig) int64 {
	return gigabytesToBytes(config.StagingSafetyReserveGB)
}

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
// Worker. qB paths use slash semantics, while Worker paths use the local OS
// semantics so native Windows qB paths such as C:\\Downloads are supported.
// The most specific mapping wins, which allows a general download root plus a
// narrower override for one qB category.
func ResolveQBPath(client protocol.QBClientConfig, rawQBPath string) (string, error) {
	qbPath, selected, err := resolveQBPathMapping(client, rawQBPath)
	if err != nil {
		return "", err
	}
	workerRoot := filepath.Clean(strings.TrimSpace(selected.WorkerPath))
	if workerRoot == "." || !filepath.IsAbs(workerRoot) {
		return "", errors.New("qB worker path mapping must be absolute")
	}
	source := normalizeQBPath(selected.QBPath)
	suffix := strings.TrimPrefix(qbPath, source)
	suffix = strings.TrimLeft(suffix, "/")
	resolved := workerRoot
	if suffix != "" {
		resolved = filepath.Join(workerRoot, filepath.FromSlash(suffix))
	}
	if !pathWithin(workerRoot, resolved) {
		return "", fmt.Errorf("qB path %q escapes worker mapping", qbPath)
	}
	return resolved, nil
}

// resolveQBPathMapping selects the most specific configured mapping for a qB
// path and returns the normalized qB path alongside it. Keeping this lookup
// separate lets capacity checks query qB using its own path while path
// resolution still returns the Worker-local path.
func resolveQBPathMapping(client protocol.QBClientConfig, rawQBPath string) (string, protocol.QBPathMapping, error) {
	qbPath := normalizeQBPath(rawQBPath)
	if qbPath == "." || !isAbsoluteQBPath(qbPath) {
		return "", protocol.QBPathMapping{}, errors.New("qB content path must be absolute")
	}
	best := -1
	var selected protocol.QBPathMapping
	for _, mapping := range client.PathMappings {
		source := normalizeQBPath(mapping.QBPath)
		if source == "." || !isAbsoluteQBPath(source) {
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
		return "", protocol.QBPathMapping{}, fmt.Errorf("qB path %q does not match any configured path mapping", qbPath)
	}
	selected.QBPath = normalizeQBPath(selected.QBPath)
	return qbPath, selected, nil
}

// ResolveQBPathForWorkerPath performs the inverse of ResolveQBPath. It is
// intentionally only successful when the Worker path is inside a declared
// mapping; a qB path must never be guessed for a Worker-only staging volume.
func ResolveQBPathForWorkerPath(client protocol.QBClientConfig, rawWorkerPath string) (string, error) {
	workerPath := filepath.Clean(strings.TrimSpace(rawWorkerPath))
	if workerPath == "." || !filepath.IsAbs(workerPath) {
		return "", errors.New("Worker path must be absolute")
	}
	best := -1
	var selected protocol.QBPathMapping
	for _, mapping := range client.PathMappings {
		workerRoot := filepath.Clean(strings.TrimSpace(mapping.WorkerPath))
		if workerRoot == "." || !filepath.IsAbs(workerRoot) || !pathWithin(workerRoot, workerPath) {
			continue
		}
		if len(workerRoot) > best {
			best = len(workerRoot)
			selected = mapping
		}
	}
	if best < 0 {
		return "", fmt.Errorf("Worker path %q does not match any configured qB path mapping", workerPath)
	}
	relative, err := filepath.Rel(filepath.Clean(selected.WorkerPath), workerPath)
	if err != nil {
		return "", fmt.Errorf("resolve Worker path relative to qB mapping: %w", err)
	}
	qbRoot := normalizeQBPath(selected.QBPath)
	if relative == "." {
		return qbRoot, nil
	}
	return path.Join(qbRoot, filepath.ToSlash(relative)), nil
}

// normalizeQBPath canonicalizes paths reported by qB without applying the
// Worker OS's path rules. qB's API may return either POSIX paths or native
// Windows paths depending on where qBittorrent is running.
func normalizeQBPath(raw string) string {
	value := strings.ReplaceAll(strings.TrimSpace(raw), `\`, "/")
	cleaned := path.Clean(value)
	if isWindowsDriveAbsolute(value) && len(cleaned) == 2 && cleaned[1] == ':' {
		return cleaned + "/"
	}
	return cleaned
}

func isAbsoluteQBPath(value string) bool {
	return path.IsAbs(value) || isWindowsDriveAbsolute(value)
}

func isWindowsDriveAbsolute(value string) bool {
	return len(value) >= 3 &&
		((value[0] >= 'a' && value[0] <= 'z') || (value[0] >= 'A' && value[0] <= 'Z')) &&
		value[1] == ':' && value[2] == '/'
}

func moviePilotUploadConcurrency(config protocol.StagingConfig) int {
	if config.MaxUploadConcurrency > 0 {
		return config.MaxUploadConcurrency
	}
	return moviePilotDefaultUploadConcurrency
}

type moviePilotDownloadTelemetry struct {
	activeCount    int
	remainingBytes int64
	rateBytes      int64
	known          bool
	health         string
}

// moviePilotDownloadTelemetry reads the complete qB list once per client and
// returns only aggregate load. qB URLs, credentials, hashes and local paths
// never enter the Coordinator inventory.
func (s *Service) moviePilotDownloadTelemetry(ctx context.Context, config protocol.QBClientConfig) moviePilotDownloadTelemetry {
	s.mu.Lock()
	factory := s.qbClientFactory
	_, hasSecret := s.qbSecrets[strings.TrimSpace(config.ID)]
	s.mu.Unlock()
	if factory == nil && !hasSecret {
		return moviePilotDownloadTelemetry{}
	}
	client, err := s.qbClientForCapacity(config)
	if err != nil {
		s.recordQBHealth(config.ID, "unhealthy")
		return moviePilotDownloadTelemetry{health: "unhealthy"}
	}
	torrents, err := client.GetTorrents(ctx)
	if err != nil {
		s.recordQBHealth(config.ID, "unhealthy")
		return moviePilotDownloadTelemetry{health: "unhealthy"}
	}
	s.recordQBHealth(config.ID, "healthy")
	telemetry := moviePilotDownloadTelemetry{known: true, health: "healthy"}
	const maxInt64 = int64(^uint64(0) >> 1)
	for _, torrent := range torrents {
		if !isIncompleteTorrent(torrent) || isPausedDownload(torrent) {
			continue
		}
		telemetry.activeCount++
		remaining := torrent.AmountLeft
		if remaining <= 0 && torrent.Size > 0 && torrent.Progress < 1 {
			remaining = int64(float64(torrent.Size) * (1 - torrent.Progress))
		}
		if remaining > 0 && telemetry.remainingBytes <= maxInt64-remaining {
			telemetry.remainingBytes += remaining
		}
		if torrent.Dlspeed > 0 && telemetry.rateBytes <= maxInt64-int64(torrent.Dlspeed) {
			telemetry.rateBytes += int64(torrent.Dlspeed)
		}
	}
	return telemetry
}

func (s *Service) moviePilotRouteInventory() []protocol.MoviePilotRouteInventory {
	s.mu.Lock()
	config := cloneDesiredConfig(s.desiredConfig)
	downloadConcurrency := effectiveConcurrency(config.DownloadConcurrency)
	active := make(map[string]int)
	activeBytes := make(map[string]int64)
	for _, task := range s.active {
		if task == nil || task.offer.JobType != model.ClusterJobTypeMediaTransfer || task.offer.TaskContext.Torrent == nil {
			continue
		}
		key := strings.TrimSpace(task.offer.TaskContext.Torrent.QBClientID)
		if key != "" {
			active[key]++
			for _, source := range task.offer.TaskContext.SourceObjects {
				if source.Size > 0 {
					activeBytes[key] += source.Size
				}
			}
		}
	}
	health := make(map[string]string, len(s.qbHealth))
	for key, value := range s.qbHealth {
		health[key] = value
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
	downloadCapacities := make(map[string]struct {
		free  int64
		known bool
	}, len(config.QBClients))
	downloadTelemetry := make(map[string]moviePilotDownloadTelemetry, len(config.QBClients))
	result := make([]protocol.MoviePilotRouteInventory, 0, len(config.MoviePilotRoutes))
	for _, route := range config.MoviePilotRoutes {
		clientID := strings.TrimSpace(route.QBClientID)
		capacity, cached := downloadCapacities[clientID]
		if !cached {
			clientConfig, clientConfigured := config.QBClient(clientID)
			if clientConfigured {
				capacity.free, capacity.known = s.downloadRootCapacity(context.Background(), clientConfig)
			}
			downloadCapacities[clientID] = capacity
		}
		telemetry, telemetryCached := downloadTelemetry[clientID]
		if !telemetryCached {
			if clientConfig, clientConfigured := config.QBClient(clientID); clientConfigured {
				telemetry = s.moviePilotDownloadTelemetry(context.Background(), clientConfig)
			}
			downloadTelemetry[clientID] = telemetry
		}
		routeHealth := firstNonEmpty(health[clientID], "unknown")
		if telemetry.health != "" {
			routeHealth = telemetry.health
		}
		result = append(result, protocol.MoviePilotRouteInventory{
			BridgeInstanceID:           strings.TrimSpace(route.BridgeInstanceID),
			Downloader:                 strings.TrimSpace(route.Downloader),
			QBClientID:                 strings.TrimSpace(route.QBClientID),
			StagingRootLabel:           "moviepilot-staging",
			StagingFreeBytes:           freeBytes,
			ActiveStagingBytes:         activeBytes[strings.TrimSpace(route.QBClientID)],
			ActiveUploadSlots:          active[strings.TrimSpace(route.QBClientID)],
			UploadConcurrency:          moviePilotUploadConcurrency(config.Staging),
			DownloadRootLabel:          "qb-download",
			DownloadFreeBytes:          capacity.free,
			DownloadLowWatermarkBytes:  downloadWatermarkLowBytes(config.Staging),
			DownloadCapacityKnown:      capacity.known,
			DownloadSafetyReserveBytes: nonNegativeInt64(stagingSafetyReserveBytes(config.Staging)),
			DownloadConcurrency:        downloadConcurrency,
			DownloadActiveCount:        telemetry.activeCount,
			DownloadRemainingBytes:     telemetry.remainingBytes,
			DownloadRateBytesPerSecond: telemetry.rateBytes,
			DownloadLoadKnown:          telemetry.known,
			QBHealth:                   routeHealth,
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func nonNegativeInt64(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func clampUint64ToInt64(value uint64) int64 {
	const maxInt64 = uint64(^uint64(0) >> 1)
	if value > maxInt64 {
		return int64(maxInt64)
	}
	return int64(value)
}

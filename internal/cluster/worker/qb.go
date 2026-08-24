package worker

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/pkg/qbittorrent"
)

var defaultMoviePilotMediaExtensions = map[string]struct{}{
	".avi": {}, ".iso": {}, ".m2ts": {}, ".m4v": {}, ".mkv": {}, ".mov": {},
	".mp4": {}, ".mpeg": {}, ".mpg": {}, ".ts": {}, ".webm": {},
}

// QBFile is a qB file plus its resolved Worker-local source path. QBPath is
// retained for diagnostics and must never be treated as a local path.
type QBFile struct {
	Hash         string  `json:"hash"`
	Name         string  `json:"name"`
	QBPath       string  `json:"qb_path"`
	WorkerPath   string  `json:"worker_path"`
	DownloadRoot string  `json:"download_root"`
	Size         int64   `json:"size"`
	Progress     float32 `json:"progress"`
}

func newWorkerQBClientWithSecret(config protocol.QBClientConfig, parameters map[string]any) (qbittorrent.Client, error) {
	webUIURL, err := workerQBWebUIURLWithSecret(config, parameters)
	if err != nil {
		return nil, err
	}
	return qbittorrent.New(webUIURL)
}

func workerQBWebUIURLWithSecret(config protocol.QBClientConfig, parameters map[string]any) (string, error) {
	username, hasUsername := firstQBSecretString(parameters, "username", "user")
	password, hasPassword := firstQBSecretString(parameters, "password", "pass")
	if !hasUsername || !hasPassword {
		return "", errors.New("qB username and password are required")
	}
	endpoint, err := url.Parse(strings.TrimSpace(config.WebUIURL))
	if err != nil {
		return "", err
	}
	endpoint.User = url.UserPassword(username, password)
	return endpoint.String(), nil
}

func firstQBSecretString(values map[string]any, keys ...string) (string, bool) {
	for _, key := range keys {
		value, ok := values[key].(string)
		if ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value), true
		}
	}
	return "", false
}

func (s *Service) discoverTorrentClient(torrent *protocol.TorrentTaskContext) (protocol.QBClientConfig, qbittorrent.Client, protocol.StagingConfig, error) {
	if torrent == nil {
		return protocol.QBClientConfig{}, nil, protocol.StagingConfig{}, errors.New("torrent context is required")
	}
	route, clientConfig, err := s.ResolveMoviePilotRoute(torrent.BridgeInstanceID, torrent.Downloader)
	if err != nil {
		return protocol.QBClientConfig{}, nil, protocol.StagingConfig{}, err
	}
	if requested := strings.TrimSpace(torrent.QBClientID); requested != "" && !strings.EqualFold(requested, route.QBClientID) {
		return protocol.QBClientConfig{}, nil, protocol.StagingConfig{}, fmt.Errorf("torrent qB client %q does not match configured route qB client %q", requested, route.QBClientID)
	}
	s.mu.Lock()
	staging := s.desiredConfig.Staging
	factory := s.qbClientFactory
	secret := s.qbSecrets[strings.TrimSpace(clientConfig.ID)]
	s.mu.Unlock()
	if factory == nil {
		if secret == nil {
			s.recordQBHealth(clientConfig.ID, "unhealthy")
			return protocol.QBClientConfig{}, nil, protocol.StagingConfig{}, fmt.Errorf("qB client %q credentials are unavailable", clientConfig.ID)
		}
		client, err := newWorkerQBClientWithSecret(clientConfig, secret)
		if err != nil {
			s.recordQBHealth(clientConfig.ID, "unhealthy")
			return protocol.QBClientConfig{}, nil, protocol.StagingConfig{}, fmt.Errorf("create qB client %q: %w", clientConfig.ID, err)
		}
		s.recordQBHealth(clientConfig.ID, "healthy")
		return clientConfig, client, staging, nil
	}
	client, err := factory(clientConfig)
	if err != nil {
		s.recordQBHealth(clientConfig.ID, "unhealthy")
		return protocol.QBClientConfig{}, nil, protocol.StagingConfig{}, fmt.Errorf("create qB client %q: %w", clientConfig.ID, err)
	}
	s.recordQBHealth(clientConfig.ID, "healthy")
	return clientConfig, client, staging, nil
}

func (s *Service) recordQBHealth(clientID, health string) {
	if s == nil || strings.TrimSpace(clientID) == "" {
		return
	}
	s.mu.Lock()
	if s.qbHealth == nil {
		s.qbHealth = make(map[string]string)
	}
	s.qbHealth[strings.TrimSpace(clientID)] = strings.TrimSpace(health)
	s.mu.Unlock()
}

// probeMoviePilotQBClients records actual WebUI authentication/connectivity
// after desired config and encrypted credentials have been applied.
func (s *Service) probeMoviePilotQBClients() {
	if s == nil {
		return
	}
	s.mu.Lock()
	clients := append([]protocol.QBClientConfig(nil), s.desiredConfig.QBClients...)
	factory := s.qbClientFactory
	secrets := make(map[string]map[string]any, len(s.qbSecrets))
	for id, values := range s.qbSecrets {
		secrets[id] = values
	}
	s.mu.Unlock()
	for _, config := range clients {
		var err error
		if factory != nil {
			_, err = factory(config)
		} else if secret := secrets[strings.TrimSpace(config.ID)]; secret != nil {
			_, err = newWorkerQBClientWithSecret(config, secret)
		} else {
			err = errors.New("qB credentials are unavailable")
		}
		health := "healthy"
		if err != nil {
			health = "unhealthy"
		}
		s.recordQBHealth(config.ID, health)
	}
}

// DiscoverTorrentFiles observes qB by its native torrent hash and resolves
// completed media files to Worker-local paths. It never changes qB state or
// the source files.
func (s *Service) DiscoverTorrentFiles(ctx context.Context, torrent *protocol.TorrentTaskContext) ([]QBFile, error) {
	if torrent == nil {
		return nil, errors.New("torrent context is required")
	}
	if strings.TrimSpace(torrent.TorrentHash) == "" {
		return nil, errors.New("torrent hash is required")
	}
	clientConfig, client, staging, err := s.discoverTorrentClient(torrent)
	if err != nil {
		return nil, err
	}
	info, err := client.GetTorrentByHash(ctx, torrent.TorrentHash)
	if err != nil {
		return nil, fmt.Errorf("query qB torrent %q: %w", torrent.TorrentHash, err)
	}
	if info.Progress < 0.999999 || info.AmountLeft > 0 {
		return nil, fmt.Errorf("qB torrent %q is not complete", torrent.TorrentHash)
	}
	files, err := client.GetFilesByHash(ctx, torrent.TorrentHash)
	if err != nil {
		return nil, fmt.Errorf("query qB torrent files %q: %w", torrent.TorrentHash, err)
	}
	contentPath := path.Clean(strings.TrimSpace(info.ContentPath))
	if contentPath == "." {
		contentPath = path.Clean(strings.TrimSpace(torrent.ContentPath))
	}
	if contentPath == "." || !path.IsAbs(contentPath) {
		return nil, errors.New("qB torrent content path must be absolute")
	}
	workerContentRoot, err := ResolveQBPath(clientConfig, contentPath)
	if err != nil {
		return nil, fmt.Errorf("resolve qB content path: %w", err)
	}
	selectedRelative := cleanTorrentRelativePath(torrent.RelativePath)
	result := make([]QBFile, 0, len(files))
	for _, file := range files {
		relative := cleanTorrentRelativePath(file.Name)
		if relative == "" {
			continue
		}
		if selectedRelative != "" && relative != selectedRelative {
			continue
		}
		if file.Progress < 0.999999 {
			if selectedRelative != "" {
				return nil, fmt.Errorf("qB torrent file %q is not complete", relative)
			}
			continue
		}
		if !torrentExtensionAllowed(relative, staging.ExtensionWhitelist) {
			if selectedRelative != "" {
				return nil, fmt.Errorf("qB torrent file %q has a disallowed extension", relative)
			}
			continue
		}
		qbPath := path.Join(contentPath, relative)
		workerPath := path.Join(workerContentRoot, relative)
		if len(files) == 1 && path.Base(contentPath) == path.Base(relative) {
			qbPath = contentPath
			workerPath, err = ResolveQBPath(clientConfig, qbPath)
			if err != nil {
				return nil, fmt.Errorf("resolve qB file path %q: %w", qbPath, err)
			}
		}
		result = append(result, QBFile{Hash: strings.TrimSpace(torrent.TorrentHash), Name: relative, QBPath: qbPath, WorkerPath: workerPath, DownloadRoot: workerContentRoot, Size: file.Size, Progress: file.Progress})
	}
	if len(result) == 0 {
		if selectedRelative != "" {
			return nil, fmt.Errorf("qB torrent file %q was not found", selectedRelative)
		}
		return nil, errors.New("qB torrent has no completed media files")
	}
	return result, nil
}

func cleanTorrentRelativePath(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	clean := path.Clean(raw)
	if clean == "." || path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains(clean, `\`) {
		return ""
	}
	return clean
}

func torrentExtensionAllowed(name string, whitelist []string) bool {
	extension := strings.ToLower(path.Ext(name))
	if extension == "" {
		return false
	}
	if len(whitelist) == 0 {
		_, ok := defaultMoviePilotMediaExtensions[extension]
		return ok
	}
	for _, allowed := range whitelist {
		if strings.EqualFold(strings.TrimSpace(allowed), extension) {
			return true
		}
	}
	return false
}

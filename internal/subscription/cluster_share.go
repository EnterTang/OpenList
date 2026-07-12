package subscription

import (
	"context"
	"errors"
	"path"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

// SaveClusterShareSelection reuses the subscription share providers to save
// only the file IDs assigned to one cluster media task. It returns the local
// OpenList paths inside the worker's provider staging mount.
func SaveClusterShareSelection(ctx context.Context, rawURL, passcode, tempRoot string, selectedFileIDs []string) ([]string, error) {
	ref, err := ParseShareURL(rawURL)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(passcode) != "" {
		ref.Passcode = strings.TrimSpace(passcode)
	}
	cfg, err := GetConfig()
	if err != nil {
		return nil, err
	}
	source, ok := telegramPanSourceForProvider(cfg.Telegram, ref.Provider)
	if !ok {
		return nil, errors.New("share provider is not configured on this worker")
	}
	source.Config = telegramPanSourceConfigWithStorageFallback(ref.Provider, source.Config)
	resolvedTempRoot, err := resolveClusterShareTempRoot(source.Config.TempTransferRoot, tempRoot)
	if err != nil {
		return nil, err
	}
	source.Config.TempTransferRoot = resolvedTempRoot
	if !telegramPanSourceCanSave(ref.Provider, source.Config) {
		return nil, errors.New("share provider credentials or staging root are incomplete")
	}
	provider, err := newShareSaverForProvider(ref.Provider, source.Config)
	if err != nil {
		return nil, err
	}
	selected := make(map[string]struct{}, len(selectedFileIDs))
	for _, id := range selectedFileIDs {
		if id = strings.TrimSpace(id); id != "" {
			selected[id] = struct{}{}
		}
	}
	if len(selected) == 0 {
		return nil, errors.New("selected share file ids are required")
	}
	entries, err := SaveShareToTemp(ctx, provider, ref, SaveShareOptions{
		TempRoot: source.Config.TempTransferRoot,
		Match: func(entry TreeEntry) bool {
			_, matched := selected[entry.ID]
			return matched
		},
	})
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		paths = append(paths, utils.FixAndCleanPath(path.Join(source.Config.TempTransferRoot, strings.TrimPrefix(entry.Path, "/"))))
	}
	if len(paths) == 0 {
		return nil, errors.New("none of the assigned share files were found")
	}
	return paths, nil
}

func resolveClusterShareTempRoot(configuredRoot, requestedRoot string) (string, error) {
	requestedRoot = strings.TrimSpace(requestedRoot)
	if requestedRoot == "" {
		return cleanConfigPath(configuredRoot), nil
	}
	if path.IsAbs(requestedRoot) {
		return cleanConfigPath(requestedRoot), nil
	}
	configuredRoot = cleanConfigPath(configuredRoot)
	if configuredRoot == "" || configuredRoot == "/" {
		return "", errors.New("share provider staging root is required before applying a cluster task namespace")
	}
	cleaned := path.Clean(requestedRoot)
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", errors.New("cluster share temp namespace cannot escape its configured root")
	}
	// Cluster callers pass a task namespace so files from concurrent jobs
	// never share the provider's configured staging directory.
	return cleanConfigPath(path.Join(configuredRoot, cleaned)), nil
}

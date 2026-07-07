package subscription

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	stdpath "path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/pkg/errors"
)

var mediaExts = map[string]struct{}{
	".mkv": {}, ".mp4": {}, ".avi": {}, ".mov": {}, ".rmvb": {}, ".webm": {},
	".flv": {}, ".m2ts": {}, ".ts": {}, ".iso": {}, ".strm": {}, ".etf": {},
}

type TreeEntry struct {
	RootPath string    `json:"root_path"`
	Path     string    `json:"path"`
	Name     string    `json:"name"`
	ID       string    `json:"id"`
	Size     int64     `json:"size"`
	Modified time.Time `json:"modified"`
	IsDir    bool      `json:"is_dir"`
}

type TreeSnapshot struct {
	Entries []TreeEntry `json:"entries"`
	Hash    string      `json:"hash"`
}

var snapshotPaths = SnapshotPaths

func SnapshotPaths(ctx context.Context, roots []string) (*TreeSnapshot, error) {
	var entries []TreeEntry
	for _, root := range roots {
		root = utils.FixAndCleanPath(strings.TrimSpace(root))
		if root == "" || root == "/" {
			continue
		}
		storage, actualPath, err := op.GetStorageAndActualPath(root)
		if err != nil {
			return nil, errors.WithMessagef(err, "failed get storage for %s", root)
		}
		if err := walk(ctx, storage, root, actualPath, &entries); err != nil {
			return nil, err
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Path < entries[j].Path
	})
	return &TreeSnapshot{Entries: entries, Hash: hashEntries(entries)}, nil
}

func walk(ctx context.Context, storage driver.Driver, rootPath, actualPath string, entries *[]TreeEntry) error {
	obj, err := op.Get(ctx, storage, actualPath)
	if err != nil {
		return errors.WithMessagef(err, "failed get %s", rootPath)
	}
	if obj.IsDir() {
		children, err := op.List(ctx, storage, actualPath, model.ListArgs{Refresh: true, SkipHook: true})
		if err != nil {
			return errors.WithMessagef(err, "failed list %s", rootPath)
		}
		for _, child := range children {
			childActual := stdpath.Join(actualPath, child.GetName())
			childRoot := utils.FixAndCleanPath(rootPath)
			childRel := relFromRoot(actualPath, childActual)
			*entries = append(*entries, TreeEntry{
				RootPath: childRoot,
				Path:     utils.FixAndCleanPath("/" + childRel),
				Name:     child.GetName(),
				ID:       child.GetID(),
				Size:     child.GetSize(),
				Modified: child.ModTime(),
				IsDir:    child.IsDir(),
			})
			if child.IsDir() {
				if err := walkChild(ctx, storage, childRoot, actualPath, childActual, entries); err != nil {
					return err
				}
			}
		}
		return nil
	}
	*entries = append(*entries, TreeEntry{
		RootPath: utils.FixAndCleanPath(stdpath.Dir(rootPath)),
		Path:     "/" + obj.GetName(),
		Name:     obj.GetName(),
		ID:       obj.GetID(),
		Size:     obj.GetSize(),
		Modified: obj.ModTime(),
		IsDir:    false,
	})
	return nil
}

func walkChild(ctx context.Context, storage driver.Driver, rootPath, rootActualPath, actualPath string, entries *[]TreeEntry) error {
	children, err := op.List(ctx, storage, actualPath, model.ListArgs{Refresh: true, SkipHook: true})
	if err != nil {
		return errors.WithMessagef(err, "failed list %s", stdpath.Join(rootPath, strings.TrimPrefix(actualPath, rootActualPath)))
	}
	for _, child := range children {
		childActual := stdpath.Join(actualPath, child.GetName())
		childRel := relFromRoot(rootActualPath, childActual)
		*entries = append(*entries, TreeEntry{
			RootPath: rootPath,
			Path:     utils.FixAndCleanPath("/" + childRel),
			Name:     child.GetName(),
			ID:       child.GetID(),
			Size:     child.GetSize(),
			Modified: child.ModTime(),
			IsDir:    child.IsDir(),
		})
		if child.IsDir() {
			if err := walkChild(ctx, storage, rootPath, rootActualPath, childActual, entries); err != nil {
				return err
			}
		}
	}
	return nil
}

func SnapshotEntriesHash(entries []TreeEntry) string {
	copied := append([]TreeEntry(nil), entries...)
	sort.Slice(copied, func(i, j int) bool {
		return copied[i].Path < copied[j].Path
	})
	return hashEntries(copied)
}

func MediaFiles(entries []TreeEntry) []TreeEntry {
	files := make([]TreeEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir {
			continue
		}
		if _, ok := mediaExts[strings.ToLower(filepath.Ext(entry.Name))]; ok {
			files = append(files, entry)
		}
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].Path == files[j].Path {
			return files[i].Name < files[j].Name
		}
		return files[i].Path < files[j].Path
	})
	return files
}

func SourceKey(entry TreeEntry) string {
	key := strings.TrimSpace(entry.ID)
	if key == "" {
		key = entry.Path
	}
	sum := sha256.Sum256([]byte(entry.RootPath + "\x00" + key))
	return hex.EncodeToString(sum[:])
}

func FileHash(entry TreeEntry) string {
	payload := struct {
		ID       string `json:"id"`
		Path     string `json:"path"`
		Size     int64  `json:"size"`
		Modified int64  `json:"modified"`
	}{
		ID:       entry.ID,
		Path:     entry.Path,
		Size:     entry.Size,
		Modified: entry.Modified.UnixNano(),
	}
	body, _ := json.Marshal(payload)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func fullPath(entry TreeEntry) string {
	if entry.Path == "" || entry.Path == "/" {
		return entry.RootPath
	}
	return utils.FixAndCleanPath(stdpath.Join(entry.RootPath, entry.Path))
}

func parentPath(entry TreeEntry) string {
	return stdpath.Dir(fullPath(entry))
}

func relFromRoot(rootActualPath, actualPath string) string {
	prefix := strings.TrimRight(rootActualPath, "/")
	if prefix == "" {
		prefix = "/"
	}
	rel := strings.TrimPrefix(actualPath, prefix)
	return strings.TrimPrefix(rel, "/")
}

func hashEntries(entries []TreeEntry) string {
	payload := make([]struct {
		Path     string `json:"path"`
		ID       string `json:"id"`
		Size     int64  `json:"size"`
		Modified int64  `json:"modified"`
		IsDir    bool   `json:"is_dir"`
	}, 0, len(entries))
	for _, entry := range entries {
		payload = append(payload, struct {
			Path     string `json:"path"`
			ID       string `json:"id"`
			Size     int64  `json:"size"`
			Modified int64  `json:"modified"`
			IsDir    bool   `json:"is_dir"`
		}{
			Path:     entry.Path,
			ID:       entry.ID,
			Size:     entry.Size,
			Modified: entry.Modified.UnixNano(),
			IsDir:    entry.IsDir,
		})
	}
	body, _ := json.Marshal(payload)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

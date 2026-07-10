package subscription

import (
	"context"
	"fmt"
	stdpath "path"
	"sort"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

type SaveShareOptions struct {
	TempRoot string
	Match    func(TreeEntry) bool
}

type shareTreePair struct {
	entry    TreeEntry
	item     ShareItem
	parentID string
}

type shareSaveGroup struct {
	parentID string
	dstDirID string
}

func SaveShareToTemp(ctx context.Context, provider ShareSaver, ref ShareRef, opts SaveShareOptions) ([]TreeEntry, error) {
	if provider == nil {
		return nil, fmt.Errorf("share provider is nil")
	}
	tempRoot := cleanConfigPath(opts.TempRoot)
	if tempRoot == "" || tempRoot == "/" {
		return nil, fmt.Errorf("share temp root is required")
	}
	dstDirID, err := provider.EnsureDir(ctx, tempRoot)
	if err != nil {
		return nil, err
	}
	pairs, err := collectShareTreePairs(ctx, provider, ref)
	if err != nil {
		return nil, err
	}
	selected := make([]TreeEntry, 0, len(pairs))
	dirIDs := map[string]string{"": dstDirID}
	grouped := map[shareSaveGroup][]ShareItem{}
	groupOrder := make([]shareSaveGroup, 0)
	for _, pair := range pairs {
		if pair.entry.IsDir {
			continue
		}
		if opts.Match != nil && !opts.Match(pair.entry) {
			continue
		}
		dirPath := importedParentPath(pair.entry.Path)
		itemDstDirID := dstDirID
		if dirPath != "" {
			itemDstDirID, err = ensureImportedDir(ctx, provider, tempRoot, dirPath, dirIDs)
			if err != nil {
				return selected, err
			}
		}
		selected = append(selected, pair.entry)
		key := shareSaveGroup{parentID: pair.parentID, dstDirID: itemDstDirID}
		if _, ok := grouped[key]; !ok {
			groupOrder = append(groupOrder, key)
		}
		grouped[key] = append(grouped[key], pair.item)
	}
	for _, key := range groupOrder {
		items := grouped[key]
		taskIDs, err := provider.SaveShareItems(ctx, ref, key.parentID, items, key.dstDirID)
		if err != nil {
			return selected, err
		}
		if len(taskIDs) > 0 {
			if err := provider.WaitSaveComplete(ctx, taskIDs); err != nil {
				return selected, err
			}
		}
	}
	return selected, nil
}

func SaveImportedFilesToTemp(ctx context.Context, provider ShareSaver, rootPath string, files []pan123ImportedFile, opts SaveShareOptions) ([]TreeEntry, error) {
	if provider == nil {
		return nil, fmt.Errorf("share provider is nil")
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("imported files are required")
	}
	tempRoot := cleanConfigPath(opts.TempRoot)
	if tempRoot == "" || tempRoot == "/" {
		return nil, fmt.Errorf("share temp root is required")
	}
	rootDirID, err := provider.EnsureDir(ctx, tempRoot)
	if err != nil {
		return nil, err
	}
	dirIDs := map[string]string{"": rootDirID}
	selected := make([]TreeEntry, 0, len(files))
	grouped := map[string][]ShareItem{}
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return selected, err
		}
		entry := TreeEntry{
			RootPath: rootPath,
			Path:     utils.FixAndCleanPath(stdpath.Join("/", file.Path)),
			Name:     file.Name,
			Size:     file.Size,
		}
		if opts.Match != nil && !opts.Match(entry) {
			continue
		}
		dirPath := importedParentPath(file.Path)
		dstDirID, err := ensureImportedDir(ctx, provider, tempRoot, dirPath, dirIDs)
		if err != nil {
			return selected, err
		}
		selected = append(selected, entry)
		grouped[dstDirID] = append(grouped[dstDirID], ShareItem{
			ID:   file.Etag,
			Name: file.Name,
			Size: file.Size,
			Raw: map[string]any{
				"etag":      file.Etag,
				"size":      file.Size,
				"file_name": file.Name,
				"type":      0,
			},
		})
	}
	groupKeys := make([]string, 0, len(grouped))
	for dstDirID := range grouped {
		groupKeys = append(groupKeys, dstDirID)
	}
	sort.Strings(groupKeys)
	for _, dstDirID := range groupKeys {
		taskIDs, err := provider.SaveShareItems(ctx, ShareRef{Provider: ShareProviderPan123, RawURL: rootPath}, "", grouped[dstDirID], dstDirID)
		if err != nil {
			return selected, err
		}
		if len(taskIDs) > 0 {
			if err := provider.WaitSaveComplete(ctx, taskIDs); err != nil {
				return selected, err
			}
		}
	}
	return selected, nil
}

func collectShareTreePairs(ctx context.Context, provider ShareTreeLister, ref ShareRef) ([]shareTreePair, error) {
	var pairs []shareTreePair
	if err := collectShareTreeChildren(ctx, provider, ref, ref.ParentID, "", 0, &pairs); err != nil {
		return nil, err
	}
	return pairs, nil
}

func collectShareTreeChildren(ctx context.Context, provider ShareTreeLister, ref ShareRef, parentID, parentPath string, depth int, pairs *[]shareTreePair) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if depth > maxShareTreeDepth {
		return fmt.Errorf("share tree depth exceeds %d", maxShareTreeDepth)
	}
	children, err := provider.ListShareChildren(ctx, ref, parentID)
	if err != nil {
		return err
	}
	for _, child := range children {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := strings.TrimSpace(child.Name)
		if name == "" {
			continue
		}
		if child.ParentID == "" {
			child.ParentID = parentID
		}
		entryPath := utils.FixAndCleanPath(stdpath.Join("/", parentPath, name))
		pair := shareTreePair{
			entry: TreeEntry{
				RootPath: ref.RawURL,
				Path:     entryPath,
				Name:     name,
				ID:       child.ID,
				Size:     child.Size,
				Modified: child.Modified,
				IsDir:    child.IsDir,
			},
			item:     child,
			parentID: parentID,
		}
		*pairs = append(*pairs, pair)
		if child.IsDir {
			if err := collectShareTreeChildren(ctx, provider, ref, child.ID, strings.TrimPrefix(entryPath, "/"), depth+1, pairs); err != nil {
				return err
			}
		}
	}
	return nil
}

func importedParentPath(path string) string {
	path = strings.TrimPrefix(utils.FixAndCleanPath(stdpath.Join("/", path)), "/")
	if path == "" {
		return ""
	}
	parent := stdpath.Dir(path)
	if parent == "." || parent == "/" {
		return ""
	}
	return strings.TrimPrefix(parent, "/")
}

func ensureImportedDir(ctx context.Context, provider ShareSaver, tempRoot, dirPath string, dirIDs map[string]string) (string, error) {
	if dirID, ok := dirIDs[dirPath]; ok {
		return dirID, nil
	}
	parts := strings.Split(dirPath, "/")
	currentPath := ""
	currentRoot := tempRoot
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if currentPath == "" {
			currentPath = part
		} else {
			currentPath = stdpath.Join(currentPath, part)
		}
		if _, ok := dirIDs[currentPath]; ok {
			currentRoot = stdpath.Join(tempRoot, currentPath)
			continue
		}
		nextRoot := stdpath.Join(currentRoot, part)
		id, err := provider.EnsureDir(ctx, nextRoot)
		if err != nil {
			return "", err
		}
		dirIDs[currentPath] = id
		currentRoot = nextRoot
	}
	if dirID, ok := dirIDs[dirPath]; ok {
		return dirID, nil
	}
	return dirIDs[""], nil
}

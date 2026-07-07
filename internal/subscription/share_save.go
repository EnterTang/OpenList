package subscription

import (
	"context"
	"fmt"
	stdpath "path"
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
	grouped := map[string][]ShareItem{}
	for _, pair := range pairs {
		if pair.entry.IsDir {
			continue
		}
		if opts.Match != nil && !opts.Match(pair.entry) {
			continue
		}
		selected = append(selected, pair.entry)
		grouped[pair.parentID] = append(grouped[pair.parentID], pair.item)
	}
	for _, pair := range pairs {
		if _, ok := grouped[pair.parentID]; !ok {
			continue
		}
		items := grouped[pair.parentID]
		delete(grouped, pair.parentID)
		taskIDs, err := provider.SaveShareItems(ctx, ref, pair.parentID, items, dstDirID)
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

package subscription

import (
	"context"
	"fmt"
	stdpath "path"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

const maxShareTreeDepth = 64

func ListShareTree(ctx context.Context, provider ShareTreeLister, ref ShareRef) ([]TreeEntry, error) {
	if provider == nil {
		return nil, fmt.Errorf("share provider is nil")
	}
	var entries []TreeEntry
	if err := listShareTreeChildren(ctx, provider, ref, ref.ParentID, "", 0, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func listShareTreeChildren(ctx context.Context, provider ShareTreeLister, ref ShareRef, parentID, parentPath string, depth int, entries *[]TreeEntry) error {
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
		entryPath := utils.FixAndCleanPath(stdpath.Join("/", parentPath, name))
		*entries = append(*entries, TreeEntry{
			RootPath: ref.RawURL,
			Path:     entryPath,
			Name:     name,
			ID:       child.ID,
			Size:     child.Size,
			Modified: child.Modified,
			IsDir:    child.IsDir,
		})
		if child.IsDir {
			if err := listShareTreeChildren(ctx, provider, ref, child.ID, strings.TrimPrefix(entryPath, "/"), depth+1, entries); err != nil {
				return err
			}
		}
	}
	return nil
}

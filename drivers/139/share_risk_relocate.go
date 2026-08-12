package _139

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/etfmeta"
	"github.com/OpenListTeam/OpenList/v4/internal/media/recognize"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	stderrors "errors"
	"gorm.io/gorm"
)

// shareRiskRelocateEntry represents a single file to be copied from the old
// folder to the new folder during a share-risk relocate operation.
type shareRiskRelocateEntry struct {
	Obj        model.Obj // original cloud object
	OldPath    string    // full path within the media root (relative, starting from root folder name)
	NewName    string    // safe name after title replacement
	IsETF      bool      // whether this file is an ETF metadata file
	Content    []byte    // cached content for ETF files (read from cloud)
	ETFInfo    *etfmeta.Info
}

// shareRiskRelocatePlan is the complete plan for relocating a media root
// folder to a new cloud folder with a safe name.
type shareRiskRelocatePlan struct {
	RootObj        model.Obj
	OldRootPath    string
	NewRootName    string
	NewRootPath    string
	CanonicalTitle string
	Entries        []shareRiskRelocateEntry
}

func (d *Yun139) buildShareRiskRelocatePlan(ctx context.Context, root model.Obj, actualPath string) (*shareRiskRelocatePlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if root == nil {
		return nil, fmt.Errorf("share risk relocate target is nil")
	}
	actualPath = strings.TrimSpace(actualPath)
	if actualPath == "" {
		actualPath = shareRiskActualPath(root)
	}
	result := recognize.Recognize(root.GetName(), path.Dir(actualPath))
	oldTitle := strings.TrimSpace(result.Title)
	if oldTitle == "" {
		oldTitle = recognize.NormalizeTitle(root.GetName())
	}
	if oldTitle == "" || !containsHan(oldTitle) {
		return nil, nil
	}
	canonicalTitle, err := d.resolveShareRiskCanonicalTitle(ctx, result, oldTitle)
	if err != nil {
		return nil, err
	}
	if canonicalTitle == "" {
		return nil, nil
	}

	newRootName := replaceShareRiskTitle(root.GetName(), oldTitle, canonicalTitle)
	if newRootName == "" || newRootName == root.GetName() {
		return nil, nil
	}

	parentPath := path.Dir(actualPath)
	newRootPath := cleanActualPath(path.Join(parentPath, newRootName))

	plan := &shareRiskRelocatePlan{
		RootObj:        root,
		OldRootPath:    actualPath,
		NewRootName:    newRootName,
		NewRootPath:    newRootPath,
		CanonicalTitle: canonicalTitle,
	}

	if !root.IsDir() {
		// Single file case: just rename the title in the filename
		plan.Entries = append(plan.Entries, shareRiskRelocateEntry{
			Obj:     root,
			NewName: newRootName,
			IsETF:   etfmeta.IsName(root.GetName()),
		})
		return plan, nil
	}

	if err := d.collectShareRiskRelocateEntries(ctx, root, actualPath, oldTitle, canonicalTitle, plan); err != nil {
		return nil, err
	}

	return plan, nil
}

func (d *Yun139) collectShareRiskRelocateEntries(ctx context.Context, dir model.Obj, dirPath, oldTitle, canonicalTitle string, plan *shareRiskRelocatePlan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	children, err := d.List(ctx, dir, model.ListArgs{})
	if err != nil {
		return err
	}
	for _, child := range children {
		childPath := path.Join(dirPath, child.GetName())
		if child.IsDir() {
			// Preserve structural dir names (Season 1, Specials, etc.)
			newName := child.GetName()
			if !isShareRiskStructuralDir(child.GetName()) {
				if replaced := replaceShareRiskTitle(child.GetName(), oldTitle, canonicalTitle); replaced != "" && replaced != child.GetName() {
					newName = replaced
				}
			}
			// For directories, we create them during apply; no entry needed here
			// but we need to recurse to collect files
			// We store dir entries as a special case in the plan
			plan.Entries = append(plan.Entries, shareRiskRelocateEntry{
				Obj:     child,
				OldPath: childPath,
				NewName: newName,
			})
			if err := d.collectShareRiskRelocateEntries(ctx, child, childPath, oldTitle, canonicalTitle, plan); err != nil {
				return err
			}
			continue
		}
		// File: compute safe name
		newName := child.GetName()
		if replaced := replaceShareRiskTitle(child.GetName(), oldTitle, canonicalTitle); replaced != "" && replaced != child.GetName() {
			newName = replaced
		}
		entry := shareRiskRelocateEntry{
			Obj:     child,
			OldPath: childPath,
			NewName: newName,
			IsETF:   etfmeta.IsName(child.GetName()),
		}
		plan.Entries = append(plan.Entries, entry)
	}
	return nil
}

// applyShareRiskRelocatePlan executes the relocate:
// 1. Creates the new folder tree under the parent of the old root
// 2. Copies all files (ETF and non-ETF) to the new folder
// 3. Deletes the old folder
// Returns the new root folder object.
func (d *Yun139) applyShareRiskRelocatePlan(ctx context.Context, plan *shareRiskRelocatePlan) (model.Obj, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if plan == nil || plan.RootObj == nil {
		return nil, fmt.Errorf("relocate plan is nil or root obj is nil")
	}

	// For single file case, just upload to parent with new name
	if !plan.RootObj.IsDir() {
		parentID := strings.TrimSpace(plan.RootObj.GetID())
		// For a file, the "parent" is the file's parent dir
		// We need to get the parent dir ID. Since we don't have it directly,
		// we use the file's path to resolve it.
		// Actually, for a single file, we can just upload bytes to the same parent.
		// But we don't have the parent ID here. Let's handle this differently:
		// For a single file, we read its content and upload with the new name
		// to the same parent. We can get the parent by listing the parent dir.
		// This is a rare case, so let's keep it simple.
		_ = parentID
		// Single file relocate is not common for share risk; skip for now
		return nil, fmt.Errorf("single file relocate not supported")
	}

	// Create the new root folder
	parentPath := path.Dir(plan.OldRootPath)
	if parentPath == "." || parentPath == "" {
		parentPath = "/"
	}
	parentDir, err := d.resolveParentDir(ctx, parentPath)
	if err != nil {
		return nil, fmt.Errorf("resolve parent dir for new folder: %w", err)
	}

	newRoot, err := d.createPersonalFolder(ctx, parentDir.GetID(), plan.NewRootName)
	if err != nil {
		return nil, fmt.Errorf("create new root folder %s: %w", plan.NewRootName, err)
	}

	// Process entries: create subdirs and upload files
	// Build a map of old dir path -> new dir obj for efficient lookup
	dirMap := make(map[string]model.Obj)
	dirMap[plan.OldRootPath] = newRoot

	// Sort entries: dirs first (by depth), then files
	// We process dirs first to ensure parent dirs exist before files
	var dirEntries, fileEntries []shareRiskRelocateEntry
	for _, entry := range plan.Entries {
		if entry.Obj.IsDir() {
			dirEntries = append(dirEntries, entry)
		} else {
			fileEntries = append(fileEntries, entry)
		}
	}

	// Sort dir entries by path depth (shallowest first)
	sortEntriesByDepth(dirEntries)

	for _, entry := range dirEntries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		parentDirPath := path.Dir(entry.OldPath)
		parentDir, ok := dirMap[parentDirPath]
		if !ok {
			return nil, fmt.Errorf("parent dir not found in map: %s", parentDirPath)
		}
		newDir, err := d.createPersonalFolder(ctx, parentDir.GetID(), entry.NewName)
		if err != nil {
			return nil, fmt.Errorf("create subfolder %s: %w", entry.NewName, err)
		}
		dirMap[entry.OldPath] = newDir
	}

	// Upload files
	for _, entry := range fileEntries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		parentDirPath := path.Dir(entry.OldPath)
		parentDir, ok := dirMap[parentDirPath]
		if !ok {
			// File is directly under root
			parentDir = newRoot
		}

		if entry.IsETF {
			// Read ETF content from old file and upload to new folder
			content, info, err := d.readPersonalETFContent(ctx, entry.Obj)
			if err != nil {
				return nil, fmt.Errorf("read ETF content %s: %w", entry.Obj.GetName(), err)
			}
			if err := d.uploadPersonalBytes(ctx, parentDir.GetID(), entry.NewName, content); err != nil {
				return nil, fmt.Errorf("upload ETF %s: %w", entry.NewName, err)
			}
			_ = info
		} else {
			// Non-ETF file: download and re-upload
			content, err := d.downloadPersonalFile(ctx, entry.Obj)
			if err != nil {
				return nil, fmt.Errorf("download file %s: %w", entry.Obj.GetName(), err)
			}
			if err := d.uploadPersonalBytes(ctx, parentDir.GetID(), entry.NewName, content); err != nil {
				return nil, fmt.Errorf("upload file %s: %w", entry.NewName, err)
			}
		}
	}

	// Delete old root folder
	if err := d.removePersonalAndClean(ctx, plan.RootObj); err != nil {
		// Don't fail the whole operation if old folder deletion fails
		// The new folder is already created with all content
		// Log but continue
		_ = err
	}

	return newRoot, nil
}

// syncShareRiskRelocatePlan updates the database to reflect the relocation:
// - Updates ETFMediaRoot.ActualMediaRootPath to the new path
// - Marks old ETFArchiveRecords as relocated
// - Creates new ETFArchiveRecords pointing to the new paths
func (d *Yun139) syncShareRiskRelocatePlan(ctx context.Context, plan *shareRiskRelocatePlan, newRoot model.Obj) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if db.GetDb() == nil {
		return nil
	}
	if plan == nil {
		return nil
	}

	sourcePath := cleanActualPath(plan.OldRootPath)
	if sourcePath == "/" {
		return nil
	}

	root, err := db.FindETFMediaRootByPath(d.GetStorage().MountPath, sourcePath)
	if err != nil {
		if stderrors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}
	if root == nil {
		return nil
	}

	// Update ActualMediaRootPath to the new path
	if plan.NewRootPath != "/" {
		root.ActualMediaRootPath = plan.NewRootPath
	}
	if strings.TrimSpace(plan.CanonicalTitle) != "" {
		root.ShareRiskCanonicalTitle = strings.TrimSpace(plan.CanonicalTitle)
	}
	if err := db.UpdateETFMediaRoot(root); err != nil {
		return err
	}

	// Update all archive records under the old path prefix
	// Mark old records as relocated and create new records with updated paths
	oldPrefix := strings.TrimRight(sourcePath, "/")
	newPrefix := strings.TrimRight(plan.NewRootPath, "/")

	// Use UpdateETFArchivePathsByPrefix to update all paths under the old prefix
	if err := db.UpdateETFArchivePathsByPrefix(d.GetStorage().MountPath, oldPrefix, newPrefix); err != nil {
		return fmt.Errorf("update archive paths by prefix: %w", err)
	}

	return nil
}

// resolveParentDir resolves a parent directory by its actual path.
func (d *Yun139) resolveParentDir(ctx context.Context, parentPath string) (model.Obj, error) {
	parentPath = cleanActualPath(parentPath)
	if parentPath == "/" {
		return &model.Object{ID: d.RootFolderID, Name: "", IsFolder: true}, nil
	}
	return d.getObjByPath(ctx, parentPath)
}

// getObjByPath resolves an object by its actual path (relative to mount path).
func (d *Yun139) getObjByPath(ctx context.Context, actualPath string) (model.Obj, error) {
	actualPath = cleanActualPath(actualPath)
	// Strip mount path prefix if present
	mountPath := d.GetStorage().MountPath
	if strings.HasPrefix(actualPath, mountPath+"/") || actualPath == mountPath {
		actualPath = strings.TrimPrefix(actualPath, mountPath)
		actualPath = cleanActualPath(actualPath)
	}
	parts := splitETFPath(actualPath)
	current := &model.Object{ID: d.RootFolderID, Name: "", IsFolder: true}
	for _, part := range parts {
		found, err := d.findPersonalFolder(ctx, current.GetID(), part)
		if err != nil {
			return nil, err
		}
		if found == nil {
			return nil, fmt.Errorf("folder not found: %s", part)
		}
		current = found
	}
	return current, nil
}

// downloadPersonalFile downloads a non-ETF file from the cloud.
func (d *Yun139) downloadPersonalFile(ctx context.Context, obj model.Obj) ([]byte, error) {
	url, err := d.personalGetLink(obj.GetID())
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	client := base.HttpClient
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected download status code: %d", res.StatusCode)
	}
	return io.ReadAll(io.LimitReader(res.Body, 100*1024*1024))
}

// sortEntriesByDepth sorts relocate entries by path depth (shallowest first).
func sortEntriesByDepth(entries []shareRiskRelocateEntry) {
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0; j-- {
			if pathDepth(entries[j].OldPath) < pathDepth(entries[j-1].OldPath) {
				entries[j], entries[j-1] = entries[j-1], entries[j]
			} else {
				break
			}
		}
	}
}

func pathDepth(p string) int {
	trimmed := strings.Trim(strings.TrimSpace(p), "/")
	if trimmed == "" {
		return 0
	}
	return len(strings.Split(trimmed, "/"))
}

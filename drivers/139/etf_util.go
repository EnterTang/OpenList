package _139

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func (d *Yun139) personalRapidCreate(ctx context.Context, parentFileID, name string, size int64, sha256 string) (model.Obj, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	data := base.Json{
		"contentHash":          strings.ToUpper(strings.TrimSpace(sha256)),
		"contentHashAlgorithm": "SHA256",
		"contentType":          "application/octet-stream",
		"parallelUpload":       false,
		"partInfos":            []PartInfo{},
		"size":                 size,
		"parentFileId":         parentFileID,
		"name":                 name,
		"type":                 "file",
		"fileRenameMode":       "auto_rename",
	}
	var resp PersonalUploadResp
	if _, err := d.personalUploadPost("/file/create", data, &resp); err != nil {
		return nil, false, err
	}
	if personalUploadNeedsPartUpload(resp) {
		return nil, false, fmt.Errorf("rapid restore unavailable: upload urls returned")
	}
	fileName := resp.Data.FileName
	if fileName == "" {
		fileName = name
	}
	return &model.Object{
		ID:   resp.Data.FileId,
		Name: fileName,
		Size: size,
	}, true, nil
}

func (d *Yun139) ensurePersonalFolderPath(ctx context.Context, rootID, relPath string) (model.Obj, error) {
	if strings.TrimSpace(rootID) == "" {
		rootID = d.RootFolderID
	}
	if strings.TrimSpace(rootID) == "" {
		rootID = "/"
	}
	current := &model.Object{ID: rootID, Name: path.Base(rootID), IsFolder: true}
	for _, segment := range splitETFPath(relPath) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		existing, err := d.findPersonalFolder(ctx, current.GetID(), segment)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			current = existing
			continue
		}
		created, err := d.createPersonalFolder(ctx, current.GetID(), segment)
		if err != nil {
			return nil, err
		}
		current = created
	}
	return current, nil
}

func (d *Yun139) emptyPersonalRecycleBin(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var lastErr error
	for _, endpoint := range []string{"/recyclebin/clean", "/recyclebin/clear", "/recyclebin/empty"} {
		_, err := d.personalPost(endpoint, base.Json{}, nil)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

func (d *Yun139) removePersonalAndClean(ctx context.Context, obj model.Obj) error {
	if obj == nil {
		return nil
	}
	if err := d.Remove(ctx, obj); err != nil {
		return err
	}
	return d.emptyPersonalRecycleBin(ctx)
}

func (d *Yun139) findPersonalFolder(ctx context.Context, parentID, name string) (*model.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data := base.Json{
		"imageThumbnailStyleList": []string{"Small", "Large"},
		"orderBy":                 "updated_at",
		"orderDirection":          "DESC",
		"pageInfo": base.Json{
			"pageCursor": "",
			"pageSize":   100,
		},
		"parentFileId": parentID,
	}
	var resp PersonalListResp
	if _, err := d.personalPost("/file/list", data, &resp); err != nil {
		return nil, err
	}
	for _, item := range resp.Data.Items {
		if item.Type == "folder" && item.Name == name {
			return &model.Object{ID: item.FileId, Name: item.Name, IsFolder: true}, nil
		}
	}
	return nil, nil
}

func (d *Yun139) createPersonalFolder(ctx context.Context, parentID, name string) (*model.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data := base.Json{
		"parentFileId":   parentID,
		"name":           name,
		"description":    "",
		"type":           "folder",
		"fileRenameMode": "force_rename",
	}
	var resp PersonalUploadResp
	if _, err := d.personalPost("/file/create", data, &resp); err != nil {
		return nil, err
	}
	id := resp.Data.FileId
	if id == "" {
		id = resp.Data.FileName
	}
	return &model.Object{ID: id, Name: name, IsFolder: true}, nil
}

func splitETFPath(relPath string) []string {
	raw := strings.FieldsFunc(relPath, func(r rune) bool {
		return r == '/' || r == '\\'
	})
	segments := make([]string, 0, len(raw))
	for _, segment := range raw {
		segment = strings.TrimSpace(segment)
		if segment != "" && segment != "." {
			segments = append(segments, segment)
		}
	}
	return segments
}

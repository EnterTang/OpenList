package _115_sy

import (
	"context"
	"encoding/json"
	"fmt"
	stdpath "path"
	"strings"
	"time"

	sy "github.com/OpenListTeam/OpenList/v4/internal/115sy"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type Pan115SY struct {
	model.Storage
	Addition
	client                *sy.Client
	authState             *sy.AuthState
	runtimeMembershipTier string
}

func (d *Pan115SY) Config() driver.Config { return config }

func (d *Pan115SY) GetAddition() driver.Additional { return &d.Addition }

func (d *Pan115SY) Init(ctx context.Context) error {
	pageCooldown := 250 * time.Millisecond
	if raw := strings.TrimSpace(d.PageCooldown); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return fmt.Errorf("invalid 115-sy page cooldown: %w", err)
		}
		pageCooldown = parsed
	}
	if d.PageSize <= 0 {
		d.PageSize = 200
	}
	if d.PageSize > 1150 {
		d.PageSize = 1150
	}
	client, err := sy.NewClient(sy.ClientOptions{
		Cookie:       d.Cookie,
		UserAgent:    d.UserAgent,
		AppVersion:   d.AppVersion,
		LimitRate:    d.LimitRate,
		PageCooldown: pageCooldown,
	})
	if err != nil {
		return err
	}
	d.client = client
	if strings.TrimSpace(d.Cookie) != "" {
		d.authState, err = client.Authenticate(ctx)
	} else {
		d.authState, err = client.LoginByQRCode(ctx, sy.QRCodeLoginOptions{Source: sy.QRCodeSource(d.QRCodeSource)})
	}
	if err != nil {
		d.client = nil
		d.authState = nil
		return err
	}
	if d.GetRootId() == "" {
		d.RootFolderID = d.Config().DefaultRoot
	}
	if _, err := client.ListFiles(ctx, d.GetRootId(), sy.ListOptions{PageSize: 1}); err != nil {
		d.client = nil
		d.authState = nil
		return fmt.Errorf("invalid 115-sy root cid %q: %w", d.GetRootId(), err)
	}
	if d.MembershipTier == "" || strings.EqualFold(d.MembershipTier, "unknown") {
		d.runtimeMembershipTier = "ordinary"
	}
	return nil
}

func (d *Pan115SY) Drop(ctx context.Context) error {
	d.client = nil
	d.authState = nil
	return nil
}

func (d *Pan115SY) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	if d.client == nil {
		return nil, fmt.Errorf("115-sy is not initialized")
	}
	cid := d.rootCID()
	if dir != nil && dir.GetID() != "" {
		cid = dir.GetID()
	}
	items, err := d.client.ListFiles(ctx, cid, sy.ListOptions{PageSize: d.PageSize})
	if err != nil {
		return nil, err
	}
	result := make([]model.Obj, 0, len(items))
	for _, item := range items {
		result = append(result, objectFromRemote(item))
	}
	return result, nil
}

func (d *Pan115SY) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	if d.client == nil {
		return nil, fmt.Errorf("115-sy is not initialized")
	}
	obj, ok := file.(*Obj)
	if !ok || obj.PickCode == "" {
		return nil, fmt.Errorf("115-sy link requires a pickcode object")
	}
	link, err := d.client.DownloadURL(ctx, obj.PickCode, args.Header.Get("User-Agent"))
	if err != nil {
		return nil, err
	}
	if link.Header == nil {
		link.Header = make(map[string][]string)
	}
	if args.Header != nil {
		if callerUA := args.Header.Get("User-Agent"); callerUA != "" && link.Header.Get("User-Agent") == "" {
			link.Header.Set("User-Agent", callerUA)
		}
	}
	return &model.Link{URL: link.URL, Header: link.Header}, nil
}

func (d *Pan115SY) GetRoot(ctx context.Context) (model.Obj, error) {
	root := d.Config().DefaultRoot
	if d.GetRootId() != "" {
		root = d.GetRootId()
	}
	return &Obj{CID: root, Name: "/", Directory: true}, nil
}

func (d *Pan115SY) Get(ctx context.Context, remotePath string) (model.Obj, error) {
	if d.client == nil {
		return nil, fmt.Errorf("115-sy is not initialized")
	}
	item, err := d.client.GetItemByPathFrom(ctx, d.rootCID(), stdpath.Clean(remotePath))
	if err != nil {
		return nil, err
	}
	if item.ID == d.rootCID() {
		return d.GetRoot(ctx)
	}
	return objectFromRemote(item), nil
}

func (d *Pan115SY) Put(ctx context.Context, dstDir model.Obj, stream model.FileStreamer, up driver.UpdateProgress) (model.Obj, error) {
	if d.client == nil {
		return nil, fmt.Errorf("115-sy is not initialized")
	}
	if d.Config().NoUpload {
		return nil, errs.UploadNotSupported
	}
	if up == nil {
		up = func(float64) {}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	hashes, err := sy.ComputeUploadHashes(stream, &up)
	if err != nil {
		return nil, err
	}
	parentCID := d.rootCID()
	if dstDir != nil && dstDir.GetID() != "" {
		parentCID = dstDir.GetID()
	}
	initResp, err := d.client.RapidUpload(ctx, sy.RapidUploadRequest{
		FileName:  stream.GetName(),
		ParentCID: parentCID,
		Size:      stream.GetSize(),
		SHA1:      hashes.SHA1,
		PreSHA1:   hashes.PreSHA1,
	}, stream)
	if err != nil {
		return nil, err
	}
	if matched, err := initResp.RapidMatched(); err != nil {
		return nil, err
	} else if matched {
		up(100)
		return d.refreshUploadedObject(ctx, parentCID, initResp.PickCode, initResp.FileID, stream)
	}

	result, err := d.client.UploadFileByOSS(ctx, initResp, stream, up)
	if err != nil {
		return nil, err
	}
	item := result.RemoteItem(parentCID)
	item.SHA1 = hashes.SHA1
	return d.refreshUploadedObject(ctx, parentCID, item.PickCode, item.ID, stream)
}

func (d *Pan115SY) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) error {
	if d.client == nil {
		return fmt.Errorf("115-sy is not initialized")
	}
	_, err := d.client.MakeDir(ctx, parentDir.GetID(), dirName)
	return err
}

func (d *Pan115SY) Move(ctx context.Context, srcObj, dstDir model.Obj) error {
	return d.mutate(ctx, func() error { return d.client.Move(ctx, srcObj.GetID(), dstDir.GetID()) })
}

func (d *Pan115SY) Rename(ctx context.Context, srcObj model.Obj, newName string) error {
	return d.mutate(ctx, func() error { return d.client.Rename(ctx, srcObj.GetID(), newName) })
}

func (d *Pan115SY) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	return d.mutate(ctx, func() error { return d.client.Copy(ctx, srcObj.GetID(), dstDir.GetID()) })
}

func (d *Pan115SY) Remove(ctx context.Context, obj model.Obj) error {
	return d.mutate(ctx, func() error { return d.client.Remove(ctx, obj.GetID(), "") })
}

func (d *Pan115SY) OfflineDownload(ctx context.Context, urls []string, dstDir model.Obj) ([]string, error) {
	if d.client == nil {
		return nil, fmt.Errorf("115-sy is not initialized")
	}
	targetCID := d.rootCID()
	if dstDir != nil && dstDir.GetID() != "" {
		targetCID = dstDir.GetID()
	}
	result, err := d.client.AddOfflineTasks(ctx, sy.OfflineRequest{TargetCID: targetCID, URLs: urls})
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(result.Items))
	for _, item := range result.Items {
		if !item.Success {
			return ids, fmt.Errorf("offline task failed for %q: %s", item.URL, firstNonEmptyLocal(item.Error, item.ErrorMsg))
		}
		ids = append(ids, item.TaskID)
	}
	return ids, nil
}

func (d *Pan115SY) OfflineList(ctx context.Context) ([]sy.OfflineTask, error) {
	if d.client == nil {
		return nil, fmt.Errorf("115-sy is not initialized")
	}
	return d.client.ListOfflineTasks(ctx)
}

func (d *Pan115SY) DeleteOfflineTasks(ctx context.Context, ids []string, deleteFiles bool) error {
	if d.client == nil {
		return fmt.Errorf("115-sy is not initialized")
	}
	return d.client.DeleteOfflineTasks(ctx, ids, deleteFiles)
}

func (d *Pan115SY) Other(ctx context.Context, args model.OtherArgs) (interface{}, error) {
	if d.client == nil {
		return nil, fmt.Errorf("115-sy is not initialized")
	}
	switch args.Method {
	case "share_parse":
		var request struct {
			URL string `json:"url"`
		}
		if err := decodeOtherData(args.Data, &request); err != nil {
			return nil, err
		}
		return sy.ParseShareURL(request.URL)
	case "share_snapshot":
		var share sy.ShareURL
		if err := decodeOtherData(args.Data, &share); err != nil {
			return nil, err
		}
		return d.client.ShareSnapshot(ctx, share)
	case "share_receive":
		var request sy.ReceiveShareRequest
		if err := decodeOtherData(args.Data, &request); err != nil {
			return nil, err
		}
		return d.client.ReceiveShare(ctx, request)
	case "offline_add":
		var request sy.OfflineRequest
		if err := decodeOtherData(args.Data, &request); err != nil {
			return nil, err
		}
		return d.client.AddOfflineTasks(ctx, request)
	case "offline_list":
		return d.client.ListOfflineTasks(ctx)
	case "offline_delete":
		var request struct {
			IDs         []string `json:"ids"`
			DeleteFiles bool     `json:"delete_files"`
		}
		if err := decodeOtherData(args.Data, &request); err != nil {
			return nil, err
		}
		return nil, d.client.DeleteOfflineTasks(ctx, request.IDs, request.DeleteFiles)
	default:
		return nil, errs.NotSupport
	}
}

func decodeOtherData(data interface{}, target interface{}) error {
	if data == nil {
		return fmt.Errorf("115-sy operation data is required")
	}
	encoded, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("invalid 115-sy operation data: %w", err)
	}
	if err := json.Unmarshal(encoded, target); err != nil {
		return fmt.Errorf("invalid 115-sy operation data: %w", err)
	}
	return nil
}

func (d *Pan115SY) mutate(ctx context.Context, fn func() error) error {
	if d.client == nil {
		return fmt.Errorf("115-sy is not initialized")
	}
	return fn()
}

func (d *Pan115SY) ClusterMembershipTier() string {
	configured := strings.ToLower(strings.TrimSpace(d.MembershipTier))
	if configured != "" && configured != "unknown" {
		return configured
	}
	return d.runtimeMembershipTier
}

func (d *Pan115SY) rootCID() string {
	root := d.GetRootId()
	if strings.TrimSpace(root) == "" {
		return d.Config().DefaultRoot
	}
	return root
}

func (d *Pan115SY) refreshUploadedObject(ctx context.Context, parentCID, pickcode, fid string, stream model.FileStreamer) (model.Obj, error) {
	lookup := strings.TrimSpace(firstNonEmptyLocal(pickcode, fid))
	if lookup != "" {
		if item, err := d.client.GetFile(ctx, lookup); err == nil {
			if item.ParentCID == "" {
				item.ParentCID = parentCID
			}
			return objectFromRemote(item), nil
		}
	}
	return objectFromRemote(sy.RemoteItem{
		ID:        fid,
		Name:      stream.GetName(),
		IsDir:     false,
		Size:      stream.GetSize(),
		PickCode:  pickcode,
		ParentCID: parentCID,
	}), nil
}

func firstNonEmptyLocal(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func objectFromRemote(item sy.RemoteItem) *Obj {
	return &Obj{
		FID:       item.ID,
		CID:       item.ID,
		Name:      item.Name,
		Directory: item.IsDir,
		Size:      item.Size,
		SHA1:      item.SHA1,
		PickCode:  item.PickCode,
		ParentCID: item.ParentCID,
		UpdatedAt: item.ModifyTime,
		Thumbnail: item.Thumbnail,
	}
}

var _ driver.Driver = (*Pan115SY)(nil)
var _ driver.GetRooter = (*Pan115SY)(nil)
var _ driver.Getter = (*Pan115SY)(nil)
var _ driver.Mkdir = (*Pan115SY)(nil)
var _ driver.Move = (*Pan115SY)(nil)
var _ driver.Rename = (*Pan115SY)(nil)
var _ driver.Copy = (*Pan115SY)(nil)
var _ driver.Remove = (*Pan115SY)(nil)
var _ driver.PutResult = (*Pan115SY)(nil)
var _ driver.Other = (*Pan115SY)(nil)

package _115_sy

import (
	"context"
	"fmt"
	stdpath "path"
	"strings"
	"time"

	sy "github.com/OpenListTeam/OpenList/v4/internal/115sy"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
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
	cid := d.Config().DefaultRoot
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
	id, err := d.client.GetIDByPath(ctx, stdpath.Clean(remotePath))
	if err != nil {
		return nil, err
	}
	if id == d.Config().DefaultRoot {
		return d.GetRoot(ctx)
	}
	item, err := d.client.GetFile(ctx, id)
	if err != nil {
		return nil, err
	}
	return objectFromRemote(item), nil
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

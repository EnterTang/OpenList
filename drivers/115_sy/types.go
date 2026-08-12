package _115_sy

import (
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

type Obj struct {
	FID       string `json:"fid"`
	CID       string `json:"cid"`
	Name      string `json:"name"`
	Directory bool   `json:"directory"`
	Size      int64  `json:"size"`
	SHA1      string `json:"sha1"`
	PickCode  string `json:"pickcode"`
	ParentCID string `json:"parent_cid"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
	Thumbnail string `json:"thumbnail"`
}

func (o *Obj) GetSize() int64 {
	return o.Size
}

func (o *Obj) GetName() string {
	return o.Name
}

func (o *Obj) ModTime() time.Time {
	return time.Unix(o.UpdatedAt, 0)
}

func (o *Obj) CreateTime() time.Time {
	return time.Unix(o.CreatedAt, 0)
}

func (o *Obj) IsDir() bool {
	return o.Directory
}

func (o *Obj) GetHash() utils.HashInfo {
	return utils.NewHashInfo(utils.SHA1, o.SHA1)
}

func (o *Obj) GetID() string {
	if o.FID != "" {
		return o.FID
	}
	return o.CID
}

func (o *Obj) GetPath() string {
	return ""
}

func (o *Obj) Thumb() string {
	return o.Thumbnail
}

var _ model.Obj = (*Obj)(nil)
var _ model.Thumb = (*Obj)(nil)

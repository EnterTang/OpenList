package subscription

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
)

const pan115WebURL = "https://115cdn.com"

type pan115ShareProvider struct {
	cfg    model.SubscriptionTelegramPanConfig
	webURL string
	client *resty.Client
}

func NewPan115ShareProvider(cfg model.SubscriptionTelegramPanConfig) ShareSaver {
	return &pan115ShareProvider{
		cfg:    normalizeTelegramPanConfig(cfg),
		webURL: pan115WebURL,
		client: newShareHTTPClient(),
	}
}

func (p *pan115ShareProvider) Name() ShareProviderName {
	return ShareProviderPan115
}

func (p *pan115ShareProvider) ParseURL(raw string) (ShareRef, error) {
	ref, err := ParseShareURL(raw)
	if err != nil {
		return ShareRef{}, err
	}
	if ref.Provider != ShareProviderPan115 {
		return ShareRef{}, fmt.Errorf("share URL provider = %s, want %s", ref.Provider, ShareProviderPan115)
	}
	return ref, nil
}

func (p *pan115ShareProvider) EnsureDir(ctx context.Context, path string) (string, error) {
	path = utils.FixAndCleanPath(path)
	if path == "" || path == "/" {
		return "", errors.New("temp transfer root is empty")
	}
	if err := ensureDir(ctx, path); err != nil {
		return "", err
	}
	obj, err := fs.Get(ctx, path, &fs.GetArgs{NoLog: true})
	if err != nil {
		return "", err
	}
	if obj == nil || obj.GetID() == "" {
		return "", errors.Errorf("temp transfer root has no remote id: %s", path)
	}
	return obj.GetID(), nil
}

func (p *pan115ShareProvider) ListShareChildren(ctx context.Context, ref ShareRef, parentID string) ([]ShareItem, error) {
	var resp pan115SnapResp
	httpResp, err := p.client.R().
		SetContext(ctx).
		SetHeader("Referer", pan115ShareReferer(p.webURL, ref)).
		SetQueryParams(map[string]string{
			"share_code":   ref.ShareID,
			"receive_code": ref.Passcode,
			"cid":          parentID,
			"offset":       "0",
			"limit":        "50",
			"asc":          "0",
			"format":       "json",
		}).
		Get(p.webURL + "/webapi/share/snap")
	if err != nil {
		return nil, err
	}
	if err := decodePan115JSON(httpResp, &resp); err != nil {
		return nil, err
	}
	if !resp.State {
		return nil, pan115Error(resp.Error)
	}
	items := make([]ShareItem, 0, len(resp.Data.List))
	for _, item := range resp.Data.List {
		items = append(items, item.shareItem(parentID))
	}
	return items, nil
}

func (p *pan115ShareProvider) SaveShareItems(ctx context.Context, ref ShareRef, parentID string, items []ShareItem, dstDirID string) ([]string, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if p.cfg.Cookie == "" {
		return nil, errors.New("115 cookie is required")
	}
	fileIDs := make([]string, 0, len(items))
	for _, item := range items {
		fileID := firstNonEmpty(shareItemToken(item), item.ID)
		if fileID != "" {
			fileIDs = append(fileIDs, fileID)
		}
	}
	if len(fileIDs) == 0 {
		return nil, errors.New("115 share item ids are empty")
	}
	var resp pan115ReceiveResp
	httpResp, err := p.client.R().
		SetContext(ctx).
		SetHeader("Cookie", p.cfg.Cookie).
		SetHeader("Origin", p.webURL).
		SetHeader("Referer", pan115ShareReferer(p.webURL, ref)).
		SetFormData(map[string]string{
			"cid":          firstNonEmpty(dstDirID, "0"),
			"share_code":   ref.ShareID,
			"receive_code": ref.Passcode,
			"file_id":      strings.Join(fileIDs, ","),
		}).
		Post(p.webURL + "/webapi/share/receive")
	if err != nil {
		return nil, err
	}
	if err := decodePan115JSON(httpResp, &resp); err != nil {
		return nil, err
	}
	if !resp.State {
		return nil, pan115Error(resp.Error)
	}
	return []string{"pan115_sync_" + ref.ShareID}, nil
}

func (p *pan115ShareProvider) WaitSaveComplete(ctx context.Context, taskIDs []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func pan115ShareReferer(baseURL string, ref ShareRef) string {
	return fmt.Sprintf("%s/s/%s?password=%s&", strings.TrimRight(baseURL, "/"), ref.ShareID, ref.Passcode)
}

func decodePan115JSON(resp *resty.Response, out any) error {
	if resp == nil {
		return errors.New("empty 115 response")
	}
	if err := json.Unmarshal(resp.Body(), out); err != nil {
		return errors.WithMessage(err, "decode 115 response")
	}
	return nil
}

func pan115Error(message string) error {
	if strings.TrimSpace(message) == "" {
		return errors.New("115 request failed")
	}
	return errors.New(message)
}

type pan115SnapResp struct {
	State bool   `json:"state"`
	Error string `json:"error"`
	Data  struct {
		Count int          `json:"count"`
		List  []pan115File `json:"list"`
	} `json:"data"`
}

type pan115File struct {
	FID      string `json:"fid"`
	CID      string `json:"cid"`
	Name     string `json:"n"`
	Size     int64  `json:"s"`
	UpdateAt string `json:"t"`
	Icon     string `json:"ico"`
}

func (f pan115File) shareItem(parentID string) ShareItem {
	isDir := f.FID == ""
	id := f.FID
	if isDir {
		id = f.CID
	}
	if parentID == "" {
		parentID = f.CID
		if isDir {
			parentID = "0"
		}
	}
	return ShareItem{
		ID:       id,
		ParentID: parentID,
		Name:     f.Name,
		Size:     f.Size,
		Modified: parsePan115Time(f.UpdateAt),
		IsDir:    isDir,
		Raw: map[string]any{
			"share_fid_token": id,
		},
	}
}

type pan115ReceiveResp struct {
	State bool   `json:"state"`
	Error string `json:"error"`
}

func parsePan115Time(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.Unix(parsed, 0)
	}
	if parsed, err := time.Parse("2006-01-02 15:04:05", value); err == nil {
		return parsed
	}
	return time.Time{}
}

var _ ShareSaver = (*pan115ShareProvider)(nil)

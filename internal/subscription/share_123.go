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

const pan123APIURL = "https://yun.123pan.com/b/api"

type pan123ShareProvider struct {
	cfg    model.SubscriptionTelegramPanConfig
	apiURL string
	client *resty.Client
}

func NewPan123ShareProvider(cfg model.SubscriptionTelegramPanConfig) ShareSaver {
	return &pan123ShareProvider{
		cfg:    normalizeTelegramPanConfig(cfg),
		apiURL: pan123APIURL,
		client: newShareHTTPClient(),
	}
}

func (p *pan123ShareProvider) Name() ShareProviderName {
	return ShareProviderPan123
}

func (p *pan123ShareProvider) ParseURL(raw string) (ShareRef, error) {
	ref, err := ParseShareURL(raw)
	if err != nil {
		return ShareRef{}, err
	}
	if ref.Provider != ShareProviderPan123 {
		return ShareRef{}, fmt.Errorf("share URL provider = %s, want %s", ref.Provider, ShareProviderPan123)
	}
	return ref, nil
}

func (p *pan123ShareProvider) EnsureDir(ctx context.Context, path string) (string, error) {
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

func (p *pan123ShareProvider) ListShareChildren(ctx context.Context, ref ShareRef, parentID string) ([]ShareItem, error) {
	if fastLink, err := parsePan123FastLinkFile(ref.RawURL); err == nil {
		if parentID != "" && parentID != "0" {
			return nil, nil
		}
		return []ShareItem{fastLink.shareItem("0")}, nil
	}
	parentID = firstNonEmpty(parentID, "0")
	var resp pan123ListResp
	req := p.request(ctx).
		SetQueryParams(map[string]string{
			"limit":          "100",
			"next":           "0",
			"orderBy":        "file_name",
			"orderDirection": "asc",
			"shareKey":       ref.ShareID,
			"ParentFileId":   parentID,
			"Page":           "1",
			"event":          "homeListFile",
			"operateType":    "1",
		})
	if ref.Passcode != "" {
		req.SetQueryParam("SharePwd", ref.Passcode)
	}
	httpResp, err := req.Get(p.apiURL + "/share/get")
	if err != nil {
		return nil, err
	}
	if err := decodePan123JSON(httpResp, &resp); err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, pan123Error(resp.Message)
	}
	items := make([]ShareItem, 0, len(resp.Data.InfoList))
	for _, item := range resp.Data.InfoList {
		items = append(items, item.shareItem(parentID))
	}
	return items, nil
}

func (p *pan123ShareProvider) SaveShareItems(ctx context.Context, ref ShareRef, parentID string, items []ShareItem, dstDirID string) ([]string, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if p.cfg.AccessToken == "" {
		return nil, errors.New("123pan access_token is required")
	}
	for _, item := range items {
		if item.IsDir {
			return nil, errors.Errorf("123pan save directory is not supported yet: %s", item.Name)
		}
		raw := shareItemRawMap(item)
		etag := rawString(raw, "etag")
		name := firstNonEmpty(rawString(raw, "file_name"), item.Name)
		size := rawInt64(raw, "size", item.Size)
		var resp pan123UploadRequestResp
		httpResp, err := p.request(ctx).
			SetHeader("authorization", "Bearer "+p.cfg.AccessToken).
			SetBody(map[string]any{
				"driveId":      0,
				"etag":         etag,
				"fileName":     name,
				"parentFileId": dstDirID,
				"size":         size,
				"type":         0,
				"duplicate":    0,
			}).
			Post(p.apiURL + "/file/upload_request")
		if err != nil {
			return nil, err
		}
		if err := decodePan123JSON(httpResp, &resp); err != nil {
			return nil, err
		}
		if resp.Code != 0 {
			return nil, pan123Error(resp.Message)
		}
	}
	return []string{"pan123_sync_" + ref.ShareID}, nil
}

func (p *pan123ShareProvider) WaitSaveComplete(ctx context.Context, taskIDs []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (p *pan123ShareProvider) request(ctx context.Context) *resty.Request {
	return p.client.R().
		SetContext(ctx).
		SetHeader("origin", "https://yun.123pan.com").
		SetHeader("referer", "https://yun.123pan.com/").
		SetHeader("platform", "web").
		SetHeader("app-version", "3")
}

func decodePan123JSON(resp *resty.Response, out any) error {
	if resp == nil {
		return errors.New("empty 123pan response")
	}
	if err := json.Unmarshal(resp.Body(), out); err != nil {
		return errors.WithMessage(err, "decode 123pan response")
	}
	return nil
}

func pan123Error(message string) error {
	if strings.TrimSpace(message) == "" {
		return errors.New("123pan request failed")
	}
	return errors.New(message)
}

type pan123ListResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		InfoList []pan123File `json:"InfoList"`
		Next     string       `json:"Next"`
	} `json:"data"`
}

type pan123File struct {
	FileID   int64  `json:"FileId"`
	FileName string `json:"FileName"`
	Type     int    `json:"Type"`
	Size     int64  `json:"Size"`
	Etag     string `json:"Etag"`
	UpdateAt string `json:"UpdateAt"`
}

func (f pan123File) shareItem(parentID string) ShareItem {
	id := strconv.FormatInt(f.FileID, 10)
	isDir := f.Type == 1
	return ShareItem{
		ID:       id,
		ParentID: parentID,
		Name:     f.FileName,
		Size:     f.Size,
		Modified: parsePan123Time(f.UpdateAt),
		IsDir:    isDir,
		Raw: map[string]any{
			"file_id":   id,
			"etag":      f.Etag,
			"size":      f.Size,
			"file_name": f.FileName,
			"type":      f.Type,
		},
	}
}

type pan123UploadRequestResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Info struct {
			FileID int64 `json:"FileId"`
		} `json:"Info"`
	} `json:"data"`
}

func parsePan123Time(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed
	}
	if parsed, err := time.Parse("2006-01-02 15:04:05", value); err == nil {
		return parsed
	}
	return time.Time{}
}

func shareItemRawMap(item ShareItem) map[string]any {
	if raw, ok := item.Raw.(map[string]any); ok {
		return raw
	}
	return nil
}

func rawString(raw map[string]any, key string) string {
	if raw == nil {
		return ""
	}
	switch value := raw[key].(type) {
	case string:
		return strings.TrimSpace(value)
	case fmt.Stringer:
		return strings.TrimSpace(value.String())
	default:
		return ""
	}
}

func rawInt64(raw map[string]any, key string, fallback int64) int64 {
	if raw == nil {
		return fallback
	}
	switch value := raw[key].(type) {
	case int:
		return int64(value)
	case int64:
		return value
	case float64:
		return int64(value)
	case json.Number:
		parsed, err := value.Int64()
		if err == nil {
			return parsed
		}
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

var _ ShareSaver = (*pan123ShareProvider)(nil)

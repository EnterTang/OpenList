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

const quarkShareBaseURL = "https://drive.quark.cn/1/clouddrive"

type quarkShareProvider struct {
	cfg     model.SubscriptionTelegramPanConfig
	baseURL string
	client  *resty.Client
}

func NewQuarkShareProvider(cfg model.SubscriptionTelegramPanConfig) ShareSaver {
	return &quarkShareProvider{
		cfg:     normalizeTelegramPanConfig(cfg),
		baseURL: quarkShareBaseURL,
		client:  newShareHTTPClient(),
	}
}

func (p *quarkShareProvider) Name() ShareProviderName {
	return ShareProviderQuark
}

func (p *quarkShareProvider) ParseURL(raw string) (ShareRef, error) {
	ref, err := ParseShareURL(raw)
	if err != nil {
		return ShareRef{}, err
	}
	if ref.Provider != ShareProviderQuark {
		return ShareRef{}, fmt.Errorf("share URL provider = %s, want %s", ref.Provider, ShareProviderQuark)
	}
	return ref, nil
}

func (p *quarkShareProvider) EnsureDir(ctx context.Context, path string) (string, error) {
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

func (p *quarkShareProvider) ListShareChildren(ctx context.Context, ref ShareRef, parentID string) ([]ShareItem, error) {
	stoken, err := p.getSToken(ctx, ref)
	if err != nil {
		return nil, err
	}
	var resp quarkDetailResp
	req := p.request(ctx).
		SetQueryParams(map[string]string{
			"pr":                          "ucpro",
			"fr":                          "pc",
			"pwd_id":                      ref.ShareID,
			"stoken":                      stoken,
			"pdir_fid":                    parentID,
			"force":                       "0",
			"_page":                       "1",
			"_size":                       "50",
			"_fetch_banner":               "0",
			"_fetch_share":                "0",
			"_fetch_total":                "1",
			"_sort":                       "file_type:asc,updated_at:desc",
			"ver":                         "2",
			"fetch_share_full_path":       "0",
			"fetch_risk_file_name":        "1",
			"fetch_all_file":              "1",
			"support_thumbnail_category":  "1",
			"support_subtitle_category":   "1",
			"support_source_directory":    "1",
			"support_file_name_extension": "1",
		})
	httpResp, err := req.Get(p.baseURL + "/share/sharepage/detail")
	if err != nil {
		return nil, err
	}
	if err := decodeQuarkJSON(httpResp, &resp); err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, quarkError(resp.Message)
	}
	items := make([]ShareItem, 0, len(resp.Data.List))
	for _, item := range resp.Data.List {
		items = append(items, item.shareItem(parentID))
	}
	return items, nil
}

func (p *quarkShareProvider) SaveShareItems(ctx context.Context, ref ShareRef, parentID string, items []ShareItem, dstDirID string) ([]string, error) {
	if len(items) == 0 {
		return nil, nil
	}
	stoken, err := p.getSToken(ctx, ref)
	if err != nil {
		return nil, err
	}
	fids := make([]string, 0, len(items))
	tokens := make([]string, 0, len(items))
	for _, item := range items {
		fids = append(fids, item.ID)
		tokens = append(tokens, shareItemToken(item))
	}
	var resp quarkSaveResp
	req := p.request(ctx).
		SetQueryParams(map[string]string{
			"pr":           "ucpro",
			"fr":           "pc",
			"uc_param_str": "",
			"app":          "clouddrive",
			"__dt":         "180000",
			"__t":          strconv.FormatInt(time.Now().Unix(), 10),
		}).
		SetBody(map[string]any{
			"fid_list":       fids,
			"fid_token_list": tokens,
			"to_pdir_fid":    dstDirID,
			"pwd_id":         ref.ShareID,
			"stoken":         stoken,
			"pdir_fid":       parentID,
			"scene":          "link",
		})
	httpResp, err := req.Post(p.baseURL + "/share/sharepage/save")
	if err != nil {
		return nil, err
	}
	if err := decodeQuarkJSON(httpResp, &resp); err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, quarkError(resp.Message)
	}
	if resp.Data.TaskID == "" {
		return nil, nil
	}
	return []string{resp.Data.TaskID}, nil
}

func (p *quarkShareProvider) WaitSaveComplete(ctx context.Context, taskIDs []string) error {
	for _, taskID := range taskIDs {
		taskID = strings.TrimSpace(taskID)
		if taskID == "" {
			continue
		}
		if err := p.waitTask(ctx, taskID); err != nil {
			return err
		}
	}
	return nil
}

func (p *quarkShareProvider) waitTask(ctx context.Context, taskID string) error {
	var resp quarkTaskResp
	httpResp, err := p.request(ctx).
		SetQueryParams(map[string]string{
			"pr":           "ucpro",
			"fr":           "pc",
			"uc_param_str": "",
			"task_id":      taskID,
			"retry_index":  "0",
			"__dt":         "180000",
			"__t":          strconv.FormatInt(time.Now().Unix(), 10),
		}).
		Get(p.baseURL + "/task")
	if err != nil {
		return err
	}
	if err := decodeQuarkJSON(httpResp, &resp); err != nil {
		return err
	}
	if resp.Status >= 400 {
		return fmt.Errorf("quark task failed: status %d", resp.Status)
	}
	if resp.Data.Status != 0 && resp.Data.Status != 2 {
		return fmt.Errorf("quark task %s not complete: status %d", taskID, resp.Data.Status)
	}
	return nil
}

func (p *quarkShareProvider) getSToken(ctx context.Context, ref ShareRef) (string, error) {
	var resp quarkTokenResp
	req := p.request(ctx).
		SetQueryParams(map[string]string{"pr": "ucpro", "fr": "pc"}).
		SetBody(map[string]string{"pwd_id": ref.ShareID, "passcode": ref.Passcode})
	httpResp, err := req.Post(p.baseURL + "/share/sharepage/token")
	if err != nil {
		return "", err
	}
	if err := decodeQuarkJSON(httpResp, &resp); err != nil {
		return "", err
	}
	if resp.Code != 0 {
		return "", quarkError(resp.Message)
	}
	return resp.Data.SToken, nil
}

func (p *quarkShareProvider) request(ctx context.Context) *resty.Request {
	return p.client.R().
		SetContext(ctx).
		SetHeader("Cookie", p.cfg.Cookie).
		SetHeader("Accept", "application/json, text/plain, */*").
		SetHeader("Referer", "https://pan.quark.cn")
}

func quarkError(message string) error {
	if strings.TrimSpace(message) == "" {
		return errors.New("quark request failed")
	}
	return errors.New(message)
}

func decodeQuarkJSON(resp *resty.Response, out any) error {
	if resp == nil {
		return errors.New("empty quark response")
	}
	if err := json.Unmarshal(resp.Body(), out); err != nil {
		return errors.WithMessage(err, "decode quark response")
	}
	return nil
}

func shareItemToken(item ShareItem) string {
	if raw, ok := item.Raw.(map[string]any); ok {
		if token, ok := raw["share_fid_token"].(string); ok {
			return token
		}
	}
	return ""
}

type quarkTokenResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		SToken string `json:"stoken"`
	} `json:"data"`
}

type quarkDetailResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		List []quarkShareFile `json:"list"`
	} `json:"data"`
}

type quarkShareFile struct {
	FID           string `json:"fid"`
	ParentFID     string `json:"pdir_fid"`
	FileName      string `json:"file_name"`
	Dir           bool   `json:"dir"`
	Size          int64  `json:"size"`
	UpdatedAt     int64  `json:"updated_at"`
	ShareFIDToken string `json:"share_fid_token"`
}

func (f quarkShareFile) shareItem(parentID string) ShareItem {
	if f.ParentFID == "" {
		f.ParentFID = parentID
	}
	return ShareItem{
		ID:       f.FID,
		ParentID: f.ParentFID,
		Name:     f.FileName,
		Size:     f.Size,
		Modified: shareUnixMilli(f.UpdatedAt),
		IsDir:    f.Dir,
		Raw: map[string]any{
			"share_fid_token": f.ShareFIDToken,
		},
	}
}

type quarkSaveResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		TaskID string `json:"task_id"`
	} `json:"data"`
}

type quarkTaskResp struct {
	Status int `json:"status"`
	Data   struct {
		Status int `json:"status"`
	} `json:"data"`
}

func shareUnixMilli(value int64) time.Time {
	if value <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(value)
}

var _ ShareSaver = (*quarkShareProvider)(nil)

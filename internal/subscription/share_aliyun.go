package subscription

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
)

const (
	aliyunDriveAPIURL  = "https://api.aliyundrive.com"
	aliyunDriveAuthURL = "https://auth.aliyundrive.com"
	aliyunDriveUserURL = "https://user.aliyundrive.com"
	aliyunCanaryHeader = "client=web,app=share,version=v2.3.1"
	aliyunWebUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

type aliyunDriveShareProvider struct {
	cfg            model.SubscriptionTelegramPanConfig
	apiURL         string
	authURL        string
	userURL        string
	client         *resty.Client
	accessToken    string
	refreshToken   string
	defaultDriveID string
	driveType      string
}

func NewAliyunDriveShareProvider(cfg model.SubscriptionTelegramPanConfig) ShareSaver {
	cfg = normalizeTelegramPanConfig(cfg)
	return &aliyunDriveShareProvider{
		cfg:            cfg,
		apiURL:         aliyunDriveAPIURL,
		authURL:        aliyunDriveAuthURL,
		userURL:        aliyunDriveUserURL,
		client:         newShareHTTPClient(),
		accessToken:    cfg.AccessToken,
		refreshToken:   cfg.RefreshToken,
		defaultDriveID: cfg.DriveID,
		driveType:      cfg.DriveType,
	}
}

func (p *aliyunDriveShareProvider) Name() ShareProviderName {
	return ShareProviderAliyunDrive
}

func (p *aliyunDriveShareProvider) ParseURL(raw string) (ShareRef, error) {
	ref, err := ParseShareURL(raw)
	if err != nil {
		return ShareRef{}, err
	}
	if ref.Provider != ShareProviderAliyunDrive {
		return ShareRef{}, fmt.Errorf("share URL provider = %s, want %s", ref.Provider, ShareProviderAliyunDrive)
	}
	return ref, nil
}

func (p *aliyunDriveShareProvider) EnsureDir(ctx context.Context, path string) (string, error) {
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

func (p *aliyunDriveShareProvider) ListShareChildren(ctx context.Context, ref ShareRef, parentID string) ([]ShareItem, error) {
	shareToken, err := p.getShareToken(ctx, ref)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(parentID) == "" {
		parentID = "root"
	}
	var items []ShareItem
	marker := ""
	for {
		var resp aliyunListByShareResp
		httpResp, err := p.request(ctx).
			SetHeader("x-share-token", shareToken).
			SetBody(map[string]any{
				"share_id":        ref.ShareID,
				"parent_file_id":  parentID,
				"limit":           100,
				"marker":          marker,
				"order_by":        "name",
				"order_direction": "ASC",
			}).
			Post(p.apiURL + "/adrive/v2/file/list_by_share")
		if err != nil {
			return nil, err
		}
		if err := decodeAliyunJSON(httpResp, &resp); err != nil {
			return nil, err
		}
		if resp.Code != "" {
			return nil, aliyunError(resp.Message, resp.Code)
		}
		for _, item := range resp.Items {
			items = append(items, item.shareItem(parentID))
		}
		marker = strings.TrimSpace(resp.NextMarker)
		if marker == "" {
			break
		}
	}
	return items, nil
}

func (p *aliyunDriveShareProvider) SaveShareItems(ctx context.Context, ref ShareRef, parentID string, items []ShareItem, dstDirID string) ([]string, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if err := p.ensureAccessToken(ctx); err != nil {
		return nil, err
	}
	if p.defaultDriveID == "" {
		return nil, errors.New("aliyun drive target drive_id is required")
	}
	if strings.TrimSpace(dstDirID) == "" {
		return nil, errors.New("aliyun drive destination folder id is required")
	}
	shareToken, err := p.getShareToken(ctx, ref)
	if err != nil {
		return nil, err
	}
	requests := make([]aliyunBatchRequest, 0, len(items))
	for _, item := range items {
		fileID := strings.TrimSpace(item.ID)
		if fileID == "" {
			return nil, errors.Errorf("aliyun drive share item has no file id: %s", item.Name)
		}
		requests = append(requests, aliyunBatchRequest{
			ID:     fileID,
			Method: http.MethodPost,
			URL:    "/file/copy",
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: map[string]any{
				"file_id":           fileID,
				"share_id":          ref.ShareID,
				"to_parent_file_id": dstDirID,
				"to_drive_id":       p.defaultDriveID,
				"auto_rename":       true,
			},
		})
	}
	var resp aliyunBatchResp
	httpResp, err := p.request(ctx).
		SetHeader("Authorization", "Bearer "+p.accessToken).
		SetHeader("x-share-token", shareToken).
		SetBody(map[string]any{"requests": requests, "resource": "file"}).
		Post(p.apiURL + "/adrive/v2/batch")
	if err != nil {
		return nil, err
	}
	if err := decodeAliyunJSON(httpResp, &resp); err != nil {
		return nil, err
	}
	if resp.Code != "" {
		return nil, aliyunError(resp.Message, resp.Code)
	}
	savedFileIDs := make([]string, 0, len(resp.Responses))
	for _, item := range resp.Responses {
		if item.Status >= 400 {
			return nil, fmt.Errorf("aliyun save failed: status %d: %s", item.Status, firstNonEmpty(item.Body.Message, item.Body.Code))
		}
		if item.Body.Code != "" {
			return nil, aliyunError(item.Body.Message, item.Body.Code)
		}
		if item.Body.FileID != "" {
			savedFileIDs = append(savedFileIDs, item.Body.FileID)
		}
	}
	return savedFileIDs, nil
}

func (p *aliyunDriveShareProvider) WaitSaveComplete(ctx context.Context, taskIDs []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (p *aliyunDriveShareProvider) ensureAccessToken(ctx context.Context) error {
	if p.accessToken != "" && p.defaultDriveID != "" {
		return nil
	}
	if p.refreshToken == "" {
		if p.accessToken != "" && p.defaultDriveID == "" {
			return errors.New("aliyun drive refresh_token or drive_id is required for transfer")
		}
		return errors.New("aliyun drive refresh_token or access_token is required")
	}
	var resp aliyunRefreshResp
	httpResp, err := p.client.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetBody(map[string]string{
			"refresh_token": p.refreshToken,
			"grant_type":    "refresh_token",
		}).
		Post(p.authURL + "/v2/account/token")
	if err != nil {
		return err
	}
	if err := decodeAliyunJSON(httpResp, &resp); err != nil {
		return err
	}
	if resp.Code != "" {
		return aliyunError(resp.Message, resp.Code)
	}
	p.accessToken = firstNonEmpty(resp.AccessToken, p.accessToken)
	p.refreshToken = firstNonEmpty(resp.RefreshToken, p.refreshToken)
	driveInfo := aliyunDriveInfo{
		DefaultDriveID:  resp.DefaultDriveID,
		ResourceDriveID: resp.ResourceDriveID,
		BackupDriveID:   resp.BackupDriveID,
	}
	if p.defaultDriveID == "" {
		p.defaultDriveID = selectAliyunDriveID(p.driveType, driveInfo)
	}
	if p.accessToken == "" {
		return errors.New("aliyun drive access token is empty")
	}
	if p.defaultDriveID == "" {
		info, err := p.getUserDriveInfo(ctx)
		if err != nil {
			return err
		}
		p.defaultDriveID = selectAliyunDriveID(p.driveType, info)
	}
	if p.defaultDriveID == "" {
		return errors.New("aliyun drive target drive_id is empty")
	}
	return nil
}

func (p *aliyunDriveShareProvider) getUserDriveInfo(ctx context.Context) (aliyunDriveInfo, error) {
	var resp aliyunDriveInfo
	httpResp, err := p.client.R().
		SetContext(ctx).
		SetHeader("Accept", "application/json, text/plain, */*").
		SetHeader("Content-Type", "application/json").
		SetHeader("Authorization", "Bearer "+p.accessToken).
		SetHeader("Referer", "https://www.aliyundrive.com/").
		SetHeader("User-Agent", aliyunWebUserAgent).
		SetBody(map[string]any{}).
		Post(p.userURL + "/v2/user/get")
	if err != nil {
		return resp, err
	}
	if err := decodeAliyunJSON(httpResp, &resp); err != nil {
		return resp, err
	}
	if resp.Code != "" {
		return resp, aliyunError(resp.Message, resp.Code)
	}
	return resp, nil
}

func (p *aliyunDriveShareProvider) getShareToken(ctx context.Context, ref ShareRef) (string, error) {
	var resp aliyunShareTokenResp
	httpResp, err := p.request(ctx).
		SetBody(map[string]string{
			"share_id":  ref.ShareID,
			"share_pwd": ref.Passcode,
		}).
		Post(p.apiURL + "/v2/share_link/get_share_token")
	if err != nil {
		return "", err
	}
	if err := decodeAliyunJSON(httpResp, &resp); err != nil {
		return "", err
	}
	if resp.Code != "" {
		return "", aliyunError(resp.Message, resp.Code)
	}
	if resp.ShareToken == "" {
		return "", errors.New("aliyun drive share token is empty")
	}
	return resp.ShareToken, nil
}

func (p *aliyunDriveShareProvider) request(ctx context.Context) *resty.Request {
	return p.client.R().
		SetContext(ctx).
		SetHeader("Accept", "application/json, text/plain, */*").
		SetHeader("Content-Type", "application/json").
		SetHeader("Origin", "https://www.alipan.com").
		SetHeader("Referer", "https://www.alipan.com/").
		SetHeader("User-Agent", aliyunWebUserAgent).
		SetHeader("X-Canary", aliyunCanaryHeader)
}

func decodeAliyunJSON(resp *resty.Response, out any) error {
	if resp == nil {
		return errors.New("empty aliyun drive response")
	}
	if err := json.Unmarshal(resp.Body(), out); err != nil {
		return errors.WithMessage(err, "decode aliyun drive response")
	}
	return nil
}

func aliyunError(message, code string) error {
	message = strings.TrimSpace(message)
	code = strings.TrimSpace(code)
	if message == "" {
		message = "aliyun drive request failed"
	}
	if code == "" {
		return errors.New(message)
	}
	return fmt.Errorf("%s: %s", code, message)
}

type aliyunRefreshResp struct {
	Code            string `json:"code"`
	Message         string `json:"message"`
	AccessToken     string `json:"access_token"`
	RefreshToken    string `json:"refresh_token"`
	DefaultDriveID  string `json:"default_drive_id"`
	ResourceDriveID string `json:"resource_drive_id"`
	BackupDriveID   string `json:"backup_drive_id"`
}

type aliyunDriveInfo struct {
	Code            string `json:"code"`
	Message         string `json:"message"`
	DefaultDriveID  string `json:"default_drive_id"`
	ResourceDriveID string `json:"resource_drive_id"`
	BackupDriveID   string `json:"backup_drive_id"`
}

func selectAliyunDriveID(driveType string, info aliyunDriveInfo) string {
	switch strings.ToLower(strings.TrimSpace(driveType)) {
	case "default":
		return info.DefaultDriveID
	case "backup":
		return info.BackupDriveID
	case "resource":
		return info.ResourceDriveID
	default:
		return firstNonEmpty(info.ResourceDriveID, info.DefaultDriveID, info.BackupDriveID)
	}
}

type aliyunShareTokenResp struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	ShareToken string `json:"share_token"`
}

type aliyunListByShareResp struct {
	Code       string            `json:"code"`
	Message    string            `json:"message"`
	Items      []aliyunShareFile `json:"items"`
	NextMarker string            `json:"next_marker"`
}

type aliyunShareFile struct {
	FileID       string `json:"file_id"`
	ParentFileID string `json:"parent_file_id"`
	Name         string `json:"name"`
	Type         string `json:"type"`
	Size         int64  `json:"size"`
	UpdatedAt    string `json:"updated_at"`
}

func (f aliyunShareFile) shareItem(parentID string) ShareItem {
	if f.ParentFileID == "" {
		f.ParentFileID = parentID
	}
	return ShareItem{
		ID:       f.FileID,
		ParentID: f.ParentFileID,
		Name:     f.Name,
		Size:     f.Size,
		Modified: parseAliyunTime(f.UpdatedAt),
		IsDir:    f.Type == "folder",
		Raw: map[string]any{
			"share_fid_token": f.FileID,
		},
	}
}

type aliyunBatchRequest struct {
	Body    map[string]any    `json:"body"`
	Headers map[string]string `json:"headers"`
	ID      string            `json:"id"`
	Method  string            `json:"method"`
	URL     string            `json:"url"`
}

type aliyunBatchResp struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Responses []struct {
		Status int `json:"status"`
		Body   struct {
			Code    string `json:"code"`
			Message string `json:"message"`
			FileID  string `json:"file_id"`
		} `json:"body"`
	} `json:"responses"`
}

func parseAliyunTime(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

var _ ShareSaver = (*aliyunDriveShareProvider)(nil)

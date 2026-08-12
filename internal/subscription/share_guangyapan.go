package subscription

import (
	"context"
	"fmt"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/drivers/guangyapan"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
)

type guangyapanShareProvider struct {
	cfg    model.SubscriptionTelegramPanConfig
	driver *guangyapan.GuangYaPan
}

func NewGuangYaPanShareProvider(cfg model.SubscriptionTelegramPanConfig) ShareSaver {
	cfg = normalizeTelegramPanConfig(cfg)
	d := &guangyapan.GuangYaPan{}
	d.AccessToken = strings.TrimSpace(cfg.AccessToken)
	d.RefreshToken = strings.TrimSpace(cfg.RefreshToken)
	return &guangyapanShareProvider{cfg: cfg, driver: d}
}

func (p *guangyapanShareProvider) Name() ShareProviderName {
	return ShareProviderGuangYaPan
}

func (p *guangyapanShareProvider) ParseURL(raw string) (ShareRef, error) {
	ref, err := ParseShareURL(raw)
	if err != nil {
		return ShareRef{}, err
	}
	if ref.Provider != ShareProviderGuangYaPan {
		return ShareRef{}, fmt.Errorf("share URL provider = %s, want %s", ref.Provider, ShareProviderGuangYaPan)
	}
	return ref, nil
}

func (p *guangyapanShareProvider) EnsureDir(ctx context.Context, path string) (string, error) {
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
	if obj == nil {
		return "", errors.Errorf("temp transfer root missing: %s", path)
	}
	// Personal-disk / share restore root parentId is empty string (not "0").
	return strings.TrimSpace(obj.GetID()), nil
}

func (p *guangyapanShareProvider) ListShareChildren(ctx context.Context, ref ShareRef, parentID string) ([]ShareItem, error) {
	log.Infof("guangyapan ListShareChildren: shareID=%s, parentID=%q", ref.ShareID, parentID)
	if err := p.ensureDriver(ctx); err != nil {
		log.Warnf("guangyapan ListShareChildren: ensureDriver failed: %v", err)
		return nil, err
	}
	log.Infof("guangyapan ListShareChildren: calling GetShareAccessToken")
	token, err := p.driver.GetShareAccessToken(ctx, ref.ShareID, ref.Passcode)
	if err != nil {
		log.Warnf("guangyapan ListShareChildren: GetShareAccessToken failed: %v", err)
		return nil, err
	}
	parentID = strings.TrimSpace(parentID)
	log.Infof("guangyapan ListShareChildren: calling ListShareFiles with parentID=%q", parentID)
	files, err := p.driver.ListShareFiles(ctx, token, parentID)
	if err != nil {
		log.Warnf("guangyapan ListShareChildren: ListShareFiles failed: %v", err)
		return nil, err
	}
	log.Infof("guangyapan ListShareChildren: got %d files", len(files))
	items := make([]ShareItem, 0, len(files))
	for _, file := range files {
		items = append(items, ShareItem{
			ID:       file.FileID,
			ParentID: parentID,
			Name:     file.FileName,
			Size:     file.FileSize,
			IsDir:    file.IsFolder,
			Raw:      file,
		})
	}
	return items, nil
}

func (p *guangyapanShareProvider) SaveShareItems(ctx context.Context, ref ShareRef, parentID string, items []ShareItem, dstDirID string) ([]string, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if err := p.ensureDriver(ctx); err != nil {
		return nil, err
	}
	token, err := p.driver.GetShareAccessToken(ctx, ref.ShareID, ref.Passcode)
	if err != nil {
		return nil, err
	}
	fileIDs := make([]string, 0, len(items))
	for _, item := range items {
		id := strings.TrimSpace(item.ID)
		if id != "" {
			fileIDs = append(fileIDs, id)
		}
	}
	if len(fileIDs) == 0 {
		return nil, errors.New("no share file ids to restore")
	}
	dstDirID = strings.TrimSpace(dstDirID)
	taskID, err := p.driver.RestoreShare(ctx, token, fileIDs, dstDirID)
	if err != nil {
		return nil, err
	}
	if taskID == "" {
		return nil, nil
	}
	return []string{taskID}, nil
}

func (p *guangyapanShareProvider) WaitSaveComplete(ctx context.Context, taskIDs []string) error {
	if err := p.ensureDriver(ctx); err != nil {
		return err
	}
	for _, taskID := range taskIDs {
		if err := p.driver.WaitShareRestoreTask(ctx, taskID); err != nil {
			return err
		}
	}
	return nil
}

func (p *guangyapanShareProvider) ensureDriver(ctx context.Context) error {
	if p.driver == nil {
		return errors.New("guangyapan driver is nil")
	}
	if strings.TrimSpace(p.driver.AccessToken) == "" && strings.TrimSpace(p.driver.RefreshToken) == "" {
		return errors.New("guangyapan access_token/refresh_token is empty")
	}
	return p.driver.Init(ctx)
}

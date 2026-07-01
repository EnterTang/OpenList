package _139

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/etfmeta"
	"github.com/OpenListTeam/OpenList/v4/internal/media/recognize"
	"github.com/OpenListTeam/OpenList/v4/internal/media/tmdb"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

const etfVideoLinkType = "etf_video"

func (d *Yun139) ETFDownloadRestoreEnabled() bool {
	return d.Addition.Type == MetaPersonalNew && d.Addition.ETFDownloadRestore
}

func (d *Yun139) ETFPreviewName(ctx context.Context, file model.Obj) (string, error) {
	if file == nil || !etfmeta.IsName(file.GetName()) || !d.etfPreviewEnabled() {
		return "", nil
	}
	info, err := d.readPersonalETFInfo(ctx, file)
	if err != nil {
		return "", err
	}
	return info.Name, nil
}

func (d *Yun139) shouldPlayETF(file model.Obj, args model.LinkArgs) bool {
	return d.Addition.Type == MetaPersonalNew &&
		d.Addition.ETFVideoPlayback &&
		args.Type == etfVideoLinkType &&
		file != nil &&
		etfmeta.IsName(file.GetName())
}

func (d *Yun139) linkETFVideo(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	info, err := d.readPersonalETFInfo(ctx, file)
	if err != nil {
		return nil, err
	}
	tempParent, err := d.resolveETFTempFolderID(ctx)
	if err != nil {
		return nil, err
	}
	tempObj, _, err := d.personalRapidCreate(ctx, tempParent, info.Name, info.Size, info.SHA256)
	if err != nil {
		return nil, err
	}
	url, err := d.personalGetLink(tempObj.GetID())
	cleanupErr := d.removePersonalAndClean(ctx, tempObj)
	if err != nil {
		return nil, err
	}
	if cleanupErr != nil {
		return nil, cleanupErr
	}
	return &model.Link{URL: url}, nil
}

func (d *Yun139) restorePersonalFromETFUpload(ctx context.Context, dstDir model.Obj, stream model.FileStreamer) (bool, error) {
	if d.Addition.Type != MetaPersonalNew || !d.Addition.RestoreSourceFromETF || !etfmeta.IsName(stream.GetName()) {
		return false, nil
	}
	data, err := io.ReadAll(stream)
	if err != nil {
		return true, err
	}
	info, err := etfmeta.Decode(data)
	if err != nil {
		return true, err
	}
	name, err := etfmeta.ResolveRestoreName(stream.GetName(), info)
	if err != nil {
		return true, err
	}
	_, _, err = d.personalRapidCreate(ctx, dstDir.GetID(), name, info.Size, info.SHA256)
	return true, err
}

func (d *Yun139) afterPersonalUploadETF(ctx context.Context, dstDir model.Obj, sourceName string, size int64, sha256 string, uploadedObj model.Obj) error {
	if !d.shouldGenerateETF(sourceName) {
		return nil
	}
	info := &etfmeta.Info{
		Name:       sourceName,
		Size:       size,
		SHA256:     sha256,
		CreateTime: time.Now().Format(time.RFC3339),
	}
	content, err := etfmeta.Encode(info)
	if err != nil {
		return err
	}
	targetDir := dstDir
	if dir, err := d.resolveETFDirectory(ctx, dstDir, sourceName); err == nil && dir != nil {
		targetDir = dir
	}
	if err := d.uploadPersonalBytes(ctx, targetDir.GetID(), etfmeta.FileName(sourceName), content); err != nil {
		return err
	}
	if d.Addition.DeleteSourceAfterETF && uploadedObj != nil {
		return d.removePersonalAndClean(ctx, uploadedObj)
	}
	return nil
}

func (d *Yun139) shouldGenerateETF(sourceName string) bool {
	if d.Addition.Type != MetaPersonalNew || !d.Addition.GenerateETF || etfmeta.IsName(sourceName) {
		return false
	}
	allowlist := strings.TrimSpace(d.Addition.ETFExtAllowlist)
	if allowlist == "" {
		return true
	}
	ext := strings.TrimPrefix(strings.ToLower(path.Ext(sourceName)), ".")
	for _, item := range strings.FieldsFunc(allowlist, func(r rune) bool { return r == ',' || r == ';' || r == '，' || r == '\n' || r == ' ' || r == '\t' }) {
		if strings.TrimPrefix(strings.ToLower(strings.TrimSpace(item)), ".") == ext {
			return true
		}
	}
	return false
}

func (d *Yun139) shouldCleanAfterPersonalRemove(obj model.Obj) bool {
	return obj != nil &&
		etfmeta.IsName(obj.GetName()) &&
		(d.Addition.GenerateETF || d.Addition.RestoreSourceFromETF || d.Addition.ETFDownloadRestore || d.Addition.ETFVideoPlayback)
}

func (d *Yun139) resolveETFDirectory(ctx context.Context, dstDir model.Obj, sourceName string) (model.Obj, error) {
	targetDir := dstDir
	if configuredRoot := strings.TrimSpace(d.Addition.ETFRootFolder); configuredRoot != "" {
		dir, err := d.ensurePersonalConfiguredFolder(ctx, configuredRoot)
		if err != nil {
			return nil, err
		}
		targetDir = dir
	} else if legacyRoot := strings.TrimSpace(d.Addition.ETFRootFolderID); legacyRoot != "" {
		if looksLikeETFPath(legacyRoot) {
			dir, err := d.ensurePersonalConfiguredFolder(ctx, legacyRoot)
			if err != nil {
				return nil, err
			}
			targetDir = dir
		} else {
			targetDir = &model.Object{ID: legacyRoot, Name: path.Base(legacyRoot), IsFolder: true}
		}
	}
	parts := splitETFPath(d.Addition.ETFRootPath)
	if meta := d.resolveETFMediaMetadata(ctx, sourceName, dstDir.GetPath()); meta != nil {
		if meta.MediaType != "" {
			parts = append(parts, meta.MediaType)
		}
		if meta.Category != "" {
			parts = append(parts, meta.Category)
		}
	}
	if len(parts) == 0 {
		return targetDir, nil
	}
	return d.ensurePersonalFolderPath(ctx, targetDir.GetID(), strings.Join(parts, "/"))
}

func (d *Yun139) resolveETFMediaMetadata(ctx context.Context, sourceName, parentPath string) *tmdb.Metadata {
	apiKey := getSettingValue(conf.TMDBApiKey)
	if apiKey == "" {
		return nil
	}
	result := recognize.Recognize(sourceName, parentPath)
	meta, err := tmdb.Resolve(ctx, tmdb.Config{
		APIKey:        apiKey,
		BaseURL:       getSettingValue(conf.TMDBApiBaseURL),
		Language:      getSettingValue(conf.TMDBLanguage),
		CategoryRules: getSettingValue(conf.MediaCategoryRules),
	}, result)
	if err != nil {
		return nil
	}
	return meta
}

func (d *Yun139) uploadPersonalBytes(ctx context.Context, parentID, name string, content []byte) error {
	sum := sha256.Sum256(content)
	fullHash := strings.ToUpper(hex.EncodeToString(sum[:]))
	partInfos := d.buildPersonalUploadPartInfos(int64(len(content)))
	data := base.Json{
		"contentHash":          fullHash,
		"contentHashAlgorithm": "SHA256",
		"contentType":          "application/octet-stream",
		"parallelUpload":       false,
		"partInfos":            partInfos,
		"size":                 int64(len(content)),
		"parentFileId":         parentID,
		"name":                 name,
		"type":                 "file",
		"fileRenameMode":       "auto_rename",
	}
	var resp PersonalUploadResp
	if _, err := d.personalUploadPost("/file/create", data, &resp); err != nil {
		return err
	}
	if !personalUploadNeedsPartUpload(resp) {
		return nil
	}
	client := base.HttpClient
	if client == nil {
		client = http.DefaultClient
	}
	reader := bytes.NewReader(content)
	for _, partInfo := range resp.Data.PartInfos {
		uploadURL := partInfo.UploadUrl
		if uploadURL == "" {
			uploadURL = partInfo.CdnUploadUrl
		}
		if uploadURL == "" {
			return fmt.Errorf("part %d upload url is empty", partInfo.PartNumber)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadURL, io.NewSectionReader(reader, 0, int64(len(content))))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("User-Agent", personalUploadUserAgent)
		req.ContentLength = int64(len(content))
		res, err := client.Do(req)
		if err != nil {
			return err
		}
		if res.StatusCode != http.StatusOK {
			_ = res.Body.Close()
			return fmt.Errorf("unexpected status code: %d", res.StatusCode)
		}
		_ = res.Body.Close()
	}
	_, err := d.personalUploadPost("/file/complete", base.Json{
		"contentHash":          fullHash,
		"contentHashAlgorithm": "SHA256",
		"fileId":               resp.Data.FileId,
		"uploadId":             resp.Data.UploadId,
	}, nil)
	return err
}

func (d *Yun139) readPersonalETFInfo(ctx context.Context, file model.Obj) (*etfmeta.Info, error) {
	url, err := d.personalGetLink(file.GetID())
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
		return nil, fmt.Errorf("unexpected etf status code: %d", res.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, 1024*1024))
	if err != nil {
		return nil, err
	}
	return etfmeta.Decode(data)
}

func (d *Yun139) etfPreviewEnabled() bool {
	return d.Addition.Type == MetaPersonalNew && (d.Addition.ETFVideoPlayback || d.Addition.ETFDownloadRestore)
}

var etfSettingValue = defaultETFSettingValue

func getSettingValue(key string) string {
	return etfSettingValue(key)
}

func defaultETFSettingValue(key string) (value string) {
	defer func() {
		_ = recover()
	}()
	item, err := op.GetSettingItemByKey(key)
	if err != nil || item == nil {
		return ""
	}
	return strings.TrimSpace(item.Value)
}

var _ driver.ETFPreviewNamer = (*Yun139)(nil)
var _ driver.ETFDownloadRestoreController = (*Yun139)(nil)

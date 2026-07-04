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
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/etfmeta"
	"github.com/OpenListTeam/OpenList/v4/internal/media/recognize"
	"github.com/OpenListTeam/OpenList/v4/internal/media/tmdb"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

const etfVideoLinkType = "etf_video"

type etfArchivePlan struct {
	targetDir model.Obj
	meta      *tmdb.Metadata
	result    recognize.Result
	pathParts []string
}

type manualETFArchiveFile struct {
	obj      model.Obj
	dirPath  string
	fullPath string
	info     *etfmeta.Info
	content  []byte
}

var manualArchiveNumberPattern = regexp.MustCompile(`\d+`)

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
	etfName := etfmeta.FileName(sourceName)
	if err := d.uploadPersonalBytes(ctx, dstDir.GetID(), etfName, content); err != nil {
		return err
	}
	if d.Addition.ETFArchive {
		d.archivePersonalETF(ctx, dstDir, sourceName, etfName, size, sha256, content)
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
	plan, err := d.resolveETFArchivePlan(ctx, dstDir, sourceName)
	if err != nil {
		return nil, err
	}
	return plan.targetDir, nil
}

func (d *Yun139) resolveETFArchivePlan(ctx context.Context, dstDir model.Obj, sourceName string) (*etfArchivePlan, error) {
	targetDir, rootParts, err := d.resolveETFArchiveRoot(ctx, dstDir)
	if err != nil {
		return nil, err
	}
	result := recognize.Recognize(sourceName, dstDir.GetPath())
	var resolvedMeta *tmdb.Metadata
	if meta := d.resolveETFMediaMetadata(ctx, result); meta != nil {
		resolvedMeta = meta
	}
	parts := d.buildETFArchiveParts(result, resolvedMeta)
	plan := &etfArchivePlan{
		targetDir: targetDir,
		meta:      resolvedMeta,
		result:    result,
		pathParts: append(rootParts, parts...),
	}
	if len(parts) == 0 {
		return plan, nil
	}
	dir, err := d.ensurePersonalFolderPath(ctx, targetDir.GetID(), strings.Join(parts, "/"))
	if err != nil {
		return nil, err
	}
	plan.targetDir = dir
	return plan, nil
}

func (d *Yun139) resolveETFArchiveRoot(ctx context.Context, dstDir model.Obj) (model.Obj, []string, error) {
	targetDir := dstDir
	rootParts := []string{}
	if configuredRoot := strings.TrimSpace(d.Addition.ETFRootFolder); configuredRoot != "" {
		dir, err := d.ensurePersonalConfiguredFolder(ctx, configuredRoot)
		if err != nil {
			return nil, nil, err
		}
		targetDir = dir
		if !isETFRootPath(configuredRoot) {
			rootParts = splitETFPath(configuredRoot)
		}
	} else if legacyRoot := strings.TrimSpace(d.Addition.ETFRootFolderID); legacyRoot != "" {
		if looksLikeETFPath(legacyRoot) {
			dir, err := d.ensurePersonalConfiguredFolder(ctx, legacyRoot)
			if err != nil {
				return nil, nil, err
			}
			targetDir = dir
			if !isETFRootPath(legacyRoot) {
				rootParts = splitETFPath(legacyRoot)
			}
		} else {
			targetDir = &model.Object{ID: legacyRoot, Name: path.Base(legacyRoot), IsFolder: true}
		}
	}
	return targetDir, rootParts, nil
}

func (d *Yun139) buildETFArchiveParts(result recognize.Result, meta *tmdb.Metadata) []string {
	parts := splitETFPath(d.Addition.ETFRootPath)
	if meta == nil {
		return parts
	}
	if meta.MediaType != "" {
		parts = append(parts, meta.MediaType)
	}
	if meta.Category != "" {
		parts = append(parts, meta.Category)
	}
	if mediaFolder := etfMediaFolderName(meta); mediaFolder != "" {
		parts = append(parts, mediaFolder)
	}
	if meta.MediaType == "tv" {
		if seasonFolder := etfSeasonFolderName(result.Season); seasonFolder != "" {
			parts = append(parts, seasonFolder)
		}
	}
	return parts
}

func (d *Yun139) resolveETFMediaMetadata(ctx context.Context, result recognize.Result) *tmdb.Metadata {
	apiKey := getSettingValue(conf.TMDBApiKey)
	if apiKey == "" {
		return nil
	}
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

func etfMediaFolderName(meta *tmdb.Metadata) string {
	if meta == nil || strings.TrimSpace(meta.Name) == "" {
		return ""
	}
	name := sanitizeETFPathSegment(meta.Name)
	if meta.Year > 0 {
		name = fmt.Sprintf("%s (%d)", name, meta.Year)
	}
	if meta.TMDBID > 0 {
		name = fmt.Sprintf("%s {tmdb-%d}", name, meta.TMDBID)
	}
	return name
}

func etfSeasonFolderName(season int) string {
	if season <= 0 {
		return ""
	}
	return fmt.Sprintf("Season %d", season)
}

func sanitizeETFPathSegment(segment string) string {
	segment = strings.NewReplacer("/", " ", "\\", " ").Replace(segment)
	return strings.Join(strings.Fields(strings.TrimSpace(segment)), " ")
}

func (d *Yun139) archivePersonalETF(ctx context.Context, dstDir model.Obj, sourceName, etfName string, size int64, sha256 string, content []byte) {
	record := d.newETFArchiveRecord(dstDir, sourceName, etfName, size, sha256)
	plan, err := d.resolveETFArchivePlan(ctx, dstDir, sourceName)
	if err != nil {
		record.Status = model.ETFArchiveStatusFailed
		record.Error = err.Error()
		_ = saveETFArchiveRecord(record)
		return
	}
	d.applyETFArchivePlan(record, plan)
	if plan.meta == nil {
		record.Status = model.ETFArchiveStatusFailed
		record.Error = "tmdb metadata not matched"
		_ = saveETFArchiveRecord(record)
		return
	}
	if err := d.uploadPersonalBytes(ctx, plan.targetDir.GetID(), etfName, content); err != nil {
		record.Status = model.ETFArchiveStatusFailed
		record.Error = err.Error()
		_ = saveETFArchiveRecord(record)
		return
	}
	record.Status = model.ETFArchiveStatusArchived
	record.ArchiveETFPath = d.fullETFArchivePath(plan.pathParts, etfName)
	_ = saveETFArchiveRecord(record)
}

func (d *Yun139) PreviewManualETFArchive(ctx context.Context, folderPath string, metadata model.ETFManualArchiveMetadata) (*model.ETFManualArchivePreview, error) {
	meta, err := d.manualArchiveMetadata(metadata)
	if err != nil {
		return nil, err
	}
	season := manualArchiveSeason(metadata, meta)
	folderPath = cleanActualPath(folderPath)
	folderObj, err := op.GetUnwrap(ctx, d, folderPath)
	if err != nil {
		return nil, err
	}
	if !folderObj.IsDir() {
		return nil, fmt.Errorf("manual ETF archive target is not a folder")
	}
	files, err := d.collectManualETFFiles(ctx, folderObj, folderPath)
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool {
		return strings.ToLower(files[i].fullPath) < strings.ToLower(files[j].fullPath)
	})
	archiveParts := d.manualArchivePathParts(meta, season)
	targetFolder := etfMediaFolderName(meta)
	if targetFolder == "" {
		targetFolder = sanitizeETFPathSegment(meta.Name)
	}
	parentPath := path.Dir(folderPath)
	if parentPath == "." {
		parentPath = "/"
	}
	renamedFolderPath := path.Join(parentPath, targetFolder)
	items := make([]model.ETFManualArchiveItem, 0, len(files))
	nextEpisode := metadata.StartEpisode
	if nextEpisode <= 0 {
		nextEpisode = 1
	}
	for _, file := range files {
		episode := manualArchiveEpisode(file, nextEpisode)
		if episode <= 0 {
			episode = nextEpisode
		}
		nextEpisode = episode + 1
		newName := d.manualArchiveETFName(meta, season, episode, file)
		relDir := strings.TrimPrefix(file.dirPath, folderPath)
		relDir = strings.Trim(relDir, "/")
		newDir := renamedFolderPath
		if relDir != "" {
			newDir = path.Join(newDir, relDir)
		}
		archivePath := d.fullETFArchivePath(archiveParts, newName)
		items = append(items, model.ETFManualArchiveItem{
			OriginalName: file.obj.GetName(),
			NewName:      newName,
			OriginalPath: d.fullETFPath(file.dirPath, file.obj.GetName()),
			NewPath:      d.fullETFPath(newDir, newName),
			ArchivePath:  archivePath,
			SourceName:   file.info.Name,
			SourceSize:   file.info.Size,
			SourceSHA256: strings.ToUpper(file.info.SHA256),
			Season:       season,
			Episode:      episode,
		})
	}
	return &model.ETFManualArchivePreview{
		SourcePath:       d.fullETFPath(folderPath, ""),
		TargetFolderName: targetFolder,
		ArchiveRoot:      d.etfArchiveRootLabel(),
		ArchiveDirPath:   d.fullETFArchivePath(archiveParts, ""),
		Items:            items,
	}, nil
}

func (d *Yun139) ApplyManualETFArchive(ctx context.Context, folderPath string, metadata model.ETFManualArchiveMetadata, _ []model.ETFManualArchiveItem) (*model.ETFManualArchivePreview, error) {
	meta, err := d.manualArchiveMetadata(metadata)
	if err != nil {
		return nil, err
	}
	season := manualArchiveSeason(metadata, meta)
	preview, err := d.PreviewManualETFArchive(ctx, folderPath, metadata)
	if err != nil {
		return nil, err
	}
	folderPath = cleanActualPath(folderPath)
	folderObj, err := op.GetUnwrap(ctx, d, folderPath)
	if err != nil {
		return nil, err
	}
	files, err := d.collectManualETFFiles(ctx, folderObj, folderPath)
	if err != nil {
		return nil, err
	}
	filesByPath := make(map[string]manualETFArchiveFile, len(files))
	for _, file := range files {
		filesByPath[d.fullETFPath(file.dirPath, file.obj.GetName())] = file
	}
	targetDir, _, err := d.manualArchiveTarget(ctx, meta, season)
	if err != nil {
		return nil, err
	}
	for _, item := range preview.Items {
		file, ok := filesByPath[item.OriginalPath]
		if !ok {
			return nil, fmt.Errorf("ETF file not found: %s", item.OriginalPath)
		}
		if err := d.Rename(ctx, file.obj, item.NewName); err != nil {
			return nil, err
		}
		if err := d.uploadPersonalBytes(ctx, targetDir.GetID(), item.NewName, file.content); err != nil {
			return nil, err
		}
		_ = saveETFArchiveRecord(&model.ETFArchiveRecord{
			StorageID:        d.GetStorage().ID,
			StorageMountPath: d.GetStorage().MountPath,
			SourceName:       item.SourceName,
			SourcePath:       item.NewPath,
			LocalETFPath:     item.NewPath,
			ArchiveETFPath:   item.ArchivePath,
			ArchiveRoot:      d.etfArchiveRootLabel(),
			ArchiveEnabled:   true,
			TMDBMatched:      true,
			TMDBID:           meta.TMDBID,
			TMDBName:         meta.Name,
			TMDBYear:         meta.Year,
			MediaType:        meta.MediaType,
			Category:         meta.Category,
			Season:           season,
			Episode:          item.Episode,
			SourceSize:       item.SourceSize,
			SourceSHA256:     item.SourceSHA256,
			Status:           model.ETFArchiveStatusArchived,
		})
	}
	if folderObj.GetName() != preview.TargetFolderName {
		if err := d.Rename(ctx, folderObj, preview.TargetFolderName); err != nil {
			return nil, err
		}
	}
	return preview, nil
}

func saveETFArchiveRecord(record *model.ETFArchiveRecord) error {
	if db.GetDb() == nil {
		return nil
	}
	return db.CreateETFArchiveRecord(record)
}

func (d *Yun139) manualArchiveMetadata(metadata model.ETFManualArchiveMetadata) (*tmdb.Metadata, error) {
	name := strings.TrimSpace(metadata.Name)
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	mediaType := strings.TrimSpace(metadata.MediaType)
	if mediaType == "" {
		if metadata.Season > 0 {
			mediaType = "tv"
		} else {
			mediaType = "movie"
		}
	}
	return &tmdb.Metadata{
		MediaType: mediaType,
		TMDBID:    metadata.TMDBID,
		Name:      name,
		Year:      metadata.Year,
		Category:  strings.TrimSpace(metadata.Category),
	}, nil
}

func manualArchiveSeason(metadata model.ETFManualArchiveMetadata, meta *tmdb.Metadata) int {
	season := metadata.Season
	if meta != nil && meta.MediaType == "tv" && season <= 0 {
		return 1
	}
	return season
}

func (d *Yun139) manualArchivePathParts(meta *tmdb.Metadata, season int) []string {
	rootParts := d.configuredETFRootParts()
	parts := d.buildETFArchiveParts(recognize.Result{Season: season}, meta)
	return append(rootParts, parts...)
}

func (d *Yun139) configuredETFRootParts() []string {
	if configuredRoot := strings.TrimSpace(d.Addition.ETFRootFolder); configuredRoot != "" && !isETFRootPath(configuredRoot) {
		return splitETFPath(configuredRoot)
	}
	if legacyRoot := strings.TrimSpace(d.Addition.ETFRootFolderID); legacyRoot != "" && looksLikeETFPath(legacyRoot) && !isETFRootPath(legacyRoot) {
		return splitETFPath(legacyRoot)
	}
	return nil
}

func (d *Yun139) manualArchiveTarget(ctx context.Context, meta *tmdb.Metadata, season int) (model.Obj, []string, error) {
	root, rootParts, err := d.resolveETFArchiveRoot(ctx, d.personalRootFolder())
	if err != nil {
		return nil, nil, err
	}
	result := recognize.Result{Season: season}
	parts := d.buildETFArchiveParts(result, meta)
	targetDir := root
	if len(parts) > 0 {
		targetDir, err = d.ensurePersonalFolderPath(ctx, root.GetID(), strings.Join(parts, "/"))
		if err != nil {
			return nil, nil, err
		}
	}
	return targetDir, append(rootParts, parts...), nil
}

func (d *Yun139) collectManualETFFiles(ctx context.Context, dir model.Obj, dirPath string) ([]manualETFArchiveFile, error) {
	objs, err := d.List(ctx, dir, model.ListArgs{Refresh: true})
	if err != nil {
		return nil, err
	}
	var files []manualETFArchiveFile
	for _, obj := range objs {
		childPath := path.Join(dirPath, obj.GetName())
		if obj.IsDir() {
			childFiles, err := d.collectManualETFFiles(ctx, obj, childPath)
			if err != nil {
				return nil, err
			}
			files = append(files, childFiles...)
			continue
		}
		if !etfmeta.IsName(obj.GetName()) {
			continue
		}
		content, info, err := d.readPersonalETFContent(ctx, obj)
		if err != nil {
			return nil, err
		}
		files = append(files, manualETFArchiveFile{
			obj:      obj,
			dirPath:  childPathDir(childPath),
			fullPath: childPath,
			info:     info,
			content:  content,
		})
	}
	return files, nil
}

func childPathDir(p string) string {
	dir := path.Dir(p)
	if dir == "." {
		return "/"
	}
	return dir
}

func manualArchiveEpisode(file manualETFArchiveFile, fallback int) int {
	if season, episode := recognize.ExtractSeasonEpisode(file.info.Name); season > 0 || episode > 0 {
		if episode > 0 {
			return episode
		}
	}
	name := strings.TrimSuffix(file.obj.GetName(), filepath.Ext(file.obj.GetName()))
	name = strings.TrimSuffix(name, filepath.Ext(name))
	matches := manualArchiveNumberPattern.FindAllString(name, -1)
	if len(matches) > 0 {
		if n, err := strconv.Atoi(matches[len(matches)-1]); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func (d *Yun139) manualArchiveETFName(meta *tmdb.Metadata, season int, episode int, file manualETFArchiveFile) string {
	sourceName := strings.TrimSpace(file.info.Name)
	if sourceName == "" {
		sourceName = strings.TrimSuffix(file.obj.GetName(), filepath.Ext(file.obj.GetName()))
	}
	mediaExt := filepath.Ext(sourceName)
	title := sanitizeETFPathSegment(meta.Name)
	if meta.MediaType == "tv" {
		if season <= 0 {
			season = 1
		}
		if meta.Year > 0 {
			return fmt.Sprintf("%s.%d.S%02dE%02d.第%d集%s.etf", title, meta.Year, season, episode, episode, mediaExt)
		}
		return fmt.Sprintf("%s.S%02dE%02d.第%d集%s.etf", title, season, episode, episode, mediaExt)
	}
	if meta.Year > 0 {
		return fmt.Sprintf("%s.%d%s.etf", title, meta.Year, mediaExt)
	}
	return fmt.Sprintf("%s%s.etf", title, mediaExt)
}

func cleanActualPath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return "/"
	}
	return path.Clean("/" + strings.TrimLeft(p, "/"))
}

func (d *Yun139) newETFArchiveRecord(dstDir model.Obj, sourceName, etfName string, size int64, sha256 string) *model.ETFArchiveRecord {
	storage := d.GetStorage()
	return &model.ETFArchiveRecord{
		StorageID:        storage.ID,
		StorageMountPath: storage.MountPath,
		SourceName:       sourceName,
		SourcePath:       d.fullETFPath(dstDir.GetPath(), sourceName),
		LocalETFPath:     d.fullETFPath(dstDir.GetPath(), etfName),
		ArchiveRoot:      d.etfArchiveRootLabel(),
		ArchiveEnabled:   true,
		SourceSize:       size,
		SourceSHA256:     strings.ToUpper(strings.TrimSpace(sha256)),
		Status:           model.ETFArchiveStatusFailed,
	}
}

func (d *Yun139) etfArchiveRootLabel() string {
	if root := strings.TrimSpace(d.Addition.ETFRootFolder); root != "" {
		return root
	}
	if legacyRoot := strings.TrimSpace(d.Addition.ETFRootFolderID); legacyRoot != "" {
		return legacyRoot
	}
	return strings.TrimSpace(d.Addition.ETFRootPath)
}

func (d *Yun139) applyETFArchivePlan(record *model.ETFArchiveRecord, plan *etfArchivePlan) {
	if record == nil || plan == nil {
		return
	}
	record.Season = plan.result.Season
	record.Episode = plan.result.Episode
	if plan.meta == nil {
		record.TMDBMatched = false
		return
	}
	record.TMDBMatched = true
	record.TMDBID = plan.meta.TMDBID
	record.TMDBName = plan.meta.Name
	record.TMDBYear = plan.meta.Year
	record.MediaType = plan.meta.MediaType
	record.Category = plan.meta.Category
}

func (d *Yun139) fullETFPath(dirPath, name string) string {
	storage := d.GetStorage()
	parts := []string{storage.MountPath}
	if strings.TrimSpace(dirPath) != "" {
		parts = append(parts, splitETFPath(dirPath)...)
	}
	parts = append(parts, name)
	return path.Join(parts...)
}

func (d *Yun139) fullETFArchivePath(pathParts []string, name string) string {
	storage := d.GetStorage()
	parts := append([]string{storage.MountPath}, pathParts...)
	parts = append(parts, name)
	return path.Join(parts...)
}

func (d *Yun139) CorrectETFArchive(ctx context.Context, record *model.ETFArchiveRecord, correction model.ETFArchiveCorrection) (*model.ETFArchiveRecord, error) {
	if record == nil {
		return nil, fmt.Errorf("archive record is nil")
	}
	meta := &tmdb.Metadata{
		MediaType: strings.TrimSpace(correction.MediaType),
		TMDBID:    correction.TMDBID,
		Name:      strings.TrimSpace(correction.TMDBName),
		Year:      correction.TMDBYear,
		Category:  strings.TrimSpace(correction.Category),
	}
	if meta.Name == "" {
		return nil, fmt.Errorf("tmdb_name is required")
	}
	season := correction.Season
	episode := correction.Episode
	if season <= 0 || episode <= 0 {
		recognized := recognize.Recognize(record.SourceName, record.SourcePath)
		if season <= 0 {
			season = recognized.Season
		}
		if episode <= 0 {
			episode = recognized.Episode
		}
	}
	if meta.MediaType == "" {
		if season > 0 {
			meta.MediaType = "tv"
		} else {
			meta.MediaType = "movie"
		}
	}
	result := recognize.Result{Season: season, Episode: episode}
	root, rootParts, err := d.resolveETFArchiveRoot(ctx, d.personalRootFolder())
	if err != nil {
		return nil, err
	}
	parts := d.buildETFArchiveParts(result, meta)
	targetDir := root
	if len(parts) > 0 {
		targetDir, err = d.ensurePersonalFolderPath(ctx, root.GetID(), strings.Join(parts, "/"))
		if err != nil {
			return nil, err
		}
	}
	info := &etfmeta.Info{
		Name:       record.SourceName,
		Size:       record.SourceSize,
		SHA256:     record.SourceSHA256,
		CreateTime: time.Now().Format(time.RFC3339),
	}
	content, err := etfmeta.Encode(info)
	if err != nil {
		return nil, err
	}
	etfName := etfmeta.FileName(record.SourceName)
	if err := d.uploadPersonalBytes(ctx, targetDir.GetID(), etfName, content); err != nil {
		return nil, err
	}
	_ = d.removeArchivedETFByPath(ctx, record.ArchiveETFPath)
	plan := &etfArchivePlan{
		targetDir: targetDir,
		meta:      meta,
		result:    result,
		pathParts: append(rootParts, parts...),
	}
	d.applyETFArchivePlan(record, plan)
	record.ArchiveEnabled = true
	record.ArchiveRoot = d.etfArchiveRootLabel()
	record.ArchiveETFPath = d.fullETFArchivePath(plan.pathParts, etfName)
	record.Status = model.ETFArchiveStatusCorrected
	record.Error = ""
	return record, db.UpdateETFArchiveRecord(record)
}

func (d *Yun139) removeArchivedETFByPath(ctx context.Context, fullPath string) error {
	fullPath = strings.TrimSpace(fullPath)
	if fullPath == "" {
		return nil
	}
	mountPath := d.GetStorage().MountPath
	actualPath := strings.TrimPrefix(fullPath, mountPath)
	actualPath = "/" + strings.TrimLeft(actualPath, "/")
	obj, err := op.GetUnwrap(ctx, d, actualPath)
	if err != nil {
		return nil
	}
	return d.Remove(ctx, obj)
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
	_, info, err := d.readPersonalETFContent(ctx, file)
	return info, err
}

func (d *Yun139) readPersonalETFContent(ctx context.Context, file model.Obj) ([]byte, *etfmeta.Info, error) {
	url, err := d.personalGetLink(file.GetID())
	if err != nil {
		return nil, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, err
	}
	client := base.HttpClient
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("unexpected etf status code: %d", res.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, 1024*1024))
	if err != nil {
		return nil, nil, err
	}
	info, err := etfmeta.Decode(data)
	if err != nil {
		return nil, nil, err
	}
	return data, info, nil
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

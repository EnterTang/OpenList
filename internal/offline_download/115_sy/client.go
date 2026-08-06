package _115_sy

import (
	"context"
	"errors"
	"fmt"

	syDriver "github.com/OpenListTeam/OpenList/v4/drivers/115_sy"
	sy "github.com/OpenListTeam/OpenList/v4/internal/115sy"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/offline_download/tool"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/setting"
)

type Cloud115SY struct{}

func (p *Cloud115SY) Name() string { return "115 SY" }

func (p *Cloud115SY) Items() []model.SettingItem { return nil }

func (p *Cloud115SY) Run(*tool.DownloadTask) error { return errs.NotSupport }

func (p *Cloud115SY) Init() (string, error) { return "ok", nil }

func (p *Cloud115SY) IsReady() bool {
	tempDir := setting.GetStr(conf.Pan115SYTempDir)
	if tempDir == "" {
		return false
	}
	storage, _, err := op.GetStorageAndActualPath(tempDir)
	if err != nil {
		return false
	}
	_, ok := storage.(*syDriver.Pan115SY)
	return ok
}

func (p *Cloud115SY) AddURL(args *tool.AddUrlArgs) (string, error) {
	storage, actualPath, err := op.GetStorageAndActualPath(args.TempDir)
	if err != nil {
		return "", err
	}
	driver115, ok := storage.(*syDriver.Pan115SY)
	if !ok {
		return "", errors.New("unsupported storage driver for offline download, only 115 SY is supported")
	}
	if err := op.MakeDir(args.Ctx, storage, actualPath); err != nil {
		return "", err
	}
	parentDir, err := op.GetUnwrap(args.Ctx, storage, actualPath)
	if err != nil {
		return "", err
	}
	ids, err := driver115.OfflineDownload(args.Ctx, []string{args.Url}, parentDir)
	if err != nil || len(ids) == 0 {
		if err == nil {
			err = errors.New("115 SY returned no offline task id")
		}
		return "", fmt.Errorf("failed to add offline download task: %w", err)
	}
	return ids[0], nil
}

func (p *Cloud115SY) Remove(task *tool.DownloadTask) error {
	storage, _, err := op.GetStorageAndActualPath(task.TempDir)
	if err != nil {
		return err
	}
	driver115, ok := storage.(*syDriver.Pan115SY)
	if !ok {
		return errors.New("unsupported storage driver for offline download, only 115 SY is supported")
	}
	return driver115.DeleteOfflineTasks(context.Background(), []string{task.GID}, false)
}

func (p *Cloud115SY) Status(task *tool.DownloadTask) (*tool.Status, error) {
	storage, _, err := op.GetStorageAndActualPath(task.TempDir)
	if err != nil {
		return nil, err
	}
	driver115, ok := storage.(*syDriver.Pan115SY)
	if !ok {
		return nil, errors.New("unsupported storage driver for offline download, only 115 SY is supported")
	}
	tasks, err := driver115.OfflineList(context.Background())
	if err != nil {
		return nil, err
	}
	status := &tool.Status{Status: "the task has been deleted"}
	for _, item := range tasks {
		if item.ID != task.GID && item.TaskID != task.GID && item.InfoHash != task.GID {
			continue
		}
		status.Progress = item.Progress
		status.TotalBytes = item.Size
		status.Completed = item.Done()
		status.Status = offlineStatus(item)
		if item.Failed() {
			status.Err = errors.New(status.Status)
		}
		return status, nil
	}
	status.Err = errors.New("the task has been deleted")
	return status, nil
}

func offlineStatus(task sy.OfflineTask) string {
	if task.Done() {
		return "离线下载完成"
	}
	if task.Failed() {
		return "离线下载失败"
	}
	if task.Status == 0 {
		return "准备开始离线下载"
	}
	return "离线任务下载中"
}

var _ tool.Tool = (*Cloud115SY)(nil)

func init() { tool.Tools.Add(&Cloud115SY{}) }

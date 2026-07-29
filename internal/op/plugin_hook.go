package op

import (
	"context"
	stdpath "path"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/plugin"
	log "github.com/sirupsen/logrus"
)

func pluginProcessOptions() plugin.ProcessOptions {
	return plugin.ProcessOptions{
		AntiHash:  settingBool(conf.PluginAntiHashEnabled),
		ISORename: settingBool(conf.PluginISORenameEnabled),
		Whitelist: settingStr(conf.PluginExtensionWhitelist),
	}
}

func maybeProcessLocalPlugin(ctx context.Context, storage driver.Driver, actualFilePath string) {
	if ctx.Value(conf.SkipPluginKey) != nil || storage == nil {
		return
	}
	if storage.Config().Name != "Local" {
		return
	}
	opts := pluginProcessOptions()
	if !opts.AntiHash && !opts.ISORename {
		return
	}
	actualFilePath = stdpath.Clean(actualFilePath)
	bg := context.WithoutCancel(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("[plugin] panic processing %s: %v", actualFilePath, r)
			}
		}()
		obj, err := GetUnwrap(bg, storage, actualFilePath)
		if err != nil {
			log.Warnf("[plugin] get %s: %v", actualFilePath, err)
			return
		}
		abs := obj.GetPath()
		if abs == "" {
			return
		}
		if _, err := plugin.ProcessAbsolutePath(abs, opts); err != nil {
			log.Errorf("[plugin] process %s: %v", abs, err)
		}
	}()
}

func maybeProcessUploadPlugin(ctx context.Context, storage driver.Driver, file model.FileStreamer) (model.FileStreamer, error) {
	if ctx.Value(conf.SkipPluginKey) != nil || storage == nil || file == nil {
		return file, nil
	}
	if storage.Config().Name != "139Yun" {
		return file, nil
	}
	opts := pluginProcessOptions()
	if !opts.AntiHash && !opts.ISORename {
		return file, nil
	}
	out, err := plugin.ProcessStreamer(file, opts)
	if err != nil {
		return file, err
	}
	return out, nil
}

func settingBool(key string) bool {
	item, _ := GetSettingItemByKey(key)
	if item == nil {
		return false
	}
	return item.Value == "true" || item.Value == "1"
}

func settingStr(key string) string {
	item, _ := GetSettingItemByKey(key)
	if item == nil {
		return ""
	}
	return item.Value
}

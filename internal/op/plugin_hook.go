package op

import (
	"context"
	stdpath "path"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/plugin"
	log "github.com/sirupsen/logrus"
)

func maybeProcessLocalPlugin(ctx context.Context, storage driver.Driver, actualFilePath string) {
	if ctx.Value(conf.SkipPluginKey) != nil || storage == nil {
		return
	}
	if storage.Config().Name != "Local" {
		return
	}
	anti := settingBool(conf.PluginAntiHashEnabled)
	iso := settingBool(conf.PluginISORenameEnabled)
	if !anti && !iso {
		return
	}
	whitelist := settingStr(conf.PluginExtensionWhitelist)
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
		if _, err := plugin.ProcessAbsolutePath(abs, plugin.ProcessOptions{
			AntiHash: anti, ISORename: iso, Whitelist: whitelist,
		}); err != nil {
			log.Errorf("[plugin] process %s: %v", abs, err)
		}
	}()
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

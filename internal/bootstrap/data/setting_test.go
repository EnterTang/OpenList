package data

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestTMDBSettingsAreEditableFromGlobalSettings(t *testing.T) {
	previousConfig := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = previousConfig })

	want := map[string]bool{
		conf.TMDBApiKey:     false,
		conf.TMDBApiBaseURL: false,
		conf.TMDBLanguage:   false,
	}

	for _, setting := range InitialSettings() {
		if _, ok := want[setting.Key]; !ok {
			continue
		}

		if setting.Group != model.GLOBAL {
			t.Errorf("%s: group = %d, want global", setting.Key, setting.Group)
		}
		if setting.Type != conf.TypeString {
			t.Errorf("%s: type = %q, want string", setting.Key, setting.Type)
		}
		if setting.Flag == model.READONLY || setting.Flag == model.DEPRECATED {
			t.Errorf("%s: flag = %d, want editable", setting.Key, setting.Flag)
		}
		if setting.Help == "" {
			t.Errorf("%s: help is empty", setting.Key)
		}

		want[setting.Key] = true
	}

	for key, found := range want {
		if !found {
			t.Errorf("%s: setting is missing", key)
		}
	}
}

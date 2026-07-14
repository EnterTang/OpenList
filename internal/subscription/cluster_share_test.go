package subscription

import (
	"context"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/stretchr/testify/require"
)

func TestSaveClusterShareSelectionBindsSelectedCredentialForAbsoluteStagingRoot(t *testing.T) {
	tests := []struct {
		name               string
		provider           ShareProviderName
		shareURL           string
		driver             string
		broadMount         string
		selectedMount      string
		globalCredential   string
		broadCredential    string
		selectedCredential string
		broadAddition      string
		selectedAddition   string
	}{
		{
			name:               "pan123 access token",
			provider:           ShareProviderPan123,
			shareURL:           "https://www.123pan.com/s/cluster-share-selected?pwd=xoxo",
			driver:             "123Pan",
			broadMount:         "/cluster-share-pan123",
			selectedMount:      "/cluster-share-pan123/selected",
			globalCredential:   "global-token",
			broadCredential:    "broad-token",
			selectedCredential: "selected-token",
			broadAddition:      `{"AccessToken":"broad-token"}`,
			selectedAddition:   `{"AccessToken":"selected-token"}`,
		},
		{
			name:               "pan115 cookie",
			provider:           ShareProviderPan115,
			shareURL:           "https://115cdn.com/s/cluster-share-selected?password=t58d",
			driver:             "115 Cloud",
			broadMount:         "/cluster-share-pan115",
			selectedMount:      "/cluster-share-pan115/selected",
			globalCredential:   "global-cookie",
			broadCredential:    "broad-cookie",
			selectedCredential: "selected-cookie",
			broadAddition:      `{"Cookie":"broad-cookie"}`,
			selectedAddition:   `{"Cookie":"selected-cookie"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setupSubscriptionRuntimeDB(t)

			cfg := model.SubscriptionConfig{}
			switch tc.provider {
			case ShareProviderPan123:
				cfg.Telegram.Pan123.AccessToken = tc.globalCredential
			case ShareProviderPan115:
				cfg.Telegram.Pan115.Cookie = tc.globalCredential
			}
			_, err := SaveConfig(cfg)
			require.NoError(t, err)

			for _, storage := range []*model.Storage{
				{MountPath: tc.broadMount, Driver: tc.driver, Status: "work", Addition: tc.broadAddition},
				{MountPath: tc.selectedMount, Driver: tc.driver, Status: "work", Addition: tc.selectedAddition},
			} {
				require.NoError(t, db.CreateStorage(storage))
			}

			const (
				fileID   = "selected-file"
				fileName = "Selected.Movie.mkv"
			)
			stagingRoot := tc.selectedMount + "/临时转存"
			saver := &fakeShareSaver{
				fakeShareTreeProvider: fakeShareTreeProvider{
					name: tc.provider,
					children: map[string][]ShareItem{
						"": {{ID: fileID, Name: fileName}},
					},
				},
				dstDirID: "selected-dst-dir",
			}

			oldFactory := newShareSaverForProvider
			t.Cleanup(func() { newShareSaverForProvider = oldFactory })
			var factoryCalls int
			var factoryProvider ShareProviderName
			var factoryConfig model.SubscriptionTelegramPanConfig
			newShareSaverForProvider = func(provider ShareProviderName, cfg model.SubscriptionTelegramPanConfig) (ShareSaver, error) {
				factoryCalls++
				factoryProvider = provider
				factoryConfig = cfg
				return saver, nil
			}

			paths, err := SaveClusterShareSelection(
				context.Background(),
				tc.shareURL,
				"",
				stagingRoot,
				[]string{fileID},
			)
			require.NoError(t, err)

			require.Equal(t, 1, factoryCalls)
			require.Equal(t, tc.provider, factoryProvider)
			require.Equal(t, stagingRoot, factoryConfig.TempTransferRoot)
			factoryCredential := factoryConfig.Cookie
			if tc.provider == ShareProviderPan123 {
				factoryCredential = factoryConfig.AccessToken
			}
			require.Equal(t, tc.selectedCredential, factoryCredential)
			require.NotEqual(t, tc.globalCredential, factoryCredential)
			require.NotEqual(t, tc.broadCredential, factoryCredential)
			require.Equal(t, []string{stagingRoot}, saver.ensureDirCalls)
			require.Equal(t, []string{fileID}, idsFromShareItems(saver.saved[""]))
			require.Equal(t, []string{fileID}, idsFromShareItems(saver.saved["selected-dst-dir"]))
			require.Equal(t, []string{"task-1"}, saver.waitedTasks)
			require.Equal(t, []string{stagingRoot + "/" + fileName}, paths)
		})
	}
}

func TestResolveClusterShareTempRootRejectsTaskNamespace(t *testing.T) {
	_, err := resolveClusterShareTempRoot("/ali/转存至移动", ".openlist-cluster/job-1/media-1")
	require.ErrorContains(t, err, "must be absolute")
}

func TestResolveClusterShareTempRootPreservesExplicitAbsoluteRoot(t *testing.T) {
	got, err := resolveClusterShareTempRoot("/ali/转存至移动", "/ali/custom")
	require.NoError(t, err)
	require.Equal(t, "/ali/custom", got)
}

func TestResolveClusterShareTempRootRejectsTraversal(t *testing.T) {
	_, err := resolveClusterShareTempRoot("/ali/转存至移动", "../../shared")
	require.Error(t, err)
}

func TestResolveClusterShareTempRootRejectsRelativeRootWithoutConfiguredBase(t *testing.T) {
	_, err := resolveClusterShareTempRoot("", ".openlist-cluster/job-1/media-1")
	require.Error(t, err)
}

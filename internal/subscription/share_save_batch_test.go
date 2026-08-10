package subscription

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/stretchr/testify/require"
)

type pan115BatchTestProvider struct {
	*pan115ShareProvider
	children       map[string][]ShareItem
	ensureDirCalls []string
	listCalls      int
	waitedTaskIDs  []string
}

func (p *pan115BatchTestProvider) EnsureDir(ctx context.Context, path string) (string, error) {
	p.ensureDirCalls = append(p.ensureDirCalls, path)
	return "dst-dir", nil
}

func (p *pan115BatchTestProvider) ListShareChildren(ctx context.Context, ref ShareRef, parentID string) ([]ShareItem, error) {
	p.listCalls++
	return p.children[parentID], nil
}

func (p *pan115BatchTestProvider) WaitSaveComplete(ctx context.Context, taskIDs []string) error {
	p.waitedTaskIDs = append(p.waitedTaskIDs, taskIDs...)
	return nil
}

func TestSaveClusterShareSelectionBatchPan115UsesSingleTreeAndReceiveRequest(t *testing.T) {
	setupSubscriptionRuntimeDB(t)

	cfg := model.SubscriptionConfig{}
	cfg.Telegram.Pan115.Cookie = "global-cookie"
	_, err := SaveConfig(cfg)
	require.NoError(t, err)

	const selectedMount = "/cluster-share-pan115-batch/selected"
	for _, storage := range []*model.Storage{
		{MountPath: "/cluster-share-pan115-batch", Driver: "115 Cloud", Status: "work", Addition: `{"Cookie":"broad-cookie"}`},
		{MountPath: selectedMount, Driver: "115 Cloud", Status: "work", Addition: `{"Cookie":"selected-cookie"}`},
	} {
		require.NoError(t, db.CreateStorage(storage))
	}

	var (
		receiveCalls int
		gotFileIDs   []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/webapi/share/receive":
			receiveCalls++
			require.NoError(t, r.ParseForm())
			gotFileIDs = strings.Split(r.Form.Get("file_id"), ",")
			_, _ = w.Write([]byte(`{"state":true}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := &pan115BatchTestProvider{
		pan115ShareProvider: NewPan115ShareProvider(model.SubscriptionTelegramPanConfig{Cookie: "selected-cookie"}).(*pan115ShareProvider),
		children: map[string][]ShareItem{
			"": {
				{ID: "file-1", Name: "One.mkv", Raw: map[string]any{"share_fid_token": "file-1"}},
				{ID: "file-2", Name: "Two.mkv", Raw: map[string]any{"share_fid_token": "file-2"}},
				{ID: "file-3", Name: "Three.mkv", Raw: map[string]any{"share_fid_token": "file-3"}},
			},
		},
	}
	provider.webURL = server.URL

	oldFactory := newShareSaverForProvider
	t.Cleanup(func() { newShareSaverForProvider = oldFactory })
	newShareSaverForProvider = func(providerName ShareProviderName, gotCfg model.SubscriptionTelegramPanConfig) (ShareSaver, error) {
		require.Equal(t, ShareProviderPan115, providerName)
		require.Equal(t, "selected-cookie", gotCfg.Cookie)
		return provider, nil
	}

	tempRoot := selectedMount + "/临时转存"
	paths, err := SaveClusterShareSelectionBatch(
		context.Background(),
		"https://115cdn.com/s/swssal13zrk?password=t58d",
		"",
		tempRoot,
		[]string{"file-3", "file-1", "file-2"},
	)
	require.NoError(t, err)
	require.Equal(t, 1, provider.listCalls)
	require.Equal(t, 1, receiveCalls)
	slices.Sort(gotFileIDs)
	require.Equal(t, []string{"file-1", "file-2", "file-3"}, gotFileIDs)
	require.Equal(t, []string{tempRoot}, provider.ensureDirCalls)
	require.Equal(t, []string{"pan115_sync_swssal13zrk"}, provider.waitedTaskIDs)
	require.Equal(t, []string{
		tempRoot + "/One.mkv",
		tempRoot + "/Two.mkv",
		tempRoot + "/Three.mkv",
	}, paths)
}

func TestSaveShareToTempPan115BatchesAcrossParentsIntoSingleReceiveRequest(t *testing.T) {
	var (
		receiveCalls int
		gotFileIDs   []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/webapi/share/receive":
			receiveCalls++
			require.NoError(t, r.ParseForm())
			gotFileIDs = strings.Split(r.Form.Get("file_id"), ",")
			_, _ = w.Write([]byte(`{"state":true}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	provider := &pan115BatchTestProvider{
		pan115ShareProvider: NewPan115ShareProvider(model.SubscriptionTelegramPanConfig{Cookie: "UID=1;CID=2"}).(*pan115ShareProvider),
		children: map[string][]ShareItem{
			"": {
				{ID: "dir-1", Name: "Season 1", IsDir: true},
				{ID: "dir-2", Name: "Season 2", IsDir: true},
			},
			"dir-1": {
				{ID: "file-1", ParentID: "dir-1", Name: "Episode1.mkv", Raw: map[string]any{"share_fid_token": "file-1"}},
			},
			"dir-2": {
				{ID: "file-2", ParentID: "dir-2", Name: "Episode2.mkv", Raw: map[string]any{"share_fid_token": "file-2"}},
				{ID: "file-3", ParentID: "dir-2", Name: "Episode3.mkv", Raw: map[string]any{"share_fid_token": "file-3"}},
			},
		},
	}
	provider.webURL = server.URL

	entries, err := SaveShareToTemp(context.Background(), provider, ShareRef{
		Provider: ShareProviderPan115,
		RawURL:   "https://115cdn.com/s/swssal13zrk?password=t58d",
		ShareID:  "swssal13zrk",
		Passcode: "t58d",
	}, SaveShareOptions{
		TempRoot: "/tmp/pan115",
		Flatten:  true,
		Match: func(entry TreeEntry) bool {
			return entry.ID == "file-1" || entry.ID == "file-2" || entry.ID == "file-3"
		},
	})
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, 1, receiveCalls)
	slices.Sort(gotFileIDs)
	require.Equal(t, []string{"file-1", "file-2", "file-3"}, gotFileIDs)
	require.Equal(t, []string{"/tmp/pan115"}, provider.ensureDirCalls)
	require.Equal(t, []string{"pan115_sync_swssal13zrk"}, provider.waitedTaskIDs)
}

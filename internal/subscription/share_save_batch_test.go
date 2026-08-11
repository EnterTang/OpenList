package subscription

import (
	"context"
	"errors"
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

type recordedSaveCall struct {
	parentID string
	dstDirID string
	itemIDs  []string
}

type groupingBoundaryTestProvider struct {
	fakeShareTreeProvider
	ensureDirCalls []string
	saveCalls      []recordedSaveCall
	waitedTaskIDs  []string
}

type pan123PartialResultProvider struct {
	fakeShareTreeProvider
	attempted []string
	failID    string
}

func (p *pan123PartialResultProvider) EnsureDir(context.Context, string) (string, error) {
	return "dst-dir", nil
}

func (p *pan123PartialResultProvider) SaveShareItems(_ context.Context, _ ShareRef, _ string, items []ShareItem, _ string) ([]string, error) {
	if len(items) != 1 {
		return nil, errors.New("expected one item per compensated save")
	}
	p.attempted = append(p.attempted, items[0].ID)
	return []string{"task-" + items[0].ID}, nil
}

func (p *pan123PartialResultProvider) WaitSaveComplete(_ context.Context, taskIDs []string) error {
	if len(taskIDs) == 1 && taskIDs[0] == "task-"+p.failID {
		return errors.New("save result unknown")
	}
	return nil
}

func TestSaveShareToTempPan123AttemptsAllItemsAfterMixedResult(t *testing.T) {
	provider := &pan123PartialResultProvider{
		fakeShareTreeProvider: fakeShareTreeProvider{
			name: ShareProviderPan123,
			children: map[string][]ShareItem{
				"": {
					{ID: "file-1", Name: "one.mkv"},
					{ID: "file-2", Name: "two.mkv"},
					{ID: "file-3", Name: "three.mkv"},
				},
			},
		},
		failID: "file-2",
	}

	entries, err := SaveShareToTemp(context.Background(), provider, ShareRef{
		Provider: ShareProviderPan123,
		RawURL:   "https://www.123pan.com/s/example",
	}, SaveShareOptions{TempRoot: "/tmp/pan123"})
	if err == nil {
		t.Fatal("expected mixed save error")
	}
	if got, want := strings.Join(provider.attempted, ","), "file-1,file-2,file-3"; got != want {
		t.Fatalf("attempted = %q, want %q", got, want)
	}
	if got, want := len(entries), 2; got != want {
		t.Fatalf("confirmed entries = %d, want %d", got, want)
	}
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

func (p *groupingBoundaryTestProvider) EnsureDir(ctx context.Context, path string) (string, error) {
	p.ensureDirCalls = append(p.ensureDirCalls, path)
	return "dir:" + path, nil
}

func (p *groupingBoundaryTestProvider) SaveShareItems(ctx context.Context, ref ShareRef, parentID string, items []ShareItem, dstDirID string) ([]string, error) {
	call := recordedSaveCall{
		parentID: parentID,
		dstDirID: dstDirID,
		itemIDs:  make([]string, 0, len(items)),
	}
	for _, item := range items {
		call.itemIDs = append(call.itemIDs, item.ID)
	}
	p.saveCalls = append(p.saveCalls, call)
	return []string{"task-" + parentID}, nil
}

func (p *groupingBoundaryTestProvider) WaitSaveComplete(ctx context.Context, taskIDs []string) error {
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

func TestSaveShareToTempFlattenNonPan115PreservesParentGroupingBoundaries(t *testing.T) {
	provider := &groupingBoundaryTestProvider{
		fakeShareTreeProvider: fakeShareTreeProvider{
			name: ShareProviderQuark,
			children: map[string][]ShareItem{
				"": {
					{ID: "dir-1", Name: "Season 1", IsDir: true},
					{ID: "dir-2", Name: "Season 2", IsDir: true},
				},
				"dir-1": {
					{ID: "file-1", ParentID: "dir-1", Name: "Episode1.mkv"},
				},
				"dir-2": {
					{ID: "file-2", ParentID: "dir-2", Name: "Episode2.mkv"},
					{ID: "file-3", ParentID: "dir-2", Name: "Episode3.mkv"},
				},
			},
		},
	}

	entries, err := SaveShareToTemp(context.Background(), provider, ShareRef{
		Provider: ShareProviderQuark,
		RawURL:   "https://pan.quark.cn/s/bc18e4ea5fb8",
	}, SaveShareOptions{
		TempRoot: "/tmp/quark",
		Flatten:  true,
		Match: func(entry TreeEntry) bool {
			return entry.ID == "file-1" || entry.ID == "file-2" || entry.ID == "file-3"
		},
	})
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, []string{"/tmp/quark"}, provider.ensureDirCalls)
	require.Equal(t, []recordedSaveCall{
		{parentID: "dir-1", dstDirID: "dir:/tmp/quark", itemIDs: []string{"file-1"}},
		{parentID: "dir-2", dstDirID: "dir:/tmp/quark", itemIDs: []string{"file-2", "file-3"}},
	}, provider.saveCalls)
	require.Equal(t, []string{"task-dir-1", "task-dir-2"}, provider.waitedTaskIDs)
}

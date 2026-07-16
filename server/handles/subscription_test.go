package handles

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func setupSubscriptionHandleDB(t *testing.T) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	database, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	conf.Conf = conf.DefaultConfig("data")
	db.Init(database)
	t.Cleanup(func() {
		sqlDB, err := database.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})
}

func newSubscriptionHandleContext(t *testing.T, method, target string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, target, nil)
	return c, recorder
}

func decodeHandleResp[T any](t *testing.T, recorder *httptest.ResponseRecorder) common.Resp[T] {
	t.Helper()
	var resp common.Resp[T]
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v body=%s", err, recorder.Body.String())
	}
	return resp
}

type subscriptionEpisodeSourcesData struct {
	Content []model.SubscriptionEpisodeSourceDetail `json:"content"`
}

type recordingSubscriptionDispatcher struct {
	inspectTasks []subscription.ClusterInspectTask
}

func (d *recordingSubscriptionDispatcher) DispatchSubscriptionInspect(_ context.Context, task subscription.ClusterInspectTask) (string, error) {
	d.inspectTasks = append(d.inspectTasks, task)
	return "handler-inspect-job", nil
}

func (d *recordingSubscriptionDispatcher) DispatchSubscriptionMedia(context.Context, []subscription.ClusterMediaTask) ([]subscription.ClusterDispatchResult, error) {
	return nil, nil
}

func TestCheckSubscriptionUsesClusterDispatchForHybridRole(t *testing.T) {
	oldConf := conf.Conf
	t.Cleanup(func() { conf.Conf = oldConf })
	setupSubscriptionHandleDB(t)
	conf.Conf.Cluster.Role = model.ClusterRoleHybrid
	dispatcher := &recordingSubscriptionDispatcher{}
	subscription.RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { subscription.RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{
		Name:            "Hybrid manual check",
		SourceType:      model.SubscriptionSourceManual,
		SourceConfig:    `{"links":["https://www.123pan.com/s/example"]}`,
		TransferEnabled: true,
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(
		http.MethodPost,
		"/api/admin/subscription/check",
		strings.NewReader(`{"id":`+strconv.Itoa(int(sub.ID))+`,"transfer":true}`),
	)
	c.Request.Header.Set("Content-Type", "application/json")

	CheckSubscription(c)

	resp := decodeHandleResp[model.SubscriptionRunResult](t, recorder)
	if resp.Code != 200 {
		t.Fatalf("code = %d, want 200: %s", resp.Code, recorder.Body.String())
	}
	if len(dispatcher.inspectTasks) != 1 {
		t.Fatalf("inspect tasks = %#v, want one cluster inspection", dispatcher.inspectTasks)
	}
}

func TestResolveSubscriptionArchiveStatusDefaultsToAll(t *testing.T) {
	for _, test := range []struct {
		value   string
		want    string
		wantErr bool
	}{
		{value: "", want: "all"},
		{value: "all", want: "all"},
		{value: "ongoing", want: model.SubscriptionArchiveStatusOngoing},
		{value: "completed", want: model.SubscriptionArchiveStatusCompleted},
		{value: "stalled", want: model.SubscriptionArchiveStatusStalled},
		{value: "unknown", wantErr: true},
	} {
		t.Run(test.value, func(t *testing.T) {
			got, err := resolveSubscriptionArchiveStatus(test.value)
			if (err != nil) != test.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, test.wantErr)
			}
			if got != test.want {
				t.Fatalf("archive status = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSubscriptionEpisodeEndFromTMDBCandidatesUsesLatestSelectedSeason(t *testing.T) {
	end := subscriptionEpisodeEndFromTMDBCandidates(&model.Subscription{
		TMDBID:  123,
		Seasons: []int{1, 2},
	}, []model.ETFArchiveTMDBCandidate{
		{TMDBID: 123, MediaType: "movie", SeasonMap: map[int]int{2: 99}},
		{TMDBID: 123, MediaType: "tv", SeasonMap: map[int]int{1: 8, 2: 12}},
	})
	if end != 12 {
		t.Fatalf("episode end = %d, want 12", end)
	}
}

func TestSubscriptionEpisodeEndFromTMDBCandidatesMatchesLegacyTitleAndYear(t *testing.T) {
	end := subscriptionEpisodeEndFromTMDBCandidates(&model.Subscription{
		TMDBName: "Example Show",
		TMDBYear: 2026,
		Seasons:  []int{1},
	}, []model.ETFArchiveTMDBCandidate{
		{TMDBID: 456, Name: "Different Show", Year: 2026, MediaType: "tv", SeasonMap: map[int]int{1: 8}},
		{TMDBID: 789, Name: "Example Show", Year: 2026, MediaType: "tv", SeasonMap: map[int]int{1: 10}},
	})
	if end != 10 {
		t.Fatalf("episode end = %d, want 10", end)
	}
}

func TestShouldHydrateSubscriptionEpisodeEndRefreshesTMDBDataDaily(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	recent := now.Add(-12 * time.Hour)
	if shouldHydrateSubscriptionEpisodeEnd(&model.Subscription{
		MediaType:              "tv",
		TMDBID:                 123,
		LatestSeasonEpisodeEnd: 8,
		TMDBEpisodeSyncedAt:    &recent,
	}, now) {
		t.Fatal("recently synchronized subscription should not refresh TMDB again")
	}
	if !shouldHydrateSubscriptionEpisodeEnd(&model.Subscription{
		MediaType:              "tv",
		TMDBID:                 123,
		LatestSeasonEpisodeEnd: 8,
	}, now) {
		t.Fatal("subscription without a synchronization timestamp should refresh TMDB")
	}
}

func TestListSubscriptionRunsRejectsInvalidView(t *testing.T) {
	c, recorder := newSubscriptionHandleContext(t, http.MethodGet, "/admin/subscription/runs?view=invalid")

	ListSubscriptionRuns(c)

	resp := decodeHandleResp[any](t, recorder)
	if resp.Code != 400 {
		t.Fatalf("code = %d, want 400: %s", resp.Code, recorder.Body.String())
	}
}

func TestListSubscriptionBoardReturnsFilteredTotals(t *testing.T) {
	setupSubscriptionHandleDB(t)

	sub := &model.Subscription{
		Name:       "Board handler subscription",
		TMDBName:   "Board handler subscription",
		SourceType: model.SubscriptionSourceTelegram,
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	other := &model.Subscription{
		Name:       "Other board subscription",
		TMDBName:   "Other board subscription",
		SourceType: model.SubscriptionSourceManual,
	}
	if err := db.CreateSubscription(other); err != nil {
		t.Fatalf("create other subscription: %v", err)
	}

	for _, run := range []*model.SubscriptionRun{
		{SubscriptionID: sub.ID, Status: model.SubscriptionStatusSuccess, AddedCount: 3, ChangedCount: 2, StartedAt: time.Now().UTC()},
		{SubscriptionID: sub.ID, Status: model.SubscriptionStatusFailed, Error: "handler failure", StartedAt: time.Now().UTC()},
		{SubscriptionID: other.ID, Status: model.SubscriptionStatusSuccess, AddedCount: 99, ChangedCount: 99, StartedAt: time.Now().UTC()},
	} {
		if err := db.CreateSubscriptionRun(run); err != nil {
			t.Fatalf("create subscription run: %v", err)
		}
	}

	c, recorder := newSubscriptionHandleContext(
		t,
		http.MethodGet,
		"/admin/subscription/board?subscription_id="+strconv.Itoa(int(sub.ID)),
	)
	ListSubscriptionBoard(c)

	resp := decodeHandleResp[model.SubscriptionBoard](t, recorder)
	if resp.Code != 200 {
		t.Fatalf("code = %d, want 200: %s", resp.Code, recorder.Body.String())
	}
	want := model.SubscriptionBoard{
		SubscriptionCount: 1,
		ChangedRunCount:   1,
		AddedCount:        3,
		ChangedCount:      2,
		FailureCount:      1,
	}
	if resp.Data != want {
		t.Fatalf("board = %#v, want %#v", resp.Data, want)
	}
}

func TestListSubscriptionEpisodeSourcesValidatesSubscriptionIDAndWorkerPrecedence(t *testing.T) {
	setupSubscriptionHandleDB(t)

	sub := &model.Subscription{
		Name:       "Handler subscription",
		TMDBName:   "Handler subscription",
		SourceType: model.SubscriptionSourceTelegram,
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	item, _, err := db.UpsertSubscriptionItem(&model.SubscriptionItem{
		SubscriptionID: sub.ID,
		SourceKey:      "handler-item",
		Season:         1,
		Episode:        1,
		Status:         model.SubscriptionItemStatusTransferred,
		LastSeenAt:     time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("create subscription item: %v", err)
	}
	database := db.GetDb()
	if err := database.Create(&model.ClusterNode{ID: "node-assigned", Name: "处理节点"}).Error; err != nil {
		t.Fatalf("create node: %v", err)
	}
	if err := database.Create(&model.ClusterJob{ID: "job-handler", SubscriptionID: sub.ID, SubscriptionItemID: item.ID, AssignedNodeID: "node-assigned"}).Error; err != nil {
		t.Fatalf("create cluster job: %v", err)
	}
	if _, err := db.UpsertSubscriptionEpisodeSource(&model.SubscriptionEpisodeSource{
		SubscriptionID: sub.ID,
		Season:         1,
		Episode:        1,
		SourceItemID:   item.ID,
		SourceType:     model.SubscriptionSourceTelegram,
		SourceProvider: "quark",
		FileName:       "handler.mkv",
		Status:         model.SubscriptionItemStatusTransferred,
		ClusterJobID:   "job-handler",
	}); err != nil {
		t.Fatalf("create episode source snapshot: %v", err)
	}

	for _, target := range []string{
		"/admin/subscription/episode_sources",
		"/admin/subscription/episode_sources?subscription_id=0",
		"/admin/subscription/episode_sources?subscription_id=bad",
	} {
		c, recorder := newSubscriptionHandleContext(t, http.MethodGet, target)
		ListSubscriptionEpisodeSources(c)
		resp := decodeHandleResp[any](t, recorder)
		if resp.Code != 400 {
			t.Fatalf("%s code = %d, want 400", target, resp.Code)
		}
	}

	c, recorder := newSubscriptionHandleContext(t, http.MethodGet, "/admin/subscription/episode_sources?subscription_id="+strconv.Itoa(int(sub.ID)))
	ListSubscriptionEpisodeSources(c)

	resp := decodeHandleResp[subscriptionEpisodeSourcesData](t, recorder)
	if resp.Code != 200 {
		t.Fatalf("code = %d, want 200: %s", resp.Code, recorder.Body.String())
	}
	if len(resp.Data.Content) != 1 {
		t.Fatalf("content len = %d, want 1: %#v", len(resp.Data.Content), resp.Data.Content)
	}
	if resp.Data.Content[0].WorkerName != "处理节点" || resp.Data.Content[0].Status != model.SubscriptionItemStatusTransferred {
		t.Fatalf("episode source detail = %#v", resp.Data.Content[0])
	}
}

func TestFilterDisplayedSubscriptionItems(t *testing.T) {
	items := []model.SubscriptionItem{
		{Status: model.SubscriptionItemStatusSkipped, SourceProvider: "123"},
		{Status: model.SubscriptionItemStatusTransferred, SourceProvider: "123"},
		{Status: model.SubscriptionItemStatusTransferred, SourceProvider: "123", FileName: "Some.Show.S01E08.mkv", Episode: 8},
		{Status: model.SubscriptionItemStatusTransferred, SourceProvider: "123", TargetPath: "/shows/Some Show/Season 1/Some.Show.S01E09.mkv", Episode: 9},
	}

	filtered := filterDisplayedSubscriptionItems(items)

	if len(filtered) != 2 {
		t.Fatalf("len(filtered) = %d, want 2", len(filtered))
	}
	if filtered[0].Episode != 8 {
		t.Fatalf("filtered[0].Episode = %d, want 8", filtered[0].Episode)
	}
	if filtered[1].Episode != 9 {
		t.Fatalf("filtered[1].Episode = %d, want 9", filtered[1].Episode)
	}
}

func TestNormalizeSubscriptionDefaultsTelegramAndSelectedSeasons(t *testing.T) {
	item := &model.Subscription{
		Name:      " Some Show ",
		TMDBName:  " Some Show ",
		MediaType: "tv",
		Season:    3,
		Seasons:   []int{3, 1, 3, 0, -1},
	}

	normalizeSubscription(item)

	if item.SourceType != model.SubscriptionSourceTelegram {
		t.Fatalf("source type = %q, want telegram", item.SourceType)
	}
	if item.Season != 1 {
		t.Fatalf("season = %d, want first selected season", item.Season)
	}
	if want := []int{1, 3}; !reflect.DeepEqual(item.Seasons, want) {
		t.Fatalf("seasons = %#v, want %#v", item.Seasons, want)
	}
	if item.Name != "Some Show" || item.TMDBName != "Some Show" {
		t.Fatalf("names were not trimmed: %#v", item)
	}
}

func TestNormalizeSubscriptionClearsMovieSeasons(t *testing.T) {
	item := &model.Subscription{
		SourceType:               model.SubscriptionSourceManual,
		MediaType:                "movie",
		Season:                   2,
		Seasons:                  []int{1, 2},
		LatestSeasonEpisodeStart: 3,
		LatestSeasonEpisodeEnd:   8,
	}

	normalizeSubscription(item)

	if item.Season != 0 || len(item.Seasons) != 0 || item.LatestSeasonEpisodeStart != 0 || item.LatestSeasonEpisodeEnd != 0 {
		t.Fatalf("movie season fields = season %d seasons %#v range %d-%d, want cleared", item.Season, item.Seasons, item.LatestSeasonEpisodeStart, item.LatestSeasonEpisodeEnd)
	}
}

func TestNormalizeSubscriptionPreservesEmptyLegacyTargetRoot(t *testing.T) {
	item := &model.Subscription{TargetRoot: "   "}

	normalizeSubscription(item)

	if item.TargetRoot != "" {
		t.Fatalf("target root = %q, want empty optional legacy target", item.TargetRoot)
	}
}

func TestValidateSubscriptionEpisodeRange(t *testing.T) {
	tests := []struct {
		name    string
		start   int
		end     int
		wantErr bool
	}{
		{name: "open range", start: 9},
		{name: "closed range", start: 9, end: 12},
		{name: "negative", start: -1, wantErr: true},
		{name: "reversed", start: 12, end: 9, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSubscriptionEpisodeRange(&model.Subscription{
				MediaType:                "tv",
				LatestSeasonEpisodeStart: tt.start,
				LatestSeasonEpisodeEnd:   tt.end,
			})
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

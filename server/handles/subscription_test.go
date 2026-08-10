package handles

import (
	"context"
	"encoding/json"
	"errors"
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
	retryResult  *subscription.ClusterRetryResult
	retryCalls   int
}

func TestUpdateSubscriptionRejectsBlankName(t *testing.T) {
	setupSubscriptionHandleDB(t)

	sub := &model.Subscription{
		Name:       "Keep this name",
		TMDBName:   "Keep this name",
		SourceType: model.SubscriptionSourceManual,
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	request := *sub
	request.Name = "   "
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/admin/subscription/update", strings.NewReader(string(body)))
	c.Request.Header.Set("Content-Type", "application/json")

	UpdateSubscription(c)

	resp := decodeHandleResp[any](t, recorder)
	if resp.Code != 400 || resp.Message != "name is required" {
		t.Fatalf("response = %#v, want blank-name validation failure", resp)
	}
	stored, err := db.GetSubscriptionByID(sub.ID)
	if err != nil {
		t.Fatalf("get stored subscription: %v", err)
	}
	if stored.Name != "Keep this name" {
		t.Fatalf("stored name = %q, want unchanged", stored.Name)
	}
}

func TestDeleteSubscriptionHonorsCancelledRequestContext(t *testing.T) {
	setupSubscriptionHandleDB(t)

	sub := &model.Subscription{
		Name:       "Cancelled handler delete",
		TMDBName:   "Cancelled handler delete",
		SourceType: model.SubscriptionSourceManual,
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(
		http.MethodPost,
		"/admin/subscription/delete",
		strings.NewReader(`{"id":`+strconv.Itoa(int(sub.ID))+`}`),
	).WithContext(ctx)
	c.Request.Header.Set("Content-Type", "application/json")

	DeleteSubscription(c)

	resp := decodeHandleResp[any](t, recorder)
	if resp.Code != 503 || resp.Message != "subscription delete timed out; no changes were committed" {
		t.Fatalf("response = %#v, want explicit cancellation response: %s", resp, recorder.Body.String())
	}
	if _, err := db.GetSubscriptionByID(sub.ID); err != nil {
		t.Fatalf("cancelled handler delete removed subscription: %v", err)
	}
}

// Regression: ISSUE-003 — SQLite writer contention left deletion without a useful terminal error.
// Found by /qa on 2026-07-18
// Report: .gstack/qa-reports/qa-report-oplistetf-entertang-work-2026-07-18.md
func TestSubscriptionDeleteDatabaseBusy(t *testing.T) {
	if !subscriptionDeleteDatabaseBusy(errors.New("database is locked (5) (SQLITE_BUSY)")) {
		t.Fatal("SQLite busy error was not classified as a temporary delete conflict")
	}
	if subscriptionDeleteDatabaseBusy(errors.New("validation failed")) {
		t.Fatal("unrelated error was classified as a SQLite busy conflict")
	}
}

func TestSubscriptionConfigResponsesRedactAndPreserveSecrets(t *testing.T) {
	setupSubscriptionHandleDB(t)

	const (
		apiHash      = "api-secret-value"
		refreshToken = "refresh-secret-value"
		accessToken  = "access-secret-value"
		proxyUserKey = "hdhive-user-key"
		proxySecret  = "hdhive-proxy-secret"
	)
	_, err := subscription.SaveConfig(model.SubscriptionConfig{
		Telegram: model.SubscriptionTelegramSourceConfig{
			APIID:    12345,
			APIHash:  apiHash,
			Channels: []string{"@configured_channel"},
			AliyunDrive: model.SubscriptionTelegramPanConfig{
				RefreshToken: refreshToken,
				AccessToken:  accessToken,
			},
			HDHive: model.SubscriptionTelegramHDHiveConfig{
				Enabled:      true,
				ProxyUserKey: proxyUserKey,
				ProxySecret:  proxySecret,
			},
		},
	})
	if err != nil {
		t.Fatalf("seed subscription config: %v", err)
	}

	c, recorder := newSubscriptionHandleContext(t, http.MethodGet, "/admin/subscription/config")
	GetSubscriptionConfig(c)
	resp := decodeHandleResp[model.SubscriptionConfigResponse](t, recorder)
	if resp.Code != 200 {
		t.Fatalf("get config code = %d: %s", resp.Code, recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), apiHash) || strings.Contains(recorder.Body.String(), refreshToken) || strings.Contains(recorder.Body.String(), accessToken) || strings.Contains(recorder.Body.String(), proxyUserKey) || strings.Contains(recorder.Body.String(), proxySecret) {
		t.Fatalf("config response exposed a stored secret: %s", recorder.Body.String())
	}
	if resp.Data.Telegram.APIHash != "" || resp.Data.Telegram.AliyunDrive.RefreshToken != "" || resp.Data.Telegram.AliyunDrive.AccessToken != "" {
		t.Fatalf("redacted config still contains secrets: %#v", resp.Data.Telegram)
	}
	if resp.Data.Telegram.HDHive.ProxyUserKey != "" || resp.Data.Telegram.HDHive.ProxySecret != "" {
		t.Fatalf("redacted HDHive config still contains secrets: %#v", resp.Data.Telegram.HDHive)
	}
	if !resp.Data.SecretStatus.Configured["telegram.api_hash"] || !resp.Data.SecretStatus.Configured["telegram.aliyun_drive.refresh_token"] || !resp.Data.SecretStatus.Configured["telegram.hdhive.proxy_secret"] {
		t.Fatalf("secret configured status = %#v", resp.Data.SecretStatus)
	}
	if resp.Data.SecretStatus.UnchangedMarker != model.SubscriptionSecretUnchangedMarker || resp.Data.SecretStatus.ClearMarker != model.SubscriptionSecretClearMarker {
		t.Fatalf("secret update protocol = %#v", resp.Data.SecretStatus)
	}
	if resp.Data.SourceCapabilities[model.SubscriptionSourcePanSou].Available {
		t.Fatalf("unconfigured pansou capability = %#v", resp.Data.SourceCapabilities[model.SubscriptionSourcePanSou])
	}

	update := resp.Data.SubscriptionConfig
	update.PanSou.BaseURL = "https://pansou.example"
	body, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	recorder = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/admin/subscription/config", strings.NewReader(string(body)))
	c.Request.Header.Set("Content-Type", "application/json")
	SaveSubscriptionConfig(c)
	savedResp := decodeHandleResp[model.SubscriptionConfigResponse](t, recorder)
	if savedResp.Code != 200 {
		t.Fatalf("save config code = %d: %s", savedResp.Code, recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), apiHash) || strings.Contains(recorder.Body.String(), refreshToken) || strings.Contains(recorder.Body.String(), accessToken) || strings.Contains(recorder.Body.String(), proxyUserKey) || strings.Contains(recorder.Body.String(), proxySecret) {
		t.Fatalf("save response exposed a stored secret: %s", recorder.Body.String())
	}
	stored, err := subscription.GetConfig()
	if err != nil {
		t.Fatalf("get saved config: %v", err)
	}
	if stored.Telegram.APIHash != apiHash || stored.Telegram.AliyunDrive.RefreshToken != refreshToken || stored.Telegram.AliyunDrive.AccessToken != accessToken || stored.Telegram.HDHive.ProxyUserKey != proxyUserKey || stored.Telegram.HDHive.ProxySecret != proxySecret {
		t.Fatalf("redacted save overwrote credentials: %#v", stored.Telegram)
	}
	if !savedResp.Data.SourceCapabilities[model.SubscriptionSourcePanSou].Available {
		t.Fatalf("configured pansou capability = %#v", savedResp.Data.SourceCapabilities[model.SubscriptionSourcePanSou])
	}

	clear := savedResp.Data.SubscriptionConfig
	clear.Telegram.APIHash = model.SubscriptionSecretClearMarker
	clear.Telegram.AliyunDrive.RefreshToken = model.SubscriptionSecretUnchangedMarker
	body, err = json.Marshal(clear)
	if err != nil {
		t.Fatal(err)
	}
	recorder = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/admin/subscription/config", strings.NewReader(string(body)))
	c.Request.Header.Set("Content-Type", "application/json")
	SaveSubscriptionConfig(c)
	clearResp := decodeHandleResp[model.SubscriptionConfigResponse](t, recorder)
	if clearResp.Code != 200 {
		t.Fatalf("clear config code = %d: %s", clearResp.Code, recorder.Body.String())
	}
	stored, err = subscription.GetConfig()
	if err != nil {
		t.Fatalf("get cleared config: %v", err)
	}
	if stored.Telegram.APIHash != "" || stored.Telegram.AliyunDrive.RefreshToken != refreshToken {
		t.Fatalf("explicit clear/unchanged markers not honored: %#v", stored.Telegram)
	}
}

func TestUnlockSubscriptionResourceRejectsInvalidHDHiveURL(t *testing.T) {
	setupSubscriptionHandleDB(t)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/admin/subscription/resource/unlock", strings.NewReader(`{"url":"https://example.com/resource/not-hdhive"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	UnlockSubscriptionResource(c)

	resp := decodeHandleResp[any](t, recorder)
	if resp.Code != 400 {
		t.Fatalf("response = %#v, want invalid URL status", resp)
	}
}

func TestBindAndUnbindSubscriptionResource(t *testing.T) {
	setupSubscriptionHandleDB(t)
	sub := &model.Subscription{
		Name:       "HDHive binding",
		TMDBName:   "HDHive binding",
		SourceType: model.SubscriptionSourceHDHive,
	}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/admin/subscription/resource/bind", strings.NewReader(`{"subscription_id":`+strconv.Itoa(int(sub.ID))+`,"resource_url":"https://hdhive.com/resource/115/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","share_url":"https://115.com/s/share","access_code":"abcd","provider":"pan115"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	BindSubscriptionResource(c)

	resp := decodeHandleResp[model.Subscription](t, recorder)
	if resp.Code != 200 {
		t.Fatalf("bind code = %d: %s", resp.Code, recorder.Body.String())
	}
	if resp.Data.BoundShare == nil || resp.Data.BoundShare.ShareURL != "https://115.com/s/share,abcd" {
		t.Fatalf("bound share = %#v", resp.Data.BoundShare)
	}
	stored, err := db.GetSubscriptionByID(sub.ID)
	if err != nil {
		t.Fatalf("get bound subscription: %v", err)
	}
	if stored.BoundShare == nil || stored.BoundShare.ResourceSlug != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("stored bound share = %#v", stored.BoundShare)
	}

	recorder = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/admin/subscription/resource/unbind", strings.NewReader(`{"subscription_id":`+strconv.Itoa(int(sub.ID))+`}`))
	c.Request.Header.Set("Content-Type", "application/json")
	UnbindSubscriptionResource(c)
	resp = decodeHandleResp[model.Subscription](t, recorder)
	if resp.Code != 200 {
		t.Fatalf("unbind code = %d: %s", resp.Code, recorder.Body.String())
	}
	stored, err = db.GetSubscriptionByID(sub.ID)
	if err != nil {
		t.Fatalf("get unbound subscription: %v", err)
	}
	if stored.BoundShare != nil {
		t.Fatalf("bound share was not removed: %#v", stored.BoundShare)
	}
}

func TestBindSubscriptionResourceDoesNotImplicitlyUnlock(t *testing.T) {
	setupSubscriptionHandleDB(t)
	if err := db.CreateSubscription(&model.Subscription{ID: 99, Name: "HDHive no unlock", TMDBName: "HDHive no unlock", SourceType: model.SubscriptionSourceHDHive}); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/admin/subscription/resource/bind", strings.NewReader(`{"subscription_id":99,"resource_url":"https://hdhive.com/resource/115/paid"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	BindSubscriptionResource(c)
	resp := decodeHandleResp[any](t, recorder)
	if resp.Code != 400 {
		t.Fatalf("response = %#v, want missing share URL validation", resp)
	}
}

func TestSubscriptionConfigSecretStatusIncludesGuangYaPanStorage(t *testing.T) {
	setupSubscriptionHandleDB(t)
	if err := db.CreateStorage(&model.Storage{
		MountPath: "/guangya",
		Driver:    "GuangYaPan",
		Addition:  `{"access_token":"storage-access","refresh_token":"storage-refresh"}`,
		Status:    "work",
	}); err != nil {
		t.Fatalf("create guangyapan storage: %v", err)
	}

	c, recorder := newSubscriptionHandleContext(t, http.MethodGet, "/admin/subscription/config")
	GetSubscriptionConfig(c)
	resp := decodeHandleResp[model.SubscriptionConfigResponse](t, recorder)
	if resp.Code != 200 {
		t.Fatalf("get config code = %d: %s", resp.Code, recorder.Body.String())
	}
	if !resp.Data.SecretStatus.Configured["telegram.guangyapan.access_token"] {
		t.Fatalf("expected guangyapan access_token configured from storage: %#v", resp.Data.SecretStatus)
	}
	if !resp.Data.SecretStatus.Configured["telegram.guangyapan.refresh_token"] {
		t.Fatalf("expected guangyapan refresh_token configured from storage: %#v", resp.Data.SecretStatus)
	}
	if strings.Contains(recorder.Body.String(), "storage-access") || strings.Contains(recorder.Body.String(), "storage-refresh") {
		t.Fatalf("config response leaked storage secrets: %s", recorder.Body.String())
	}
}

func (d *recordingSubscriptionDispatcher) DispatchSubscriptionInspect(_ context.Context, task subscription.ClusterInspectTask) (string, error) {
	d.inspectTasks = append(d.inspectTasks, task)
	return "handler-inspect-job", nil
}

func (d *recordingSubscriptionDispatcher) DispatchSubscriptionMedia(context.Context, []subscription.ClusterMediaTask) ([]subscription.ClusterDispatchResult, error) {
	return nil, nil
}

func (d *recordingSubscriptionDispatcher) RetryFailedSubscriptionItems(context.Context, uint) (subscription.ClusterRetryResult, error) {
	d.retryCalls++
	if d.retryResult == nil {
		return subscription.ClusterRetryResult{}, errors.New("retry result not configured")
	}
	return *d.retryResult, nil
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

func TestRetryFailedSubscriptionUsesClusterReplayForHybridRole(t *testing.T) {
	oldConf := conf.Conf
	t.Cleanup(func() { conf.Conf = oldConf })
	setupSubscriptionHandleDB(t)
	conf.Conf.Cluster.Role = model.ClusterRoleHybrid
	dispatcher := &recordingSubscriptionDispatcher{retryResult: &subscription.ClusterRetryResult{Requeued: 2}}
	subscription.RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { subscription.RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{Name: "Hybrid retry", TMDBName: "Hybrid retry", TransferEnabled: true}
	if err := db.CreateSubscription(sub); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(
		http.MethodPost,
		"/api/admin/subscription/retry_failed",
		strings.NewReader(`{"id":`+strconv.Itoa(int(sub.ID))+`}`),
	)
	c.Request.Header.Set("Content-Type", "application/json")

	RetryFailedSubscription(c)

	resp := decodeHandleResp[model.SubscriptionRunResult](t, recorder)
	if resp.Code != 200 {
		t.Fatalf("code = %d, want 200: %s", resp.Code, recorder.Body.String())
	}
	if dispatcher.retryCalls != 1 {
		t.Fatalf("retry calls = %d, want 1", dispatcher.retryCalls)
	}
	if resp.Data.Run == nil || resp.Data.Run.TransferredCount != 2 {
		t.Fatalf("response run = %#v, want two requeued tasks", resp.Data.Run)
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

func TestNormalizeSubscriptionTrimsPreferredWorkerNodeID(t *testing.T) {
	item := &model.Subscription{PreferredWorkerNodeID: "  worker-139  "}

	normalizeSubscription(item)

	if item.PreferredWorkerNodeID != "worker-139" {
		t.Fatalf("preferred worker node id = %q, want trimmed value", item.PreferredWorkerNodeID)
	}
}

func TestValidateSubscriptionPreferredWorkerNodeIDLength(t *testing.T) {
	if err := validateSubscriptionPreferredWorkerNodeID(&model.Subscription{PreferredWorkerNodeID: strings.Repeat("a", 64)}); err != nil {
		t.Fatalf("64-byte worker id rejected: %v", err)
	}
	if err := validateSubscriptionPreferredWorkerNodeID(&model.Subscription{PreferredWorkerNodeID: strings.Repeat("a", 65)}); err == nil {
		t.Fatal("65-byte worker id was accepted")
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

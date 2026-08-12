package handles

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/gin-gonic/gin"
)

func TestExternalSubscriptionCreateProjectAndLookup(t *testing.T) {
	setupSubscriptionHandleDB(t)
	conf.Conf.ExternalSubscription.Enabled = true
	conf.Conf.ExternalSubscription.AllowUnauthenticated = true
	conf.Conf.ExternalSubscription.RunOnCreate = false

	create := func(body, idempotencyKey string) (map[string]any, *httptest.ResponseRecorder) {
		t.Helper()
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPost, "/api/subscriptions", strings.NewReader(body))
		c.Request.Header.Set("Content-Type", "application/json")
		c.Request.Header.Set("Idempotency-Key", idempotencyKey)
		ExternalCreateSubscription(c)
		var response map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode create response: %v body=%s", err, recorder.Body.String())
		}
		return response, recorder
	}

	body := `{"name":"测试剧集","media_type":"tv","tmdb_id":123456,"source_type":"manual","source_config":{"links":[],"_request":{"trace_id":"do-not-store"}},"seasons_selected":[2,1,2],"episode_start":1,"episode_end":10}`
	first, recorder := create(body, "etflix-request-1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("create status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	id, ok := first["id"].(float64)
	if !ok || id <= 0 {
		t.Fatalf("create response id = %#v", first["id"])
	}
	for _, field := range []string{"last_status", "last_message", "progress_json", "seasons_json", "completed"} {
		if _, exists := first[field]; !exists {
			t.Fatalf("create response missing etflix field %q: %#v", field, first)
		}
	}
	if first["last_status"] != "pending" || first["last_message"] != "queued" || first["completed"] != false {
		t.Fatalf("initial status projection = %#v", first)
	}
	seasons, ok := first["seasons_selected"].([]any)
	if !ok || len(seasons) != 2 || seasons[0].(float64) != 1 || seasons[1].(float64) != 2 {
		t.Fatalf("normalized seasons = %#v", first["seasons_selected"])
	}

	replay, recorder := create(body, "etflix-request-1")
	if recorder.Code != http.StatusOK || replay["id"] != id {
		t.Fatalf("idempotent replay = status %d response %#v, want id %v", recorder.Code, replay, id)
	}

	getRecorder := httptest.NewRecorder()
	getContext, _ := gin.CreateTestContext(getRecorder)
	getContext.Request = httptest.NewRequest(http.MethodGet, "/api/subscriptions?id="+strconv.FormatInt(int64(id), 10), nil)
	ExternalGetSubscription(getContext)
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("get status = %d body=%s", getRecorder.Code, getRecorder.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(getRecorder.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["id"] != id || got["subscription_id"] != id {
		t.Fatalf("get identity = %#v", got)
	}

	lookupRecorder := httptest.NewRecorder()
	lookupContext, _ := gin.CreateTestContext(lookupRecorder)
	lookupContext.Request = httptest.NewRequest(http.MethodGet, "/api/subscriptions/lookup?media_type=TV&tmdb_id=123456", nil)
	ExternalLookupSubscription(lookupContext)
	if lookupRecorder.Code != http.StatusOK {
		t.Fatalf("lookup status = %d body=%s", lookupRecorder.Code, lookupRecorder.Body.String())
	}
	var lookup map[string]any
	if err := json.Unmarshal(lookupRecorder.Body.Bytes(), &lookup); err != nil {
		t.Fatal(err)
	}
	if lookup["exists"] != true || lookup["subscription_id"] != id {
		t.Fatalf("lookup response = %#v", lookup)
	}

	stored, err := modelSubscriptionByName("测试剧集")
	if err != nil {
		t.Fatal(err)
	}
	if stored.SourceType != model.SubscriptionSourceManual || stored.TMDBID != 123456 || stored.Season != 1 {
		t.Fatalf("stored subscription mapping = %#v", stored)
	}
	if strings.Contains(stored.SourceConfig, "_request") {
		t.Fatalf("stored source config retained request metadata: %s", stored.SourceConfig)
	}
}

func TestExternalSubscriptionRejectsConflictingLookup(t *testing.T) {
	setupSubscriptionHandleDB(t)
	conf.Conf.ExternalSubscription.Enabled = true
	conf.Conf.ExternalSubscription.AllowUnauthenticated = true
	conf.Conf.ExternalSubscription.RunOnCreate = false

	create := func(body, idempotencyKey string) int {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPost, "/api/subscriptions", strings.NewReader(body))
		c.Request.Header.Set("Content-Type", "application/json")
		c.Request.Header.Set("Idempotency-Key", idempotencyKey)
		ExternalCreateSubscription(c)
		return recorder.Code
	}

	if code := create(`{"name":"A","media_type":"movie","tmdb_id":888,"source_type":"manual","source_config":{}}`, "request-a"); code != http.StatusOK {
		t.Fatalf("first create status = %d", code)
	}
	if code := create(`{"name":"B","media_type":"movie","tmdb_id":888,"source_type":"manual","source_config":{}}`, "request-b"); code != http.StatusConflict {
		t.Fatalf("conflicting create status = %d, want %d", code, http.StatusConflict)
	}
}

func modelSubscriptionByName(name string) (*model.Subscription, error) {
	items, _, err := db.ListSubscriptions(db.SubscriptionFilter{Keyword: name, PerPage: 1})
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, errors.New("subscription not found")
	}
	return &items[0], nil
}

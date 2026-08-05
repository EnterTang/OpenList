package handles

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	cd2 "github.com/OpenListTeam/OpenList/v4/drivers/115_cd2"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func setupCD2OAuthHandleDB(t *testing.T) *gorm.DB {
	t.Helper()
	gin.SetMode(gin.TestMode)
	previousConf := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	database, err := gorm.Open(sqlite.Open("file:"+url.QueryEscape(t.Name())+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.Init(database)
	t.Cleanup(func() {
		conf.Conf = previousConf
		if sqlDB, err := database.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	return database
}

func newCD2OAuthHandleContext(t *testing.T, target string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, target, nil)
	return c, recorder
}

func TestCD2OAuthCallbackPersistsTokensAndReloadsStorage(t *testing.T) {
	database := setupCD2OAuthHandleDB(t)
	addition := cd2.Addition{AuthMode: "oauth", OAuthState: "state-123", OAuthURL: "https://passportapi.115.com/open/authorize"}
	addition.RootFolderID = "0"
	rawAddition, err := utils.Json.MarshalToString(&addition)
	if err != nil {
		t.Fatal(err)
	}
	storage := &model.Storage{MountPath: "/cd2-oauth-" + strings.ToLower(t.Name()), Driver: "115 CD2", Addition: rawAddition}
	if err := db.CreateStorage(storage); err != nil {
		t.Fatalf("create storage: %v", err)
	}

	c, recorder := newCD2OAuthHandleContext(t, "/api/115-cd2/oauth/callback?storage_id="+url.QueryEscape(stringID(storage.ID))+"&state=state-123&access_token=access-new&refresh_token=refresh-new")
	CD2OAuthCallback(c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "authorization complete") {
		t.Fatalf("success body = %q", recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "access-new") || strings.Contains(recorder.Body.String(), "refresh-new") {
		t.Fatal("callback response leaked token material")
	}

	var saved model.Storage
	if err := database.First(&saved, storage.ID).Error; err != nil {
		t.Fatalf("reload storage: %v", err)
	}
	var savedAddition cd2.Addition
	if err := utils.Json.UnmarshalFromString(saved.Addition, &savedAddition); err != nil {
		t.Fatal(err)
	}
	if savedAddition.AccessToken != "access-new" || savedAddition.RefreshToken != "refresh-new" {
		t.Fatalf("saved tokens = (%q, %q)", savedAddition.AccessToken, savedAddition.RefreshToken)
	}
	if savedAddition.OAuthState != "" || savedAddition.OAuthURL != "" {
		t.Fatal("saved OAuth state was not cleared")
	}
	replayContext, replayRecorder := newCD2OAuthHandleContext(t, "/api/115-cd2/oauth/callback?storage_id="+url.QueryEscape(stringID(storage.ID))+"&state=state-123&access_token=access-new&refresh_token=refresh-new")
	CD2OAuthCallback(replayContext)
	if replayRecorder.Code != http.StatusBadRequest {
		t.Fatalf("replayed callback status = %d, want 400", replayRecorder.Code)
	}
}

func TestCD2OAuthCallbackRejectsStateMismatchWithoutMutation(t *testing.T) {
	setupCD2OAuthHandleDB(t)
	addition := cd2.Addition{OAuthState: "expected-state", AccessToken: "old-access", RefreshToken: "old-refresh"}
	rawAddition, err := utils.Json.MarshalToString(&addition)
	if err != nil {
		t.Fatal(err)
	}
	storage := &model.Storage{MountPath: "/cd2-oauth-state-" + strings.ToLower(t.Name()), Driver: "115 CD2", Addition: rawAddition}
	if err := db.CreateStorage(storage); err != nil {
		t.Fatalf("create storage: %v", err)
	}
	c, recorder := newCD2OAuthHandleContext(t, "/api/115-cd2/oauth/callback?storage_id="+url.QueryEscape(stringID(storage.ID))+"&state=wrong-state&access_token=new-access&refresh_token=new-refresh")
	CD2OAuthCallback(c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	var saved model.Storage
	if err := db.GetDb().First(&saved, storage.ID).Error; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(saved.Addition, "old-access") || strings.Contains(saved.Addition, "new-access") {
		t.Fatalf("storage addition was modified: %s", saved.Addition)
	}
}

func TestCD2OAuthCallbackRejectsNonCD2Storage(t *testing.T) {
	setupCD2OAuthHandleDB(t)
	addition, err := json.Marshal(map[string]string{"oauth_state": "expected-state"})
	if err != nil {
		t.Fatal(err)
	}
	storage := &model.Storage{MountPath: "/not-cd2-" + strings.ToLower(t.Name()), Driver: "Local", Addition: string(addition)}
	if err := db.CreateStorage(storage); err != nil {
		t.Fatalf("create storage: %v", err)
	}
	c, recorder := newCD2OAuthHandleContext(t, "/api/115-cd2/oauth/callback?storage_id="+url.QueryEscape(stringID(storage.ID))+"&state=expected-state&access_token=access&refresh_token=refresh")
	CD2OAuthCallback(c)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
}

func TestCD2OAuthCallbackRejectsProviderErrorWithoutMutation(t *testing.T) {
	setupCD2OAuthHandleDB(t)
	addition := cd2.Addition{OAuthState: "expected-state", AccessToken: "old-access", RefreshToken: "old-refresh"}
	rawAddition, err := utils.Json.MarshalToString(&addition)
	if err != nil {
		t.Fatal(err)
	}
	storage := &model.Storage{MountPath: "/cd2-oauth-error-" + strings.ToLower(t.Name()), Driver: "115 CD2", Addition: rawAddition}
	if err := db.CreateStorage(storage); err != nil {
		t.Fatalf("create storage: %v", err)
	}
	c, recorder := newCD2OAuthHandleContext(t, "/api/115-cd2/oauth/callback?storage_id="+url.QueryEscape(stringID(storage.ID))+"&state=expected-state&error=access_denied&error_description=denied")
	CD2OAuthCallback(c)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	var saved model.Storage
	if err := db.GetDb().First(&saved, storage.ID).Error; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(saved.Addition, "old-access") || strings.Contains(saved.Addition, "denied") {
		t.Fatalf("provider error modified storage: %s", saved.Addition)
	}
}

func TestCD2OAuthCallbackRejectsIncompleteTokensWithoutMutation(t *testing.T) {
	setupCD2OAuthHandleDB(t)
	addition := cd2.Addition{OAuthState: "expected-state", AccessToken: "old-access", RefreshToken: "old-refresh"}
	rawAddition, err := utils.Json.MarshalToString(&addition)
	if err != nil {
		t.Fatal(err)
	}
	storage := &model.Storage{MountPath: "/cd2-oauth-incomplete-" + strings.ToLower(t.Name()), Driver: "115 CD2", Addition: rawAddition}
	if err := db.CreateStorage(storage); err != nil {
		t.Fatalf("create storage: %v", err)
	}
	c, recorder := newCD2OAuthHandleContext(t, "/api/115-cd2/oauth/callback?storage_id="+url.QueryEscape(stringID(storage.ID))+"&state=expected-state&access_token=new-access")
	CD2OAuthCallback(c)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	var saved model.Storage
	if err := db.GetDb().First(&saved, storage.ID).Error; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(saved.Addition, "old-access") || strings.Contains(saved.Addition, "new-access") {
		t.Fatalf("incomplete callback modified storage: %s", saved.Addition)
	}
}

func stringID(id uint) string {
	return strconv.FormatUint(uint64(id), 10)
}

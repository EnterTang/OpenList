package middlewares

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/gin-gonic/gin"
)

func TestExternalSubscriptionAuthRequiresConfiguredToken(t *testing.T) {
	previous := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	conf.Conf.ExternalSubscription.Enabled = true
	conf.Conf.ExternalSubscription.APIToken = "secret-token"
	t.Cleanup(func() { conf.Conf = previous })

	request := func(authorization, headerToken string) int {
		recorder := httptest.NewRecorder()
		engine := gin.New()
		engine.Use(ExternalSubscriptionAuth)
		engine.GET("/subscriptions", func(c *gin.Context) { c.Status(http.StatusNoContent) })
		req := httptest.NewRequest(http.MethodGet, "/subscriptions", nil)
		if authorization != "" {
			req.Header.Set("Authorization", authorization)
		}
		if headerToken != "" {
			req.Header.Set("X-OpenList-Subscription-Token", headerToken)
		}
		engine.ServeHTTP(recorder, req)
		return recorder.Code
	}

	if code := request("", ""); code != http.StatusUnauthorized {
		t.Fatalf("missing token status = %d", code)
	}
	if code := request("Bearer wrong", ""); code != http.StatusUnauthorized {
		t.Fatalf("wrong token status = %d", code)
	}
	if code := request("Bearer secret-token", ""); code != http.StatusNoContent {
		t.Fatalf("bearer token status = %d", code)
	}
	if code := request("", "secret-token"); code != http.StatusNoContent {
		t.Fatalf("header token status = %d", code)
	}
}

func TestExternalSubscriptionAuthRequiresExplicitUnauthenticatedOptIn(t *testing.T) {
	previous := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	conf.Conf.ExternalSubscription.Enabled = true
	t.Cleanup(func() { conf.Conf = previous })

	recorder := httptest.NewRecorder()
	engine := gin.New()
	engine.Use(ExternalSubscriptionAuth)
	engine.GET("/subscriptions", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/subscriptions", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured token status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}

	conf.Conf.ExternalSubscription.AllowUnauthenticated = true
	recorder = httptest.NewRecorder()
	engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/subscriptions", nil))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("explicit unauthenticated status = %d", recorder.Code)
	}
}

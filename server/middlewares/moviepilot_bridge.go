package middlewares

import (
	"bytes"
	"io"
	"net/http"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/moviepilotbridge"
	"github.com/gin-gonic/gin"
)

const (
	MoviePilotBridgeInstanceContextKey = "moviepilot_bridge_instance"
	maxMoviePilotBridgeBodySize        = 4 << 20
)

func MoviePilotBridgeAuth(service *moviepilotbridge.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !requestUsesHTTPS(c.Request) {
			c.AbortWithStatusJSON(http.StatusUpgradeRequired, gin.H{"detail": "MoviePilot Bridge requires HTTPS"})
			return
		}
		if service == nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"detail": "MoviePilot Bridge is not configured"})
			return
		}
		body, err := io.ReadAll(io.LimitReader(c.Request.Body, maxMoviePilotBridgeBodySize+1))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"detail": "could not read Bridge request body"})
			return
		}
		if len(body) > maxMoviePilotBridgeBodySize {
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{"detail": "Bridge request body is too large"})
			return
		}
		c.Request.Body = io.NopCloser(bytes.NewReader(body))
		path := c.Request.URL.RequestURI()
		if path == "" {
			path = c.Request.URL.Path
		}
		bridge, err := service.Verifier().VerifySignature(c.Request.Context(), c.Request.Header, c.Request.Method, path, body)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"detail": err.Error()})
			return
		}
		c.Set(MoviePilotBridgeInstanceContextKey, bridge)
		c.Next()
	}
}

func requestUsesHTTPS(request *http.Request) bool {
	if request == nil {
		return false
	}
	if request.TLS != nil {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(request.Header.Get("X-Forwarded-Proto")), "https")
}

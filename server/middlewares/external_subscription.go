package middlewares

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/gin-gonic/gin"
)

// ExternalSubscriptionAuth protects the machine-to-machine subscription API
// without granting the caller an OpenList user or administrator session.
func ExternalSubscriptionAuth(c *gin.Context) {
	config := conf.ExternalSubscription{}
	if conf.Conf != nil {
		config = conf.Conf.ExternalSubscription
	}
	if !config.Enabled {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"detail": "external subscription API is disabled"})
		return
	}

	expected := strings.TrimSpace(config.APIToken)
	if expected == "" {
		if config.AllowUnauthenticated {
			c.Next()
			return
		}
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"detail": "external subscription API token is not configured"})
		return
	}

	provided := strings.TrimSpace(c.GetHeader("X-OpenList-Subscription-Token"))
	if provided == "" {
		authorization := strings.TrimSpace(c.GetHeader("Authorization"))
		if len(authorization) >= len("Bearer ") && strings.EqualFold(authorization[:len("Bearer ")], "Bearer ") {
			provided = strings.TrimSpace(authorization[len("Bearer "):])
		}
	}
	if len(provided) != len(expected) || subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"detail": "invalid external subscription API token"})
		return
	}
	c.Next()
}

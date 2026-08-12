package handles

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func ExternalCreateSubscription(c *gin.Context) {
	var input subscription.ExternalSubscriptionCreateRequest
	if err := c.ShouldBindJSON(&input); err != nil {
		externalSubscriptionError(c, err, http.StatusBadRequest)
		return
	}
	response, created, err := subscription.CreateExternalSubscription(c.Request.Context(), input, externalSubscriptionIdempotencyKey(c))
	if err != nil {
		externalSubscriptionError(c, err, externalSubscriptionStatusCode(err))
		return
	}
	if shouldRunExternalSubscription(response, created) {
		if err := subscription.QueueExternalSubscriptionRun(response.ID); err != nil {
			externalSubscriptionError(c, err, http.StatusServiceUnavailable)
			return
		}
		response, err = subscription.ProjectExternalSubscription(c.Request.Context(), response.ID)
		if err != nil {
			externalSubscriptionError(c, err, http.StatusInternalServerError)
			return
		}
	}
	c.JSON(http.StatusOK, response)
}

func ExternalGetSubscription(c *gin.Context) {
	id, ok := externalSubscriptionID(c)
	if !ok {
		return
	}
	response, err := subscription.ProjectExternalSubscription(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			externalSubscriptionError(c, err, http.StatusNotFound)
			return
		}
		externalSubscriptionError(c, err, http.StatusInternalServerError)
		return
	}
	c.JSON(http.StatusOK, response)
}

func ExternalLookupSubscription(c *gin.Context) {
	tmdbID, err := strconv.ParseInt(strings.TrimSpace(c.Query("tmdb_id")), 10, 64)
	if err != nil {
		externalSubscriptionError(c, errors.New("tmdb_id is required"), http.StatusBadRequest)
		return
	}
	response, err := subscription.LookupExternalSubscription(c.Request.Context(), c.Query("media_type"), tmdbID)
	if err != nil {
		externalSubscriptionError(c, err, externalSubscriptionStatusCode(err))
		return
	}
	c.JSON(http.StatusOK, response)
}

func ExternalCheckSubscription(c *gin.Context) {
	externalQueueSubscription(c)
}

func ExternalUpdateSubscription(c *gin.Context) {
	externalQueueSubscription(c)
}

func externalQueueSubscription(c *gin.Context) {
	id, ok := externalSubscriptionID(c)
	if !ok {
		return
	}
	if err := subscription.QueueExternalSubscriptionRun(id); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			externalSubscriptionError(c, err, http.StatusNotFound)
			return
		}
		externalSubscriptionError(c, err, http.StatusServiceUnavailable)
		return
	}
	response, err := subscription.ProjectExternalSubscription(c.Request.Context(), id)
	if err != nil {
		externalSubscriptionError(c, err, http.StatusInternalServerError)
		return
	}
	c.JSON(http.StatusOK, response)
}

func shouldRunExternalSubscription(response *subscription.ExternalSubscriptionResponse, created bool) bool {
	if response == nil {
		return false
	}
	if created {
		return conf.Conf != nil && conf.Conf.ExternalSubscription.RunOnCreate
	}
	return response.Status == "pending" && conf.Conf != nil && conf.Conf.ExternalSubscription.RunOnCreate
}

func externalSubscriptionID(c *gin.Context) (uint, bool) {
	value := strings.TrimSpace(c.Param("id"))
	if value == "" {
		value = strings.TrimSpace(c.Query("id"))
	}
	id, err := strconv.ParseUint(value, 10, 64)
	if err != nil || id == 0 || uint64(uint(id)) != id {
		externalSubscriptionError(c, errors.New("id is required"), http.StatusBadRequest)
		return 0, false
	}
	return uint(id), true
}

func externalSubscriptionIdempotencyKey(c *gin.Context) string {
	if value := strings.TrimSpace(c.GetHeader("Idempotency-Key")); value != "" {
		return value
	}
	return strings.TrimSpace(c.GetHeader("X-Idempotency-Key"))
}

func externalSubscriptionStatusCode(err error) int {
	if errors.Is(err, subscription.ErrExternalSubscriptionInvalid) {
		return http.StatusBadRequest
	}
	if errors.Is(err, subscription.ErrExternalSubscriptionConflict) {
		return http.StatusConflict
	}
	return http.StatusInternalServerError
}

func externalSubscriptionError(c *gin.Context, err error, status int) {
	detail := "external subscription request failed"
	if err != nil {
		detail = err.Error()
	}
	c.AbortWithStatusJSON(status, gin.H{"detail": detail})
}

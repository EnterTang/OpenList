package handles

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster"
	"github.com/OpenListTeam/OpenList/v4/internal/moviepilotbridge"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type moviePilotBridgeWriteRequest struct {
	ID      string `json:"id,omitempty"`
	Name    string `json:"name"`
	BaseURL string `json:"base_url"`
	HMACKey string `json:"hmac_key,omitempty"`
	Enabled *bool  `json:"enabled,omitempty"`
}

type moviePilotBridgeView struct {
	ID                string     `json:"id"`
	Name              string     `json:"name"`
	BaseURL           string     `json:"base_url"`
	Enabled           bool       `json:"enabled"`
	SecretConfigured  bool       `json:"secret_configured"`
	SecretFingerprint string     `json:"secret_fingerprint,omitempty"`
	LastHealth        string     `json:"last_health,omitempty"`
	LastSeenAt        *time.Time `json:"last_seen_at,omitempty"`
	LastError         string     `json:"last_error,omitempty"`
}

func ListMoviePilotBridges(service *moviepilotbridge.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		items, err := service.ListInstances(c.Request.Context())
		if err != nil {
			common.ErrorResp(c, err, http.StatusInternalServerError)
			return
		}
		views := make([]moviePilotBridgeView, 0, len(items))
		for _, item := range items {
			views = append(views, moviePilotBridgeView{
				ID: item.ID, Name: item.Name, BaseURL: item.BaseURL, Enabled: item.Enabled,
				SecretConfigured: strings.TrimSpace(item.SecretFingerprint) != "", SecretFingerprint: item.SecretFingerprint,
				LastHealth: item.LastHealth, LastSeenAt: item.LastSeenAt, LastError: item.LastError,
			})
		}
		common.SuccessResp(c, views)
	}
}

func UpsertMoviePilotBridge(service *moviepilotbridge.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req moviePilotBridgeWriteRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			common.ErrorResp(c, err, http.StatusBadRequest)
			return
		}
		id := strings.TrimSpace(req.ID)
		if id == "" {
			id = uuid.NewString()
		}
		existing, lookupErr := service.GetInstance(c.Request.Context(), id)
		if lookupErr != nil && !errors.Is(lookupErr, gorm.ErrRecordNotFound) {
			common.ErrorResp(c, lookupErr, http.StatusInternalServerError)
			return
		}
		secretRef := ""
		secretFingerprint := ""
		if existing != nil {
			secretRef = existing.SecretRef
			secretFingerprint = existing.SecretFingerprint
		}
		if strings.TrimSpace(req.HMACKey) != "" {
			if len([]byte(req.HMACKey)) < 16 {
				common.ErrorStrResp(c, "hmac_key must contain at least 16 bytes", http.StatusBadRequest)
				return
			}
			secret, err := cluster.WriteSecret(c.Request.Context(), cluster.SecretWriteRequest{
				ID: secretRef, Alias: "moviepilot_bridge_" + id, Kind: "moviepilot_bridge_hmac",
				Value: map[string]any{"hmac_key": req.HMACKey},
			}, clusterControlActor(c))
			if err != nil {
				common.ErrorResp(c, err, http.StatusBadRequest)
				return
			}
			secretRef, secretFingerprint = secret.ID, secret.Fingerprint
		}
		if secretRef == "" {
			common.ErrorStrResp(c, "hmac_key is required for a new Bridge instance", http.StatusBadRequest)
			return
		}
		enabled := true
		if existing != nil {
			enabled = existing.Enabled
		}
		if req.Enabled != nil {
			enabled = *req.Enabled
		}
		item, err := service.UpsertInstance(c.Request.Context(), moviepilotbridge.InstanceUpsertRequest{
			ID: id, Name: req.Name, BaseURL: req.BaseURL, SecretRef: secretRef,
			SecretFingerprint: secretFingerprint, Enabled: enabled,
		})
		if err != nil {
			common.ErrorResp(c, err, http.StatusBadRequest)
			return
		}
		common.SuccessResp(c, moviePilotBridgeView{
			ID: item.ID, Name: item.Name, BaseURL: item.BaseURL, Enabled: item.Enabled,
			SecretConfigured: item.SecretFingerprint != "", SecretFingerprint: item.SecretFingerprint,
			LastHealth: item.LastHealth, LastSeenAt: item.LastSeenAt, LastError: item.LastError,
		})
	}
}

func DisableMoviePilotBridge(service *moviepilotbridge.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if err := service.DisableInstance(c.Request.Context(), c.Param("id")); err != nil {
			common.ErrorResp(c, err, http.StatusBadRequest)
			return
		}
		common.SuccessResp(c, gin.H{"disabled": true})
	}
}

func ConsumeMoviePilotBridgeEvent(service *moviepilotbridge.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		body, err := io.ReadAll(io.LimitReader(c.Request.Body, 4<<20))
		if err != nil {
			common.ErrorResp(c, err, http.StatusBadRequest)
			return
		}
		var event moviepilotbridge.BridgeEvent
		if err := json.Unmarshal(body, &event); err != nil {
			common.ErrorResp(c, err, http.StatusBadRequest)
			return
		}
		path := c.Request.URL.RequestURI()
		if path == "" {
			path = c.Request.URL.Path
		}
		result, err := service.ConsumeBridgeEvent(c.Request.Context(), c.Request.Header, c.Request.Method, path, body, event)
		if err != nil {
			common.ErrorResp(c, err, http.StatusBadRequest)
			return
		}
		if _, processErr := service.ProcessPendingEvents(c.Request.Context(), 1); processErr != nil {
			// The inbox is already durable. Return acceptance while leaving the
			// event marked failed for a duplicate delivery or later retry.
			common.SuccessResp(c, gin.H{"accepted": true, "duplicate": result.Duplicate, "stored": result.Stored, "processing": "queued", "processing_error": processErr.Error()})
			return
		}
		common.SuccessResp(c, gin.H{"accepted": true, "duplicate": result.Duplicate, "stored": result.Stored})
	}
}

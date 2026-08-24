package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/moviepilotbridge"
)

func newMoviePilotBridgeService() *moviepilotbridge.Service {
	return moviepilotbridge.NewService(db.GetDb(), resolveMoviePilotBridgeSecret, http.DefaultClient)
}

func resolveMoviePilotBridgeSecret(ctx context.Context, secretRef string) ([]byte, error) {
	raw, _, err := cluster.ResolveSecret(ctx, secretRef)
	if err != nil {
		return nil, err
	}
	var payload struct {
		HMACKey string `json:"hmac_key"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil || payload.HMACKey == "" {
		return nil, errors.New("stored MoviePilot Bridge secret is invalid")
	}
	return []byte(payload.HMACKey), nil
}

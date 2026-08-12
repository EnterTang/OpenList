package handles

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	cd2 "github.com/OpenListTeam/OpenList/v4/drivers/115_cd2"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

// CD2OAuthCallback completes the browser-based CloudDrive2 115 OAuth flow.
// It is public because the provider redirects the browser here without an
// OpenList login cookie. The one-time state in the storage Addition is the
// only authorization for this callback.
func CD2OAuthCallback(c *gin.Context) {
	storageID, err := strconv.ParseUint(strings.TrimSpace(c.Query("storage_id")), 10, 64)
	if err != nil || storageID == 0 {
		common.ErrorPage(c, fmt.Errorf("115 CD2 OAuth callback has an invalid storage_id"), http.StatusBadRequest)
		return
	}

	storage, err := db.GetStorageById(uint(storageID))
	if err != nil {
		common.ErrorPage(c, fmt.Errorf("115 CD2 OAuth storage not found"), http.StatusNotFound)
		return
	}
	if storage.Driver != "115 CD2" {
		common.ErrorPage(c, fmt.Errorf("115 CD2 OAuth callback does not belong to this storage"), http.StatusBadRequest)
		return
	}

	var addition cd2.Addition
	if err := utils.Json.UnmarshalFromString(storage.Addition, &addition); err != nil {
		common.ErrorPage(c, fmt.Errorf("115 CD2 OAuth storage configuration is invalid"), http.StatusInternalServerError)
		return
	}
	if err := cd2.CompleteOAuthCallback(&addition, c.Request.URL.Query()); err != nil {
		common.ErrorPage(c, err, http.StatusBadRequest)
		return
	}
	storage.Addition, err = utils.Json.MarshalToString(&addition)
	if err != nil {
		common.ErrorPage(c, fmt.Errorf("115 CD2 OAuth token persistence failed"), http.StatusInternalServerError)
		return
	}
	if err := db.UpdateStorage(storage); err != nil {
		common.ErrorPage(c, fmt.Errorf("115 CD2 OAuth token persistence failed"), http.StatusInternalServerError)
		return
	}
	if err := op.LoadStorage(c.Request.Context(), *storage); err != nil {
		common.ErrorPage(c, fmt.Errorf("115 CD2 storage reload failed: %w", err), http.StatusInternalServerError)
		return
	}

	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>115 CD2 authorization complete</title></head>
<body><h1>115 CD2 authorization complete</h1><p>You can close this window.</p></body></html>`))
}

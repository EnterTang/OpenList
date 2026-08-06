package automation

import (
	"context"
	"fmt"
	"strings"
	"time"

	sy "github.com/OpenListTeam/OpenList/v4/internal/115sy"
)

type CleanupRequest struct {
	RootCID         string
	Before          time.Time
	NamePrefixes    []string
	DryRun          bool
	CleanRecycleBin bool
	SecurityCode    string
}

type CleanupResult struct {
	Matched int
	Deleted int
	Failed  []string
}

func Clean(ctx context.Context, client *sy.Client, req CleanupRequest) (CleanupResult, error) {
	if client == nil {
		return CleanupResult{}, fmt.Errorf("115-sy client is nil")
	}
	rootCID := strings.TrimSpace(req.RootCID)
	if rootCID == "" {
		rootCID = "0"
	}
	if req.CleanRecycleBin && strings.TrimSpace(req.SecurityCode) == "" {
		return CleanupResult{}, fmt.Errorf("recycle-bin cleanup requires a request-scoped security code")
	}
	if req.Before.IsZero() && !hasNonEmptyPrefix(req.NamePrefixes) {
		return CleanupResult{}, fmt.Errorf("cleanup requires an age or name-prefix filter")
	}
	items, err := StarSync(ctx, client, rootCID)
	if err != nil {
		return CleanupResult{}, err
	}
	result := CleanupResult{}
	for _, item := range items {
		if item.ID == rootCID || item.IsDir {
			continue
		}
		if !cleanupMatch(item, req) {
			continue
		}
		result.Matched++
		if req.DryRun {
			continue
		}
		if err := client.Remove(ctx, item.ID, item.ParentID); err != nil {
			result.Failed = append(result.Failed, item.ID)
			continue
		}
		result.Deleted++
	}
	if len(result.Failed) > 0 {
		return result, fmt.Errorf("cleanup failed for %d items", len(result.Failed))
	}
	if req.CleanRecycleBin && !req.DryRun {
		if err := client.RecyclebinClean(ctx, req.SecurityCode); err != nil {
			return result, err
		}
	}
	return result, nil
}

func hasNonEmptyPrefix(prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.TrimSpace(prefix) != "" {
			return true
		}
	}
	return false
}

func cleanupMatch(item TreeRecord, req CleanupRequest) bool {
	if !req.Before.IsZero() {
		if item.ModifyTime == 0 || !time.Unix(item.ModifyTime, 0).Before(req.Before) {
			return false
		}
	}
	if len(req.NamePrefixes) == 0 {
		return true
	}
	for _, prefix := range req.NamePrefixes {
		if prefix = strings.TrimSpace(prefix); prefix != "" && strings.HasPrefix(item.Name, prefix) {
			return true
		}
	}
	return false
}

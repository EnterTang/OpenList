package guangyapan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// shareRootParentID is the parentId for share root listing/restore.
// Real API rejects "0" with code 143 (file not found); empty string works.
const shareRootParentID = ""

// shareRestoreSynchronousTaskID is an in-memory marker used when the restore
// endpoint explicitly reports synchronous success and does not return a task.
// It is never sent back to the provider.
const shareRestoreSynchronousTaskID = "__guangyapan_restore_synchronous_success__"

var (
	// ErrShareRestoreResultUnknown means the provider accepted neither a
	// durable task identifier nor a provable synchronous result. Callers must
	// probe the target before retrying the restore operation.
	ErrShareRestoreResultUnknown = errors.New("guangyapan share restore result unknown")

	shareURLPattern = regexp.MustCompile(`(?i)(?:https?://)?(?:www\.)?guangyapan\.com/s/([A-Za-z0-9_-]+)`)
	shareCodeQuery  = regexp.MustCompile(`(?i)[?&](?:code|pwd)=([A-Za-z0-9]{1,16})`)
	shareCodeText   = regexp.MustCompile(`(?i)(?:提取码|访问码|密码|code|pwd)\s*[:：=]\s*([A-Za-z0-9]{1,16})`)
)

type guangYaShareError struct {
	message string
	code    string
}

func (e *guangYaShareError) Error() string {
	if e == nil {
		return "guangyapan share operation failed"
	}
	return e.message
}

func (e *guangYaShareError) ClusterErrorCode() string {
	if e == nil {
		return ""
	}
	return e.code
}

func newGuangYaShareError(message string, resultUnknown bool) error {
	message = strings.TrimSpace(message)
	if message == "" {
		message = "guangyapan share operation failed"
	}
	code := classifyGuangYaShareBusinessError(message)
	if resultUnknown {
		code = "share_save_result_unknown"
	}
	return &guangYaShareError{message: message, code: code}
}

func classifyGuangYaShareBusinessError(message string) string {
	value := strings.ToLower(strings.TrimSpace(message))
	switch {
	case strings.Contains(value, "token"), strings.Contains(value, "unauthorized"), strings.Contains(value, "登录"), strings.Contains(value, "认证"):
		return "reauthorization_required"
	case strings.Contains(value, "rate"), strings.Contains(value, "频繁"), strings.Contains(value, "too many"), strings.Contains(value, "限流"):
		return "share_save_retryable"
	default:
		return "share_save_terminal"
	}
}

type ShareSummary struct {
	ShareID   string
	Title     string
	FileCount int
}

type ShareFileItem struct {
	FileID   string
	FileName string
	FileSize int64
	IsFolder bool
	ParentID string
}

// ParseShareURL extracts shareId and optional passcode from a GuangYa share URL or text.
func ParseShareURL(raw string, defaultCode string) (shareID, code string, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", errors.New("empty guangyapan share url")
	}
	m := shareURLPattern.FindStringSubmatch(raw)
	if len(m) < 2 {
		return "", "", fmt.Errorf("invalid guangyapan share url: %s", raw)
	}
	shareID = m[1]
	code = strings.TrimSpace(defaultCode)
	if qm := shareCodeQuery.FindStringSubmatch(raw); len(qm) >= 2 {
		code = qm[1]
	} else if tm := shareCodeText.FindStringSubmatch(raw); len(tm) >= 2 {
		code = tm[1]
	}
	return shareID, code, nil
}

func (d *GuangYaPan) GetShareSummary(ctx context.Context, shareID, code string) (*ShareSummary, error) {
	shareID = strings.TrimSpace(shareID)
	if shareID == "" {
		return nil, errors.New("share id is empty")
	}
	var resp shareSummaryResp
	if err := d.postShareAPI(ctx, "/userres/v1/get_share_summary", map[string]any{
		"shareId": shareID,
		"code":    strings.TrimSpace(code),
	}, false, &resp); err != nil {
		return nil, err
	}
	if !isBizOK(resp.Code) {
		return nil, newGuangYaShareError(fmt.Sprintf("get share summary failed: %s", firstNonEmpty(resp.Msg, resp.Message, fmt.Sprintf("code=%v", resp.Code))), false)
	}
	return &ShareSummary{
		ShareID:   firstNonEmpty(resp.Data.ShareID, shareID),
		Title:     resp.Data.Title,
		FileCount: resp.Data.FileCount,
	}, nil
}

func (d *GuangYaPan) GetShareAccessToken(ctx context.Context, shareID, code string) (string, error) {
	shareID = strings.TrimSpace(shareID)
	if shareID == "" {
		return "", errors.New("share id is empty")
	}
	var resp shareAccessTokenResp
	if err := d.postShareAPI(ctx, "/userres/v1/get_share_access_token", map[string]any{
		"shareId": shareID,
		"code":    strings.TrimSpace(code),
	}, false, &resp); err != nil {
		return "", err
	}
	if !isBizOK(resp.Code) {
		return "", newGuangYaShareError(fmt.Sprintf("get share access token failed: %s", firstNonEmpty(resp.Msg, resp.Message, fmt.Sprintf("code=%v", resp.Code))), false)
	}
	token := strings.TrimSpace(resp.Data.AccessToken)
	if token == "" {
		return "", errors.New("empty share access token")
	}
	return token, nil
}

func (d *GuangYaPan) ListShareFiles(ctx context.Context, shareAccessToken, parentID string) ([]ShareFileItem, error) {
	shareAccessToken = strings.TrimSpace(shareAccessToken)
	if shareAccessToken == "" {
		return nil, errors.New("share access token is empty")
	}
	parentID = strings.TrimSpace(parentID)
	pageSize := d.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}

	out := make([]ShareFileItem, 0, pageSize)
	seen := make(map[string]struct{})
	for page := 0; ; page++ {
		if page >= 10000 {
			return nil, errors.New("share pagination exceeded safety limit")
		}
		var resp shareListResp
		body := map[string]any{
			"accessToken": shareAccessToken,
			"parentId":    parentID, // root must be ""
			"pageSize":    pageSize,
			"orderBy":     0,
			"sortType":    0,
			"page":        page,
		}
		if err := d.postShareAPI(ctx, "/userres/v1/get_share_page_files_list", body, false, &resp); err != nil {
			return nil, err
		}
		if !isBizOK(resp.Code) {
			return nil, newGuangYaShareError(fmt.Sprintf("list share files failed: %s", firstNonEmpty(resp.Msg, resp.Message, fmt.Sprintf("code=%v", resp.Code))), false)
		}
		items := resp.Data.List
		if len(items) == 0 {
			items = resp.Data.Items
		}
		before := len(out)
		out = appendGuangYaShareItems(out, items, parentID, seen)
		if len(items) > 0 && len(out) == before {
			return nil, errors.New("share pagination made no progress")
		}
		if resp.Data.Total > 0 && len(out) >= resp.Data.Total {
			break
		}
		if len(items) < pageSize {
			break
		}
	}
	return out, nil
}

func appendGuangYaShareItems(out []ShareFileItem, items []shareListItem, parentID string, seen map[string]struct{}) []ShareFileItem {
	for _, item := range items {
		id := firstNonEmpty(item.FileID, item.ID)
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, ShareFileItem{
			FileID:   id,
			FileName: firstNonEmpty(item.FileName, item.Name),
			FileSize: item.FileSize,
			IsFolder: item.IsFolder || item.ResType == 2,
			ParentID: parentID,
		})
	}
	return out
}

func (d *GuangYaPan) RestoreShare(ctx context.Context, shareAccessToken string, fileIDs []string, parentID string) (string, error) {
	if err := d.ensureAccessToken(ctx); err != nil {
		return "", err
	}
	shareAccessToken = strings.TrimSpace(shareAccessToken)
	if shareAccessToken == "" {
		return "", errors.New("share access token is empty")
	}
	ids := make([]string, 0, len(fileIDs))
	for _, id := range fileIDs {
		id = strings.TrimSpace(id)
		if id != "" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return "", errors.New("file ids is empty")
	}
	parentID = strings.TrimSpace(parentID)

	var resp shareRestoreResp
	if err := d.postShareAPI(ctx, "/userres/v1/restore_share", map[string]any{
		"accessToken": shareAccessToken,
		"fileIds":     ids,
		"parentId":    parentID, // personal-disk root uses ""
	}, true, &resp); err != nil {
		return "", err
	}
	if !isBizOK(resp.Code) {
		return "", newGuangYaShareError(fmt.Sprintf("restore share failed: %s", firstNonEmpty(resp.Msg, resp.Message, resp.Data.Message, fmt.Sprintf("code=%v", resp.Code))), false)
	}
	taskID := strings.TrimSpace(resp.Data.TaskID)
	if taskID == "" {
		if resp.Data.Success || strings.EqualFold(strings.TrimSpace(resp.Data.Message), "success") || strings.EqualFold(strings.TrimSpace(resp.Msg), "success") {
			return shareRestoreSynchronousTaskID, nil
		}
		// An empty task ID is not proof of synchronous completion. The caller
		// must probe the destination before deciding whether to retry.
		return "", ErrShareRestoreResultUnknown
	}
	return taskID, nil
}

func (d *GuangYaPan) WaitShareRestoreTask(ctx context.Context, taskID string) error {
	taskID = strings.TrimSpace(taskID)
	if taskID == shareRestoreSynchronousTaskID {
		return nil
	}
	if taskID == "" {
		return ErrShareRestoreResultUnknown
	}
	const (
		maxTry   = 120
		interval = 500 * time.Millisecond
	)
	for i := 0; i < maxTry; i++ {
		var resp shareTaskStatusResp
		if err := d.postShareAPI(ctx, "/userres/v1/get_task_status", map[string]any{
			"taskId": taskID,
		}, true, &resp); err != nil {
			// Fall back to personal-disk task status endpoint used by Alist.
			if err2 := d.waitTaskDone(ctx, taskID); err2 == nil {
				return nil
			}
			return err
		}
		if !isBizOK(resp.Code) && strings.TrimSpace(resp.Msg) != "" && !strings.EqualFold(strings.TrimSpace(resp.Msg), "success") {
			return newGuangYaShareError(fmt.Sprintf("get restore task status failed: %s", firstNonEmpty(resp.Msg, resp.Message, fmt.Sprintf("code=%v", resp.Code))), false)
		}
		if shareTaskSucceeded(resp.Data.Status, resp.Data.Progress) {
			return nil
		}
		if shareTaskFailed(resp.Data.Status) {
			return newGuangYaShareError(fmt.Sprintf("restore task failed with status=%v", resp.Data.Status), false)
		}
		if i == maxTry-1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
	return newGuangYaShareError("restore task timeout", true)
}

// TransferShareLink restores all root items from a share URL into parentID.
func (d *GuangYaPan) TransferShareLink(ctx context.Context, shareURL, parentID, code string) (taskID string, fileCount int, err error) {
	shareID, code, err := ParseShareURL(shareURL, code)
	if err != nil {
		return "", 0, err
	}
	token, err := d.GetShareAccessToken(ctx, shareID, code)
	if err != nil {
		return "", 0, err
	}
	items, err := d.ListShareFiles(ctx, token, shareRootParentID)
	if err != nil {
		return "", 0, err
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		if item.FileID != "" {
			ids = append(ids, item.FileID)
		}
	}
	if len(ids) == 0 {
		return "", 0, errors.New("no files found in share link")
	}
	taskID, err = d.RestoreShare(ctx, token, ids, parentID)
	if err != nil {
		return "", 0, err
	}
	if err := d.WaitShareRestoreTask(ctx, taskID); err != nil {
		return taskID, len(ids), err
	}
	return taskID, len(ids), nil
}

func (d *GuangYaPan) postShareAPI(ctx context.Context, path string, body any, withUserAuth bool, out any) error {
	if d.apiClient == nil {
		return errors.New("api client is not initialized")
	}
	idempotent := guangYaPathIdempotent(path)
	authRefreshed := false
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			if err := waitGuangYaRetry(ctx, attempt); err != nil {
				return err
			}
		}
		if err := d.apiRateLimitWait(ctx, path); err != nil {
			return err
		}
		req := d.apiClient.R().
			SetContext(ctx).
			SetHeaders(d.shareHeaders(withUserAuth)).
			SetBody(body)
		if out != nil {
			req.SetResult(out)
		}
		resp, err := req.Post(path)
		if err != nil {
			if idempotent && attempt+1 < maxAttempts {
				continue
			}
			return &guangYaRequestError{Disposition: guangYaResultUnknown, Message: err.Error(), ResultUnknown: !idempotent}
		}

		disposition := classifyGuangYaHTTPStatus(resp.StatusCode(), idempotent)
		if (resp.StatusCode() == http.StatusUnauthorized || resp.StatusCode() == http.StatusForbidden) && withUserAuth && !authRefreshed && strings.TrimSpace(d.RefreshToken) != "" {
			authRefreshed = true
			if err := d.refreshToken(ctx); err != nil {
				return &guangYaRequestError{Status: resp.StatusCode(), Disposition: guangYaReauthorize, Message: err.Error()}
			}
			attempt--
			continue
		}
		if disposition == guangYaRetry && attempt+1 < maxAttempts {
			continue
		}
		if resp.IsError() {
			return &guangYaRequestError{Status: resp.StatusCode(), Disposition: disposition, ResultUnknown: disposition == guangYaResultUnknown}
		}
		return nil
	}
	return &guangYaRequestError{Disposition: guangYaRetry, Message: "retry limit exceeded"}
}

func (d *GuangYaPan) shareHeaders(withUserAuth bool) map[string]string {
	headers := map[string]string{
		"Accept":             "application/json, text/plain, */*",
		"Content-Type":       "application/json",
		"Did":                d.DeviceID,
		"did":                d.DeviceID,
		"Dt":                 "web",
		"dt":                 "web",
		"x-client-id":        "301",
		"X-Client-Id":        "301",
		"x-sdk-version":      "9.0.2",
		"X-SDK-Version":      "9.0.2",
		"x-protocol-version": "0.0.1",
		"Origin":             "https://www.guangyapan.com",
		"Referer":            "https://www.guangyapan.com/",
	}
	if withUserAuth && strings.TrimSpace(d.AccessToken) != "" {
		headers["Authorization"] = "Bearer " + d.AccessToken
	}
	return headers
}

func isBizOK(code any) bool {
	switch v := code.(type) {
	case nil:
		return true
	case int:
		return v == 0 || v == 200
	case int64:
		return v == 0 || v == 200
	case float64:
		return v == 0 || v == 200
	case string:
		s := strings.TrimSpace(v)
		return s == "" || s == "0" || s == "200" || strings.EqualFold(s, "success") || strings.EqualFold(s, "ok")
	case json.Number:
		s := strings.TrimSpace(v.String())
		return s == "" || s == "0" || s == "200"
	default:
		s := strings.TrimSpace(fmt.Sprint(v))
		return s == "" || s == "0" || s == "200" || strings.EqualFold(s, "success") || strings.EqualFold(s, "ok") || s == "<nil>"
	}
}

func shareTaskSucceeded(status any, progress any) bool {
	switch v := status.(type) {
	case int:
		return v == 2
	case int64:
		return v == 2
	case float64:
		return int(v) == 2
	case string:
		s := strings.ToLower(strings.TrimSpace(v))
		if s == "success" || s == "completed" || s == "done" || s == "2" {
			return true
		}
	}
	switch v := progress.(type) {
	case int:
		return v >= 100
	case int64:
		return v >= 100
	case float64:
		return v >= 100
	case string:
		return strings.TrimSpace(v) == "100"
	}
	return false
}

func shareTaskFailed(status any) bool {
	switch v := status.(type) {
	case int:
		return v == -1 || v == 3 || v == 4
	case int64:
		return v == -1 || v == 3 || v == 4
	case float64:
		i := int(v)
		return i == -1 || i == 3 || i == 4
	case string:
		s := strings.ToLower(strings.TrimSpace(v))
		return s == "failed" || s == "fail" || s == "error" || s == "canceled" || s == "cancelled" || s == "-1" || s == "3" || s == "4"
	}
	return false
}

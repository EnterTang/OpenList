package guangyapan

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/go-resty/resty/v2"
	"golang.org/x/time/rate"
)

func TestWaitShareRestoreTaskRejectsEmptyTaskIDAsUnknown(t *testing.T) {
	var driver GuangYaPan

	err := driver.WaitShareRestoreTask(context.Background(), "")
	if !errors.Is(err, ErrShareRestoreResultUnknown) {
		t.Fatalf("WaitShareRestoreTask(empty) error = %v, want ErrShareRestoreResultUnknown", err)
	}
}

func TestWaitShareRestoreTaskAcceptsSynchronousSuccessMarker(t *testing.T) {
	var driver GuangYaPan
	if err := driver.WaitShareRestoreTask(context.Background(), shareRestoreSynchronousTaskID); err != nil {
		t.Fatalf("WaitShareRestoreTask(synchronous) error = %v", err)
	}
}

func TestGuangYaRequestErrorMapsClusterDisposition(t *testing.T) {
	tests := []struct {
		disposition guangYaRetryDisposition
		want        string
	}{
		{guangYaRetry, "share_save_retryable"},
		{guangYaResultUnknown, "share_save_result_unknown"},
		{guangYaReauthorize, "reauthorization_required"},
		{guangYaTerminal, "share_save_terminal"},
	}
	for _, test := range tests {
		err := &guangYaRequestError{Disposition: test.disposition}
		if got := err.ClusterErrorCode(); got != test.want {
			t.Fatalf("ClusterErrorCode(%q) = %q, want %q", test.disposition, got, test.want)
		}
	}
}

func TestGuangYaShareBusinessErrorMapsRetryDisposition(t *testing.T) {
	tests := []struct {
		message string
		want    string
	}{
		{"请求过于频繁", "share_save_retryable"},
		{"refresh token expired", "reauthorization_required"},
		{"share canceled", "share_save_terminal"},
	}
	for _, test := range tests {
		err := newGuangYaShareError(test.message, false)
		if got := err.(interface{ ClusterErrorCode() string }).ClusterErrorCode(); got != test.want {
			t.Fatalf("share error %q code = %q, want %q", test.message, got, test.want)
		}
	}
}

func TestGuangYaRetryDispositionHonorsIdempotency(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		idempotent bool
		want       guangYaRetryDisposition
	}{
		{name: "rate limited read", status: http.StatusTooManyRequests, idempotent: true, want: guangYaRetry},
		{name: "server error read", status: http.StatusBadGateway, idempotent: true, want: guangYaRetry},
		{name: "rate limited mutation", status: http.StatusTooManyRequests, idempotent: false, want: guangYaResultUnknown},
		{name: "auth failure", status: http.StatusUnauthorized, idempotent: true, want: guangYaReauthorize},
		{name: "bad request", status: http.StatusBadRequest, idempotent: true, want: guangYaTerminal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyGuangYaHTTPStatus(tt.status, tt.idempotent); got != tt.want {
				t.Fatalf("classifyGuangYaHTTPStatus(%d, %t) = %q, want %q", tt.status, tt.idempotent, got, tt.want)
			}
		})
	}
}

func TestAppendGuangYaShareItemsDeduplicatesProviderIDs(t *testing.T) {
	seen := make(map[string]struct{})
	items := []shareListItem{
		{FileID: "a", FileName: "one.mkv", FileSize: 10},
		{ID: "a", Name: "one.mkv", FileSize: 10},
		{FileID: "b", FileName: "two.mkv", FileSize: 20},
	}

	got := appendGuangYaShareItems(nil, items, "root", seen)
	if len(got) != 2 || got[0].FileID != "a" || got[1].FileID != "b" {
		t.Fatalf("appendGuangYaShareItems() = %#v, want two unique items", got)
	}
}

func TestGuangYaPostAPIRetriesIdempotentServerErrors(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"msg":"success"}`))
	}))
	defer server.Close()

	driver := &GuangYaPan{Addition: Addition{AccessToken: "test-token", DeviceID: "device"}}
	driver.apiClient = resty.New().SetBaseURL(server.URL)
	keyMaterial := "test-token"
	// Keep this test focused on retry semantics instead of the production
	// limiter's 500ms account interval.
	digest := sha256.Sum256([]byte(keyMaterial))
	key := hex.EncodeToString(digest[:])
	guangYaAccountLimiters.Store(key, rate.NewLimiter(rate.Inf, 1))
	defer guangYaAccountLimiters.Delete(key)

	var out commonResp
	if err := driver.postAPI(context.Background(), "/file/list", nil, &out); err != nil {
		t.Fatalf("postAPI() error = %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("postAPI() calls = %d, want 3", got)
	}
}

func TestGuangYaAccountErrDoesNotEchoResponseBody(t *testing.T) {
	resp := &resty.Response{RawResponse: &http.Response{
		StatusCode: http.StatusMethodNotAllowed,
		Header:     http.Header{"Content-Type": []string{"text/html"}},
	}}
	driver := &GuangYaPan{}
	got := driver.accountErr("", "", resp)
	if got != "status=405 content_type=text/html" {
		t.Fatalf("accountErr() = %q, want redacted status metadata", got)
	}
}

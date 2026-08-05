package _115_cd2

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
)

type recordingWaiter struct {
	calls int
}

func (w *recordingWaiter) Wait(context.Context) error {
	w.calls++
	return nil
}

func TestRequestThrottlerUsesIndependentBuckets(t *testing.T) {
	fileList := &recordingWaiter{}
	downloadURL := &recordingWaiter{}
	restAPI := &recordingWaiter{}
	downloadRequest := &recordingWaiter{}
	throttler := &requestThrottler{
		fileList:        fileList,
		downloadURL:     downloadURL,
		restAPI:         restAPI,
		downloadRequest: downloadRequest,
	}

	tests := []struct {
		name  string
		class requestClass
		want  *recordingWaiter
	}{
		{name: "file list", class: requestFileList, want: fileList},
		{name: "download URL", class: requestDownloadURL, want: downloadURL},
		{name: "REST API", class: requestRESTAPI, want: restAPI},
		{name: "download request", class: requestDownload, want: downloadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := throttler.wait(context.Background(), test.class); err != nil {
				t.Fatalf("wait() error = %v", err)
			}
			if test.want.calls != 1 {
				t.Fatalf("selected waiter calls = %d, want 1", test.want.calls)
			}
		})
	}
}

func TestRequestThrottlerPropagatesContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	throttler := &requestThrottler{}

	if err := throttler.wait(ctx, requestRESTAPI); err != context.Canceled {
		t.Fatalf("wait() error = %v, want %v", err, context.Canceled)
	}
}

type recordingRangeReader struct {
	called bool
}

func (r *recordingRangeReader) RangeRead(context.Context, http_range.Range) (io.ReadCloser, error) {
	r.called = true
	return io.NopCloser(strings.NewReader("ok")), nil
}

func TestThrottledRangeReaderWaitsBeforeEachDownloadRequest(t *testing.T) {
	waiter := &recordingWaiter{}
	upstream := &recordingRangeReader{}
	rangeReader := &throttledRangeReader{upstream: upstream, waiter: waiter}

	reader, err := rangeReader.RangeRead(context.Background(), http_range.Range{Start: 0, Length: 2})
	if err != nil {
		t.Fatalf("RangeRead() error = %v", err)
	}
	defer reader.Close()
	if waiter.calls != 1 {
		t.Fatalf("waiter calls = %d, want 1", waiter.calls)
	}
	if !upstream.called {
		t.Fatal("upstream RangeRead was not called")
	}
}

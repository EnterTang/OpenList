package _123

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
)

func TestMain(m *testing.M) {
	conf.Conf = conf.DefaultConfig(os.TempDir())
	os.Exit(m.Run())
}

func TestPan123DownloadReaderKeepsSuccessfulSingleStream(t *testing.T) {
	payload := []byte("single-stream-remains-the-default")
	var initialRequests atomic.Int32
	var refreshes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		initialRequests.Add(1)
		if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
			t.Errorf("initial full request Range = %q, want empty", rangeHeader)
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		_, _ = w.Write(payload)
	}))
	t.Cleanup(server.Close)

	reader, err := newPan123DownloadReader(
		context.Background(),
		int64(len(payload)),
		http_range.Range{Length: -1},
		pan123ResolvedDownload{URL: server.URL},
		func(context.Context) (pan123ResolvedDownload, error) {
			refreshes.Add(1)
			return pan123ResolvedDownload{URL: server.URL}, nil
		},
		pan123FallbackConfig{ChunkSize: 4, MaxRetries: 3},
	)
	if err != nil {
		t.Fatalf("create reader: %v", err)
	}
	defer reader.Close()

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
	if initialRequests.Load() != 1 {
		t.Fatalf("initial requests = %d, want 1", initialRequests.Load())
	}
	if refreshes.Load() != 0 {
		t.Fatalf("refreshes = %d, want 0", refreshes.Load())
	}
}

func TestPan123DownloadReaderFallsBackFromExactOffset(t *testing.T) {
	payload := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	const initialBytes = 11
	var refreshes atomic.Int32
	var fallbackRequests atomic.Int32
	var firstFallbackRange atomic.Value

	initial := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		_, _ = w.Write(payload[:initialBytes])
	}))
	t.Cleanup(initial.Close)

	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackRequests.Add(1)
		rangeHeader := r.Header.Get("Range")
		if fallbackRequests.Load() == 1 {
			firstFallbackRange.Store(rangeHeader)
		}
		start, end := parseTestRange(t, rangeHeader, int64(len(payload)))
		w.Header().Set("Content-Range", http_range.Range{Start: start, Length: end - start + 1}.ContentRange(int64(len(payload))))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload[start : end+1])
	}))
	t.Cleanup(fallback.Close)

	reader, err := newPan123DownloadReader(
		context.Background(),
		int64(len(payload)),
		http_range.Range{Length: -1},
		pan123ResolvedDownload{URL: initial.URL},
		func(context.Context) (pan123ResolvedDownload, error) {
			refreshes.Add(1)
			return pan123ResolvedDownload{URL: fallback.URL}, nil
		},
		pan123FallbackConfig{ChunkSize: 7, MaxRetries: 3},
	)
	if err != nil {
		t.Fatalf("create reader: %v", err)
	}
	defer reader.Close()

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
	if refreshes.Load() != 1 {
		t.Fatalf("refreshes = %d, want 1", refreshes.Load())
	}
	if value, _ := firstFallbackRange.Load().(string); value != "bytes=11-17" {
		t.Fatalf("first fallback range = %q, want %q", value, "bytes=11-17")
	}
	if fallbackRequests.Load() < 2 {
		t.Fatalf("fallback requests = %d, want multiple chunks", fallbackRequests.Load())
	}
}

func TestPan123DownloadReaderRefreshesAgainAfterFallbackShortRead(t *testing.T) {
	payload := []byte("abcdefghijklmnopqrstuvwxyz")
	var refreshes atomic.Int32
	var secondFallbackRange atomic.Value
	var healthyRequests atomic.Int32

	initial := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		_, _ = w.Write(payload[:5])
	}))
	t.Cleanup(initial.Close)

	shortFallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end := parseTestRange(t, r.Header.Get("Range"), int64(len(payload)))
		w.Header().Set("Content-Range", http_range.Range{Start: start, Length: end - start + 1}.ContentRange(int64(len(payload))))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload[start : start+2])
	}))
	t.Cleanup(shortFallback.Close)

	healthyFallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if healthyRequests.Add(1) == 1 {
			secondFallbackRange.Store(r.Header.Get("Range"))
		}
		start, end := parseTestRange(t, r.Header.Get("Range"), int64(len(payload)))
		w.Header().Set("Content-Range", http_range.Range{Start: start, Length: end - start + 1}.ContentRange(int64(len(payload))))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload[start : end+1])
	}))
	t.Cleanup(healthyFallback.Close)

	reader, err := newPan123DownloadReader(
		context.Background(),
		int64(len(payload)),
		http_range.Range{Length: -1},
		pan123ResolvedDownload{URL: initial.URL},
		func(context.Context) (pan123ResolvedDownload, error) {
			if refreshes.Add(1) == 1 {
				return pan123ResolvedDownload{URL: shortFallback.URL}, nil
			}
			return pan123ResolvedDownload{URL: healthyFallback.URL}, nil
		},
		pan123FallbackConfig{ChunkSize: 8, MaxRetries: 3},
	)
	if err != nil {
		t.Fatalf("create reader: %v", err)
	}
	defer reader.Close()

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
	if refreshes.Load() < 2 {
		t.Fatalf("refreshes = %d, want at least 2", refreshes.Load())
	}
	if value, _ := secondFallbackRange.Load().(string); value != "bytes=7-14" {
		t.Fatalf("second fallback range = %q, want %q", value, "bytes=7-14")
	}
}

func TestPan123DownloadReaderDoesNotRecoverCanceledContext(t *testing.T) {
	payload := []byte("cancelled")
	ctx, cancel := context.WithCancel(context.Background())
	var refreshes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cancel()
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		_, _ = w.Write(payload[:2])
	}))
	t.Cleanup(server.Close)

	reader, err := newPan123DownloadReader(
		ctx,
		int64(len(payload)),
		http_range.Range{Length: -1},
		pan123ResolvedDownload{URL: server.URL},
		func(context.Context) (pan123ResolvedDownload, error) {
			refreshes.Add(1)
			return pan123ResolvedDownload{URL: server.URL}, nil
		},
		pan123FallbackConfig{ChunkSize: 4, MaxRetries: 3},
	)
	if err != nil {
		if !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("create reader: %v", err)
		}
		if refreshes.Load() != 0 {
			t.Fatalf("refreshes = %d, want 0", refreshes.Load())
		}
		return
	}
	defer reader.Close()

	_, err = io.ReadAll(reader)
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("read error = %v, want context canceled", err)
	}
	if refreshes.Load() != 0 {
		t.Fatalf("refreshes = %d, want 0", refreshes.Load())
	}
}

func TestPan123DownloadReaderBoundsRepeatedFallbackShortReads(t *testing.T) {
	payload := []byte("0123456789abcdef")
	var refreshes atomic.Int32

	initial := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		_, _ = w.Write(payload[:2])
	}))
	t.Cleanup(initial.Close)

	shortFallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end := parseTestRange(t, r.Header.Get("Range"), int64(len(payload)))
		w.Header().Set("Content-Range", http_range.Range{Start: start, Length: end - start + 1}.ContentRange(int64(len(payload))))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload[start : start+1])
	}))
	t.Cleanup(shortFallback.Close)

	reader, err := newPan123DownloadReader(
		context.Background(),
		int64(len(payload)),
		http_range.Range{Length: -1},
		pan123ResolvedDownload{URL: initial.URL},
		func(context.Context) (pan123ResolvedDownload, error) {
			refreshes.Add(1)
			return pan123ResolvedDownload{URL: shortFallback.URL}, nil
		},
		pan123FallbackConfig{ChunkSize: 8, MaxRetries: 3},
	)
	if err != nil {
		t.Fatalf("create reader: %v", err)
	}
	defer reader.Close()

	_, err = io.ReadAll(reader)
	if err == nil {
		t.Fatal("repeated short reads unexpectedly succeeded")
	}
	var coded interface{ ClusterErrorCode() string }
	if !errors.As(err, &coded) || coded.ClusterErrorCode() != "source_unexpected_eof" {
		t.Fatalf("error = %v, want source_unexpected_eof", err)
	}
	if refreshes.Load() != 3 {
		t.Fatalf("refreshes = %d, want 3", refreshes.Load())
	}
}

func parseTestRange(t *testing.T, value string, total int64) (int64, int64) {
	t.Helper()
	if !strings.HasPrefix(value, "bytes=") {
		t.Fatalf("range header = %q", value)
	}
	parts := strings.Split(strings.TrimPrefix(value, "bytes="), "-")
	if len(parts) != 2 {
		t.Fatalf("range header = %q", value)
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		t.Fatalf("parse range start: %v", err)
	}
	end := total - 1
	if parts[1] != "" {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			t.Fatalf("parse range end: %v", err)
		}
	}
	return start, end
}

package subscription

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestProbePanSouStatusEmptyURL(t *testing.T) {
	t.Parallel()
	ok, msg, latency := ProbePanSouStatus(context.Background(), "  ")
	if ok {
		t.Fatal("empty url should not be ok")
	}
	if msg == "" {
		t.Fatal("expected message")
	}
	if latency != 0 {
		t.Fatalf("latency=%d", latency)
	}
}

func TestProbePanSouStatusInvalidURL(t *testing.T) {
	t.Parallel()
	ok, msg, _ := ProbePanSouStatus(context.Background(), "not-a-url")
	if ok {
		t.Fatal("invalid url should not be ok")
	}
	if msg == "" {
		t.Fatal("expected message")
	}
}

func TestProbePanSouStatusHealthy(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("missing"))
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ok, msg, latency := ProbePanSouStatus(ctx, srv.URL)
	if !ok {
		t.Fatalf("expected ok for reachable host, msg=%s", msg)
	}
	if latency < 0 {
		t.Fatalf("latency=%d", latency)
	}
}

func TestProbePanSouStatusServerError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ok, msg, _ := ProbePanSouStatus(ctx, srv.URL)
	if ok {
		t.Fatalf("5xx should not be ok, msg=%s", msg)
	}
}

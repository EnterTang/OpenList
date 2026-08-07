package automation

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sy "github.com/OpenListTeam/OpenList/v4/internal/115sy"
)

func TestTreeIndexReplaceIsAtomic(t *testing.T) {
	index := &TreeIndex{}
	if err := index.Replace(strings.NewReader("id,parent_id,path,name,is_dir\nroot,0,/,Root,true\nfile,root,/file,movie.mkv,false\n")); err != nil {
		t.Fatal(err)
	}
	if len(index.Records) != 2 || index.Records[1].ID != "file" {
		t.Fatalf("records = %#v", index.Records)
	}
	if err := index.Replace(strings.NewReader(`[{"id":"broken","parent_id":"missing","path":"/broken","name":"broken","is_dir":false}]`)); err == nil {
		t.Fatal("expected invalid tree error")
	}
	if len(index.Records) != 2 || index.Records[0].ID != "root" {
		t.Fatalf("invalid replace changed existing index: %#v", index.Records)
	}
}

func TestTreeIndexParsesUTF16(t *testing.T) {
	data := []byte{0xff, 0xfe, 'i', 0, 'd', 0, '\t', 0, 'p', 0, 'a', 0, 'r', 0, 'e', 0, 'n', 0, 't', 0, '\t', 0, 'p', 0, 'a', 0, 't', 0, 'h', 0, '\t', 0, 'n', 0, 'a', 0, 'm', 0, 'e', 0, '\t', 0, 'i', 0, 's', 0, '_', 0, 'd', 0, 'i', 0, 'r', 0, '\n', 0, 'r', 0, 'o', 0, 'o', 0, 't', 0, '\t', 0, '0', 0, '\t', 0, '/', 0, '\t', 0, 'R', 0, 'o', 0, 'o', 0, 't', 0, '\t', 0, 't', 0, 'r', 0, 'u', 0, 'e', 0, '\n', 0}
	records, err := ParseTree(strings.NewReader(string(data)))
	if err != nil || len(records) != 1 || !records[0].IsDir {
		t.Fatalf("records = %#v, error = %v", records, err)
	}
}

func TestFastDeleteRejectsRootAndDeduplicatesIDs(t *testing.T) {
	client, err := sy.NewClient(sy.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := FastDelete(context.Background(), client, FastDeleteRequest{IDs: []string{"1", "0"}}); err == nil {
		t.Fatal("expected root deletion to be rejected")
	}
}

func TestFastDeleteUsesBoundedWorkers(t *testing.T) {
	var active int32
	var maximum int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != sy.EndpointFileDelete {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		current := atomic.AddInt32(&active, 1)
		for {
			old := atomic.LoadInt32(&maximum)
			if current <= old || atomic.CompareAndSwapInt32(&maximum, old, current) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"state":true,"errno":0}`)
	}))
	defer server.Close()
	client, err := sy.NewClient(sy.ClientOptions{WebBaseURL: server.URL, AndroidBaseURL: server.URL, LimitRate: 1e6})
	if err != nil {
		t.Fatal(err)
	}
	result, err := FastDelete(context.Background(), client, FastDeleteRequest{IDs: []string{"1", "2", "2", "3", "4"}, Concurrency: 2, BatchSize: 2})
	if err != nil || len(result.Deleted) != 4 || len(result.Failed) != 0 {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if maximum > 2 {
		t.Fatalf("maximum concurrent deletes = %d", maximum)
	}
}

func TestSchedulerStartIsIdempotentAndStopIsSafe(t *testing.T) {
	scheduler, err := NewScheduler(time.Hour, func() {})
	if err != nil {
		t.Fatal(err)
	}
	scheduler.Start()
	scheduler.Start()
	if !scheduler.started {
		t.Fatal("scheduler did not start")
	}
	scheduler.Stop()
	scheduler.Stop()
	if scheduler.cron != nil || scheduler.started {
		t.Fatal("scheduler did not stop cleanly")
	}
}

func TestCleanupRequiresRequestScopedSecurityCode(t *testing.T) {
	client, err := sy.NewClient(sy.ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = Clean(context.Background(), client, CleanupRequest{CleanRecycleBin: true})
	if err == nil || !strings.Contains(err.Error(), "security code") {
		t.Fatalf("error = %v", err)
	}
	if _, err := Clean(context.Background(), client, CleanupRequest{}); err == nil || !strings.Contains(err.Error(), "filter") {
		t.Fatalf("unfiltered cleanup error = %v", err)
	}
}

func TestTaskRedactsCredentialsAndClampsProgress(t *testing.T) {
	var task Task
	task.Update("failed", 120, fmt.Errorf("UID=uid-secret security_code=code-secret"))
	snapshot := task.Snapshot()
	if snapshot.Progress != 100 || strings.Contains(snapshot.Error, "uid-secret") || strings.Contains(snapshot.Error, "code-secret") {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	task.Update("succeeded", -1, nil)
	if snapshot = task.Snapshot(); snapshot.Progress != 0 || snapshot.Error != "" {
		t.Fatalf("reset snapshot = %#v", snapshot)
	}
}

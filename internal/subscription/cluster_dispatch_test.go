package subscription

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

type recordingClusterDispatcher struct {
	tasks []ClusterMediaTask
	err   error
}

func (d *recordingClusterDispatcher) DispatchSubscriptionInspect(_ context.Context, task ClusterInspectTask) (string, error) {
	if d.err != nil {
		return "", d.err
	}
	return "inspect-" + task.IdempotencyKey, nil
}

func (d *recordingClusterDispatcher) DispatchSubscriptionMedia(_ context.Context, tasks []ClusterMediaTask) ([]ClusterDispatchResult, error) {
	d.tasks = append(d.tasks, tasks...)
	if d.err != nil {
		return nil, d.err
	}
	results := make([]ClusterDispatchResult, 0, len(tasks))
	for _, task := range tasks {
		results = append(results, ClusterDispatchResult{SourceKey: task.SourceKey, JobID: "job-" + task.SourceKey})
	}
	return results, nil
}

func TestClusterDispatchPersistsContextAndTransitionsStatus(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{ID: 41, Name: "Example", TransferEnabled: true, TMDBID: 123, TMDBName: "Example", TMDBYear: 2026, MediaType: "tv", TargetRoot: "/TV"}
	ref := ShareRef{Provider: ShareProviderAliyunDrive, RawURL: "https://www.alipan.com/s/example", ShareID: "example", Passcode: "1234"}
	message := clusterSourceMessage{ID: "9001", Channel: "shows", URL: "https://t.me/shows/9001", Text: "Example S01E02"}
	item := clusterItemFromShareEntry(sub, ref, TreeEntry{Path: "/Example.S01E02.mkv", Name: "Example.S01E02.mkv", ID: "file-2", Size: 2048, Modified: time.Unix(100, 0)}, message, time.Now())
	stored, _, _, err := upsertClusterItems([]*model.SubscriptionItem{item})
	if err != nil {
		t.Fatalf("upsert cluster item: %v", err)
	}
	count, err := dispatchClusterItems(context.Background(), sub, stored, ref, message)
	if err != nil || count != 1 {
		t.Fatalf("dispatch count=%d err=%v", count, err)
	}
	if len(dispatcher.tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(dispatcher.tasks))
	}
	task := dispatcher.tasks[0]
	if task.SourceFileID != "file-2" || task.Season != 1 || task.Episode != 2 || task.LogicalTargetPath == "" {
		t.Fatalf("task lost media context: %#v", task)
	}
	if task.SourceMessageID != "9001" || task.SourceMessageURL != message.URL || task.SharePasscode != "1234" {
		t.Fatalf("task lost source context: %#v", task)
	}
	got, err := db.GetSubscriptionItem(sub.ID, item.SourceKey)
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	if got.Status != model.SubscriptionItemStatusTransferring || got.ClusterJobID == "" {
		t.Fatalf("item status/job = %q/%q", got.Status, got.ClusterJobID)
	}
	if err := CompleteClusterTransfer(sub.ID, item.SourceKey, got.ClusterJobID); err != nil {
		t.Fatalf("complete transfer: %v", err)
	}
	got, _ = db.GetSubscriptionItem(sub.ID, item.SourceKey)
	if got.Status != model.SubscriptionItemStatusTransferred {
		t.Fatalf("completed status = %q", got.Status)
	}
}

func TestClusterItemIsIdempotentAcrossMessagesButRedispatchesChangedObject(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	dispatcher := &recordingClusterDispatcher{}
	RegisterClusterDispatcher(dispatcher)
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })

	sub := &model.Subscription{ID: 42, Name: "Example", TransferEnabled: true, TMDBName: "Example", MediaType: "tv", TargetRoot: "/TV"}
	ref := ShareRef{Provider: ShareProviderQuark, RawURL: "https://pan.quark.cn/s/example", ShareID: "example"}
	first := clusterItemFromShareEntry(sub, ref, TreeEntry{Path: "/Example.S01E03.mkv", Name: "Example.S01E03.mkv", ID: "same-file", Size: 100, Modified: time.Unix(100, 0)}, clusterSourceMessage{ID: "1"}, time.Now())
	stored, _, _, err := upsertClusterItems([]*model.SubscriptionItem{first})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatchClusterItems(context.Background(), sub, stored, ref, clusterSourceMessage{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	jobID := "job-" + first.SourceKey
	if err := CompleteClusterTransfer(sub.ID, first.SourceKey, jobID); err != nil {
		t.Fatal(err)
	}

	unchanged := clusterItemFromShareEntry(sub, ref, TreeEntry{Path: first.FilePath, Name: first.FileName, ID: "same-file", Size: 100, Modified: time.Unix(100, 0)}, clusterSourceMessage{ID: "2"}, time.Now())
	stored, _, _, err = upsertClusterItems([]*model.SubscriptionItem{unchanged})
	if err != nil {
		t.Fatal(err)
	}
	count, err := dispatchClusterItems(context.Background(), sub, stored, ref, clusterSourceMessage{ID: "2"})
	if err != nil || count != 0 {
		t.Fatalf("unchanged repost dispatched count=%d err=%v", count, err)
	}

	changed := clusterItemFromShareEntry(sub, ref, TreeEntry{Path: first.FilePath, Name: first.FileName, ID: "same-file", Size: 200, Modified: time.Unix(200, 0)}, clusterSourceMessage{ID: "3"}, time.Now())
	stored, _, changedCount, err := upsertClusterItems([]*model.SubscriptionItem{changed})
	if err != nil {
		t.Fatal(err)
	}
	if changedCount != 1 {
		t.Fatalf("changed count = %d", changedCount)
	}
	count, err = dispatchClusterItems(context.Background(), sub, stored, ref, clusterSourceMessage{ID: "3"})
	if err != nil || count != 1 {
		t.Fatalf("changed object dispatch count=%d err=%v", count, err)
	}
	if len(dispatcher.tasks) != 2 {
		t.Fatalf("total tasks = %d, want 2", len(dispatcher.tasks))
	}
	if dispatcher.tasks[0].IdempotencyKey == dispatcher.tasks[1].IdempotencyKey {
		t.Fatal("changed object reused dispatch idempotency key")
	}
}

func TestClusterInspectIdentityChangesByMessageButMediaIdentityUsesCanonicalShare(t *testing.T) {
	sub := &model.Subscription{ID: 99, Name: "Example"}
	firstRef := ShareRef{Provider: ShareProviderAliyunDrive, RawURL: "https://www.alipan.com/s/example?foo=1", ShareID: "example"}
	secondRef := ShareRef{Provider: ShareProviderAliyunDrive, RawURL: "https://www.aliyundrive.com/s/example", ShareID: "example"}
	firstInspect := clusterInspectTask(sub, firstRef, clusterSourceMessage{ID: "100"})
	secondInspect := clusterInspectTask(sub, secondRef, clusterSourceMessage{ID: "101"})
	if firstInspect.IdempotencyKey == secondInspect.IdempotencyKey {
		t.Fatal("different source messages reused a share inspection idempotency key")
	}
	entry := TreeEntry{ID: "file-1", Path: "/Example.S01E01.mkv", Name: "Example.S01E01.mkv", Size: 100}
	firstItem := clusterItemFromShareEntry(sub, firstRef, entry, clusterSourceMessage{ID: "100"}, time.Now())
	secondItem := clusterItemFromShareEntry(sub, secondRef, entry, clusterSourceMessage{ID: "101"}, time.Now())
	if firstItem.SourceKey != secondItem.SourceKey {
		t.Fatal("URL aliases for the same canonical share produced different media identities")
	}
}

func TestClusterDispatchFailureMarksItemFailed(t *testing.T) {
	setupSubscriptionRuntimeDB(t)
	RegisterClusterDispatcher(&recordingClusterDispatcher{err: errors.New("no worker")})
	t.Cleanup(func() { RegisterClusterDispatcher(nil) })
	sub := &model.Subscription{ID: 43, TransferEnabled: true, TMDBName: "Movie", MediaType: "movie", TargetRoot: "/Movies"}
	ref := ShareRef{Provider: ShareProviderQuark, RawURL: "https://pan.quark.cn/s/example", ShareID: "example"}
	item := clusterItemFromShareEntry(sub, ref, TreeEntry{Path: "/Movie.mkv", Name: "Movie.mkv", ID: "movie", Size: 100}, clusterSourceMessage{}, time.Now())
	stored, _, _, err := upsertClusterItems([]*model.SubscriptionItem{item})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatchClusterItems(context.Background(), sub, stored, ref, clusterSourceMessage{}); err == nil {
		t.Fatal("expected dispatch error")
	}
	got, _ := db.GetSubscriptionItem(sub.ID, item.SourceKey)
	if got.Status != model.SubscriptionItemStatusFailed || got.LastError != "no worker" {
		t.Fatalf("failed item = %#v", got)
	}
}

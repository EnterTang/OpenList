package resultqueue

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func testQueue(t *testing.T) (*Queue, context.Context) {
	t.Helper()
	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	q := New(client, Config{Consumer: "test"})
	q.durabilityCheck = func(context.Context) error { return nil }
	ctx := context.Background()
	require.NoError(t, q.EnsureGroup(ctx))
	return q, ctx
}

func TestEnqueueReadAckAndStats(t *testing.T) {
	q, ctx := testQueue(t)
	id, err := q.EnqueueDurably(ctx, map[string]string{"sha256": "abc"})
	require.NoError(t, err)
	require.NotEmpty(t, id)

	results, err := q.Read(ctx, 10, -1)
	require.NoError(t, err)
	require.Len(t, results, 1)
	var payload map[string]string
	require.NoError(t, json.Unmarshal(results[0].Payload, &payload))
	require.Equal(t, "abc", payload["sha256"])

	stats, err := q.Stats(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.Queued)
	require.EqualValues(t, 1, stats.Pending)

	require.NoError(t, q.AckAndDelete(ctx, results[0].ID))
	stats, err = q.Stats(ctx)
	require.NoError(t, err)
	require.Zero(t, stats.Queued)
	require.Zero(t, stats.Pending)
}

func TestReclaimAndDLQ(t *testing.T) {
	q, ctx := testQueue(t)
	_, err := q.EnqueueDurably(ctx, map[string]string{"name": "episode.mkv"})
	require.NoError(t, err)
	_, err = q.Read(ctx, 1, -1)
	require.NoError(t, err)

	time.Sleep(2 * time.Millisecond)
	reclaimed, _, err := q.Reclaim(ctx, time.Millisecond, "0-0", 10)
	require.NoError(t, err)
	require.Len(t, reclaimed, 1)
	require.NoError(t, q.MoveToDLQ(ctx, reclaimed[0], "context_mismatch"))

	stats, err := q.Stats(ctx)
	require.NoError(t, err)
	require.Zero(t, stats.Queued)
	require.EqualValues(t, 1, stats.DLQ)
}

func TestEnqueueFailureBlocksDeletion(t *testing.T) {
	q, ctx := testQueue(t)
	q.durabilityCheck = func(context.Context) error { return ErrNotDurable }
	_, err := q.EnqueueDurably(ctx, map[string]string{"sha256": "abc"})
	require.ErrorIs(t, err, ErrNotDurable)
}

func TestUnavailableClientBlocksDeletion(t *testing.T) {
	q := New(nil, Config{})
	_, err := q.EnqueueDurably(context.Background(), struct{}{})
	require.True(t, errors.Is(err, ErrUnavailable))
}

func TestClaimAttemptSurvivesQueueRecreation(t *testing.T) {
	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	ctx := context.Background()
	first := New(client, Config{Stream: "cluster:test-results"})
	claimed, err := first.ClaimAttempt(ctx, "job-1:attempt-1:1", time.Hour)
	require.NoError(t, err)
	require.True(t, claimed)

	second := New(client, Config{Stream: "cluster:test-results"})
	claimed, err = second.ClaimAttempt(ctx, "job-1:attempt-1:1", time.Hour)
	require.NoError(t, err)
	require.False(t, claimed)

	require.NoError(t, second.ReleaseAttempt(ctx, "job-1:attempt-1:1"))
	claimed, err = first.ClaimAttempt(ctx, "job-1:attempt-1:1", time.Hour)
	require.NoError(t, err)
	require.True(t, claimed)
}

func TestCleanupBacklogTracksUnfinishedCleanup(t *testing.T) {
	q, ctx := testQueue(t)
	cleanup := CleanupRequest{
		Version: "v1", JobID: "job-1", MediaItemID: "media-1",
		OpenListPath:     "/mobile/.openlist-cluster/job-1/media-1/episode.mkv",
		StorageMountPath: "/mobile", Name: "episode.mkv", CreatedAt: time.Now().UTC(),
	}
	_, cleanupID, err := q.EnqueueResultAndCleanupDurably(ctx, map[string]string{"sha256": "abc"}, cleanup)
	require.NoError(t, err)
	count, err := q.CleanupBacklog(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	require.NoError(t, q.EnsureCleanupGroup(ctx))
	require.NoError(t, q.AckAndDeleteCleanup(ctx, cleanupID))
	count, err = q.CleanupBacklog(ctx)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestCleanupRequestAllowsOwnedSourceAndMobileTargets(t *testing.T) {
	request := CleanupRequest{
		Version: "v1", JobID: "job-1", MediaItemID: "media-1",
		OpenListPath:     "/mobile/.openlist-cluster/job-1/media-1/episode.mkv",
		StorageMountPath: "/mobile", Name: "episode.mkv", EmptyRecycleBin: true,
		AdditionalTargets: []CleanupTarget{{
			OpenListPath:     "/aliyun/transfer/.openlist-cluster/job-1/media-1/episode.mkv",
			StorageMountPath: "/aliyun", Name: "episode.mkv",
		}},
	}
	require.NoError(t, request.Validate())
	request.AdditionalTargets[0].OpenListPath = "/aliyun/transfer/.openlist-cluster/job-2/media-1/episode.mkv"
	require.Error(t, request.Validate())
}

func TestEnqueueResultAndCleanupDurablyCommitsBothStreams(t *testing.T) {
	q, ctx := testQueue(t)
	require.NoError(t, q.EnsureCleanupGroup(ctx))
	request := CleanupRequest{
		Version:          "v1",
		JobID:            "job-1",
		MediaItemID:      "media-1",
		OpenListPath:     "/mobile/.openlist-cluster/job-1/media-1/Episode.mkv",
		StorageMountPath: "/mobile",
		RemoteFileID:     "remote-1",
		Name:             "Episode.mkv",
		EmptyRecycleBin:  true,
		CreatedAt:        time.Now().UTC(),
	}
	resultID, cleanupID, err := q.EnqueueResultAndCleanupDurably(ctx, map[string]string{"sha256": "abc"}, request)
	require.NoError(t, err)
	require.NotEmpty(t, resultID)
	require.NotEmpty(t, cleanupID)

	cleanups, err := q.ReadCleanup(ctx, 1, -1)
	require.NoError(t, err)
	require.Len(t, cleanups, 1)
	var persisted CleanupRequest
	require.NoError(t, json.Unmarshal(cleanups[0].Payload, &persisted))
	require.Equal(t, request.OpenListPath, persisted.OpenListPath)
	require.NoError(t, q.AckAndDeleteCleanup(ctx, cleanups[0].ID))
}

func TestCleanupRequestRejectsPathOutsideTaskNamespace(t *testing.T) {
	request := CleanupRequest{
		Version:          "v1",
		JobID:            "job-1",
		MediaItemID:      "media-1",
		OpenListPath:     "/mobile/Movies/Episode.mkv",
		StorageMountPath: "/mobile",
		Name:             "Episode.mkv",
	}
	require.Error(t, request.Validate())
}

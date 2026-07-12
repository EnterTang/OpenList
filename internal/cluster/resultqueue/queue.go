package resultqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrUnavailable = errors.New("result queue unavailable; source media must be retained")
	ErrNotDurable  = errors.New("result queue durability requirements not met; source media must be retained")
)

type Config struct {
	Stream          string
	Group           string
	DLQ             string
	Consumer        string
	CleanupStream   string
	CleanupGroup    string
	CleanupDLQ      string
	CleanupConsumer string
}

func (c Config) withDefaults() Config {
	if c.Stream == "" {
		c.Stream = "cluster:upload-results:v1"
	}
	if c.Group == "" {
		c.Group = "cluster-upload-reporters-v1"
	}
	if c.DLQ == "" {
		c.DLQ = c.Stream + ":dlq"
	}
	if c.Consumer == "" {
		c.Consumer = "worker"
	}
	if c.CleanupStream == "" {
		c.CleanupStream = "cluster:local-cleanup:v1"
	}
	if c.CleanupGroup == "" {
		c.CleanupGroup = "cluster-local-cleaners-v1"
	}
	if c.CleanupDLQ == "" {
		c.CleanupDLQ = c.CleanupStream + ":dlq"
	}
	if c.CleanupConsumer == "" {
		c.CleanupConsumer = c.Consumer
	}
	return c
}

type Result struct {
	ID      string          `json:"id,omitempty"`
	Payload json.RawMessage `json:"payload"`
	Created time.Time       `json:"created_at"`
}

type CleanupRequest struct {
	Version           string          `json:"version"`
	JobID             string          `json:"job_id"`
	MediaItemID       string          `json:"media_item_id"`
	OpenListPath      string          `json:"openlist_path"`
	StorageMountPath  string          `json:"storage_mount_path"`
	RemoteFileID      string          `json:"remote_file_id,omitempty"`
	Name              string          `json:"name"`
	EmptyRecycleBin   bool            `json:"empty_recycle_bin"`
	CreatedAt         time.Time       `json:"created_at"`
	AdditionalTargets []CleanupTarget `json:"additional_targets,omitempty"`
}

type CleanupTarget struct {
	OpenListPath     string `json:"openlist_path"`
	StorageMountPath string `json:"storage_mount_path"`
	RemoteFileID     string `json:"remote_file_id,omitempty"`
	Name             string `json:"name"`
	EmptyRecycleBin  bool   `json:"empty_recycle_bin"`
}

func (r CleanupRequest) Validate() error {
	if r.Version != "v1" {
		return errors.New("cleanup request version must be v1")
	}
	if strings.TrimSpace(r.JobID) == "" || strings.TrimSpace(r.MediaItemID) == "" {
		return errors.New("cleanup request job and media item are required")
	}
	if err := validateCleanupTarget(r.JobID, r.MediaItemID, CleanupTarget{
		OpenListPath: r.OpenListPath, StorageMountPath: r.StorageMountPath, RemoteFileID: r.RemoteFileID,
		Name: r.Name, EmptyRecycleBin: r.EmptyRecycleBin,
	}); err != nil {
		return err
	}
	for i, target := range r.AdditionalTargets {
		if err := validateCleanupTarget(r.JobID, r.MediaItemID, target); err != nil {
			return fmt.Errorf("additional cleanup target %d: %w", i, err)
		}
	}
	return nil
}

func validateCleanupTarget(jobID, mediaItemID string, target CleanupTarget) error {
	cleanPath := path.Clean(strings.TrimSpace(target.OpenListPath))
	mountPath := path.Clean(strings.TrimSpace(target.StorageMountPath))
	if !strings.HasPrefix(cleanPath, "/") || mountPath == "." || !strings.HasPrefix(mountPath, "/") {
		return errors.New("cleanup request paths must be absolute")
	}
	if cleanPath != mountPath && !strings.HasPrefix(cleanPath, strings.TrimSuffix(mountPath, "/")+"/") {
		return errors.New("cleanup request path must remain inside its storage mount")
	}
	relative := strings.TrimPrefix(cleanPath, strings.TrimSuffix(mountPath, "/"))
	parts := strings.Split(strings.TrimPrefix(relative, "/"), "/")
	namespaceIndex := -1
	for i, part := range parts {
		if part == ".openlist-cluster" {
			namespaceIndex = i
			break
		}
	}
	if namespaceIndex < 0 || len(parts) < namespaceIndex+4 || parts[namespaceIndex+1] != jobID || parts[namespaceIndex+2] != mediaItemID {
		return errors.New("cleanup request must target its .openlist-cluster job/media namespace")
	}
	if path.Base(cleanPath) != target.Name || target.Name == "" || target.Name == "." || target.Name == ".." || strings.ContainsAny(target.Name, `/\\`) {
		return errors.New("cleanup request name does not match its exact path")
	}
	return nil
}

type Stats struct {
	Queued         int64 `json:"queued"`
	Pending        int64 `json:"pending"`
	DLQ            int64 `json:"dlq"`
	CleanupQueued  int64 `json:"cleanup_queued"`
	CleanupPending int64 `json:"cleanup_pending"`
	CleanupDLQ     int64 `json:"cleanup_dlq"`
}

type Queue struct {
	client          redis.UniversalClient
	cfg             Config
	durabilityCheck func(context.Context) error
}

// ClaimAttempt durably journals a Worker execution attempt before any share
// save or upload side effect. Replayed job.offer messages for the same
// attempt return false, including after a Worker process restart.
func (q *Queue) ClaimAttempt(ctx context.Context, attemptKey string, retainFor time.Duration) (bool, error) {
	if q.client == nil {
		return false, ErrUnavailable
	}
	attemptKey = strings.TrimSpace(attemptKey)
	if attemptKey == "" {
		return false, errors.New("cluster attempt key is required")
	}
	if retainFor <= 0 {
		retainFor = 7 * 24 * time.Hour
	}
	claimed, err := q.client.SetNX(ctx, q.cfg.Stream+":attempt:"+attemptKey, time.Now().UTC().Format(time.RFC3339Nano), retainFor).Result()
	if err != nil {
		return false, fmt.Errorf("%w: claim attempt: %v", ErrUnavailable, err)
	}
	return claimed, nil
}

func (q *Queue) ReleaseAttempt(ctx context.Context, attemptKey string) error {
	if q.client == nil {
		return ErrUnavailable
	}
	if err := q.client.Del(ctx, q.cfg.Stream+":attempt:"+strings.TrimSpace(attemptKey)).Err(); err != nil {
		return fmt.Errorf("%w: release attempt: %v", ErrUnavailable, err)
	}
	return nil
}

func (q *Queue) CleanupBacklog(ctx context.Context) (int64, error) {
	if q.client == nil {
		return 0, ErrUnavailable
	}
	count, err := q.client.XLen(ctx, q.cfg.CleanupStream).Result()
	if err != nil {
		return 0, fmt.Errorf("%w: cleanup backlog: %v", ErrUnavailable, err)
	}
	return count, nil
}

func New(client redis.UniversalClient, cfg Config) *Queue {
	q := &Queue{client: client, cfg: cfg.withDefaults()}
	q.durabilityCheck = q.ValidateDurability
	return q
}

// ValidateDurability verifies the Redis settings required before a caller may
// delete the media represented by a successfully enqueued result.
func (q *Queue) ValidateDurability(ctx context.Context) error {
	if q.client == nil {
		return ErrUnavailable
	}
	if err := q.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	checks := map[string]string{"appendonly": "yes", "appendfsync": "always", "maxmemory-policy": "noeviction"}
	for key, want := range checks {
		values, err := q.client.ConfigGet(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("%w: read %s: %v", ErrNotDurable, key, err)
		}
		got := configValue(values, key)
		if !strings.EqualFold(got, want) {
			return fmt.Errorf("%w: redis %s=%q, require %q", ErrNotDurable, key, got, want)
		}
	}
	return nil
}

func configValue(values map[string]string, key string) string { return values[key] }

func (q *Queue) EnsureGroup(ctx context.Context) error {
	if q.client == nil {
		return ErrUnavailable
	}
	err := q.client.XGroupCreateMkStream(ctx, q.cfg.Stream, q.cfg.Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("%w: create group: %v", ErrUnavailable, err)
	}
	return nil
}

func (q *Queue) EnsureCleanupGroup(ctx context.Context) error {
	if q.client == nil {
		return ErrUnavailable
	}
	err := q.client.XGroupCreateMkStream(ctx, q.cfg.CleanupStream, q.cfg.CleanupGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("%w: create cleanup group: %v", ErrUnavailable, err)
	}
	return nil
}

// EnqueueDurably validates Redis durability on every enqueue. Success is the
// deletion barrier: callers may only remove source media after this returns.
func (q *Queue) EnqueueDurably(ctx context.Context, payload any) (string, error) {
	if err := q.durabilityCheck(ctx); err != nil {
		return "", err
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal result: %w", err)
	}
	id, err := q.client.XAdd(ctx, &redis.XAddArgs{Stream: q.cfg.Stream, Values: map[string]any{"payload": string(b), "created_at": time.Now().UTC().Format(time.RFC3339Nano)}}).Result()
	if err != nil {
		return "", fmt.Errorf("%w: enqueue: %v", ErrUnavailable, err)
	}
	return id, nil
}

// EnqueueResultAndCleanupDurably writes the upload result and its exact local
// cleanup request in one Redis transaction. Callers may only treat the upload
// as finalized after both stream entries have been committed.
func (q *Queue) EnqueueResultAndCleanupDurably(ctx context.Context, payload any, cleanup CleanupRequest) (string, string, error) {
	if err := q.durabilityCheck(ctx); err != nil {
		return "", "", err
	}
	if err := cleanup.Validate(); err != nil {
		return "", "", err
	}
	resultJSON, err := json.Marshal(payload)
	if err != nil {
		return "", "", fmt.Errorf("marshal result: %w", err)
	}
	cleanupJSON, err := json.Marshal(cleanup)
	if err != nil {
		return "", "", fmt.Errorf("marshal cleanup request: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	pipe := q.client.TxPipeline()
	resultCmd := pipe.XAdd(ctx, &redis.XAddArgs{Stream: q.cfg.Stream, Values: map[string]any{"payload": string(resultJSON), "created_at": now}})
	cleanupCmd := pipe.XAdd(ctx, &redis.XAddArgs{Stream: q.cfg.CleanupStream, Values: map[string]any{"payload": string(cleanupJSON), "created_at": now}})
	if _, err := pipe.Exec(ctx); err != nil {
		return "", "", fmt.Errorf("%w: enqueue result and cleanup: %v", ErrUnavailable, err)
	}
	return resultCmd.Val(), cleanupCmd.Val(), nil
}

func (q *Queue) ReadCleanup(ctx context.Context, count int64, block time.Duration) ([]Result, error) {
	return q.readStream(ctx, q.cfg.CleanupStream, q.cfg.CleanupGroup, q.cfg.CleanupConsumer, count, block)
}

func (q *Queue) ReclaimCleanup(ctx context.Context, minIdle time.Duration, start string, count int64) ([]Result, string, error) {
	return q.reclaimStream(ctx, q.cfg.CleanupStream, q.cfg.CleanupGroup, q.cfg.CleanupConsumer, minIdle, start, count)
}

func (q *Queue) AckAndDeleteCleanup(ctx context.Context, ids ...string) error {
	return q.ackAndDeleteStream(ctx, q.cfg.CleanupStream, q.cfg.CleanupGroup, ids...)
}

func (q *Queue) MoveCleanupToDLQ(ctx context.Context, result Result, reason string) error {
	return q.moveToDLQ(ctx, q.cfg.CleanupStream, q.cfg.CleanupGroup, q.cfg.CleanupDLQ, result, reason)
}

func (q *Queue) Read(ctx context.Context, count int64, block time.Duration) ([]Result, error) {
	return q.readStream(ctx, q.cfg.Stream, q.cfg.Group, q.cfg.Consumer, count, block)
}

func (q *Queue) readStream(ctx context.Context, stream, group, consumer string, count int64, block time.Duration) ([]Result, error) {
	if count <= 0 {
		count = 1
	}
	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: consumer, Streams: []string{stream, ">"}, Count: count, Block: block}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: read: %v", ErrUnavailable, err)
	}
	return decodeStreams(streams)
}

func (q *Queue) Reclaim(ctx context.Context, minIdle time.Duration, start string, count int64) ([]Result, string, error) {
	return q.reclaimStream(ctx, q.cfg.Stream, q.cfg.Group, q.cfg.Consumer, minIdle, start, count)
}

func (q *Queue) reclaimStream(ctx context.Context, stream, group, consumer string, minIdle time.Duration, start string, count int64) ([]Result, string, error) {
	if start == "" {
		start = "0-0"
	}
	if count <= 0 {
		count = 100
	}
	messages, next, err := q.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: stream, Group: group, Consumer: consumer, MinIdle: minIdle, Start: start, Count: count}).Result()
	if err != nil {
		return nil, start, fmt.Errorf("%w: reclaim: %v", ErrUnavailable, err)
	}
	results, err := decodeMessages(messages)
	return results, next, err
}

func (q *Queue) AckAndDelete(ctx context.Context, ids ...string) error {
	return q.ackAndDeleteStream(ctx, q.cfg.Stream, q.cfg.Group, ids...)
}

func (q *Queue) ackAndDeleteStream(ctx context.Context, stream, group string, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	pipe := q.client.TxPipeline()
	pipe.XAck(ctx, stream, group, ids...)
	pipe.XDel(ctx, stream, ids...)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("%w: ack/delete: %v", ErrUnavailable, err)
	}
	return nil
}

func (q *Queue) MoveToDLQ(ctx context.Context, result Result, reason string) error {
	return q.moveToDLQ(ctx, q.cfg.Stream, q.cfg.Group, q.cfg.DLQ, result, reason)
}

func (q *Queue) moveToDLQ(ctx context.Context, stream, group, dlq string, result Result, reason string) error {
	values := map[string]any{"source_id": result.ID, "payload": string(result.Payload), "reason": reason, "failed_at": time.Now().UTC().Format(time.RFC3339Nano)}
	pipe := q.client.TxPipeline()
	pipe.XAdd(ctx, &redis.XAddArgs{Stream: dlq, Values: values})
	pipe.XAck(ctx, stream, group, result.ID)
	pipe.XDel(ctx, stream, result.ID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("%w: move to dlq: %v", ErrUnavailable, err)
	}
	return nil
}

func (q *Queue) Stats(ctx context.Context) (Stats, error) {
	if err := q.EnsureGroup(ctx); err != nil {
		return Stats{}, err
	}
	if err := q.EnsureCleanupGroup(ctx); err != nil {
		return Stats{}, err
	}
	queued, err := q.client.XLen(ctx, q.cfg.Stream).Result()
	if err != nil {
		return Stats{}, fmt.Errorf("%w: stats: %v", ErrUnavailable, err)
	}
	pending, err := q.client.XPending(ctx, q.cfg.Stream, q.cfg.Group).Result()
	if err != nil {
		return Stats{}, fmt.Errorf("%w: stats: %v", ErrUnavailable, err)
	}
	dlq, err := q.client.XLen(ctx, q.cfg.DLQ).Result()
	if err != nil {
		return Stats{}, fmt.Errorf("%w: stats: %v", ErrUnavailable, err)
	}
	cleanupQueued, err := q.client.XLen(ctx, q.cfg.CleanupStream).Result()
	if err != nil {
		return Stats{}, fmt.Errorf("%w: cleanup stats: %v", ErrUnavailable, err)
	}
	cleanupPending, err := q.client.XPending(ctx, q.cfg.CleanupStream, q.cfg.CleanupGroup).Result()
	if err != nil {
		return Stats{}, fmt.Errorf("%w: cleanup stats: %v", ErrUnavailable, err)
	}
	cleanupDLQ, err := q.client.XLen(ctx, q.cfg.CleanupDLQ).Result()
	if err != nil {
		return Stats{}, fmt.Errorf("%w: cleanup stats: %v", ErrUnavailable, err)
	}
	return Stats{
		Queued: queued, Pending: pending.Count, DLQ: dlq,
		CleanupQueued: cleanupQueued, CleanupPending: cleanupPending.Count, CleanupDLQ: cleanupDLQ,
	}, nil
}

func decodeStreams(streams []redis.XStream) ([]Result, error) {
	var messages []redis.XMessage
	for _, stream := range streams {
		messages = append(messages, stream.Messages...)
	}
	return decodeMessages(messages)
}

func decodeMessages(messages []redis.XMessage) ([]Result, error) {
	results := make([]Result, 0, len(messages))
	for _, message := range messages {
		payload, ok := message.Values["payload"].(string)
		if !ok {
			return nil, fmt.Errorf("message %s has invalid payload", message.ID)
		}
		result := Result{ID: message.ID, Payload: json.RawMessage(payload)}
		if created, ok := message.Values["created_at"].(string); ok {
			result.Created, _ = time.Parse(time.RFC3339Nano, created)
		}
		results = append(results, result)
	}
	return results, nil
}

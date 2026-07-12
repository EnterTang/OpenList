package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
	"gorm.io/gorm"
)

type subscriptionDispatcher struct{ runtime *Runtime }

const (
	ClusterInspectWorkflowVersion = "subscription-share-inspect/v1"
	clusterInspectManifestVersion = "share-inspect/v1"
)

type dispatchTarget struct {
	nodeID        string
	targetProfile string
	assignedBytes int64
}

func (d subscriptionDispatcher) DispatchSubscriptionInspect(ctx context.Context, task subscription.ClusterInspectTask) (string, error) {
	job, err := d.runtime.DispatchShareInspect(ctx, DispatchShareInspectRequest{
		IdempotencyKey: task.IdempotencyKey,
		TaskContext: protocol.TaskContext{
			WorkflowVersion: ClusterInspectWorkflowVersion, SealedManifestVersion: clusterInspectManifestVersion,
			Subscription: protocol.SubscriptionTaskContext{
				SubscriptionID: task.SubscriptionID, SubscriptionName: task.SubscriptionName,
				SourceMessageID: task.SourceMessageID, SourceMessageChannel: task.SourceMessageChannel,
				SourceMessageURL: task.SourceMessageURL, SourceMessageText: task.SourceMessageText,
				ShareRefFingerprint: task.ShareRefFingerprint,
			},
			Share: protocol.ShareTaskContext{Provider: task.ShareProvider, URL: task.ShareURL, Passcode: task.SharePasscode},
		},
	})
	if err != nil {
		return "", err
	}
	return job.ID, nil
}

func consumeSubscriptionShareInspect(ctx context.Context, record model.ClusterShareInspectManifest, manifest protocol.ShareInspectManifest) error {
	var job model.ClusterJob
	if err := db.GetDb().WithContext(ctx).First(&job, "id = ?", record.JobID).Error; err != nil {
		return err
	}
	var taskContext protocol.TaskContext
	if err := json.Unmarshal([]byte(job.TaskContextJSON), &taskContext); err != nil {
		return err
	}
	task := subscription.ClusterInspectTask{
		SubscriptionID: taskContext.Subscription.SubscriptionID, SubscriptionName: taskContext.Subscription.SubscriptionName,
		SourceMessageID: taskContext.Subscription.SourceMessageID, SourceMessageChannel: taskContext.Subscription.SourceMessageChannel,
		SourceMessageURL: taskContext.Subscription.SourceMessageURL, SourceMessageText: taskContext.Subscription.SourceMessageText,
		ShareProvider: taskContext.Share.Provider, ShareURL: taskContext.Share.URL, SharePasscode: taskContext.Share.Passcode,
		ShareRefFingerprint: taskContext.Subscription.ShareRefFingerprint,
	}
	objects := make([]subscription.ClusterInspectObject, 0, len(manifest.Objects))
	for _, object := range manifest.Objects {
		objects = append(objects, subscription.ClusterInspectObject{
			FileID: object.SourceFileID, RelativePath: object.SourceRelativePath,
			Size: object.Size, Hash: object.Hash, ModifiedAt: object.ModifiedAt,
		})
	}
	_, err := subscription.ApplyClusterInspectManifest(ctx, task, objects)
	return err
}

func (d subscriptionDispatcher) DispatchSubscriptionMedia(ctx context.Context, tasks []subscription.ClusterMediaTask) ([]subscription.ClusterDispatchResult, error) {
	if d.runtime == nil || len(tasks) == 0 {
		return nil, errors.New("cluster subscription dispatcher is unavailable")
	}
	targets, err := d.runtime.subscriptionDispatchTargets(ctx)
	if err != nil {
		return nil, err
	}
	requests := make([]DispatchMediaJobRequest, 0, len(tasks))
	results := make([]subscription.ClusterDispatchResult, len(tasks))
	requestTaskIndexes := make([]int, 0, len(tasks))
	for i, task := range tasks {
		results[i].SourceKey = task.SourceKey
		target := d.runtime.chooseDispatchTarget(ctx, targets, task)
		if target == nil {
			results[i].Error = errors.New("no connected cluster worker has a compatible writable ETF target")
			continue
		}
		target.assignedBytes += max64(task.SourceSize, 0)
		requests = append(requests, DispatchMediaJobRequest{
			NodeID:         target.nodeID,
			IdempotencyKey: task.IdempotencyKey,
			ExpectedBytes:  task.SourceSize,
			RequiredCapabilities: []string{
				"share.save", "mobile.upload", "result.report",
			},
			TaskContext: protocol.TaskContext{
				MediaItemID: task.MediaItemID, WorkflowVersion: task.WorkflowVersion,
				SealedManifestVersion: task.SealedManifestVersion, TargetProfile: target.targetProfile,
				Subscription: protocol.SubscriptionTaskContext{
					SubscriptionID: task.SubscriptionID, SubscriptionItemID: task.SubscriptionItemID,
					SubscriptionName: task.SubscriptionName, SourceKey: task.SourceKey,
					SourceMessageID: task.SourceMessageID, ShareRefFingerprint: task.ShareRefFingerprint,
				},
				Share: protocol.ShareTaskContext{Provider: task.ShareProvider, URL: task.ShareURL, Passcode: task.SharePasscode},
				Media: protocol.MediaTaskContext{
					MediaType: task.MediaType, TMDBID: task.TMDBID, Season: task.Season, Episode: task.Episode,
					LogicalMediaRoot: task.LogicalMediaRoot, LogicalTargetPath: task.LogicalTargetPath,
				},
				SourceObjects: []protocol.SourceObject{{
					Provider: task.ShareProvider, SourceFileID: task.SourceFileID,
					SourceRelativePath: task.SourceRelativePath, Size: task.SourceSize, Hash: task.SourceHash,
				}},
			},
		})
		requestTaskIndexes = append(requestTaskIndexes, i)
	}
	if len(requests) == 0 {
		return results, errors.New("no subscription media task could be assigned")
	}
	batch, dispatchErr := d.runtime.DispatchMediaBatch(ctx, DispatchMediaBatchRequest{BatchID: subscriptionBatchID(tasks), Items: requests})
	jobsByItemID := make(map[uint]*model.ClusterJob)
	if batch != nil {
		for _, job := range batch.Jobs {
			if job != nil {
				jobsByItemID[job.SubscriptionItemID] = job
			}
		}
	}
	for requestIndex, taskIndex := range requestTaskIndexes {
		task := tasks[taskIndex]
		if job := jobsByItemID[task.SubscriptionItemID]; job != nil {
			results[taskIndex].JobID = job.ID
			continue
		}
		if batch != nil && requestIndex < len(batch.Errors) {
			results[taskIndex].Error = errors.New(batch.Errors[requestIndex])
		} else if dispatchErr != nil {
			results[taskIndex].Error = dispatchErr
		} else {
			results[taskIndex].Error = errors.New("cluster media task was not persisted")
		}
	}
	return results, dispatchErr
}

func (r *Runtime) subscriptionDispatchTargets(ctx context.Context) ([]*dispatchTarget, error) {
	r.mu.RLock()
	hub := r.hub
	r.mu.RUnlock()
	if hub == nil {
		return nil, errors.New("cluster coordinator is disabled")
	}
	connected := hub.ConnectedNodes()
	if len(connected) == 0 {
		return nil, errors.New("no cluster worker is connected")
	}
	var nodes []model.ClusterNode
	if err := db.GetDb().WithContext(ctx).Where("id IN ? AND disabled = ? AND drain = ?", connected, false, false).Find(&nodes).Error; err != nil {
		return nil, err
	}
	allowed := make(map[string]struct{}, len(nodes))
	for i := range nodes {
		allowed[nodes[i].ID] = struct{}{}
	}
	var desiredConfigs []model.ClusterNodeDesiredConfig
	if err := db.GetDb().WithContext(ctx).
		Where("node_id IN ? AND status = ? AND observed_revision >= revision", connected, model.ClusterDesiredStatusApplied).
		Find(&desiredConfigs).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	targets := make([]*dispatchTarget, 0, len(desiredConfigs))
	for i := range desiredConfigs {
		state := &desiredConfigs[i]
		if _, ok := allowed[state.NodeID]; !ok {
			continue
		}
		var desired protocol.WorkerDesiredConfig
		if json.Unmarshal([]byte(state.ConfigJSON), &desired) != nil {
			continue
		}
		for binding := range desired.TargetBindings {
			targets = append(targets, &dispatchTarget{nodeID: state.NodeID, targetProfile: binding})
		}
	}
	if len(targets) == 0 {
		return nil, errors.New("no applied worker target binding is available")
	}
	return targets, nil
}

func (r *Runtime) chooseDispatchTarget(ctx context.Context, targets []*dispatchTarget, task subscription.ClusterMediaTask) *dispatchTarget {
	eligible := make([]*dispatchTarget, 0, len(targets))
	for _, target := range targets {
		context := protocol.TaskContext{
			Share: protocol.ShareTaskContext{Provider: task.ShareProvider}, TargetProfile: target.targetProfile,
		}
		ok, err := nodeInventorySupports(ctx, target.nodeID, context, []string{"share.save", "mobile.upload", "result.report"}, task.SourceSize+target.assignedBytes)
		if err != nil || !ok {
			continue
		}
		eligible = append(eligible, target)
	}
	if len(eligible) == 0 {
		return nil
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		if eligible[i].assignedBytes == eligible[j].assignedBytes {
			return eligible[i].nodeID < eligible[j].nodeID
		}
		return eligible[i].assignedBytes < eligible[j].assignedBytes
	})
	return eligible[0]
}

func subscriptionBatchID(tasks []subscription.ClusterMediaTask) string {
	keys := make([]string, 0, len(tasks))
	for _, task := range tasks {
		keys = append(keys, task.IdempotencyKey)
	}
	sort.Strings(keys)
	return fmt.Sprintf("subscription-batch-%x", sha256Bytes(strings.Join(keys, "\x00")))[:63]
}

func sha256Bytes(value string) []byte {
	sum := sha256.Sum256([]byte(value))
	return sum[:]
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

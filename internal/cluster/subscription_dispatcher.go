package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/coordinator"
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
	nodeID             string
	targetProfile      string
	pendingAssignments int
	match              nodeProviderAccountMatch
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
				ObservationKey:      task.ObservationKey, ObservationExpected: task.ObservationExpected,
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
	expected := record.ObservationExpected
	if expected <= 0 {
		expected = 1
	}
	observationKey := strings.TrimSpace(record.ObservationKey)
	if observationKey == "" {
		observationKey = record.JobID
	}
	var records []model.ClusterShareInspectManifest
	query := db.GetDb().WithContext(ctx).Where("subscription_id = ? AND observation_key = ?", record.SubscriptionID, observationKey).
		Order("created_at ASC, id ASC")
	if err := query.Find(&records).Error; err != nil {
		return err
	}
	if len(records) < expected {
		return fmt.Errorf("%w: received %d of %d manifests", coordinator.ErrShareInspectObservationIncomplete, len(records), expected)
	}
	if len(records) > expected {
		return fmt.Errorf("cluster share inspect observation %s has %d manifests, expected %d", observationKey, len(records), expected)
	}
	inputs := make([]subscription.ClusterInspectManifestInput, 0, len(records))
	for _, item := range records {
		var decoded protocol.ShareInspectManifest
		if item.ID == record.ID {
			decoded = manifest
		} else if err := json.Unmarshal([]byte(item.PayloadJSON), &decoded); err != nil {
			return err
		}
		var job model.ClusterJob
		if err := db.GetDb().WithContext(ctx).First(&job, "id = ?", item.JobID).Error; err != nil {
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
			ObservationKey:      taskContext.Subscription.ObservationKey, ObservationExpected: taskContext.Subscription.ObservationExpected,
		}
		objects := make([]subscription.ClusterInspectObject, 0, len(decoded.Objects))
		for _, object := range decoded.Objects {
			objects = append(objects, subscription.ClusterInspectObject{
				FileID: object.SourceFileID, RelativePath: object.SourceRelativePath,
				Size: object.Size, Hash: object.Hash, ModifiedAt: object.ModifiedAt,
			})
		}
		inputs = append(inputs, subscription.ClusterInspectManifestInput{Task: task, Objects: objects})
	}
	if _, err := subscription.ApplyClusterInspectObservation(ctx, inputs); err != nil {
		return err
	}
	now := time.Now().UTC()
	ids := make([]string, 0, len(records)-1)
	for _, item := range records {
		if item.ID != record.ID {
			ids = append(ids, item.ID)
		}
	}
	if len(ids) > 0 {
		if err := db.GetDb().WithContext(ctx).Model(&model.ClusterShareInspectManifest{}).
			Where("id IN ? AND status = ?", ids, model.ClusterShareInspectStatusPending).
			Updates(map[string]any{"status": model.ClusterShareInspectStatusConsumed, "consumed_at": now, "last_error": ""}).Error; err != nil {
			return err
		}
	}
	return nil
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
		target.pendingAssignments++
		taskContext := subscriptionMediaTaskContext(task, target.targetProfile)
		bindTaskContextProviderAccounts(&taskContext, target.match)
		requests = append(requests, DispatchMediaJobRequest{
			NodeID:         target.nodeID,
			IdempotencyKey: task.IdempotencyKey,
			ExpectedBytes:  task.SourceSize,
			RequiredCapabilities: []string{
				"share.save", "mobile.upload", "result.report",
			},
			TaskContext: taskContext,
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

func subscriptionMediaTaskContext(task subscription.ClusterMediaTask, targetProfile string) protocol.TaskContext {
	staging := protocol.ProviderTargetRequirement{
		Provider:      strings.TrimSpace(task.ShareProvider),
		NeedShareSave: true,
		RequiredBytes: max64(task.SourceSize, 0),
	}
	delivery := protocol.ProviderTargetRequirement{
		Provider:      "yidong139",
		NeedUpload:    true,
		RequiredBytes: max64(task.SourceSize, 0),
	}
	return protocol.TaskContext{
		MediaItemID: task.MediaItemID, WorkflowVersion: task.WorkflowVersion,
		SealedManifestVersion: task.SealedManifestVersion, TargetProfile: targetProfile,
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
		StagingTarget:  staging,
		DeliveryTarget: delivery,
	}
}

func (r *Runtime) subscriptionDispatchTargets(ctx context.Context) ([]*dispatchTarget, error) {
	dispatchTransport := r.mediaDispatchTransport()
	if dispatchTransport == nil {
		return nil, errors.New("cluster coordinator is disabled")
	}
	connected := dispatchTransport.ConnectedNodes()
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
	var inventories []model.ClusterNodeInventory
	if err := db.GetDb().WithContext(ctx).Where("node_id IN ?", connected).Order("node_id ASC, revision DESC").Find(&inventories).Error; err != nil {
		return nil, err
	}
	latest := make(map[string]model.ClusterNodeInventory, len(inventories))
	for _, inventory := range inventories {
		if _, exists := latest[inventory.NodeID]; !exists {
			latest[inventory.NodeID] = inventory
		}
	}
	targets := make([]*dispatchTarget, 0, len(connected))
	for nodeID, inventory := range latest {
		if _, ok := allowed[nodeID]; !ok {
			continue
		}
		var accounts []protocol.ProviderAccountInventory
		if json.Unmarshal([]byte(inventory.ProviderAccountsJSON), &accounts) != nil {
			continue
		}
		hasProviderTarget := false
		for _, account := range accounts {
			if !providerAccountHealthy(account) || !account.SupportsUpload || !account.SupportsETF {
				continue
			}
			hasProviderTarget = true
			break
		}
		if hasProviderTarget {
			targets = append(targets, &dispatchTarget{nodeID: nodeID})
		}
	}
	if len(targets) > 0 {
		return targets, nil
	}
	// Compatibility fallback for workers that have not yet reported provider
	// account inventories. New workers are scheduled directly from the
	// capability pool above, not from user-visible mount-path configuration.
	var desiredConfigs []model.ClusterNodeDesiredConfig
	if err := db.GetDb().WithContext(ctx).
		Where("node_id IN ? AND status = ? AND observed_revision >= revision", connected, model.ClusterDesiredStatusApplied).
		Find(&desiredConfigs).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	targets = make([]*dispatchTarget, 0, len(desiredConfigs))
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
		taskContext := subscriptionMediaTaskContext(task, target.targetProfile)
		match, ok, err := nodeInventoryProviderMatch(ctx, target.nodeID, taskContext, []string{"share.save", "mobile.upload", "result.report"}, task.SourceSize)
		if err != nil || !ok {
			continue
		}
		match.ActiveJobs += target.pendingAssignments
		match.NodeActiveJobs += int64(target.pendingAssignments)
		target.match = match
		eligible = append(eligible, target)
	}
	if len(eligible) == 0 {
		return nil
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		left, right := eligible[i].match, eligible[j].match
		if left.MembershipWeight != right.MembershipWeight {
			return left.MembershipWeight > right.MembershipWeight
		}
		if left.FreeBytes != right.FreeBytes {
			return left.FreeBytes > right.FreeBytes
		}
		if left.ActiveJobs != right.ActiveJobs {
			return left.ActiveJobs < right.ActiveJobs
		}
		if left.NodeActiveJobs != right.NodeActiveJobs {
			return left.NodeActiveJobs < right.NodeActiveJobs
		}
		if eligible[i].nodeID != eligible[j].nodeID {
			return eligible[i].nodeID < eligible[j].nodeID
		}
		return eligible[i].targetProfile < eligible[j].targetProfile
	})
	return eligible[0]
}

func bindTaskContextProviderAccounts(taskContext *protocol.TaskContext, match nodeProviderAccountMatch) {
	if taskContext == nil {
		return
	}
	taskContext.StagingTarget.StorageID = match.Staging.StorageID
	taskContext.StagingTarget.NodeMountID = match.Staging.NodeMountID
	taskContext.StagingTarget.AccountFingerprint = match.Staging.AccountFingerprint
	taskContext.DeliveryTarget.StorageID = match.Delivery.StorageID
	taskContext.DeliveryTarget.NodeMountID = match.Delivery.NodeMountID
	taskContext.DeliveryTarget.AccountFingerprint = match.Delivery.AccountFingerprint
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

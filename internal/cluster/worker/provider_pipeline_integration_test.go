package worker

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestWorkerOfferHandlerResolvesBoundPan115And139BeforeTransfer(t *testing.T) {
	original := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = original })
	setWorkerSubscriptionConfig(t, model.SubscriptionConfig{
		DefaultTarget: model.SubscriptionStorageTarget{Provider: "yidong139", Folder: "worker-delivery"},
		Telegram: model.SubscriptionTelegramSourceConfig{
			Pan115: model.SubscriptionTelegramPanConfig{
				TempTransferTarget: model.SubscriptionStorageTarget{Provider: "pan115", Folder: "worker-staging"},
			},
		},
	})

	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))), &gorm.Config{})
	require.NoError(t, err)
	db.Init(database)
	pan115 := model.Storage{ID: 115, MountPath: "/worker-115-vip", Driver: "115 Cloud", Status: "work", Addition: `{"membership_tier":"vip"}`}
	mobile139 := model.Storage{ID: 139, MountPath: "/worker-139-diamond", Driver: "139Yun", Status: "work", Addition: `{"type":"personal_new","cluster_dedicated_account":true,"membership_tier":"diamond"}`}
	require.NoError(t, database.Create(&pan115).Error)
	require.NoError(t, database.Create(&mobile139).Error)

	oldEnsure := ensureResolvedProviderFolder
	ensureResolvedProviderFolder = func(_ context.Context, target subscription.ResolvedProviderTarget) (subscription.ResolvedProviderTarget, error) {
		return target, nil
	}
	t.Cleanup(func() { ensureResolvedProviderFolder = oldEnsure })

	const nodeID = "hybrid-local"
	sender := &providerPipelineWorkerSender{result: make(chan protocol.JobResult, 1)}
	service := New(&fakeResultQueue{}, sender)
	sender.service = service
	service.controlNodeID = nodeID
	stagingAccount := providerAccountInventory(nodeID, pan115, 200<<30, 1<<40)
	deliveryAccount := providerAccountInventory(nodeID, mobile139, 800<<30, 2<<40)
	task := protocol.TaskContext{
		ParentBatchID: "batch-1", MediaItemID: "media-1", WorkflowVersion: "v1", SealedManifestVersion: "v1",
		Subscription: protocol.SubscriptionTaskContext{SubscriptionID: 1, SubscriptionItemID: 2, SourceKey: "source-1"},
		Share:        protocol.ShareTaskContext{Provider: "pan115", URL: "https://115.com/s/test"},
		Media:        protocol.MediaTaskContext{MediaType: "tv", LogicalTargetPath: "/剧集/episode.mkv"},
		StagingTarget: protocol.ProviderTargetRequirement{
			Provider: "pan115", StorageID: stagingAccount.StorageID,
			NodeMountID: stagingAccount.NodeMountID, AccountFingerprint: stagingAccount.AccountFingerprint,
			NeedShareSave: true, RequiredBytes: 12 << 30,
		},
		DeliveryTarget: protocol.ProviderTargetRequirement{
			Provider: "yidong139", StorageID: deliveryAccount.StorageID,
			NodeMountID: deliveryAccount.NodeMountID, AccountFingerprint: deliveryAccount.AccountFingerprint,
			NeedUpload: true, RequiredBytes: 12 << 30,
		},
		SourceObjects: []protocol.SourceObject{{Provider: "pan115", SourceFileID: "file-1", Size: 12 << 30}},
	}
	resolved := make(chan resolvedMediaTransferTargets, 1)
	service.mediaTransferBoundary = func(_ context.Context, _ protocol.JobOffer, targets resolvedMediaTransferTargets) error {
		resolved <- targets
		return nil
	}
	contextHash, err := protocol.HashTaskContext(task)
	require.NoError(t, err)
	offer := protocol.JobOffer{
		AttemptRef:     protocol.AttemptRef{JobID: "job-1", AttemptID: "attempt-1", Generation: 1, LeaseToken: "lease"},
		IdempotencyKey: "operation-1", JobType: model.ClusterJobTypeMediaTransfer,
		LeaseUntil: time.Now().Add(time.Minute), RequiredCapabilities: []string{"share.save", "mobile.upload", "result.report"},
		TaskContext: task, TaskContextHash: contextHash,
	}
	envelope, err := protocol.NewEnvelope(protocol.MessageJobOffer, offer)
	require.NoError(t, err)
	require.NoError(t, service.HandleMessage(t.Context(), nil, *envelope))

	select {
	case targets := <-resolved:
		require.Equal(t, "/worker-115-vip/worker-staging", targets.StagingRoot)
		require.NotContains(t, targets.StagingRoot, ".openlist-cluster")
		require.Equal(t, "/worker-139-diamond/worker-delivery", targets.DeliveryRoot)
		require.Equal(t, "/worker-139-diamond", targets.DeliveryMount)
		require.NotContains(t, targets.StagingRoot, "/115/转存至移动")
		require.NotContains(t, targets.DeliveryRoot, "/139_60t/")
	case result := <-sender.result:
		t.Fatalf("worker failed before transfer boundary: %s", result.Error)
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not reach media transfer boundary")
	}
	select {
	case result := <-sender.result:
		require.Empty(t, result.Error)
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not report job result")
	}
	require.Contains(t, sender.stages, model.ClusterStageSavingShare)
}

type providerPipelineWorkerSender struct {
	service *Service
	stages  []string
	result  chan protocol.JobResult
}

func (s *providerPipelineWorkerSender) Send(ctx context.Context, message protocol.Envelope) error {
	switch message.Type {
	case protocol.MessageStagePermitRequest:
		request, err := protocol.DecodePayload[protocol.StagePermitRequest](message)
		if err != nil {
			return err
		}
		s.stages = append(s.stages, request.Stage)
		permit, err := protocol.NewEnvelope(protocol.MessageStagePermit, protocol.StagePermit{
			AttemptRef: request.AttemptRef, Stage: request.Stage, OperationKey: request.OperationKey,
			PermitToken: "permit", PermitExpiresAt: time.Now().Add(time.Minute),
		})
		if err != nil {
			return err
		}
		permit.CorrelationID = message.MessageID
		return s.service.HandleMessage(ctx, nil, *permit)
	case protocol.MessageJobResult:
		result, err := protocol.DecodePayload[protocol.JobResult](message)
		if err != nil {
			return err
		}
		s.result <- result
	}
	return nil
}

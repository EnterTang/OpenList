package worker

import (
	"context"
	"errors"
	"sync"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/resultqueue"
	"github.com/OpenListTeam/OpenList/v4/internal/task_group"
)

var defaultService struct {
	sync.RWMutex
	value *Service
}

func WithUploadManifest(ctx context.Context, manifest protocol.UploadETFManifest) context.Context {
	binding, _ := task_group.ClusterTransferBindingFromContext(ctx)
	binding.UploadManifest = &manifest
	return task_group.WithClusterTransferBinding(ctx, binding)
}

func UploadManifestFromContext(ctx context.Context) (protocol.UploadETFManifest, bool) {
	return task_group.UploadManifestFromContext(ctx)
}

func WithAdditionalCleanupTargets(ctx context.Context, targets ...resultqueue.CleanupTarget) context.Context {
	binding, _ := task_group.ClusterTransferBindingFromContext(ctx)
	binding.AdditionalCleanupTargets = append([]resultqueue.CleanupTarget(nil), targets...)
	return task_group.WithClusterTransferBinding(ctx, binding)
}

func AdditionalCleanupTargetsFromContext(ctx context.Context) []resultqueue.CleanupTarget {
	return task_group.AdditionalCleanupTargetsFromContext(ctx)
}

func SetDefaultService(service *Service) {
	defaultService.Lock()
	defaultService.value = service
	defaultService.Unlock()
}

func CompleteClusterUpload(ctx context.Context, manifest protocol.UploadETFManifest, cleanup resultqueue.CleanupRequest) (string, error) {
	defaultService.RLock()
	service := defaultService.value
	defaultService.RUnlock()
	if service == nil {
		return "", errors.New("cluster worker result service is unavailable")
	}
	return service.EnqueueThenCleanup(ctx, manifest, cleanup)
}

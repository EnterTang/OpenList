package worker

import (
	"context"
	"errors"
	"sync"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/resultqueue"
)

type uploadManifestContextKey struct{}
type additionalCleanupContextKey struct{}

var defaultService struct {
	sync.RWMutex
	value *Service
}

func WithUploadManifest(ctx context.Context, manifest protocol.UploadETFManifest) context.Context {
	return context.WithValue(ctx, uploadManifestContextKey{}, manifest)
}

func UploadManifestFromContext(ctx context.Context) (protocol.UploadETFManifest, bool) {
	if ctx == nil {
		return protocol.UploadETFManifest{}, false
	}
	manifest, ok := ctx.Value(uploadManifestContextKey{}).(protocol.UploadETFManifest)
	return manifest, ok
}

func WithAdditionalCleanupTargets(ctx context.Context, targets ...resultqueue.CleanupTarget) context.Context {
	return context.WithValue(ctx, additionalCleanupContextKey{}, append([]resultqueue.CleanupTarget(nil), targets...))
}

func AdditionalCleanupTargetsFromContext(ctx context.Context) []resultqueue.CleanupTarget {
	if ctx == nil {
		return nil
	}
	targets, _ := ctx.Value(additionalCleanupContextKey{}).([]resultqueue.CleanupTarget)
	return append([]resultqueue.CleanupTarget(nil), targets...)
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

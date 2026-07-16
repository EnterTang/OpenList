package task_group

import (
	"context"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/cluster/resultqueue"
)

// TransferFinalizePayload is persisted with native file-transfer tasks so the
// same post-transfer rename/finalization behavior survives task recovery.
type TransferFinalizePayload struct {
	SubscriptionID     uint   `json:"subscription_id,omitempty"`
	SubscriptionItemID uint   `json:"subscription_item_id,omitempty"`
	SourceKey          string `json:"source_key,omitempty"`
	FileHash           string `json:"file_hash,omitempty"`
	TargetDir          string `json:"target_dir"`
	FileName           string `json:"file_name"`
	TargetName         string `json:"target_name,omitempty"`
}

// ClusterTransferBinding contains only serializable values required by a
// recovered native Move task. Runtime-only context values must not be added.
type ClusterTransferBinding struct {
	UploadManifest           *protocol.UploadETFManifest `json:"upload_manifest,omitempty"`
	AdditionalCleanupTargets []resultqueue.CleanupTarget `json:"additional_cleanup_targets,omitempty"`
	FinalizePayload          *TransferFinalizePayload    `json:"finalize_payload,omitempty"`
}

type clusterTransferBindingContextKey struct{}

func WithClusterTransferBinding(ctx context.Context, binding ClusterTransferBinding) context.Context {
	return context.WithValue(ctx, clusterTransferBindingContextKey{}, binding)
}

func ClusterTransferBindingFromContext(ctx context.Context) (ClusterTransferBinding, bool) {
	if ctx == nil {
		return ClusterTransferBinding{}, false
	}
	binding, ok := ctx.Value(clusterTransferBindingContextKey{}).(ClusterTransferBinding)
	return binding, ok
}

func UploadManifestFromContext(ctx context.Context) (protocol.UploadETFManifest, bool) {
	binding, ok := ClusterTransferBindingFromContext(ctx)
	if !ok || binding.UploadManifest == nil {
		return protocol.UploadETFManifest{}, false
	}
	return *binding.UploadManifest, true
}

func AdditionalCleanupTargetsFromContext(ctx context.Context) []resultqueue.CleanupTarget {
	binding, ok := ClusterTransferBindingFromContext(ctx)
	if !ok {
		return nil
	}
	return append([]resultqueue.CleanupTarget(nil), binding.AdditionalCleanupTargets...)
}

func TransferFinalizePayloadFromContext(ctx context.Context) (TransferFinalizePayload, bool) {
	binding, ok := ClusterTransferBindingFromContext(ctx)
	if !ok || binding.FinalizePayload == nil {
		return TransferFinalizePayload{}, false
	}
	return *binding.FinalizePayload, true
}

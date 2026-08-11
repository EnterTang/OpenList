package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"path"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
)

const (
	directDownloadErrorCodeUnavailable       = "direct_share_unavailable"
	directDownloadErrorCodeReauthorize       = "direct_share_reauthorize"
	directDownloadErrorCodeTerminal          = "direct_share_terminal"
	directDownloadErrorCodeTargetFailed      = "direct_download_target_failed"
	directDownloadErrorCodeSourceUnavailable = "direct_download_source_unavailable"
)

type directDownloadError struct {
	code     string
	message  string
	fallback bool
}

func (e *directDownloadError) Error() string {
	if e == nil {
		return "direct share download failed"
	}
	if strings.TrimSpace(e.message) == "" {
		return e.code
	}
	return e.message
}

func (e *directDownloadError) ClusterErrorCode() string {
	if e == nil {
		return ""
	}
	return e.code
}

func directDownloadCanFallback(err error) bool {
	var classified *directDownloadError
	if errors.As(err, &classified) {
		return classified.fallback
	}
	return false
}

func executeDirectShareDownload(ctx context.Context, offer protocol.JobOffer, targetRootBase string) (map[string]any, error) {
	if strings.TrimSpace(offer.TaskContext.DeliveryMode) != model.SubscriptionDeliveryModeDirectDownload {
		return nil, &directDownloadError{code: directDownloadErrorCodeUnavailable, message: "direct share download is not selected", fallback: true}
	}
	cfg, err := subscription.GetConfig()
	if err != nil {
		return nil, &directDownloadError{code: directDownloadErrorCodeUnavailable, message: "direct share download configuration is unavailable", fallback: true}
	}
	if !cfg.DirectShareLinkEnabled || !cfg.DirectDownloadFirstEnabled {
		return nil, &directDownloadError{code: directDownloadErrorCodeUnavailable, message: "direct share download is disabled", fallback: true}
	}
	ref, err := subscription.ParseShareURL(offer.TaskContext.Share.URL)
	if err != nil {
		return nil, &directDownloadError{code: directDownloadErrorCodeTerminal, message: "direct share URL is invalid", fallback: false}
	}
	if passcode := strings.TrimSpace(offer.TaskContext.Share.Passcode); passcode != "" {
		ref.Passcode = passcode
	}

	var downloader subscription.ShareDirectDownloader
	switch ref.Provider {
	case subscription.ShareProviderPan123:
		providerConfig := subscription.ResolveShareInspectConfig(ref.Provider, cfg.Telegram.Pan123)
		provider := subscription.NewPan123ShareProvider(providerConfig)
		var ok bool
		downloader, ok = provider.(subscription.ShareDirectDownloader)
		if !ok {
			return nil, &directDownloadError{code: directDownloadErrorCodeUnavailable, message: "123pan direct share download is unavailable", fallback: true}
		}
	case subscription.ShareProviderPan115:
		providerConfig := subscription.ResolveShareInspectConfig(ref.Provider, cfg.Telegram.Pan115)
		provider := subscription.NewPan115ShareProvider(providerConfig)
		var ok bool
		downloader, ok = provider.(subscription.ShareDirectDownloader)
		if !ok {
			return nil, &directDownloadError{code: directDownloadErrorCodeUnavailable, message: "115 direct share download is unavailable", fallback: true}
		}
	default:
		return nil, &directDownloadError{code: directDownloadErrorCodeUnavailable, message: "provider direct share download is not supported", fallback: true}
	}

	primary := primarySourceObject(offer.TaskContext.SourceObjects)
	if primary.SourceFileID == "" {
		return nil, &directDownloadError{code: directDownloadErrorCodeTerminal, message: "direct share download has no source object", fallback: false}
	}
	item := subscription.ShareItem{
		ID:   primary.SourceFileID,
		Name: path.Base(strings.TrimSpace(primary.SourceRelativePath)),
		Size: primary.Size,
		Raw:  mapStringValuesAsAny(primary.ProviderData),
	}
	if item.Name == "." || item.Name == "/" || item.Name == "" {
		return nil, &directDownloadError{code: directDownloadErrorCodeTerminal, message: "direct share download has no safe file name", fallback: false}
	}

	targetFilePath, err := mapClusterDeliveryPath(targetRootBase, offer.TaskContext.Media.LogicalMediaRoot, offer.TaskContext.Media.LogicalTargetPath)
	if err != nil {
		return nil, &directDownloadError{code: directDownloadErrorCodeTargetFailed, message: "map direct download target failed", fallback: false}
	}
	targetRoot := path.Dir(targetFilePath)
	targetName := path.Base(targetFilePath)
	if err := fs.MakeDir(ctx, targetRoot); err != nil {
		return nil, &directDownloadError{code: directDownloadErrorCodeTargetFailed, message: "create direct download target failed", fallback: false}
	}

	tempName := directDownloadTempName(offer.IdempotencyKey, targetName)
	tempPath := path.Join(targetRoot, tempName)
	defer func() { _ = fs.Remove(context.WithoutCancel(ctx), tempPath) }()
	for attempt := 0; attempt < 2; attempt++ {
		link, err := downloader.GetShareDownloadURL(ctx, ref, item)
		if err != nil {
			return nil, classifyDirectShareProviderError(err)
		}
		if strings.TrimSpace(link.URL) == "" {
			return nil, &directDownloadError{code: directDownloadErrorCodeSourceUnavailable, message: "direct share download URL is empty", fallback: true}
		}
		linkHeaders := make(http.Header, len(link.Headers))
		for key, value := range link.Headers {
			if strings.TrimSpace(key) != "" && value != "" {
				linkHeaders.Set(key, value)
			}
		}
		size := link.Size
		if size <= 0 {
			size = primary.Size
		}
		streamObj := &model.Object{ID: link.FileID, Name: tempName, Size: size}
		file, err := stream.NewSeekableStream(&stream.FileStream{Ctx: ctx, Obj: streamObj}, &model.Link{
			URL: link.URL, Header: linkHeaders, ContentLength: size,
		})
		if err != nil {
			return nil, &directDownloadError{code: directDownloadErrorCodeSourceUnavailable, message: "create direct download stream failed", fallback: true}
		}
		putErr := fs.PutDirectly(ctx, targetRoot, file, true)
		if putErr == nil {
			verified, verifyErr := fs.Get(ctx, tempPath, &fs.GetArgs{NoLog: true})
			if verifyErr != nil || verified == nil || verified.IsDir() || (size > 0 && verified.GetSize() != size) {
				_ = fs.Remove(context.WithoutCancel(ctx), tempPath)
				return nil, &directDownloadError{code: directDownloadErrorCodeSourceUnavailable, message: "direct download size verification failed", fallback: true}
			}
			if err := fs.Rename(ctx, tempPath, targetName, true); err != nil {
				return nil, &directDownloadError{code: directDownloadErrorCodeTargetFailed, message: "commit direct download target failed", fallback: false}
			}
			return map[string]any{
				"delivery_mode": model.SubscriptionDeliveryModeDirectDownload,
				"verified_size": size,
			}, nil
		}
		_ = fs.Remove(context.WithoutCancel(ctx), tempPath)
		if attempt == 0 && directDownloadSourceRetryable(putErr) {
			continue
		}
		if directDownloadSourceRetryable(putErr) {
			return nil, &directDownloadError{code: directDownloadErrorCodeSourceUnavailable, message: "direct share download source failed", fallback: true}
		}
		return nil, &directDownloadError{code: directDownloadErrorCodeTargetFailed, message: "direct download target write failed", fallback: false}
	}
	return nil, &directDownloadError{code: directDownloadErrorCodeSourceUnavailable, message: "direct share download source failed", fallback: true}
}

func (s *Service) executeMediaDirectFirst(ctx context.Context, offer protocol.JobOffer) (map[string]any, error) {
	if s.mediaTransferBoundary != nil {
		// Boundary-backed tests and integrations do not expose a real target
		// storage. Keep them on the established share-save path.
		return nil, s.executeMediaTransfer(ctx, offer)
	}
	targetRootBase, _, err := s.resolveDeliveryTargetRoot(ctx, offer.TaskContext)
	if err != nil {
		return nil, err
	}
	result, directErr := executeDirectShareDownload(ctx, offer, targetRootBase)
	if directErr == nil {
		return result, nil
	}
	if !directDownloadCanFallback(directErr) {
		return nil, directErr
	}
	// The same durable media job continues through share-save. It never
	// creates a second job and the temporary direct URL is discarded before
	// the fallback starts.
	if err := s.executeMediaTransfer(ctx, offer); err != nil {
		return map[string]any{
			"delivery_mode":   model.SubscriptionDeliveryModeTransfer,
			"fallback_reason": directErr.Error(),
		}, err
	}
	return map[string]any{
		"delivery_mode":   model.SubscriptionDeliveryModeTransfer,
		"fallback_reason": directErr.Error(),
	}, nil
}

func mapStringValuesAsAny(values map[string]string) map[string]any {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]any, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func directDownloadTempName(operationKey, targetName string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(operationKey) + "\x00" + targetName))
	return ".openlist-direct-" + hex.EncodeToString(sum[:8]) + ".part"
}

func classifyDirectShareProviderError(err error) error {
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(message, "401"), strings.Contains(message, "unauthorized"), strings.Contains(message, "forbidden"), strings.Contains(message, "invalid token"), strings.Contains(message, "refresh token"), strings.Contains(message, "credential"), strings.Contains(message, "signature"):
		return &directDownloadError{code: directDownloadErrorCodeReauthorize, message: "direct share credentials require reauthorization", fallback: false}
	case strings.Contains(message, "invalid share"), strings.Contains(message, "password"), strings.Contains(message, "param"):
		return &directDownloadError{code: directDownloadErrorCodeTerminal, message: "direct share request is not valid", fallback: false}
	default:
		return &directDownloadError{code: directDownloadErrorCodeSourceUnavailable, message: "direct share endpoint is unavailable", fallback: true}
	}
}

func directDownloadSourceRetryable(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{"408", "429", "500", "502", "503", "504", "timeout", "timed out", "connection reset", "unexpected eof", "eof", "temporary"} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

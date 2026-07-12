package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/subscription"
)

var inspectShareTree = inspectConfiguredShareTree

func (s *Service) executeShareInspect(ctx context.Context, offer protocol.JobOffer) (map[string]any, error) {
	manifest, err := inspectShareTree(ctx, offer.TaskContext.Share, offer.TaskContext.SealedManifestVersion)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func inspectConfiguredShareTree(ctx context.Context, share protocol.ShareTaskContext, version string) (protocol.ShareInspectManifest, error) {
	ref, err := subscription.ParseShareURL(share.URL)
	if err != nil {
		return protocol.ShareInspectManifest{}, err
	}
	if passcode := strings.TrimSpace(share.Passcode); passcode != "" {
		ref.Passcode = passcode
	}
	cfg, err := subscription.GetConfig()
	if err != nil {
		return protocol.ShareInspectManifest{}, err
	}
	var provider subscription.ShareTreeLister
	switch ref.Provider {
	case subscription.ShareProviderQuark:
		provider = subscription.NewQuarkShareProvider(withoutTempRoot(cfg.Telegram.Quark))
	case subscription.ShareProviderAliyunDrive:
		provider = subscription.NewAliyunDriveShareProvider(withoutTempRoot(cfg.Telegram.AliyunDrive))
	case subscription.ShareProviderPan123:
		provider = subscription.NewPan123ShareProvider(withoutTempRoot(cfg.Telegram.Pan123))
	case subscription.ShareProviderPan115:
		provider = subscription.NewPan115ShareProvider(withoutTempRoot(cfg.Telegram.Pan115))
	default:
		return protocol.ShareInspectManifest{}, errors.New("share provider is not supported by this worker")
	}
	entries, err := subscription.ListShareTree(ctx, provider, ref)
	if err != nil {
		return protocol.ShareInspectManifest{}, err
	}
	objects := make([]protocol.SourceObject, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir {
			continue
		}
		objects = append(objects, protocol.SourceObject{Provider: string(ref.Provider), SourceFileID: entry.ID, SourceRelativePath: strings.TrimPrefix(entry.Path, "/"), Size: entry.Size})
		objects[len(objects)-1].ModifiedAt = entry.Modified.UTC()
	}
	sort.Slice(objects, func(i, j int) bool {
		if objects[i].SourceRelativePath == objects[j].SourceRelativePath {
			return objects[i].SourceFileID < objects[j].SourceFileID
		}
		return objects[i].SourceRelativePath < objects[j].SourceRelativePath
	})
	raw, err := json.Marshal(objects)
	if err != nil {
		return protocol.ShareInspectManifest{}, err
	}
	sum := sha256.Sum256(raw)
	canonicalRef := string(ref.Provider) + ":" + strings.TrimSpace(ref.ShareID)
	if parentID := strings.TrimSpace(ref.ParentID); parentID != "" {
		canonicalRef += ":" + parentID
	}
	return protocol.ShareInspectManifest{Version: version, Share: protocol.ShareTaskContext{Provider: string(ref.Provider), URL: share.URL, Passcode: share.Passcode}, CanonicalRef: canonicalRef, Objects: objects, ObjectHash: hex.EncodeToString(sum[:]), InspectedAt: time.Now().UTC()}, nil
}

func withoutTempRoot(cfg model.SubscriptionTelegramPanConfig) model.SubscriptionTelegramPanConfig {
	cfg.TempTransferRoot = ""
	return cfg
}

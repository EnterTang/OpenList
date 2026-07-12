package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
)

const maxManifestFileNameBytes = 255

func HashTaskContext(context TaskContext) (string, error) {
	raw, err := json.Marshal(context)
	if err != nil {
		return "", fmt.Errorf("marshal task context: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func HashUploadETFManifest(manifest UploadETFManifest) (string, error) {
	raw, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("marshal upload etf manifest: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (c TaskContext) Validate() error {
	if c.ParentBatchID == "" {
		return errors.New("parent batch id is required")
	}
	if c.MediaItemID == "" {
		return errors.New("media item id is required")
	}
	if c.WorkflowVersion == "" {
		return errors.New("workflow version is required")
	}
	if c.SealedManifestVersion == "" {
		return errors.New("sealed manifest version is required")
	}
	if c.TargetProfile == "" {
		return errors.New("target profile is required")
	}
	if c.Subscription.SubscriptionID == 0 {
		return errors.New("subscription id is required")
	}
	if c.Subscription.SubscriptionItemID == 0 {
		return errors.New("subscription item id is required")
	}
	if c.Subscription.SourceKey == "" {
		return errors.New("subscription source key is required")
	}
	if c.Media.MediaType == "" {
		return errors.New("media type is required")
	}
	if c.Media.LogicalTargetPath == "" {
		return errors.New("logical target path is required")
	}
	if len(c.SourceObjects) == 0 {
		return errors.New("at least one source object is required")
	}
	for i, object := range c.SourceObjects {
		if object.Provider == "" || object.SourceFileID == "" {
			return fmt.Errorf("source object %d requires provider and source file id", i)
		}
	}
	return nil
}

func (o JobOffer) Validate() error {
	if err := validateAttemptRef(o.AttemptRef, true); err != nil {
		return err
	}
	if o.JobType == "" {
		return errors.New("job type is required")
	}
	if o.IdempotencyKey == "" {
		return errors.New("idempotency key is required")
	}
	if o.LeaseUntil.IsZero() {
		return errors.New("lease_until is required")
	}
	if (o.JobType == "media.transfer" || o.JobType == "share.inspect") && o.TaskContext.Share.URL == "" {
		return errors.New("share url is required")
	}
	if o.JobType == "share.inspect" {
		if strings.TrimSpace(o.TaskContext.WorkflowVersion) == "" || strings.TrimSpace(o.TaskContext.SealedManifestVersion) == "" {
			return errors.New("share.inspect requires workflow and sealed manifest versions")
		}
		return validateTaskContextHash(o.TaskContext, o.TaskContextHash)
	}
	if err := o.TaskContext.Validate(); err != nil {
		return fmt.Errorf("invalid task context: %w", err)
	}
	if o.JobType == "media.transfer" {
		if len(o.TaskContext.SourceObjects) != 1 {
			return errors.New("media.transfer requires exactly one source object; split multiple media files into separate child jobs")
		}
	}
	return validateTaskContextHash(o.TaskContext, o.TaskContextHash)
}

func (m UploadETFManifest) Validate() error {
	if err := validateAttemptRef(m.AttemptRef, true); err != nil {
		return err
	}
	if m.MediaItemID == "" || m.MediaItemID != m.TaskContext().MediaItemID {
		return errors.New("media item id is missing or inconsistent")
	}
	if m.ParentBatchID != m.TaskContext().ParentBatchID {
		return errors.New("parent batch id is inconsistent")
	}
	if m.OperationKey == "" {
		return errors.New("operation key is required")
	}
	if m.StagePermitToken == "" {
		return errors.New("upload stage permit token is required")
	}
	if err := validateManifestFileName(m.Name); err != nil {
		return err
	}
	if m.Size <= 0 {
		return errors.New("file size must be positive")
	}
	if !isSHA256(m.SHA256) {
		return errors.New("sha256 must be 64 hexadecimal characters")
	}
	if m.HashSource == "" {
		return errors.New("hash source is required")
	}
	if m.MobileAccountBinding == "" {
		return errors.New("mobile account binding is required")
	}
	if m.RemoteFileID == "" {
		return errors.New("remote file id is required")
	}
	context := m.TaskContext()
	if err := context.Validate(); err != nil {
		return fmt.Errorf("invalid task context: %w", err)
	}
	return validateTaskContextHash(context, m.TaskContextHash)
}

func validateManifestFileName(name string) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("file name is required")
	}
	if len(name) > maxManifestFileNameBytes {
		return fmt.Errorf("file name exceeds %d bytes", maxManifestFileNameBytes)
	}
	if name == "." || name == ".." {
		return errors.New("file name must not be a dot directory")
	}
	if strings.Contains(name, `\`) || path.Base(name) != name {
		return errors.New("file name must not contain a path")
	}
	return nil
}

// TaskContext reconstructs the immutable coordinator context echoed by a
// worker result. Coordinator-side code must compare it with the stored job
// snapshot before accepting the result.
func (m UploadETFManifest) TaskContext() TaskContext {
	return TaskContext{
		ParentBatchID:         m.ParentBatchID,
		MediaItemID:           m.MediaItemID,
		WorkflowVersion:       m.WorkflowVersion,
		SealedManifestVersion: m.SealedManifestVersion,
		Subscription:          m.Subscription,
		Share:                 m.Share,
		Media:                 m.Media,
		SourceObjects:         m.SourceObjects,
		TargetProfile:         m.TargetProfile,
	}
}

func validateAttemptRef(ref AttemptRef, requireLease bool) error {
	if ref.JobID == "" {
		return errors.New("job id is required")
	}
	if ref.AttemptID == "" {
		return errors.New("attempt id is required")
	}
	if ref.Generation == 0 {
		return errors.New("generation must be positive")
	}
	if requireLease && ref.LeaseToken == "" {
		return errors.New("lease token is required")
	}
	return nil
}

func validateTaskContextHash(context TaskContext, expected string) error {
	if !isSHA256(expected) {
		return errors.New("task context hash must be 64 hexadecimal characters")
	}
	actual, err := HashTaskContext(context)
	if err != nil {
		return err
	}
	if actual != expected {
		return errors.New("task context hash mismatch")
	}
	return nil
}

func isSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

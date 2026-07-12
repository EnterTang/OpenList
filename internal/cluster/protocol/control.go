package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

const KeyAgreementX25519 = "X25519"

type NodeKeyAgreement struct {
	Algorithm string `json:"algorithm"`
	KeyID     string `json:"key_id"`
	PublicKey string `json:"public_key"`
}

type WorkerDesiredConfig struct {
	ProviderTempRoots   map[string]string        `json:"provider_temp_roots,omitempty"`
	TargetBindings      map[string]TargetBinding `json:"target_bindings,omitempty"`
	DownloadConcurrency int                      `json:"download_concurrency,omitempty"`
	UploadConcurrency   int                      `json:"upload_concurrency,omitempty"`
}

type TargetBinding struct {
	MountPath      string `json:"mount_path"`
	MaxConcurrency int    `json:"max_concurrency,omitempty"`
}

type ConfigObserved struct {
	Revision     uint64    `json:"revision"`
	DesiredHash  string    `json:"desired_hash"`
	ObservedHash string    `json:"observed_hash,omitempty"`
	Status       string    `json:"status"`
	ErrorCode    string    `json:"error_code,omitempty"`
	Error        string    `json:"error,omitempty"`
	ObservedAt   time.Time `json:"observed_at"`
}

type StorageApplyResult struct {
	Revision    uint64    `json:"revision"`
	DesiredHash string    `json:"desired_hash"`
	NodeMountID string    `json:"node_mount_id,omitempty"`
	StorageID   uint      `json:"storage_id,omitempty"`
	MountPath   string    `json:"mount_path"`
	Status      string    `json:"status"`
	ErrorCode   string    `json:"error_code,omitempty"`
	Error       string    `json:"error,omitempty"`
	AppliedAt   time.Time `json:"applied_at"`
}

func HashWorkerDesiredConfig(config WorkerDesiredConfig) (string, error) {
	raw, err := json.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("marshal worker desired config: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (c WorkerDesiredConfig) Validate() error {
	if c.DownloadConcurrency < 0 || c.UploadConcurrency < 0 {
		return errors.New("worker concurrency limits must not be negative")
	}
	for provider, root := range c.ProviderTempRoots {
		if strings.TrimSpace(provider) == "" {
			return errors.New("provider temp root key is required")
		}
		if err := validateControlMountPath(root, "provider temp root"); err != nil {
			return fmt.Errorf("provider %q: %w", provider, err)
		}
	}
	for name, binding := range c.TargetBindings {
		if strings.TrimSpace(name) == "" {
			return errors.New("target binding name is required")
		}
		if err := validateControlMountPath(binding.MountPath, "target mount path"); err != nil {
			return fmt.Errorf("target binding %q: %w", name, err)
		}
		if binding.MaxConcurrency < 0 {
			return fmt.Errorf("target binding %q concurrency must not be negative", name)
		}
	}
	return nil
}

func (c ConfigApply) DecodeDesiredConfig() (WorkerDesiredConfig, error) {
	if c.Revision == 0 {
		return WorkerDesiredConfig{}, errors.New("config revision must be positive")
	}
	if c.DesiredHash == "" {
		return WorkerDesiredConfig{}, errors.New("config desired hash is required")
	}
	var desired WorkerDesiredConfig
	if c.DesiredConfig != nil {
		desired = *c.DesiredConfig
	} else if strings.TrimSpace(c.ConfigJSON) != "" {
		if err := json.Unmarshal([]byte(c.ConfigJSON), &desired); err != nil {
			return WorkerDesiredConfig{}, fmt.Errorf("decode desired config: %w", err)
		}
	} else {
		return WorkerDesiredConfig{}, errors.New("desired config is required")
	}
	if err := desired.Validate(); err != nil {
		return WorkerDesiredConfig{}, err
	}
	hash, err := HashWorkerDesiredConfig(desired)
	if err != nil {
		return WorkerDesiredConfig{}, err
	}
	if !strings.EqualFold(hash, c.DesiredHash) {
		return WorkerDesiredConfig{}, errors.New("config desired hash mismatch")
	}
	return desired, nil
}

func (s StorageApply) Validate() error {
	if s.Revision == 0 {
		return errors.New("storage revision must be positive")
	}
	if strings.TrimSpace(s.DesiredHash) == "" {
		return errors.New("storage desired hash is required")
	}
	if strings.TrimSpace(s.Driver) == "" {
		return errors.New("storage driver is required")
	}
	if err := validateControlMountPath(s.MountPath, "storage mount path"); err != nil {
		return err
	}
	operation := strings.ToLower(strings.TrimSpace(s.Operation))
	if operation != "" && operation != "upsert" && operation != "create" && operation != "update" {
		return fmt.Errorf("unsupported storage operation %q", s.Operation)
	}
	if strings.TrimSpace(s.SecretEnvelope) == "" {
		return errors.New("storage secret envelope is required")
	}
	return nil
}

func StorageApplyAAD(nodeID string, apply StorageApply) []byte {
	return []byte(strings.Join([]string{
		"openlist-cluster-storage-v1",
		strings.TrimSpace(nodeID),
		fmt.Sprint(apply.Revision),
		strings.TrimSpace(apply.DesiredHash),
		path.Clean(apply.MountPath),
		strings.TrimSpace(apply.Driver),
	}, "\x00"))
}

func validateControlMountPath(value, label string) error {
	value = strings.TrimSpace(value)
	if value == "" || value == "/" || !path.IsAbs(value) {
		return fmt.Errorf("%s must be an absolute non-root OpenList path", label)
	}
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned == "/" || strings.Contains(value, `\\`) {
		return fmt.Errorf("%s is invalid", label)
	}
	return nil
}

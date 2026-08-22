package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
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
	QBClients           []QBClientConfig         `json:"qb_clients,omitempty"`
	MoviePilotRoutes    []MoviePilotRoute        `json:"moviepilot_routes,omitempty"`
	Staging             StagingConfig            `json:"staging,omitempty"`
}

type QBClientConfig struct {
	ID           string          `json:"id"`
	WebUIURL     string          `json:"webui_url"`
	SecretRef    string          `json:"secret_ref"`
	PathMappings []QBPathMapping `json:"path_mappings,omitempty"`
}

type QBPathMapping struct {
	QBPath     string `json:"qb_path"`
	WorkerPath string `json:"worker_path"`
}

type MoviePilotRoute struct {
	BridgeInstanceID string `json:"bridge_instance_id"`
	Downloader       string `json:"downloader"`
	QBClientID       string `json:"qb_client_id"`
}

type StagingConfig struct {
	Root                 string   `json:"root,omitempty"`
	MaxUploadConcurrency int      `json:"max_upload_concurrency,omitempty"`
	MaxFileBytes         int64    `json:"max_file_bytes,omitempty"`
	ExtensionWhitelist   []string `json:"extension_whitelist,omitempty"`
	AntiHashEnabled      bool     `json:"antihash_enabled"`
	ISORenameEnabled     bool     `json:"iso_rename_enabled"`
}

const maxMoviePilotStagingFileBytes int64 = 150 * 1024 * 1024 * 1024

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
	if c.Staging.MaxUploadConcurrency < 0 || c.Staging.MaxUploadConcurrency > 2 {
		return errors.New("MoviePilot staging upload concurrency must be between 1 and 2")
	}
	if c.Staging.MaxFileBytes < 0 || c.Staging.MaxFileBytes > maxMoviePilotStagingFileBytes {
		return fmt.Errorf("MoviePilot staging max file size must not exceed %d bytes", maxMoviePilotStagingFileBytes)
	}
	if root := strings.TrimSpace(c.Staging.Root); root != "" {
		if err := validateControlMountPath(root, "MoviePilot staging root"); err != nil {
			return err
		}
	}
	for _, extension := range c.Staging.ExtensionWhitelist {
		extension = strings.TrimSpace(extension)
		if extension == "" || !strings.HasPrefix(extension, ".") || strings.ContainsAny(extension, `/\\`) {
			return fmt.Errorf("MoviePilot staging extension %q is invalid", extension)
		}
	}
	clients := make(map[string]QBClientConfig, len(c.QBClients))
	for _, client := range c.QBClients {
		id := strings.TrimSpace(client.ID)
		if id == "" {
			return errors.New("qB client id is required")
		}
		clientKey := strings.ToLower(id)
		if _, exists := clients[clientKey]; exists {
			return fmt.Errorf("qB client %q is duplicated", id)
		}
		clients[clientKey] = client
		if err := validateQBWebUIURL(client.WebUIURL); err != nil {
			return fmt.Errorf("qB client %q: %w", id, err)
		}
		if len(client.PathMappings) == 0 {
			return fmt.Errorf("qB client %q requires at least one path mapping", id)
		}
		if err := validateLocalSecretRef(client.SecretRef); err != nil {
			return fmt.Errorf("qB client %q: %w", id, err)
		}
		seenQBPaths := make(map[string]struct{}, len(client.PathMappings))
		for _, mapping := range client.PathMappings {
			if err := validateControlMountPath(mapping.QBPath, "qB path mapping source"); err != nil {
				return fmt.Errorf("qB client %q: %w", id, err)
			}
			if err := validateControlMountPath(mapping.WorkerPath, "qB path mapping worker path"); err != nil {
				return fmt.Errorf("qB client %q: %w", id, err)
			}
			qbPath := path.Clean(strings.TrimSpace(mapping.QBPath))
			if _, exists := seenQBPaths[qbPath]; exists {
				return fmt.Errorf("qB client %q path mapping %q is duplicated", id, qbPath)
			}
			seenQBPaths[qbPath] = struct{}{}
		}
	}
	routes := make(map[string]struct{}, len(c.MoviePilotRoutes))
	for _, route := range c.MoviePilotRoutes {
		bridgeID := strings.TrimSpace(route.BridgeInstanceID)
		downloader := strings.TrimSpace(route.Downloader)
		clientID := strings.TrimSpace(route.QBClientID)
		if bridgeID == "" || downloader == "" || clientID == "" {
			return errors.New("MoviePilot route bridge_instance_id, downloader, and qb_client_id are required")
		}
		if _, exists := clients[strings.ToLower(clientID)]; !exists {
			return fmt.Errorf("MoviePilot route references unknown qB client %q", clientID)
		}
		key := strings.ToLower(bridgeID) + "\x00" + strings.ToLower(downloader)
		if _, exists := routes[key]; exists {
			return fmt.Errorf("MoviePilot route %q/%q is duplicated", bridgeID, downloader)
		}
		routes[key] = struct{}{}
	}
	return nil
}

func (c WorkerDesiredConfig) ResolveMoviePilotRoute(bridgeInstanceID, downloader string) (MoviePilotRoute, bool) {
	bridgeInstanceID = strings.TrimSpace(bridgeInstanceID)
	downloader = strings.TrimSpace(downloader)
	for _, route := range c.MoviePilotRoutes {
		if strings.EqualFold(strings.TrimSpace(route.BridgeInstanceID), bridgeInstanceID) && strings.EqualFold(strings.TrimSpace(route.Downloader), downloader) {
			return route, true
		}
	}
	return MoviePilotRoute{}, false
}

func (c WorkerDesiredConfig) QBClient(id string) (QBClientConfig, bool) {
	for _, client := range c.QBClients {
		if strings.EqualFold(strings.TrimSpace(client.ID), strings.TrimSpace(id)) {
			return client, true
		}
	}
	return QBClientConfig{}, false
}

func validateQBWebUIURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (!strings.EqualFold(u.Scheme, "http") && !strings.EqualFold(u.Scheme, "https")) || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("webui_url must be a local HTTP or HTTPS URL")
	}
	host := u.Hostname()
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("webui_url must point to a loopback address")
	}
	return nil
}

func validateLocalSecretRef(raw string) error {
	value := strings.TrimSpace(raw)
	if value == "" {
		return errors.New("qB client secret_ref is required")
	}
	if strings.ContainsAny(value, "\r\n") || strings.Contains(value, "://") {
		return errors.New("qB client secret_ref must be a local reference")
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

func QBSecretApplyAAD(nodeID string, apply ConfigApply, clientID string) []byte {
	return []byte(strings.Join([]string{
		"openlist-cluster-qb-v1",
		strings.TrimSpace(nodeID),
		fmt.Sprint(apply.Revision),
		strings.TrimSpace(apply.DesiredHash),
		strings.TrimSpace(clientID),
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

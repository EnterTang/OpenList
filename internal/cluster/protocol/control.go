package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	Root                             string `json:"root,omitempty"`
	MaxUploadConcurrency             int    `json:"max_upload_concurrency,omitempty"`
	StagingMaxFileSizeGB             int64  `json:"staging_max_file_size_gb,omitempty"`
	StagingSafetyReserveGB           int64  `json:"staging_safety_reserve_gb,omitempty"`
	StagingPauseDownloadWatermarkGB  int64  `json:"staging_pause_download_watermark_gb,omitempty"`
	StagingResumeDownloadWatermarkGB int64  `json:"staging_resume_download_watermark_gb,omitempty"`
	// Download disk watermarks are configured in GB fields using 1024^3 bytes
	// per unit. They apply to every configured qB client; zero disables them.
	DownloadDiskPauseWatermarkGB  int64    `json:"download_disk_pause_watermark_gb,omitempty"`
	DownloadDiskResumeWatermarkGB int64    `json:"download_disk_resume_watermark_gb,omitempty"`
	ExtensionWhitelist            []string `json:"extension_whitelist,omitempty"`
	AntiHashEnabled               bool     `json:"antihash_enabled"`
	ISORenameEnabled              bool     `json:"iso_rename_enabled"`
}

const maxMoviePilotStagingFileGB int64 = 150
const bytesPerGB int64 = 1024 * 1024 * 1024

// UnmarshalJSON keeps stored Worker configurations readable after the
// capacity settings were renamed and converted from bytes to GB. New config
// writes use only the canonical GB fields above.
func (c *StagingConfig) UnmarshalJSON(data []byte) error {
	type stagingConfig StagingConfig
	legacy := struct {
		*stagingConfig
		MaxFileBytes                     *int64 `json:"max_file_bytes"`
		SafetyReserveBytes               *int64 `json:"safety_reserve_bytes"`
		PauseDownloadLowWatermarkBytes   *int64 `json:"pause_download_low_watermark_bytes"`
		ResumeDownloadHighWatermarkBytes *int64 `json:"resume_download_high_watermark_bytes"`
		DownloadDiskLowWatermarkGB       *int64 `json:"download_disk_low_watermark_gb"`
		DownloadDiskHighWatermarkGB      *int64 `json:"download_disk_high_watermark_gb"`
	}{stagingConfig: (*stagingConfig)(c)}
	if err := json.Unmarshal(data, &legacy); err != nil {
		return err
	}
	if c.StagingMaxFileSizeGB == 0 && legacy.MaxFileBytes != nil {
		c.StagingMaxFileSizeGB = bytesToGB(*legacy.MaxFileBytes)
	}
	if c.StagingSafetyReserveGB == 0 && legacy.SafetyReserveBytes != nil {
		c.StagingSafetyReserveGB = bytesToGB(*legacy.SafetyReserveBytes)
	}
	if c.StagingPauseDownloadWatermarkGB == 0 && legacy.PauseDownloadLowWatermarkBytes != nil {
		c.StagingPauseDownloadWatermarkGB = bytesToGB(*legacy.PauseDownloadLowWatermarkBytes)
	}
	if c.StagingResumeDownloadWatermarkGB == 0 && legacy.ResumeDownloadHighWatermarkBytes != nil {
		c.StagingResumeDownloadWatermarkGB = bytesToGB(*legacy.ResumeDownloadHighWatermarkBytes)
	}
	if c.DownloadDiskPauseWatermarkGB == 0 && legacy.DownloadDiskLowWatermarkGB != nil {
		c.DownloadDiskPauseWatermarkGB = *legacy.DownloadDiskLowWatermarkGB
	}
	if c.DownloadDiskResumeWatermarkGB == 0 && legacy.DownloadDiskHighWatermarkGB != nil {
		c.DownloadDiskResumeWatermarkGB = *legacy.DownloadDiskHighWatermarkGB
	}
	return nil
}

func bytesToGB(value int64) int64 {
	if value <= 0 {
		return value
	}
	result := value / bytesPerGB
	if value%bytesPerGB != 0 {
		result++
	}
	return result
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
	if c.Staging.MaxUploadConcurrency < 0 || c.Staging.MaxUploadConcurrency > 2 {
		return errors.New("MoviePilot staging upload concurrency must be between 1 and 2")
	}
	if c.Staging.StagingMaxFileSizeGB < 0 || c.Staging.StagingMaxFileSizeGB > maxMoviePilotStagingFileGB {
		return fmt.Errorf("MoviePilot staging max file size must not exceed %d GB", maxMoviePilotStagingFileGB)
	}
	if c.Staging.StagingSafetyReserveGB < 0 || c.Staging.StagingPauseDownloadWatermarkGB < 0 || c.Staging.StagingResumeDownloadWatermarkGB < 0 || c.Staging.DownloadDiskPauseWatermarkGB < 0 || c.Staging.DownloadDiskResumeWatermarkGB < 0 {
		return errors.New("MoviePilot staging and download disk capacity values must not be negative")
	}
	low := c.Staging.StagingPauseDownloadWatermarkGB
	high := c.Staging.StagingResumeDownloadWatermarkGB
	if (low == 0) != (high == 0) {
		return errors.New("MoviePilot staging pause and resume watermarks must be configured together")
	}
	if low > 0 && high < low {
		return errors.New("MoviePilot staging resume watermark must not be below pause watermark")
	}
	downloadLow := c.Staging.DownloadDiskPauseWatermarkGB
	downloadHigh := c.Staging.DownloadDiskResumeWatermarkGB
	if (downloadLow == 0) != (downloadHigh == 0) {
		return errors.New("MoviePilot download disk low and high watermarks must be configured together")
	}
	if downloadLow > 0 && downloadHigh <= downloadLow {
		return errors.New("MoviePilot download disk high watermark must be greater than low watermark")
	}
	if downloadHigh > 0 && downloadHigh > (int64(^uint64(0)>>1))/bytesPerGB {
		return errors.New("MoviePilot download disk watermark is too large")
	}
	if root := strings.TrimSpace(c.Staging.Root); root != "" {
		if err := validateWorkerLocalPath(root, "MoviePilot staging root"); err != nil {
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
			if err := validateQBPath(mapping.QBPath, "qB path mapping source"); err != nil {
				return fmt.Errorf("qB client %q: %w", id, err)
			}
			if err := validateWorkerLocalPath(mapping.WorkerPath, "qB path mapping worker path"); err != nil {
				return fmt.Errorf("qB client %q: %w", id, err)
			}
			qbPath := normalizePortablePath(mapping.QBPath)
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
	if err != nil || u.Opaque != "" || (!strings.EqualFold(u.Scheme, "http") && !strings.EqualFold(u.Scheme, "https")) || u.Host == "" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("webui_url must be an HTTP or HTTPS URL without credentials, query, or fragment")
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

// validateQBPath validates a path reported by qB. qB may run on Windows or
// Unix, so its path syntax must be accepted independently of the Coordinator's
// operating system.
func validateQBPath(value, label string) error {
	return validatePortableAbsolutePath(value, label)
}

// validateWorkerLocalPath validates a path consumed by the selected Worker.
// The Coordinator can run on a different OS, so this accepts both POSIX paths
// and native Windows drive/UNC paths.
func validateWorkerLocalPath(value, label string) error {
	return validatePortableAbsolutePath(value, label)
}

func validatePortableAbsolutePath(value, label string) error {
	value = strings.TrimSpace(value)
	normalized := normalizePortablePath(value)
	if value == "" || !isPortableAbsolutePath(normalized) || isPortableRoot(normalized) {
		return fmt.Errorf("%s must be an absolute non-root path", label)
	}
	return nil
}

func normalizePortablePath(value string) string {
	return path.Clean(strings.ReplaceAll(strings.TrimSpace(value), `\`, "/"))
}

func isPortableAbsolutePath(value string) bool {
	return path.IsAbs(value) || isWindowsDriveAbsolutePath(value)
}

func isWindowsDriveAbsolutePath(value string) bool {
	return len(value) >= 3 &&
		((value[0] >= 'a' && value[0] <= 'z') || (value[0] >= 'A' && value[0] <= 'Z')) &&
		value[1] == ':' && value[2] == '/'
}

func isPortableRoot(value string) bool {
	if value == "/" {
		return true
	}
	return len(value) == 2 &&
		((value[0] >= 'a' && value[0] <= 'z') || (value[0] >= 'A' && value[0] <= 'Z')) &&
		value[1] == ':'
}

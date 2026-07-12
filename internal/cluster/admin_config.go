package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
)

type AdminConfig struct {
	Role                      string           `json:"role"`
	ActiveRole                string           `json:"active_role"`
	NodeID                    string           `json:"node_id"`
	CoordinatorURL            string           `json:"coordinator_url"`
	EnrollmentTokenConfigured bool             `json:"enrollment_token_configured"`
	WorkerKeyFile             string           `json:"worker_key_file"`
	WebSocketPath             string           `json:"websocket_path"`
	ETFRootPath               string           `json:"etf_root_path"`
	TargetBaseURL             string           `json:"target_base_url"`
	TargetAPITokenConfigured  bool             `json:"target_api_token_configured"`
	TargetSupportsIdempotency bool             `json:"target_supports_idempotency"`
	Redis                     AdminRedisConfig `json:"redis"`
	RestartRequired           bool             `json:"restart_required"`
}

type AdminRedisConfig struct {
	Address            string `json:"address"`
	Username           string `json:"username"`
	PasswordConfigured bool   `json:"password_configured"`
	DB                 int    `json:"db"`
	RequireAOF         bool   `json:"require_aof"`
}

// AdminConfigUpdate deliberately keeps secrets write-only. Empty secret values
// preserve the existing value; callers must set the matching clear flag to
// remove a persisted secret.
type AdminConfigUpdate struct {
	Role                      string                 `json:"role"`
	NodeID                    string                 `json:"node_id"`
	CoordinatorURL            string                 `json:"coordinator_url"`
	EnrollmentToken           string                 `json:"enrollment_token"`
	ClearEnrollmentToken      bool                   `json:"clear_enrollment_token"`
	WorkerKeyFile             string                 `json:"worker_key_file"`
	WebSocketPath             string                 `json:"websocket_path"`
	ETFRootPath               string                 `json:"etf_root_path"`
	TargetBaseURL             string                 `json:"target_base_url"`
	TargetAPIToken            string                 `json:"target_api_token"`
	ClearTargetAPIToken       bool                   `json:"clear_target_api_token"`
	TargetSupportsIdempotency bool                   `json:"target_supports_idempotency"`
	Redis                     AdminRedisConfigUpdate `json:"redis"`
}

type AdminRedisConfigUpdate struct {
	Address       string `json:"address"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	ClearPassword bool   `json:"clear_password"`
	DB            int    `json:"db"`
	RequireAOF    bool   `json:"require_aof"`
}

var adminConfigMu sync.Mutex

func GetAdminConfig() (AdminConfig, error) {
	adminConfigMu.Lock()
	defer adminConfigMu.Unlock()

	cfg, err := readPersistedClusterConfig()
	if err != nil {
		return AdminConfig{}, err
	}
	return publicAdminConfig(cfg, false), nil
}

func SaveAdminConfig(req AdminConfigUpdate) (AdminConfig, error) {
	adminConfigMu.Lock()
	defer adminConfigMu.Unlock()

	cfg, root, clusterRaw, _, err := readPersistedClusterConfigDocument()
	if err != nil {
		return AdminConfig{}, err
	}
	applyAdminConfigUpdate(&cfg, req)
	if err := validateAdminConfig(cfg); err != nil {
		return AdminConfig{}, err
	}
	if err := updateClusterDocument(clusterRaw, cfg); err != nil {
		return AdminConfig{}, err
	}
	clusterBody, err := json.Marshal(clusterRaw)
	if err != nil {
		return AdminConfig{}, fmt.Errorf("marshal cluster config: %w", err)
	}
	root["cluster"] = clusterBody
	if err := writeConfigDocument(root); err != nil {
		return AdminConfig{}, err
	}
	return publicAdminConfig(cfg, true), nil
}

func applyAdminConfigUpdate(cfg *conf.Cluster, req AdminConfigUpdate) {
	cfg.Role = strings.ToLower(strings.TrimSpace(req.Role))
	cfg.NodeID = strings.TrimSpace(req.NodeID)
	cfg.CoordinatorURL = strings.TrimSpace(req.CoordinatorURL)
	cfg.WorkerKeyFile = strings.TrimSpace(req.WorkerKeyFile)
	cfg.WebSocketPath = normalizeWebSocketPath(req.WebSocketPath)
	cfg.ETFRootPath = strings.TrimSpace(req.ETFRootPath)
	cfg.TargetBaseURL = strings.TrimRight(strings.TrimSpace(req.TargetBaseURL), "/")
	cfg.TargetSupportsIdempotency = req.TargetSupportsIdempotency
	cfg.Redis.Address = strings.TrimSpace(req.Redis.Address)
	cfg.Redis.Username = strings.TrimSpace(req.Redis.Username)
	cfg.Redis.DB = req.Redis.DB
	cfg.Redis.RequireAOF = req.Redis.RequireAOF

	if req.ClearEnrollmentToken {
		cfg.EnrollmentToken = ""
	} else if token := strings.TrimSpace(req.EnrollmentToken); token != "" {
		cfg.EnrollmentToken = token
	}
	if req.ClearTargetAPIToken {
		cfg.TargetAPIToken = ""
	} else if token := strings.TrimSpace(req.TargetAPIToken); token != "" {
		cfg.TargetAPIToken = token
	}
	if req.Redis.ClearPassword {
		cfg.Redis.Password = ""
	} else if password := strings.TrimSpace(req.Redis.Password); password != "" {
		cfg.Redis.Password = password
	}
}

func validateAdminConfig(cfg conf.Cluster) error {
	roleText := strings.ToLower(strings.TrimSpace(cfg.Role))
	switch Role(roleText) {
	case RoleStandalone, RoleCoordinator, RoleWorker, RoleHybrid:
	default:
		return fmt.Errorf("unsupported cluster role %q", cfg.Role)
	}
	if cfg.WebSocketPath == "" || !strings.HasPrefix(cfg.WebSocketPath, "/") {
		return errors.New("cluster websocket path must start with /")
	}
	if cfg.Redis.DB < 0 {
		return errors.New("cluster Redis DB must not be negative")
	}
	if cfg.TargetBaseURL != "" {
		if err := validateHTTPURL("target service", cfg.TargetBaseURL, false); err != nil {
			return err
		}
	}
	role := Role(roleText)
	if role.RunsCoordinator() {
		if strings.TrimSpace(cfg.EnrollmentToken) == "" {
			return errors.New("cluster enrollment token is required for coordinator and hybrid roles")
		}
		if strings.TrimSpace(cfg.ETFRootPath) == "" {
			return errors.New("cluster ETF root path is required for coordinator and hybrid roles")
		}
	}
	if role.RunsWorker() {
		if strings.TrimSpace(cfg.NodeID) == "" {
			return errors.New("cluster node ID is required for worker and hybrid roles")
		}
		if strings.TrimSpace(cfg.EnrollmentToken) == "" {
			return errors.New("cluster enrollment token is required for worker and hybrid roles")
		}
		if role == RoleWorker {
			coordinatorURL := cfg.CoordinatorURL
			if coordinatorURL == "" {
				return errors.New("cluster coordinator URL is required for worker role")
			}
			if err := validateHTTPURL("coordinator", coordinatorURL, true); err != nil {
				return err
			}
		}
		if strings.TrimSpace(cfg.Redis.Address) == "" {
			return errors.New("cluster Redis address is required for worker and hybrid roles")
		}
	}
	return nil
}

func validateHTTPURL(name, raw string, allowWebSocket bool) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("invalid %s URL", name)
	}
	allowed := parsed.Scheme == "http" || parsed.Scheme == "https"
	if allowWebSocket {
		allowed = allowed || parsed.Scheme == "ws" || parsed.Scheme == "wss"
	}
	if !allowed {
		return fmt.Errorf("unsupported %s URL scheme %q", name, parsed.Scheme)
	}
	if allowWebSocket && parsed.Scheme != "https" && parsed.Scheme != "wss" && !isLoopbackHost(parsed.Hostname()) {
		return errors.New("remote cluster coordinator connections must use https or wss")
	}
	return nil
}

func normalizeWebSocketPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "/api/cluster/ws"
	}
	return value
}

func publicAdminConfig(cfg conf.Cluster, restartRequired bool) AdminConfig {
	return AdminConfig{
		Role:                      string(ParseRole(cfg.Role)),
		ActiveRole:                string(ParseRole(conf.Conf.Cluster.Role)),
		NodeID:                    cfg.NodeID,
		CoordinatorURL:            cfg.CoordinatorURL,
		EnrollmentTokenConfigured: cfg.EnrollmentToken != "",
		WorkerKeyFile:             cfg.WorkerKeyFile,
		WebSocketPath:             cfg.WebSocketPath,
		ETFRootPath:               cfg.ETFRootPath,
		TargetBaseURL:             cfg.TargetBaseURL,
		TargetAPITokenConfigured:  cfg.TargetAPIToken != "",
		TargetSupportsIdempotency: cfg.TargetSupportsIdempotency,
		Redis: AdminRedisConfig{
			Address:            cfg.Redis.Address,
			Username:           cfg.Redis.Username,
			PasswordConfigured: cfg.Redis.Password != "",
			DB:                 cfg.Redis.DB,
			RequireAOF:         cfg.Redis.RequireAOF,
		},
		RestartRequired: restartRequired,
	}
}

func readPersistedClusterConfig() (conf.Cluster, error) {
	cfg, _, _, _, err := readPersistedClusterConfigDocument()
	return cfg, err
}

func readPersistedClusterConfigDocument() (conf.Cluster, map[string]json.RawMessage, map[string]json.RawMessage, os.FileMode, error) {
	if strings.TrimSpace(conf.ConfigPath) == "" {
		return conf.Cluster{}, nil, nil, 0, errors.New("config path is not initialized")
	}
	body, err := os.ReadFile(conf.ConfigPath)
	if err != nil {
		return conf.Cluster{}, nil, nil, 0, fmt.Errorf("read config file: %w", err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil {
		return conf.Cluster{}, nil, nil, 0, fmt.Errorf("parse config file: %w", err)
	}
	clusterRaw := make(map[string]json.RawMessage)
	if raw := root["cluster"]; len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &clusterRaw); err != nil {
			return conf.Cluster{}, nil, nil, 0, fmt.Errorf("parse cluster config: %w", err)
		}
	}
	cfg := conf.DefaultConfig(filepath.Dir(conf.ConfigPath)).Cluster
	if raw := root["cluster"]; len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return conf.Cluster{}, nil, nil, 0, fmt.Errorf("parse cluster config: %w", err)
		}
	}
	mode := os.FileMode(0o600)
	if info, statErr := os.Stat(conf.ConfigPath); statErr == nil {
		mode = info.Mode().Perm()
	}
	return cfg, root, clusterRaw, mode, nil
}

func updateClusterDocument(raw map[string]json.RawMessage, cfg conf.Cluster) error {
	body, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal cluster config: %w", err)
	}
	var known map[string]json.RawMessage
	if err := json.Unmarshal(body, &known); err != nil {
		return fmt.Errorf("prepare cluster config: %w", err)
	}
	for key, value := range known {
		raw[key] = value
	}
	return nil
}

func writeConfigDocument(root map[string]json.RawMessage) error {
	body, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config file: %w", err)
	}
	dir := filepath.Dir(conf.ConfigPath)
	tmp, err := os.CreateTemp(dir, ".config-*.json")
	if err != nil {
		return fmt.Errorf("create temporary config file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	// Cluster configuration may contain enrollment credentials, target API
	// tokens, and Redis passwords. Never preserve a legacy group/world-readable
	// mode when the admin UI writes these values.
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("set temporary config permissions: %w", err)
	}
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temporary config file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temporary config file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary config file: %w", err)
	}
	if err := os.Rename(tmpName, conf.ConfigPath); err != nil {
		return fmt.Errorf("replace config file: %w", err)
	}
	return nil
}

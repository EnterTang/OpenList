package cluster

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
)

func setupAdminConfigTest(t *testing.T, clusterConfig conf.Cluster) string {
	t.Helper()
	oldConf, oldPath := conf.Conf, conf.ConfigPath
	t.Cleanup(func() {
		conf.Conf = oldConf
		conf.ConfigPath = oldPath
	})
	dir := t.TempDir()
	conf.ConfigPath = filepath.Join(dir, "config.json")
	conf.Conf = conf.DefaultConfig(dir)
	conf.Conf.Cluster = clusterConfig
	body, err := json.Marshal(map[string]any{
		"cluster":           clusterConfig,
		"unknown_top_level": map[string]any{"keep": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(conf.ConfigPath, body, 0o640); err != nil {
		t.Fatal(err)
	}
	return conf.ConfigPath
}

func TestGetAdminConfigRedactsSecrets(t *testing.T) {
	cfg := conf.DefaultConfig(t.TempDir()).Cluster
	cfg.EnrollmentToken = "enrollment-secret"
	cfg.TargetAPIToken = "target-secret"
	cfg.Redis.Password = "redis-secret"
	setupAdminConfigTest(t, cfg)

	got, err := GetAdminConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !got.EnrollmentTokenConfigured || !got.TargetAPITokenConfigured || !got.Redis.PasswordConfigured {
		t.Fatalf("secret configured flags were not returned: %+v", got)
	}
	body, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"enrollment-secret", "target-secret", "redis-secret"} {
		if string(body) == secret || containsJSONText(body, secret) {
			t.Fatalf("admin config leaked secret %q: %s", secret, body)
		}
	}
}

func TestSaveAdminConfigPreservesBlankSecretsAndUnknownFields(t *testing.T) {
	cfg := conf.DefaultConfig(t.TempDir()).Cluster
	cfg.Role = string(RoleCoordinator)
	cfg.NodeID = "coordinator-old"
	cfg.EnrollmentToken = "enrollment-secret"
	cfg.ETFRootPath = "/mobile/ETF"
	cfg.TargetAPIToken = "target-secret"
	cfg.Redis.Password = "redis-secret"
	path := setupAdminConfigTest(t, cfg)

	var root map[string]json.RawMessage
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(body, &root); err != nil {
		t.Fatal(err)
	}
	var clusterRaw map[string]json.RawMessage
	if err := json.Unmarshal(root["cluster"], &clusterRaw); err != nil {
		t.Fatal(err)
	}
	clusterRaw["future_field"] = json.RawMessage(`{"enabled":true}`)
	root["cluster"], _ = json.Marshal(clusterRaw)
	body, _ = json.Marshal(root)
	if err := os.WriteFile(path, body, 0o640); err != nil {
		t.Fatal(err)
	}

	got, err := SaveAdminConfig(AdminConfigUpdate{
		Role:          string(RoleCoordinator),
		NodeID:        "coordinator-new",
		WebSocketPath: "/api/cluster/ws",
		ETFRootPath:   "/mobile/ETF",
		TargetBaseURL: "https://target.example/api/v1",
		Redis: AdminRedisConfigUpdate{
			Address:    "127.0.0.1:6379",
			RequireAOF: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.RestartRequired || got.ActiveRole != string(RoleCoordinator) {
		t.Fatalf("unexpected runtime state: %+v", got)
	}

	body, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(body, &root); err != nil {
		t.Fatal(err)
	}
	if _, ok := root["unknown_top_level"]; !ok {
		t.Fatal("save removed unknown top-level config")
	}
	if err := json.Unmarshal(root["cluster"], &clusterRaw); err != nil {
		t.Fatal(err)
	}
	if _, ok := clusterRaw["future_field"]; !ok {
		t.Fatal("save removed unknown cluster config")
	}
	var saved conf.Cluster
	if err := json.Unmarshal(root["cluster"], &saved); err != nil {
		t.Fatal(err)
	}
	if saved.EnrollmentToken != "enrollment-secret" || saved.TargetAPIToken != "target-secret" || saved.Redis.Password != "redis-secret" {
		t.Fatalf("blank secret input did not preserve persisted secrets: %+v", saved)
	}
	if saved.NodeID != "coordinator-new" || saved.TargetBaseURL != "https://target.example/api/v1" {
		t.Fatalf("non-secret fields were not updated: %+v", saved)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("config mode = %o, want 600", info.Mode().Perm())
	}
}

func TestSaveAdminConfigCanClearSecrets(t *testing.T) {
	cfg := conf.DefaultConfig(t.TempDir()).Cluster
	cfg.EnrollmentToken = "enrollment-secret"
	cfg.TargetAPIToken = "target-secret"
	cfg.Redis.Password = "redis-secret"
	setupAdminConfigTest(t, cfg)

	got, err := SaveAdminConfig(AdminConfigUpdate{
		Role:                 string(RoleStandalone),
		WebSocketPath:        "/api/cluster/ws",
		ClearEnrollmentToken: true,
		ClearTargetAPIToken:  true,
		Redis: AdminRedisConfigUpdate{
			Address:       "127.0.0.1:6379",
			ClearPassword: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// conf.Conf represents the still-running process and intentionally remains
	// unchanged until restart, so verify the persisted values directly.
	persisted, err := readPersistedClusterConfig()
	if err != nil {
		t.Fatal(err)
	}
	if persisted.EnrollmentToken != "" || persisted.TargetAPIToken != "" || persisted.Redis.Password != "" {
		t.Fatalf("clear flags did not remove persisted secrets: %+v", persisted)
	}
	if !got.RestartRequired {
		t.Fatal("save must require restart")
	}
}

func TestSaveAdminConfigRejectsInvalidRole(t *testing.T) {
	cfg := conf.DefaultConfig(t.TempDir()).Cluster
	setupAdminConfigTest(t, cfg)
	_, err := SaveAdminConfig(AdminConfigUpdate{Role: "leader", WebSocketPath: "/api/cluster/ws"})
	if err == nil {
		t.Fatal("invalid cluster role was accepted")
	}
}

func containsJSONText(body []byte, value string) bool {
	for i := 0; i+len(value) <= len(body); i++ {
		if string(body[i:i+len(value)]) == value {
			return true
		}
	}
	return false
}

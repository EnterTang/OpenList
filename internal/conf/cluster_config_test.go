package conf

import "testing"

func TestDefaultClusterConfigIsStandalone(t *testing.T) {
	cfg := DefaultConfig(t.TempDir())
	if cfg.Cluster.Role != "standalone" {
		t.Fatalf("default cluster role = %q, want standalone", cfg.Cluster.Role)
	}
	if cfg.Cluster.WebSocketPath == "" {
		t.Fatal("default cluster websocket path must not be empty")
	}
	if cfg.Cluster.Redis.ResultStream == "" || cfg.Cluster.Redis.ConsumerGroup == "" {
		t.Fatal("default cluster redis streams must not be empty")
	}
	if !cfg.Cluster.Redis.RequireAOF {
		t.Fatal("worker result queue must require AOF by default")
	}
}

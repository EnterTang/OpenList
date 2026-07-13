package embeddedredis

import (
	"strings"
	"testing"
)

func TestShouldManage(t *testing.T) {
	tests := []struct {
		name string
		goos string
		opts Options
		want bool
	}{
		{"worker IPv4 loopback", "windows", Options{Role: "worker", Address: "127.0.0.1:6379"}, true},
		{"hybrid localhost", "windows", Options{Role: " HYBRID ", Address: "localhost:6380", Username: "default"}, true},
		{"worker IPv6 loopback", "windows", Options{Role: "Worker", Address: "[::1]:6379"}, true},
		{"remote address", "windows", Options{Role: "worker", Address: "192.0.2.1:6379"}, false},
		{"malformed address", "windows", Options{Role: "worker", Address: "localhost"}, false},
		{"linux", "linux", Options{Role: "worker", Address: "127.0.0.1:6379"}, false},
		{"coordinator", "windows", Options{Role: "coordinator", Address: "127.0.0.1:6379"}, false},
		{"standalone", "windows", Options{Role: "standalone", Address: "127.0.0.1:6379"}, false},
		{"custom username", "windows", Options{Role: "worker", Address: "127.0.0.1:6379", Username: "operator"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ShouldManage(tt.goos, tt.opts); got != tt.want {
				t.Fatalf("ShouldManage(%q, %+v) = %v, want %v", tt.goos, tt.opts, got, tt.want)
			}
		})
	}
}

func TestRenderConfig(t *testing.T) {
	got, err := RenderConfig(6380, `C:\OpenList\redis data`, "secret")
	if err != nil {
		t.Fatalf("RenderConfig() error = %v", err)
	}

	want := "bind 127.0.0.1\n" +
		"protected-mode yes\n" +
		"port 6380\n" +
		"daemonize no\n" +
		`dir "C:/OpenList/redis data"` + "\n" +
		"appendonly yes\n" +
		"appendfsync always\n" +
		"maxmemory-policy noeviction\n" +
		`requirepass "secret"` + "\n"
	if string(got) != want {
		t.Fatalf("RenderConfig() = %q, want %q", got, want)
	}
	if !strings.HasSuffix(string(got), "\n") {
		t.Fatal("RenderConfig() must end with a newline")
	}
}

func TestRenderConfigRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name     string
		port     int
		dataDir  string
		password string
	}{
		{"zero port", 0, "data", "secret"},
		{"port too large", 65536, "data", "secret"},
		{"empty data directory", 6379, "", "secret"},
		{"empty password", 6379, "data", ""},
		{"password CR", 6379, "data", "bad\rpassword"},
		{"password LF", 6379, "data", "bad\npassword"},
		{"password NUL", 6379, "data", "bad\x00password"},
		{"password quote", 6379, "data", `bad"password`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := RenderConfig(tt.port, tt.dataDir, tt.password); err == nil {
				t.Fatal("RenderConfig() error = nil, want error")
			}
		})
	}
}

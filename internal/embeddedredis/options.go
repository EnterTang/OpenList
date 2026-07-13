package embeddedredis

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	Version         = "7.2.14"
	PayloadFilename = "redis-windows.zip"
)

type Options struct {
	Role           string
	Address        string
	Username       string
	Password       string
	DB             int
	DataDir        string
	RequireAOF     bool
	StartupTimeout time.Duration
}

type EffectiveOptions struct {
	Address  string
	Username string
	Password string
	DB       int
}

func ShouldManage(goos string, opts Options) bool {
	if !strings.EqualFold(strings.TrimSpace(goos), "windows") {
		return false
	}

	role := strings.ToLower(strings.TrimSpace(opts.Role))
	if role != "worker" && role != "hybrid" {
		return false
	}

	username := strings.TrimSpace(opts.Username)
	if username != "" && username != "default" {
		return false
	}

	host, port, err := net.SplitHostPort(opts.Address)
	if err != nil || !isValidPort(port) {
		return false
	}
	host = strings.ToLower(host)
	return host == "127.0.0.1" || host == "localhost" || host == "::1"
}

func RenderConfig(port int, dataDir, password string) ([]byte, error) {
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid port %d", port)
	}
	if dataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	if password == "" {
		return nil, fmt.Errorf("password is required")
	}
	if strings.ContainsRune(password, '"') || containsC0Control(password) {
		return nil, fmt.Errorf("password contains an unsupported character")
	}

	normalizedDir := strings.ReplaceAll(dataDir, `\`, "/")
	config := "bind 127.0.0.1\n" +
		"protected-mode yes\n" +
		"port " + strconv.Itoa(port) + "\n" +
		"daemonize no\n" +
		"dir " + strconv.Quote(normalizedDir) + "\n" +
		"appendonly yes\n" +
		"appendfsync always\n" +
		"maxmemory-policy noeviction\n" +
		"requirepass " + strconv.Quote(password) + "\n"
	return []byte(config), nil
}

func containsC0Control(value string) bool {
	for i := range value {
		if value[i] < 0x20 {
			return true
		}
	}
	return false
}

func isValidPort(port string) bool {
	if port == "" {
		return false
	}
	for i := range port {
		if port[i] < '0' || port[i] > '9' {
			return false
		}
	}
	n, err := strconv.Atoi(port)
	return err == nil && n >= 1 && n <= 65535
}

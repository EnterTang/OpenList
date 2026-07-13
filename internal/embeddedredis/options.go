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

	username := opts.Username
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
	normalizedDir := strings.ReplaceAll(dataDir, `\`, "/")
	quotedDir, err := quoteRedisConfigValue(normalizedDir)
	if err != nil {
		return nil, fmt.Errorf("invalid data directory: %w", err)
	}
	if password == "" {
		return nil, fmt.Errorf("password is required")
	}
	if strings.ContainsRune(password, '"') {
		return nil, fmt.Errorf("password contains an unsupported character")
	}
	quotedPassword, err := quoteRedisConfigValue(password)
	if err != nil {
		return nil, fmt.Errorf("invalid password: %w", err)
	}

	config := "bind 127.0.0.1\n" +
		"protected-mode yes\n" +
		"port " + strconv.Itoa(port) + "\n" +
		"daemonize no\n" +
		"dir " + quotedDir + "\n" +
		"appendonly yes\n" +
		"appendfsync always\n" +
		"maxmemory-policy noeviction\n" +
		"requirepass " + quotedPassword + "\n"
	return []byte(config), nil
}

func quoteRedisConfigValue(value string) (string, error) {
	var quoted strings.Builder
	quoted.Grow(len(value) + 2)
	quoted.WriteByte('"')
	for i := 0; i < len(value); i++ {
		if value[i] < 0x20 || value[i] == 0x7f {
			return "", fmt.Errorf("value contains an unsupported control character")
		}
		switch value[i] {
		case '\\', '"':
			quoted.WriteByte('\\')
		}
		quoted.WriteByte(value[i])
	}
	quoted.WriteByte('"')
	return quoted.String(), nil
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

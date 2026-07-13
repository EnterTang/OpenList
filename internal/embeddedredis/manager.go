package embeddedredis

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	secretFilename        = "redis.auth"
	defaultStartupTimeout = 15 * time.Second
	probeTimeout          = 750 * time.Millisecond
)

type managedProcess interface {
	Wait() error
	Kill() error
}

type commandProcess struct{ cmd *exec.Cmd }

func (p commandProcess) Wait() error { return p.cmd.Wait() }
func (p commandProcess) Kill() error {
	if p.cmd.Process == nil {
		return nil
	}
	return p.cmd.Process.Kill()
}

type managerDependencies struct {
	goos     string
	probe    func(context.Context, EffectiveOptions, bool) error
	occupied func(context.Context, string) (bool, error)
	payload  func() ([]byte, error)
	extract  func(string, []byte) (ExtractedRuntime, error)
	start    func(*exec.Cmd) (managedProcess, error)
	shutdown func(context.Context, EffectiveOptions) error
}

func (d managerDependencies) withDefaults() managerDependencies {
	if d.goos == "" {
		d.goos = runtime.GOOS
	}
	if d.probe == nil {
		d.probe = probeRedis
	}
	if d.occupied == nil {
		d.occupied = endpointOccupied
	}
	if d.payload == nil {
		d.payload = EmbeddedPayload
	}
	if d.extract == nil {
		d.extract = ExtractPayload
	}
	if d.start == nil {
		d.start = func(cmd *exec.Cmd) (managedProcess, error) {
			if err := cmd.Start(); err != nil {
				return nil, err
			}
			return commandProcess{cmd: cmd}, nil
		}
	}
	if d.shutdown == nil {
		d.shutdown = shutdownRedis
	}
	return d
}

var currentManagerDeps = (managerDependencies{}).withDefaults()

type Manager struct {
	owned     bool
	process   managedProcess
	effective EffectiveOptions
	shutdown  func(context.Context, EffectiveOptions) error
	exit      chan error
	stopDone  chan struct{}
	stopOnce  sync.Once
	killOnce  sync.Once
	stopErr   error
}

func (m *Manager) Owned() bool { return m != nil && m.owned }

func Prepare(ctx context.Context, opts Options) (*Manager, EffectiveOptions, error) {
	effective := effectiveFromOptions(opts)
	deps := currentManagerDeps
	if !ShouldManage(deps.goos, opts) {
		return nil, effective, nil
	}

	configuredErr := boundedProbe(ctx, deps.probe, effective, opts.RequireAOF)
	if configuredErr == nil {
		return reusedManager(effective, deps.shutdown), effective, nil
	}

	redisDir := filepath.Join(opts.DataDir, "redis")
	secretPath := filepath.Join(redisDir, secretFilename)
	persistedSecret, secretErr := readManagedSecret(secretPath)
	if secretErr != nil && !os.IsNotExist(secretErr) {
		return nil, effective, fmt.Errorf("read managed Redis secret: %w", secretErr)
	}
	if persistedSecret != "" && persistedSecret != effective.Password {
		managedEffective := effective
		managedEffective.Password = persistedSecret
		if err := boundedProbe(ctx, deps.probe, managedEffective, opts.RequireAOF); err == nil {
			return reusedManager(managedEffective, deps.shutdown), managedEffective, nil
		}
	}

	occupied, err := deps.occupied(ctx, opts.Address)
	if err != nil {
		return nil, effective, fmt.Errorf("inspect Redis endpoint: %w", err)
	}
	if occupied {
		return nil, effective, fmt.Errorf("Redis endpoint %s is occupied but incompatible or authentication failed: %w", opts.Address, configuredErr)
	}

	password := opts.Password
	if password == "" {
		if persistedSecret != "" {
			password = persistedSecret
		} else {
			if err := os.MkdirAll(redisDir, 0700); err != nil {
				return nil, effective, fmt.Errorf("create Redis data directory: %w", err)
			}
			password, err = createManagedSecret(secretPath)
			if err != nil {
				return nil, effective, err
			}
		}
	}
	effective.Password = password

	payload, err := deps.payload()
	if err != nil {
		return nil, effectiveFromOptions(opts), fmt.Errorf("load embedded Redis payload: %w", err)
	}
	extracted, err := deps.extract(opts.DataDir, payload)
	if err != nil {
		return nil, effectiveFromOptions(opts), fmt.Errorf("extract embedded Redis payload: %w", err)
	}
	if err := os.MkdirAll(redisDir, 0700); err != nil {
		return nil, effectiveFromOptions(opts), fmt.Errorf("create Redis data directory: %w", err)
	}
	_, portString, err := net.SplitHostPort(opts.Address)
	if err != nil {
		return nil, effectiveFromOptions(opts), fmt.Errorf("parse Redis address: %w", err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		return nil, effectiveFromOptions(opts), fmt.Errorf("parse Redis port: %w", err)
	}
	config, err := RenderConfig(port, redisDir, password)
	if err != nil {
		return nil, effectiveFromOptions(opts), err
	}
	configPath := filepath.Join(extracted.Dir, "redis.conf")
	if err := writeFileAtomic(configPath, config, 0600); err != nil {
		return nil, effectiveFromOptions(opts), fmt.Errorf("write Redis config: %w", err)
	}
	logFile, err := os.OpenFile(filepath.Join(redisDir, "redis.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, effectiveFromOptions(opts), fmt.Errorf("open Redis log: %w", err)
	}
	cmd := exec.Command(extracted.ServerPath, configPath)
	cmd.Dir = extracted.Dir
	cmd.Stdout, cmd.Stderr = logFile, logFile
	configureManagedCommand(cmd)
	process, err := deps.start(cmd)
	if err != nil {
		_ = logFile.Close()
		return nil, effectiveFromOptions(opts), fmt.Errorf("start embedded Redis: %w", err)
	}
	m := &Manager{owned: true, process: process, effective: effective, shutdown: deps.shutdown, exit: make(chan error, 1), stopDone: make(chan struct{})}
	go func() { m.exit <- process.Wait(); close(m.exit); _ = logFile.Close() }()

	timeout := opts.StartupTimeout
	if timeout <= 0 {
		timeout = defaultStartupTimeout
	}
	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := waitUntilReady(readyCtx, deps.probe, effective, opts.RequireAOF, m.exit); err != nil {
		_ = process.Kill()
		<-m.exit
		return nil, effectiveFromOptions(opts), err
	}
	return m, effective, nil
}

func (m *Manager) Stop(ctx context.Context) error {
	if m == nil || !m.owned {
		return nil
	}
	m.stopOnce.Do(func() {
		go func() {
			defer close(m.stopDone)
			shutdownErr := m.shutdown(ctx, m.effective)
			if shutdownErr != nil && !isExpectedShutdownError(shutdownErr) {
				m.kill()
			}
			select {
			case <-m.exit:
				if shutdownErr != nil && !isExpectedShutdownError(shutdownErr) {
					m.stopErr = fmt.Errorf("shutdown embedded Redis: %w", shutdownErr)
				}
			case <-ctx.Done():
				m.kill()
				<-m.exit
				m.stopErr = ctx.Err()
			}
		}()
	})
	select {
	case <-m.stopDone:
		return m.stopErr
	case <-ctx.Done():
		m.kill()
		<-m.stopDone
		return m.stopErr
	}
}

func (m *Manager) kill() { m.killOnce.Do(func() { _ = m.process.Kill() }) }

func reusedManager(effective EffectiveOptions, shutdown func(context.Context, EffectiveOptions) error) *Manager {
	return &Manager{effective: effective, shutdown: shutdown}
}

func effectiveFromOptions(opts Options) EffectiveOptions {
	return EffectiveOptions{Address: opts.Address, Username: opts.Username, Password: opts.Password, DB: opts.DB}
}

func boundedProbe(ctx context.Context, probe func(context.Context, EffectiveOptions, bool) error, effective EffectiveOptions, requireAOF bool) error {
	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	return probe(probeCtx, effective, requireAOF)
}

func probeRedis(ctx context.Context, effective EffectiveOptions, requireAOF bool) error {
	client := redis.NewClient(&redis.Options{Addr: effective.Address, Username: effective.Username, Password: effective.Password, DB: effective.DB})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("PING: %w", err)
	}
	if !requireAOF {
		return nil
	}
	checks := []struct{ key, want string }{{"appendonly", "yes"}, {"appendfsync", "always"}, {"maxmemory-policy", "noeviction"}}
	var durabilityErrors []error
	for _, check := range checks {
		values, err := client.ConfigGet(ctx, check.key).Result()
		if err != nil {
			durabilityErrors = append(durabilityErrors, fmt.Errorf("CONFIG GET %s: %w", check.key, err))
			continue
		}
		if got := values[check.key]; !strings.EqualFold(got, check.want) {
			durabilityErrors = append(durabilityErrors, fmt.Errorf("Redis durability mismatch: %s=%q, require %q", check.key, got, check.want))
		}
	}
	return errors.Join(durabilityErrors...)
}

func endpointOccupied(ctx context.Context, address string) (bool, error) {
	dialer := net.Dialer{Timeout: probeTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err == nil {
		_ = conn.Close()
		return true, nil
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && (errors.Is(opErr.Err, os.ErrNotExist) || strings.Contains(strings.ToLower(err.Error()), "refused")) {
		return false, nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false, err
	}
	return false, err
}

func readManagedSecret(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	secret := strings.TrimSpace(string(data))
	if secret == "" {
		return "", errors.New("managed Redis secret is empty")
	}
	return secret, nil
}

func createManagedSecret(path string) (string, error) {
	random := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, random); err != nil {
		return "", fmt.Errorf("generate managed Redis secret: %w", err)
	}
	secret := base64.RawURLEncoding.EncodeToString(random)
	if err := writeFileAtomic(path, []byte(secret+"\n"), 0600); err != nil {
		return "", fmt.Errorf("write managed Redis secret: %w", err)
	}
	return secret, nil
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) (err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-tmp-")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer func() { _ = os.Remove(name) }()
	if err = temp.Chmod(mode); err == nil {
		_, err = temp.Write(data)
	}
	if err == nil {
		err = temp.Sync()
	}
	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return replaceFileAtomic(name, path)
}

func waitUntilReady(ctx context.Context, probe func(context.Context, EffectiveOptions, bool) error, effective EffectiveOptions, requireAOF bool, exit <-chan error) error {
	delay := 20 * time.Millisecond
	var lastErr error
	for {
		if err := boundedProbe(ctx, probe, effective, requireAOF); err == nil {
			return nil
		} else {
			lastErr = err
		}
		timer := time.NewTimer(delay)
		select {
		case err := <-exit:
			timer.Stop()
			if err == nil {
				return errors.New("embedded Redis exited before readiness")
			}
			return fmt.Errorf("embedded Redis exited before readiness: %w", err)
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("wait for embedded Redis readiness: %w (last probe: %v)", ctx.Err(), lastErr)
		case <-timer.C:
		}
		if delay < 250*time.Millisecond {
			delay *= 2
		}
	}
}

func shutdownRedis(ctx context.Context, effective EffectiveOptions) error {
	client := redis.NewClient(&redis.Options{Addr: effective.Address, Username: effective.Username, Password: effective.Password, DB: effective.DB})
	defer client.Close()
	return client.Shutdown(ctx).Err()
}

func isExpectedShutdownError(err error) bool {
	if err == nil || errors.Is(err, io.EOF) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "eof") || strings.Contains(message, "closed") || strings.Contains(message, "reset by peer")
}

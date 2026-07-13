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

type stopAttempt struct {
	done chan struct{}
	err  error
}

type Manager struct {
	owned             bool
	process           managedProcess
	effective         EffectiveOptions
	shutdown          func(context.Context, EffectiveOptions) error
	lifecycleLockPath string
	acquireLock       func(context.Context, string) (*installLock, error)
	exit              chan error
	stopDone          chan struct{}
	stopMu            sync.Mutex
	stopping          bool
	stopAttempt       *stopAttempt
	stopErr           error
}

func (m *Manager) Owned() bool { return m != nil && m.owned }

func Prepare(ctx context.Context, opts Options) (manager *Manager, effective EffectiveOptions, err error) {
	effective = effectiveFromOptions(opts)
	deps := currentManagerDeps
	if !ShouldManage(deps.goos, opts) {
		return nil, effective, nil
	}
	lifecycleDir := filepath.Join(opts.DataDir, "runtime", "redis")
	if err := os.MkdirAll(lifecycleDir, 0700); err != nil {
		return nil, effective, fmt.Errorf("create Redis lifecycle directory: %w", err)
	}
	lifecycleLockPath := filepath.Join(lifecycleDir, ".lifecycle.lock")
	lifecycleLock, err := acquireInstallLockContext(ctx, lifecycleLockPath)
	if err != nil {
		return nil, effective, fmt.Errorf("acquire Redis lifecycle lock: %w", err)
	}
	defer func() { err = errors.Join(err, lifecycleLock.release()) }()
	return prepareLocked(ctx, opts, deps, lifecycleLockPath)
}

func prepareLocked(ctx context.Context, opts Options, deps managerDependencies, lifecycleLockPath string) (*Manager, EffectiveOptions, error) {
	effective := effectiveFromOptions(opts)
	managedAddress := managedEndpointAddress(opts.Address)
	redisDir := filepath.Join(opts.DataDir, "redis")
	if err := os.MkdirAll(redisDir, 0700); err != nil {
		return nil, effective, fmt.Errorf("create Redis data directory: %w", err)
	}
	if err := secureDirectory(redisDir); err != nil {
		return nil, effective, fmt.Errorf("secure Redis data directory: %w", err)
	}

	configuredErr := boundedProbe(ctx, deps.probe, effective, opts.RequireAOF)
	if configuredErr == nil {
		return reusedManager(effective, deps.shutdown), effective, nil
	}
	if managedAddress != opts.Address {
		managedEffective := effective
		managedEffective.Address = managedAddress
		if err := boundedProbe(ctx, deps.probe, managedEffective, opts.RequireAOF); err == nil {
			return reusedManager(managedEffective, deps.shutdown), managedEffective, nil
		}
	}

	secretPath := filepath.Join(redisDir, secretFilename)
	persistedSecret, secretErr := readManagedSecret(secretPath)
	if secretErr != nil && !os.IsNotExist(secretErr) {
		return nil, effective, fmt.Errorf("read managed Redis secret: %w", secretErr)
	}
	if secretErr == nil {
		if err := secureFile(secretPath); err != nil {
			return nil, effective, fmt.Errorf("secure managed Redis secret: %w", err)
		}
	}
	if persistedSecret != "" && persistedSecret != effective.Password {
		managedEffective := effective
		managedEffective.Address = managedAddress
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
	if managedAddress != opts.Address {
		occupied, err = deps.occupied(ctx, managedAddress)
		if err != nil {
			return nil, effective, fmt.Errorf("inspect managed Redis endpoint: %w", err)
		}
		if occupied {
			return nil, effective, fmt.Errorf("managed Redis endpoint %s is occupied", managedAddress)
		}
	}

	password := opts.Password
	if password == "" {
		if persistedSecret != "" {
			password = persistedSecret
		} else {
			password, err = createManagedSecret(secretPath)
			if err != nil {
				return nil, effective, err
			}
		}
	}
	effective.Address = managedAddress
	effective.Password = password

	payload, err := deps.payload()
	if err != nil {
		return nil, effectiveFromOptions(opts), fmt.Errorf("load embedded Redis payload: %w", err)
	}
	extracted, err := deps.extract(opts.DataDir, payload)
	if err != nil {
		return nil, effectiveFromOptions(opts), fmt.Errorf("extract embedded Redis payload: %w", err)
	}
	if err := secureDirectory(extracted.Dir); err != nil {
		return nil, effectiveFromOptions(opts), fmt.Errorf("secure extracted Redis runtime directory: %w", err)
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
	if err := secureFile(configPath); err != nil {
		return nil, effectiveFromOptions(opts), fmt.Errorf("secure Redis config: %w", err)
	}
	logFile, err := os.OpenFile(filepath.Join(redisDir, "redis.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, effectiveFromOptions(opts), fmt.Errorf("open Redis log: %w", err)
	}
	if err := secureFile(logFile.Name()); err != nil {
		_ = logFile.Close()
		return nil, effectiveFromOptions(opts), fmt.Errorf("secure Redis log: %w", err)
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
	m := &Manager{
		owned:             true,
		process:           process,
		effective:         effective,
		shutdown:          deps.shutdown,
		lifecycleLockPath: lifecycleLockPath,
		acquireLock:       acquireInstallLockContext,
		exit:              make(chan error, 1),
		stopDone:          make(chan struct{}),
	}
	go func() { m.exit <- process.Wait(); close(m.exit); _ = logFile.Close() }()

	timeout := opts.StartupTimeout
	if timeout <= 0 {
		timeout = defaultStartupTimeout
	}
	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := waitUntilReady(readyCtx, deps.probe, effective, opts.RequireAOF, m.exit); err != nil {
		cleanupErr := process.Kill()
		select {
		case <-m.exit:
		default:
		}
		if cleanupErr != nil {
			cleanupErr = fmt.Errorf("kill failed embedded Redis child: %w", cleanupErr)
		}
		return nil, effectiveFromOptions(opts), errors.Join(err, cleanupErr)
	}
	return m, effective, nil
}

func (m *Manager) Stop(ctx context.Context) error {
	if m == nil || !m.owned {
		return nil
	}
	m.stopMu.Lock()
	if m.stopping {
		attempt := m.stopAttempt
		m.stopMu.Unlock()
		if err := waitForStopAttempt(ctx, attempt); err != nil {
			return err
		}
		return m.waitForStop(ctx)
	}
	select {
	case <-m.exit:
		m.stopMu.Unlock()
		return nil
	default:
	}
	attempt := &stopAttempt{done: make(chan struct{})}
	m.stopping = true
	m.stopAttempt = attempt
	m.stopMu.Unlock()

	var lifecycleLock *installLock
	if m.lifecycleLockPath != "" {
		acquireLock := m.acquireLock
		if acquireLock == nil {
			acquireLock = acquireInstallLockContext
		}
		var err error
		lifecycleLock, err = acquireLock(ctx, m.lifecycleLockPath)
		if err != nil {
			err = fmt.Errorf("acquire Redis lifecycle lock: %w", err)
			m.stopMu.Lock()
			attempt.err = err
			if m.stopAttempt == attempt {
				m.stopping = false
				m.stopAttempt = nil
			}
			close(attempt.done)
			m.stopMu.Unlock()
			return err
		}
	}
	m.stopMu.Lock()
	close(attempt.done)
	m.stopMu.Unlock()
	go func() {
		stopErr := m.stopOwned(ctx)
		if lifecycleLock != nil {
			if err := lifecycleLock.release(); err != nil {
				stopErr = errors.Join(stopErr, fmt.Errorf("release Redis lifecycle lock: %w", err))
			}
		}
		m.stopErr = stopErr
		close(m.stopDone)
	}()
	return m.waitForStop(ctx)
}

func waitForStopAttempt(ctx context.Context, attempt *stopAttempt) error {
	if attempt == nil {
		return nil
	}
	select {
	case <-attempt.done:
		return attempt.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) waitForStop(ctx context.Context) error {
	select {
	case <-m.stopDone:
		return m.stopErr
	default:
	}
	select {
	case <-m.stopDone:
		return m.stopErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) stopOwned(ctx context.Context) error {
	select {
	case <-m.exit:
		return nil
	default:
	}
	shutdownErr := m.shutdown(ctx, m.effective)
	if isExpectedShutdownError(shutdownErr) {
		shutdownErr = nil
	}
	select {
	case <-m.exit:
		if shutdownErr != nil {
			return fmt.Errorf("shutdown embedded Redis: %w", shutdownErr)
		}
		return nil
	case <-ctx.Done():
		killErr := m.process.Kill()
		if killErr != nil {
			killErr = fmt.Errorf("kill embedded Redis child: %w", killErr)
		}
		select {
		case <-m.exit:
		default:
		}
		if shutdownErr != nil {
			return errors.Join(fmt.Errorf("shutdown embedded Redis: %w", shutdownErr), ctx.Err(), killErr)
		}
		return errors.Join(ctx.Err(), killErr)
	}
}

func reusedManager(effective EffectiveOptions, shutdown func(context.Context, EffectiveOptions) error) *Manager {
	return &Manager{effective: effective, shutdown: shutdown}
}

func effectiveFromOptions(opts Options) EffectiveOptions {
	return EffectiveOptions{Address: opts.Address, Username: opts.Username, Password: opts.Password, DB: opts.DB}
}

func managedEndpointAddress(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err == nil && host == "::1" {
		return net.JoinHostPort("127.0.0.1", port)
	}
	return address
}

func boundedProbe(ctx context.Context, probe func(context.Context, EffectiveOptions, bool) error, effective EffectiveOptions, requireAOF bool) error {
	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	return probe(probeCtx, effective, requireAOF)
}

func probeRedis(ctx context.Context, effective EffectiveOptions, requireAOF bool) error {
	client := redis.NewClient(redisClientOptions(effective))
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
	if err := secureFile(path); err != nil {
		return "", fmt.Errorf("secure managed Redis secret: %w", err)
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
		if err := ownedProcessExit(exit); err != nil {
			return err
		}
		if err := boundedProbe(ctx, probe, effective, requireAOF); err == nil {
			if err := ownedProcessExit(exit); err != nil {
				return err
			}
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

func ownedProcessExit(exit <-chan error) error {
	select {
	case err := <-exit:
		if err == nil {
			return errors.New("embedded Redis exited before readiness")
		}
		return fmt.Errorf("embedded Redis exited before readiness: %w", err)
	default:
		return nil
	}
}

func shutdownRedis(ctx context.Context, effective EffectiveOptions) error {
	client := redis.NewClient(redisClientOptions(effective))
	defer client.Close()
	return client.Shutdown(ctx).Err()
}

func redisClientOptions(effective EffectiveOptions) *redis.Options {
	return &redis.Options{
		Addr:                  effective.Address,
		Username:              effective.Username,
		Password:              effective.Password,
		DB:                    effective.DB,
		MaxRetries:            -1,
		ContextTimeoutEnabled: true,
	}
}

func isExpectedShutdownError(err error) bool {
	if err == nil || errors.Is(err, io.EOF) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "eof") || strings.Contains(message, "closed") || strings.Contains(message, "reset by peer")
}

package embeddedredis

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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
	managedMarkerFilename = "managed.json"
	ownerLockFilename     = ".owner.lock"
	managedMarkerVersion  = 1
	defaultStartupTimeout = 15 * time.Second
	probeTimeout          = 750 * time.Millisecond
	stopReapTimeout       = 500 * time.Millisecond
	adoptedStopTimeout    = 500 * time.Millisecond
)

var errOwnerLockHeld = errors.New("managed Redis is owned by another OpenList process")

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

type managedMarker struct {
	Version             int    `json:"version"`
	Address             string `json:"address"`
	PasswordFingerprint string `json:"password_fingerprint"`
}

type Manager struct {
	owned             bool
	process           managedProcess
	effective         EffectiveOptions
	shutdown          func(context.Context, EffectiveOptions) error
	probe             func(context.Context, EffectiveOptions, bool) error
	occupied          func(context.Context, string) (bool, error)
	requireAOF        bool
	lifecycleLockPath string
	acquireLock       func(context.Context, string) (*installLock, error)
	ownerLock         *installLock
	exit              chan error
	stopMu            sync.Mutex
	stopAttempt       *stopAttempt
	stopped           bool
	forceKill         bool
	stopErr           error
}

func (m *Manager) Owned() bool { return m != nil && m.owned }

func (m *Manager) Stopped() bool {
	if m == nil {
		return true
	}
	m.stopMu.Lock()
	defer m.stopMu.Unlock()
	return m.stopped
}

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
	if err := secureDirectory(lifecycleDir); err != nil {
		return nil, effective, fmt.Errorf("secure Redis lifecycle directory: %w", err)
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
	lifecycleDir := filepath.Dir(lifecycleLockPath)
	ownerLockPath := filepath.Join(lifecycleDir, ownerLockFilename)
	markerPath := filepath.Join(lifecycleDir, managedMarkerFilename)
	marker, markerErr := readManagedMarker(markerPath)
	if markerErr != nil && !os.IsNotExist(markerErr) {
		return nil, effective, fmt.Errorf("read managed Redis marker: %w", markerErr)
	}
	redisDir := filepath.Join(opts.DataDir, "redis")
	if err := os.MkdirAll(redisDir, 0700); err != nil {
		return nil, effective, fmt.Errorf("create Redis data directory: %w", err)
	}
	if err := secureDirectory(redisDir); err != nil {
		return nil, effective, fmt.Errorf("secure Redis data directory: %w", err)
	}

	configuredErr := boundedProbe(ctx, deps.probe, effective, opts.RequireAOF)
	if configuredErr == nil {
		manager, err := managerForReadyEndpoint(effective, opts.RequireAOF, deps, lifecycleLockPath, ownerLockPath, marker)
		return manager, effective, err
	}
	if managedAddress != opts.Address {
		managedEffective := effective
		managedEffective.Address = managedAddress
		if err := boundedProbe(ctx, deps.probe, managedEffective, opts.RequireAOF); err == nil {
			manager, err := managerForReadyEndpoint(managedEffective, opts.RequireAOF, deps, lifecycleLockPath, ownerLockPath, marker)
			return manager, managedEffective, err
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
			manager, err := managerForReadyEndpoint(managedEffective, opts.RequireAOF, deps, lifecycleLockPath, ownerLockPath, marker)
			return manager, managedEffective, err
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
	ownerLock, err := acquireOwnerLock(ownerLockPath)
	if err != nil {
		_ = logFile.Close()
		return nil, effectiveFromOptions(opts), err
	}
	releaseOwner := true
	defer func() {
		if releaseOwner {
			_ = ownerLock.release()
		}
	}()
	if err := writeManagedMarker(markerPath, managedMarkerFor(effective)); err != nil {
		_ = logFile.Close()
		return nil, effectiveFromOptions(opts), fmt.Errorf("write managed Redis marker: %w", err)
	}
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
		probe:             deps.probe,
		occupied:          deps.occupied,
		requireAOF:        opts.RequireAOF,
		lifecycleLockPath: lifecycleLockPath,
		acquireLock:       acquireInstallLockContext,
		ownerLock:         ownerLock,
		exit:              make(chan error, 1),
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
		confirmed := waitForExit(m.exit, stopReapTimeout)
		if cleanupErr != nil {
			cleanupErr = fmt.Errorf("kill failed embedded Redis child: %w", cleanupErr)
		}
		if confirmed {
			releaseOwner = false
			releaseErr := m.releaseOwnerLock()
			return nil, effectiveFromOptions(opts), errors.Join(err, cleanupErr, releaseErr)
		}
		releaseOwner = false
		return m, effectiveFromOptions(opts), errors.Join(err, cleanupErr, errors.New("embedded Redis child exit was not confirmed"))
	}
	releaseOwner = false
	return m, effective, nil
}

func (m *Manager) Stop(ctx context.Context) error {
	if m == nil {
		return nil
	}
	m.stopMu.Lock()
	if m.stopped {
		err := m.stopErr
		m.stopMu.Unlock()
		return err
	}
	if !m.owned {
		m.stopped = true
		m.stopMu.Unlock()
		return nil
	}
	if m.stopAttempt != nil {
		attempt := m.stopAttempt
		m.stopMu.Unlock()
		return waitForStopAttempt(ctx, attempt)
	}
	attempt := &stopAttempt{done: make(chan struct{})}
	m.stopAttempt = attempt
	m.stopMu.Unlock()
	if m.process != nil && processExited(m.exit) {
		stopErr := m.releaseOwnerLock()
		m.finishStopAttempt(attempt, true, stopErr)
		return stopErr
	}

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
			m.finishStopAttempt(attempt, false, err)
			return err
		}
	}
	confirmed, stopErr := m.stopOwned(ctx)
	if confirmed {
		stopErr = errors.Join(stopErr, m.releaseOwnerLock())
	}
	if lifecycleLock != nil {
		if err := lifecycleLock.release(); err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("release Redis lifecycle lock: %w", err))
		}
	}
	m.finishStopAttempt(attempt, confirmed, stopErr)
	return stopErr
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

func (m *Manager) finishStopAttempt(attempt *stopAttempt, confirmed bool, err error) {
	m.stopMu.Lock()
	attempt.err = err
	if confirmed {
		m.stopped = true
		m.stopErr = err
	}
	if m.stopAttempt == attempt {
		m.stopAttempt = nil
	}
	close(attempt.done)
	m.stopMu.Unlock()
}

func (m *Manager) stopOwned(ctx context.Context) (bool, error) {
	if m.process == nil {
		return m.stopAdopted(ctx)
	}
	if processExited(m.exit) {
		return true, nil
	}
	if m.forceKill {
		return m.killAndConfirm(nil)
	}
	shutdownErr := m.shutdown(ctx, m.effective)
	if isExpectedShutdownError(shutdownErr) {
		shutdownErr = nil
	}
	if processExited(m.exit) {
		if shutdownErr != nil {
			return true, fmt.Errorf("shutdown embedded Redis: %w", shutdownErr)
		}
		return true, nil
	}
	select {
	case <-m.exit:
		if shutdownErr != nil {
			return true, fmt.Errorf("shutdown embedded Redis: %w", shutdownErr)
		}
		return true, nil
	case <-ctx.Done():
		diagnostic := error(ctx.Err())
		if shutdownErr != nil {
			diagnostic = errors.Join(fmt.Errorf("shutdown embedded Redis: %w", shutdownErr), diagnostic)
		}
		return m.killAndConfirm(diagnostic)
	}
}

func (m *Manager) killAndConfirm(diagnostic error) (bool, error) {
	killErr := m.process.Kill()
	if killErr != nil {
		m.forceKill = true
		killErr = fmt.Errorf("kill embedded Redis child: %w", killErr)
		if processExited(m.exit) {
			return true, errors.Join(diagnostic, killErr)
		}
		return false, errors.Join(diagnostic, killErr)
	}
	if waitForExit(m.exit, stopReapTimeout) {
		return true, diagnostic
	}
	m.forceKill = true
	return false, errors.Join(diagnostic, errors.New("timed out waiting for embedded Redis child exit after kill"))
}

func (m *Manager) stopAdopted(ctx context.Context) (bool, error) {
	shutdownErr := m.shutdown(ctx, m.effective)
	if isExpectedShutdownError(shutdownErr) {
		shutdownErr = nil
	}
	confirmCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), adoptedStopTimeout)
	defer cancel()
	for {
		occupied, occupiedErr := m.occupied(confirmCtx, m.effective.Address)
		if occupiedErr == nil && !occupied {
			if shutdownErr != nil {
				return true, fmt.Errorf("shutdown adopted embedded Redis: %w", shutdownErr)
			}
			return true, nil
		}
		select {
		case <-confirmCtx.Done():
			return false, errors.Join(fmt.Errorf("adopted embedded Redis shutdown was not confirmed: %w", confirmCtx.Err()), shutdownErr, occupiedErr)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func processExited(exit <-chan error) bool {
	if exit == nil {
		return false
	}
	select {
	case <-exit:
		return true
	default:
		return false
	}
}

func waitForExit(exit <-chan error, timeout time.Duration) bool {
	if processExited(exit) {
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-exit:
		return true
	case <-timer.C:
		return false
	}
}

func (m *Manager) releaseOwnerLock() error {
	if m.ownerLock == nil {
		return nil
	}
	lock := m.ownerLock
	m.ownerLock = nil
	if err := lock.release(); err != nil {
		return fmt.Errorf("release managed Redis owner lock: %w", err)
	}
	return nil
}

func reusedManager(effective EffectiveOptions, shutdown func(context.Context, EffectiveOptions) error) *Manager {
	return &Manager{effective: effective, shutdown: shutdown}
}

func managerForReadyEndpoint(effective EffectiveOptions, requireAOF bool, deps managerDependencies, lifecycleLockPath, ownerLockPath string, marker *managedMarker) (*Manager, error) {
	if marker == nil || *marker != managedMarkerFor(effective) {
		return reusedManager(effective, deps.shutdown), nil
	}
	ownerLock, err := acquireOwnerLock(ownerLockPath)
	if err != nil {
		return nil, err
	}
	return &Manager{
		owned:             true,
		effective:         effective,
		shutdown:          deps.shutdown,
		probe:             deps.probe,
		occupied:          deps.occupied,
		requireAOF:        requireAOF,
		lifecycleLockPath: lifecycleLockPath,
		acquireLock:       acquireInstallLockContext,
		ownerLock:         ownerLock,
	}, nil
}

func acquireOwnerLock(path string) (*installLock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open managed Redis owner lock: %w", err)
	}
	if err := secureFile(path); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("secure managed Redis owner lock: %w", err)
	}
	locked, err := tryLockFile(file)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock managed Redis ownership: %w", err)
	}
	if !locked {
		_ = file.Close()
		return nil, errOwnerLockHeld
	}
	return &installLock{file: file}, nil
}

func managedMarkerFor(effective EffectiveOptions) managedMarker {
	fingerprint := sha256.Sum256([]byte(effective.Password))
	return managedMarker{
		Version:             managedMarkerVersion,
		Address:             managedEndpointAddress(effective.Address),
		PasswordFingerprint: hex.EncodeToString(fingerprint[:]),
	}
}

func readManagedMarker(path string) (*managedMarker, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := secureFile(path); err != nil {
		return nil, err
	}
	var marker managedMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return nil, err
	}
	if marker.Version != managedMarkerVersion || marker.Address == "" || marker.PasswordFingerprint == "" {
		return nil, errors.New("managed Redis marker is invalid")
	}
	return &marker, nil
}

func writeManagedMarker(path string, marker managedMarker) error {
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := writeFileAtomic(path, data, 0600); err != nil {
		return err
	}
	return secureFile(path)
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

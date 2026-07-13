package embeddedredis

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPrepareBypassesIneligibleOptionsWithoutSideEffects(t *testing.T) {
	base := Options{Role: "worker", Address: "127.0.0.1:6379", Password: "original", DB: 2, DataDir: t.TempDir()}
	tests := []Options{
		func() Options { o := base; o.Address = "redis.example:6379"; return o }(),
		func() Options { o := base; o.Username = "service"; return o }(),
		func() Options { o := base; o.Role = "api"; return o }(),
	}
	for _, opts := range tests {
		var calls atomic.Int32
		restoreManagerDeps(t, managerDependencies{
			goos:    "windows",
			probe:   func(context.Context, EffectiveOptions, bool) error { calls.Add(1); return nil },
			payload: func() ([]byte, error) { calls.Add(1); return nil, nil },
			start:   func(*exec.Cmd) (managedProcess, error) { calls.Add(1); return nil, nil },
		})
		manager, effective, err := Prepare(context.Background(), opts)
		if err != nil || manager != nil {
			t.Fatalf("Prepare() = (%v, %#v, %v), want nil manager and no error", manager, effective, err)
		}
		if effective != effectiveFrom(opts) {
			t.Fatalf("effective = %#v, want unchanged %#v", effective, effectiveFrom(opts))
		}
		if calls.Load() != 0 {
			t.Fatalf("side effect calls = %d, want 0", calls.Load())
		}
	}

	if runtime.GOOS != "windows" {
		var calls atomic.Int32
		restoreManagerDeps(t, managerDependencies{goos: runtime.GOOS, probe: func(context.Context, EffectiveOptions, bool) error { calls.Add(1); return nil }})
		manager, effective, err := Prepare(context.Background(), base)
		if err != nil || manager != nil || effective != effectiveFrom(base) || calls.Load() != 0 {
			t.Fatalf("native non-Windows bypass = (%v, %#v, %v, calls %d)", manager, effective, err, calls.Load())
		}
	}
}

func TestPrepareReusesCompatibleConfiguredRedis(t *testing.T) {
	opts := eligibleOptions(t)
	opts.Password = "configured"
	var got EffectiveOptions
	restoreManagerDeps(t, managerDependencies{
		goos: "windows",
		probe: func(_ context.Context, effective EffectiveOptions, requireAOF bool) error {
			got = effective
			if !requireAOF {
				t.Fatal("probe did not require durability")
			}
			return nil
		},
		payload: func() ([]byte, error) { t.Fatal("payload called"); return nil, nil },
	})
	manager, effective, err := Prepare(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if manager == nil || manager.Owned() {
		t.Fatalf("manager = %#v, want reused manager", manager)
	}
	if effective != effectiveFrom(opts) || got != effectiveFrom(opts) {
		t.Fatalf("effective/probe = %#v/%#v", effective, got)
	}
}

func TestPrepareReusesPersistedManagedSecret(t *testing.T) {
	opts := eligibleOptions(t)
	secret := "persisted-secret"
	redisDir := filepath.Join(opts.DataDir, "redis")
	if err := os.MkdirAll(redisDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(redisDir, secretFilename), []byte(secret+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var probes []string
	restoreManagerDeps(t, managerDependencies{
		goos: "windows",
		probe: func(_ context.Context, effective EffectiveOptions, _ bool) error {
			probes = append(probes, effective.Password)
			if effective.Password == secret {
				return nil
			}
			return errors.New("NOAUTH")
		},
	})
	manager, effective, err := Prepare(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if manager == nil || manager.Owned() {
		t.Fatalf("manager = %#v, want reused", manager)
	}
	if effective.Password != secret || strings.Join(probes, ",") != ","+secret {
		t.Fatalf("effective password/probes = %q/%q", effective.Password, probes)
	}
}

func TestProbeRequiresEveryDurabilitySetting(t *testing.T) {
	var mu sync.Mutex
	var configKeys []string
	server, address := newRESPServer(t, func(command []string) string {
		switch strings.ToLower(command[0]) {
		case "auth", "select":
			return "+OK\r\n"
		case "ping":
			return "+PONG\r\n"
		case "config":
			key := command[len(command)-1]
			mu.Lock()
			configKeys = append(configKeys, key)
			mu.Unlock()
			value := map[string]string{"appendonly": "yes", "appendfsync": "everysec", "maxmemory-policy": "allkeys-lru"}[key]
			return "*2\r\n$" + itoa(len(key)) + "\r\n" + key + "\r\n$" + itoa(len(value)) + "\r\n" + value + "\r\n"
		}
		return "-ERR unsupported\r\n"
	})
	defer server.Close()
	err := probeRedis(context.Background(), EffectiveOptions{Address: address}, true)
	if err == nil || !strings.Contains(err.Error(), "appendfsync") {
		t.Fatalf("probeRedis error = %v, want appendfsync durability mismatch", err)
	}
	mu.Lock()
	gotKeys := strings.Join(configKeys, ",")
	mu.Unlock()
	if gotKeys != "appendonly,appendfsync,maxmemory-policy" {
		t.Fatalf("CONFIG GET keys = %q, want every durability setting", gotKeys)
	}
}

func TestPrepareRejectsOccupiedConflictWithoutStarting(t *testing.T) {
	opts := eligibleOptions(t)
	var starts atomic.Int32
	restoreManagerDeps(t, managerDependencies{
		goos:     "windows",
		probe:    func(context.Context, EffectiveOptions, bool) error { return errors.New("invalid protocol") },
		occupied: func(context.Context, string) (bool, error) { return true, nil },
		start:    func(*exec.Cmd) (managedProcess, error) { starts.Add(1); return nil, nil },
	})
	manager, _, err := Prepare(context.Background(), opts)
	if err == nil || !strings.Contains(err.Error(), "occupied") || manager != nil || starts.Load() != 0 {
		t.Fatalf("Prepare() = (%v, %v), starts=%d", manager, err, starts.Load())
	}
}

func TestPrepareStartsManagedRedisWithGeneratedSecret(t *testing.T) {
	opts := eligibleOptions(t)
	proc := newFakeProcess()
	var probes atomic.Int32
	var command *exec.Cmd
	restoreManagerDeps(t, managerDependencies{
		goos: "windows",
		probe: func(_ context.Context, effective EffectiveOptions, _ bool) error {
			if probes.Add(1) < 2 {
				return errors.New("connection refused")
			}
			if effective.Password == "" {
				t.Fatal("readiness password empty")
			}
			return nil
		},
		occupied: func(context.Context, string) (bool, error) { return false, nil },
		payload:  func() ([]byte, error) { return []byte("payload"), nil },
		extract: func(dataDir string, payload []byte) (ExtractedRuntime, error) {
			if dataDir != opts.DataDir || string(payload) != "payload" {
				t.Fatalf("extract args = %q/%q", dataDir, payload)
			}
			runtimeDir := filepath.Join(opts.DataDir, "fake-runtime")
			if err := os.MkdirAll(runtimeDir, 0700); err != nil {
				return ExtractedRuntime{}, err
			}
			return ExtractedRuntime{Dir: runtimeDir, ServerPath: filepath.Join(runtimeDir, "redis-server.exe")}, nil
		},
		start: func(cmd *exec.Cmd) (managedProcess, error) { command = cmd; return proc, nil },
	})
	manager, effective, err := Prepare(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if manager == nil || !manager.Owned() || effective.Password == "" {
		t.Fatalf("manager/effective = %#v/%#v", manager, effective)
	}
	if opts.Password != "" {
		t.Fatalf("caller password mutated to %q", opts.Password)
	}
	if command == nil || command.Dir == "" || len(command.Args) != 2 || filepath.Base(command.Args[1]) != "redis.conf" {
		t.Fatalf("command = %#v", command)
	}
	config, err := os.ReadFile(command.Args[1])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(config), "requirepass \""+effective.Password+"\"") {
		t.Fatalf("config missing password: %s", config)
	}
	secret, err := os.ReadFile(filepath.Join(opts.DataDir, "redis", secretFilename))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(secret)) != effective.Password {
		t.Fatalf("secret = %q", secret)
	}
	info, err := os.Stat(filepath.Join(opts.DataDir, "redis", secretFilename))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0600 {
		t.Fatalf("secret mode = %o", info.Mode().Perm())
	}
	proc.exit(nil)
}

func TestPreparePreservesConfiguredPasswordForManagedStart(t *testing.T) {
	opts := eligibleOptions(t)
	opts.Password = "chosen-password"
	proc := newFakeProcess()
	var calls atomic.Int32
	restoreManagerDeps(t, managerDependencies{
		goos: "windows",
		probe: func(_ context.Context, effective EffectiveOptions, _ bool) error {
			if calls.Add(1) == 1 {
				return errors.New("refused")
			}
			if effective.Password != opts.Password {
				t.Fatalf("password = %q", effective.Password)
			}
			return nil
		},
		occupied: func(context.Context, string) (bool, error) { return false, nil },
		payload:  func() ([]byte, error) { return []byte("x"), nil },
		extract:  fakeExtract(opts.DataDir),
		start:    func(*exec.Cmd) (managedProcess, error) { return proc, nil },
	})
	manager, effective, err := Prepare(context.Background(), opts)
	if err != nil || effective.Password != opts.Password || !manager.Owned() {
		t.Fatalf("Prepare = %#v %#v %v", manager, effective, err)
	}
	if _, err := os.Stat(filepath.Join(opts.DataDir, "redis", secretFilename)); !os.IsNotExist(err) {
		t.Fatalf("managed secret created for configured password: %v", err)
	}
	proc.exit(nil)
}

func TestPrepareNormalizesIPv6LoopbackForManagedStartAndReuse(t *testing.T) {
	opts := eligibleOptions(t)
	opts.Address = "[::1]:16379"
	opts.StartupTimeout = 100 * time.Millisecond
	proc := newFakeProcess()
	var starts atomic.Int32
	var probesMu sync.Mutex
	var probes []EffectiveOptions
	restoreManagerDeps(t, managerDependencies{
		goos: "windows",
		probe: func(_ context.Context, effective EffectiveOptions, _ bool) error {
			probesMu.Lock()
			probes = append(probes, effective)
			probesMu.Unlock()
			if effective.Address == "127.0.0.1:16379" && effective.Password != "" {
				return nil
			}
			return errors.New("not available")
		},
		occupied: func(context.Context, string) (bool, error) { return false, nil },
		payload:  func() ([]byte, error) { return []byte("x"), nil },
		extract:  fakeExtract(opts.DataDir),
		start: func(*exec.Cmd) (managedProcess, error) {
			starts.Add(1)
			return proc, nil
		},
	})

	owned, firstEffective, err := Prepare(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if !owned.Owned() || firstEffective.Address != "127.0.0.1:16379" {
		t.Fatalf("first Prepare = owned %v, address %q", owned.Owned(), firstEffective.Address)
	}
	reused, secondEffective, err := Prepare(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if reused == nil || reused.Owned() || secondEffective.Address != "127.0.0.1:16379" {
		t.Fatalf("second Prepare = %#v, address %q", reused, secondEffective.Address)
	}
	if starts.Load() != 1 {
		t.Fatalf("starts = %d, want one managed process", starts.Load())
	}
	probesMu.Lock()
	defer probesMu.Unlock()
	if len(probes) != 6 || probes[0].Address != opts.Address || probes[3].Address != opts.Address {
		t.Fatalf("probe addresses = %#v, want user IPv6 endpoint first on each Prepare", probes)
	}
	proc.exit(nil)
}

func TestPrepareReusesIPv6ManagedRedisWithConfiguredPassword(t *testing.T) {
	opts := eligibleOptions(t)
	opts.Address = "[::1]:16379"
	opts.Password = "configured-password"
	var probes []EffectiveOptions
	restoreManagerDeps(t, managerDependencies{
		goos: "windows",
		probe: func(_ context.Context, effective EffectiveOptions, _ bool) error {
			probes = append(probes, effective)
			if effective.Address == "127.0.0.1:16379" && effective.Password == opts.Password {
				return nil
			}
			return errors.New("not available")
		},
		payload: func() ([]byte, error) { t.Fatal("payload called for surviving managed Redis"); return nil, nil },
		start: func(*exec.Cmd) (managedProcess, error) {
			t.Fatal("process started for surviving managed Redis")
			return nil, nil
		},
	})

	manager, effective, err := Prepare(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if manager == nil || manager.Owned() {
		t.Fatalf("manager = %#v, want reused", manager)
	}
	if effective.Address != "127.0.0.1:16379" || effective.Password != opts.Password {
		t.Fatalf("effective = %#v", effective)
	}
	if len(probes) != 2 || probes[0].Address != opts.Address || probes[1].Address != effective.Address {
		t.Fatalf("probes = %#v, want configured endpoint then normalized managed endpoint", probes)
	}
}

func TestRedisClientsHonorContextDeadlines(t *testing.T) {
	server := newSilentTCPServer(t)
	defer server.Close()

	t.Run("probe", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		started := time.Now()
		err := probeRedis(ctx, EffectiveOptions{Address: server.Addr().String()}, false)
		if err == nil {
			t.Fatal("probeRedis error = nil, want timeout")
		}
		if elapsed := time.Since(started); elapsed > 750*time.Millisecond {
			t.Fatalf("probeRedis ignored context deadline: %v", elapsed)
		}
	})

	t.Run("stop shutdown", func(t *testing.T) {
		proc := newFakeProcess()
		m := ownedTestManager(proc, shutdownRedis)
		m.effective.Address = server.Addr().String()
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()
		started := time.Now()
		err := m.Stop(ctx)
		if err == nil {
			t.Fatal("Stop error = nil, want timeout")
		}
		if elapsed := time.Since(started); elapsed > 750*time.Millisecond {
			t.Fatalf("Stop ignored context deadline: %v", elapsed)
		}
		if proc.kills.Load() != 1 || proc.waits.Load() != 1 {
			t.Fatalf("kill/wait = %d/%d", proc.kills.Load(), proc.waits.Load())
		}
	})
}

func TestPrepareReportsEarlyExitAndTimeoutKillsAndReaps(t *testing.T) {
	for _, test := range []struct {
		name  string
		early bool
	}{{"early exit", true}, {"timeout", false}} {
		t.Run(test.name, func(t *testing.T) {
			opts := eligibleOptions(t)
			opts.StartupTimeout = 25 * time.Millisecond
			proc := newFakeProcess()
			restoreManagerDeps(t, managerDependencies{
				goos: "windows", probe: func(context.Context, EffectiveOptions, bool) error { return errors.New("not ready") },
				occupied: func(context.Context, string) (bool, error) { return false, nil }, payload: func() ([]byte, error) { return []byte("x"), nil },
				extract: fakeExtract(opts.DataDir), start: func(*exec.Cmd) (managedProcess, error) {
					if test.early {
						proc.exit(nil)
					}
					return proc, nil
				},
			})
			manager, _, err := Prepare(context.Background(), opts)
			if err == nil || manager != nil {
				t.Fatalf("Prepare = %v, %v", manager, err)
			}
			if test.early && strings.Contains(err.Error(), "%!w") {
				t.Fatalf("early exit error contains formatting failure: %v", err)
			}
			if !test.early && proc.kills.Load() != 1 {
				t.Fatalf("kills = %d, want 1", proc.kills.Load())
			}
			if proc.waits.Load() != 1 {
				t.Fatalf("waits = %d, want 1", proc.waits.Load())
			}
		})
	}
}

func TestManagerStopOwnershipTimeoutAndConcurrency(t *testing.T) {
	t.Run("reused", func(t *testing.T) {
		var shutdowns atomic.Int32
		m := &Manager{shutdown: func(context.Context, EffectiveOptions) error { shutdowns.Add(1); return nil }}
		if err := m.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		if shutdowns.Load() != 0 {
			t.Fatalf("shutdowns = %d", shutdowns.Load())
		}
	})
	t.Run("owned concurrent graceful", func(t *testing.T) {
		proc := newFakeProcess()
		var shutdowns atomic.Int32
		m := ownedTestManager(proc, func(context.Context, EffectiveOptions) error {
			shutdowns.Add(1)
			proc.exit(nil)
			return errors.New("EOF")
		})
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := m.Stop(context.Background()); err != nil {
					t.Errorf("Stop: %v", err)
				}
			}()
		}
		wg.Wait()
		if shutdowns.Load() != 1 || proc.waits.Load() != 1 || proc.kills.Load() != 0 {
			t.Fatalf("shutdown/wait/kill = %d/%d/%d", shutdowns.Load(), proc.waits.Load(), proc.kills.Load())
		}
	})
	t.Run("deadline kills", func(t *testing.T) {
		proc := newFakeProcess()
		m := ownedTestManager(proc, func(context.Context, EffectiveOptions) error { return nil })
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if err := m.Stop(ctx); err == nil {
			t.Fatal("Stop error = nil, want deadline")
		}
		if proc.kills.Load() != 1 || proc.waits.Load() != 1 {
			t.Fatalf("kill/wait = %d/%d", proc.kills.Load(), proc.waits.Load())
		}
		if err := m.Stop(context.Background()); err == nil {
			t.Fatal("idempotent Stop should retain result")
		}
	})
	t.Run("shutdown error waits for deadline before kill", func(t *testing.T) {
		proc := newFakeProcess()
		shutdownCalled := make(chan struct{})
		m := ownedTestManager(proc, func(context.Context, EffectiveOptions) error {
			close(shutdownCalled)
			return errors.New("ERR shutdown denied")
		})
		ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
		defer cancel()
		stopDone := make(chan error, 1)
		go func() { stopDone <- m.Stop(ctx) }()
		<-shutdownCalled
		time.Sleep(20 * time.Millisecond)
		if proc.kills.Load() != 0 {
			t.Fatalf("kills before deadline = %d, want 0", proc.kills.Load())
		}
		err := <-stopDone
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "shutdown denied") {
			t.Fatalf("Stop error = %v, want shutdown and deadline errors", err)
		}
		if proc.kills.Load() != 1 {
			t.Fatalf("kills after deadline = %d, want 1", proc.kills.Load())
		}
	})
	t.Run("already exited does not shutdown endpoint", func(t *testing.T) {
		proc := newFakeProcess()
		var shutdowns atomic.Int32
		m := ownedTestManager(proc, func(context.Context, EffectiveOptions) error { shutdowns.Add(1); return nil })
		proc.exit(nil)
		waitForCount(t, &proc.waits, 1)
		deadline := time.Now().Add(time.Second)
		for len(m.exit) == 0 {
			if time.Now().After(deadline) {
				t.Fatal("process exit was not published")
			}
			time.Sleep(time.Millisecond)
		}
		if err := m.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		if shutdowns.Load() != 0 || proc.kills.Load() != 0 {
			t.Fatalf("shutdown/kill = %d/%d, want 0/0", shutdowns.Load(), proc.kills.Load())
		}
	})
	t.Run("concurrent waiter respects own context", func(t *testing.T) {
		proc := newFakeProcess()
		shutdownStarted := make(chan struct{})
		releaseShutdown := make(chan struct{})
		m := ownedTestManager(proc, func(context.Context, EffectiveOptions) error {
			close(shutdownStarted)
			<-releaseShutdown
			proc.exit(nil)
			return nil
		})
		ownerDone := make(chan error, 1)
		go func() { ownerDone <- m.Stop(context.Background()) }()
		<-shutdownStarted
		waiterCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		started := time.Now()
		err := m.Stop(waiterCtx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waiter Stop error = %v", err)
		}
		if time.Since(started) > 200*time.Millisecond {
			t.Fatalf("waiter did not return promptly: %v", time.Since(started))
		}
		if proc.kills.Load() != 0 {
			t.Fatalf("waiter killed owned process: %d", proc.kills.Load())
		}
		close(releaseShutdown)
		if err := <-ownerDone; err != nil {
			t.Fatal(err)
		}
	})
}

func eligibleOptions(t *testing.T) Options {
	return Options{Role: "worker", Address: "127.0.0.1:16379", DataDir: t.TempDir(), RequireAOF: true, StartupTimeout: time.Second}
}

func effectiveFrom(opts Options) EffectiveOptions {
	return EffectiveOptions{Address: opts.Address, Username: opts.Username, Password: opts.Password, DB: opts.DB}
}

func restoreManagerDeps(t *testing.T, deps managerDependencies) {
	t.Helper()
	old := currentManagerDeps
	currentManagerDeps = deps.withDefaults()
	t.Cleanup(func() { currentManagerDeps = old })
}

func fakeExtract(dataDir string) func(string, []byte) (ExtractedRuntime, error) {
	return func(string, []byte) (ExtractedRuntime, error) {
		dir := filepath.Join(dataDir, "runtime-test")
		if err := os.MkdirAll(dir, 0700); err != nil {
			return ExtractedRuntime{}, err
		}
		return ExtractedRuntime{Dir: dir, ServerPath: filepath.Join(dir, "redis-server.exe")}, nil
	}
}

type fakeProcess struct {
	done         chan struct{}
	once         sync.Once
	err          error
	waits, kills atomic.Int32
}

func newFakeProcess() *fakeProcess    { return &fakeProcess{done: make(chan struct{})} }
func (p *fakeProcess) Wait() error    { p.waits.Add(1); <-p.done; return p.err }
func (p *fakeProcess) Kill() error    { p.kills.Add(1); p.exit(errors.New("killed")); return nil }
func (p *fakeProcess) exit(err error) { p.once.Do(func() { p.err = err; close(p.done) }) }

func ownedTestManager(proc *fakeProcess, shutdown func(context.Context, EffectiveOptions) error) *Manager {
	m := &Manager{owned: true, process: proc, effective: EffectiveOptions{Address: "127.0.0.1:1"}, shutdown: shutdown, exit: make(chan error, 1), stopDone: make(chan struct{})}
	go func() { m.exit <- proc.Wait(); close(m.exit) }()
	return m
}

func waitForCount(t *testing.T, value *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for value.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("count = %d, want %d", value.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func itoa(n int) string {
	const digits = "0123456789"
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = digits[n%10]
		n /= 10
	}
	return string(b[i:])
}

type respServer struct{ net.Listener }

type silentTCPServer struct {
	net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func newSilentTCPServer(t *testing.T) *silentTCPServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &silentTCPServer{Listener: listener}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			server.mu.Lock()
			server.conns = append(server.conns, conn)
			server.mu.Unlock()
		}
	}()
	return server
}

func (s *silentTCPServer) Close() error {
	err := s.Listener.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range s.conns {
		_ = conn.Close()
	}
	return err
}

func newRESPServer(t *testing.T, handler func([]string) string) (*respServer, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &respServer{ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go serveRESP(c, handler)
		}
	}()
	return s, ln.Addr().String()
}
func serveRESP(conn net.Conn, handler func([]string) string) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		command, err := readRESPCommand(reader)
		if err != nil {
			return
		}
		if _, err = conn.Write([]byte(handler(command))); err != nil {
			return
		}
	}
}
func readRESPCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil {
		return nil, err
	}
	command := make([]string, 0, count)
	for i := 0; i < count; i++ {
		lengthLine, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		length, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(lengthLine, "$")))
		if err != nil {
			return nil, err
		}
		value := make([]byte, length+2)
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}
		command = append(command, string(value[:length]))
	}
	return command, nil
}

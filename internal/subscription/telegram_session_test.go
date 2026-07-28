package subscription

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLockTelegramSessionSerializesSameSessionFile(t *testing.T) {
	const sessionFile = "data/telegram-session-lock-test.session"
	telegramSessionLocks.Delete(sessionFile)

	var holding atomic.Bool
	var overlaps atomic.Int64
	started := make(chan struct{}, 2)
	done := make(chan struct{})

	run := func() {
		unlock, err := lockTelegramSession(context.Background(), sessionFile)
		if err != nil {
			t.Errorf("lock telegram session: %v", err)
			return
		}
		defer unlock()
		started <- struct{}{}
		if holding.Swap(true) {
			overlaps.Add(1)
		}
		time.Sleep(80 * time.Millisecond)
		holding.Store(false)
	}

	go run()
	go run()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for lock holders to start")
		}
	}
	close(done)
	time.Sleep(50 * time.Millisecond)
	if overlaps.Load() != 0 {
		t.Fatalf("session lock allowed overlapping holders: %d", overlaps.Load())
	}
}

func TestLockTelegramSessionRespectsContextCancel(t *testing.T) {
	const sessionFile = "data/telegram-session-cancel-test.session"
	telegramSessionLocks.Delete(sessionFile)

	unlock, err := lockTelegramSession(context.Background(), sessionFile)
	if err != nil {
		t.Fatalf("lock telegram session: %v", err)
	}
	defer unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := lockTelegramSession(ctx, sessionFile); err == nil {
		t.Fatal("expected busy/canceled lock error")
	} else if err.Error() != "telegram session is busy" {
		t.Fatalf("error = %v, want telegram session is busy", err)
	}
}

func TestSchedulerMarkRunningRespectsGlobalConcurrencyLimit(t *testing.T) {
	s := &scheduler{
		running:           map[uint]struct{}{},
		maxConcurrentRuns: 2,
	}
	if !s.markRunning(1) || !s.markRunning(2) {
		t.Fatal("expected first two runs to start")
	}
	if s.markRunning(3) {
		t.Fatal("expected third run to be deferred by concurrency limit")
	}
	s.markDone(1)
	if !s.markRunning(3) {
		t.Fatal("expected deferred run to start after a slot freed")
	}
	if s.markRunning(1) {
		t.Fatal("expected duplicate subscription id to stay blocked")
	}
}

func TestLockMutexWithContextTryLockFastPath(t *testing.T) {
	var mu sync.Mutex
	if err := lockMutexWithContext(context.Background(), &mu); err != nil {
		t.Fatalf("trylock path: %v", err)
	}
	mu.Unlock()
}

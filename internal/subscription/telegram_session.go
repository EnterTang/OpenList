package subscription

import (
	"context"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
)

var telegramSessionLocks sync.Map // session path -> *telegramSessionGate

type telegramSessionGate struct {
	mu sync.Mutex
}

func telegramSessionGateFor(sessionFile string) *telegramSessionGate {
	key := filepath.Clean(sessionFile)
	value, _ := telegramSessionLocks.LoadOrStore(key, &telegramSessionGate{})
	return value.(*telegramSessionGate)
}

// lockTelegramSession serializes gotd clients that share one session file.
// The command timeout must start after this returns, otherwise queued callers
// burn their deadline while waiting for the lock.
func lockTelegramSession(ctx context.Context, sessionFile string) (func(), error) {
	gate := telegramSessionGateFor(sessionFile)
	if err := lockMutexWithContext(ctx, &gate.mu); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, errors.New("telegram session is busy")
		}
		return nil, err
	}
	return gate.mu.Unlock, nil
}

func lockMutexWithContext(ctx context.Context, mu *sync.Mutex) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if mu.TryLock() {
		return nil
	}
	done := make(chan struct{})
	go func() {
		mu.Lock()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		go func() {
			<-done
			mu.Unlock()
		}()
		return ctx.Err()
	}
}

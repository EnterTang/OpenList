package subscription

import (
	"context"
	"errors"
	"testing"
	"time"
)

type stubTelegramNetError struct {
	message   string
	timeout   bool
	temporary bool
}

func (e stubTelegramNetError) Error() string   { return e.message }
func (e stubTelegramNetError) Timeout() bool   { return e.timeout }
func (e stubTelegramNetError) Temporary() bool { return e.temporary }

func TestBuiltinTelegramRetryDelayCaps(t *testing.T) {
	if got := builtinTelegramRetryDelay(1); got != 0 {
		t.Fatalf("delay(1) = %s, want 0", got)
	}
	if got := builtinTelegramRetryDelay(2); got != 100*time.Millisecond {
		t.Fatalf("delay(2) = %s, want 100ms", got)
	}
	if got := builtinTelegramRetryDelay(3); got != 200*time.Millisecond {
		t.Fatalf("delay(3) = %s, want 200ms", got)
	}
	if got := builtinTelegramRetryDelay(99); got != 200*time.Millisecond {
		t.Fatalf("delay(99) = %s, want capped 200ms", got)
	}
}

func TestRunBuiltinTelegramRetryLoopRetriesTransientTimeoutsAtMostThreeTimes(t *testing.T) {
	var attempts int
	var sleeps []time.Duration
	err := runBuiltinTelegramRetryLoop(context.Background(), true, func(context.Context) error {
		attempts++
		return stubTelegramNetError{message: "i/o timeout", timeout: true}
	}, func(ctx context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	})
	if err == nil || err.Error() != "telegram request timed out" {
		t.Fatalf("error = %v, want telegram request timed out", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if len(sleeps) != 2 {
		t.Fatalf("sleep count = %d, want 2", len(sleeps))
	}
	if sleeps[0] != 100*time.Millisecond || sleeps[1] != 200*time.Millisecond {
		t.Fatalf("sleeps = %#v, want [100ms 200ms]", sleeps)
	}
}

func TestRunBuiltinTelegramRetryLoopDoesNotRetryWhenDisabled(t *testing.T) {
	var attempts int
	err := runBuiltinTelegramRetryLoop(context.Background(), false, func(context.Context) error {
		attempts++
		return stubTelegramNetError{message: "temporary network failure", temporary: true}
	}, func(context.Context, time.Duration) error {
		t.Fatal("sleep should not be called when retries are disabled")
		return nil
	})
	if err == nil || err.Error() != "temporary network failure" {
		t.Fatalf("error = %v, want temporary network failure", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestRunBuiltinTelegramRetryLoopDoesNotRetryPermanentErrors(t *testing.T) {
	var attempts int
	err := runBuiltinTelegramRetryLoop(context.Background(), true, func(context.Context) error {
		attempts++
		return errors.New("telegram is not logged in")
	}, func(context.Context, time.Duration) error {
		t.Fatal("sleep should not be called for permanent errors")
		return nil
	})
	if err == nil || err.Error() != "telegram is not logged in" {
		t.Fatalf("error = %v, want telegram is not logged in", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestRunBuiltinTelegramRetryLoopStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var attempts int
	var sleeps int
	err := runBuiltinTelegramRetryLoop(ctx, true, func(context.Context) error {
		attempts++
		return stubTelegramNetError{message: "temporary network failure", temporary: true}
	}, func(ctx context.Context, d time.Duration) error {
		sleeps++
		cancel()
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	if sleeps != 1 {
		t.Fatalf("sleep count = %d, want 1", sleeps)
	}
}

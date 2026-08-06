package _115sy

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLimiterWaitDisabledWhenRateNonPositive(t *testing.T) {
	t.Parallel()

	limiter := newAccountLimiter(0)
	if err := limiter.Wait(context.Background()); err != nil {
		t.Fatalf("Wait() error = %v, want nil when disabled", err)
	}
}

func TestLimiterClientDisablesPageCooldownWhenRateNonPositive(t *testing.T) {
	t.Parallel()

	client, err := NewClient(ClientOptions{
		LimitRate:    0,
		PageCooldown: time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	client.pageLimiter.MarkCompleted()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err := client.pageLimiter.WaitCooldown(ctx); err != nil {
		t.Fatalf("WaitCooldown() error = %v, want nil when limiter disabled", err)
	}
}

func TestLimiterWaitCooldownCancellation(t *testing.T) {
	t.Parallel()

	limiter := newPageLimiter(150 * time.Millisecond)
	limiter.MarkCompleted()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := limiter.WaitCooldown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitCooldown() error = %v, want deadline exceeded", err)
	}
}

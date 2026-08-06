package _115sy

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type accountLimiter struct {
	limiter *rate.Limiter
}

func newAccountLimiter(limit float64) *accountLimiter {
	if limit <= 0 {
		return &accountLimiter{}
	}
	return &accountLimiter{
		limiter: rate.NewLimiter(rate.Limit(limit), 1),
	}
}

func (l *accountLimiter) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if l == nil || l.limiter == nil {
		return nil
	}
	return l.limiter.Wait(ctx)
}

type pageLimiter struct {
	cooldown time.Duration

	mu          sync.Mutex
	nextAllowed time.Time
}

func newPageLimiter(cooldown time.Duration) *pageLimiter {
	return &pageLimiter{cooldown: cooldown}
}

func (l *pageLimiter) WaitCooldown(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if l == nil || l.cooldown <= 0 {
		return nil
	}

	l.mu.Lock()
	nextAllowed := l.nextAllowed
	l.mu.Unlock()

	if nextAllowed.IsZero() {
		return nil
	}

	wait := time.Until(nextAllowed)
	if wait <= 0 {
		return nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (l *pageLimiter) MarkCompleted() {
	if l == nil || l.cooldown <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	nextAllowed := time.Now().Add(l.cooldown)
	if nextAllowed.After(l.nextAllowed) {
		l.nextAllowed = nextAllowed
	}
}

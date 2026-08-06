package _115sy

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
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

func TestLimiterWaitCooldownReservesDistinctSlots(t *testing.T) {
	t.Parallel()

	const (
		callers  = 4
		cooldown = 40 * time.Millisecond
		slack    = 10 * time.Millisecond
	)

	limiter := newPageLimiter(cooldown)
	limiter.MarkCompleted()

	start := make(chan struct{})
	finishedAt := make([]time.Duration, callers)
	errs := make([]error, callers)
	testStart := time.Now()

	var wg sync.WaitGroup
	wg.Add(callers)
	for i := range callers {
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = limiter.WaitCooldown(context.Background())
			finishedAt[i] = time.Since(testStart)
		}(i)
	}

	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("WaitCooldown() caller %d error = %v, want nil", i, err)
		}
	}

	slices.Sort(finishedAt)

	for i := 1; i < len(finishedAt); i++ {
		gap := finishedAt[i] - finishedAt[i-1]
		if gap < cooldown-slack {
			t.Fatalf("completion gap %d = %v, want at least %v; completion times: %v", i, gap, cooldown-slack, finishedAt)
		}
	}

	totalSpan := finishedAt[len(finishedAt)-1] - finishedAt[0]
	minSpan := time.Duration(callers-1)*cooldown - slack
	if totalSpan < minSpan {
		t.Fatalf("completion span = %v, want at least %v; completion times: %v", totalSpan, minSpan, finishedAt)
	}
}

func TestSanitizeMessageRedactsRepeatedSensitiveTokens(t *testing.T) {
	t.Parallel()

	secrets := map[string]string{
		"receive_code":  "recv-secret",
		"security_code": "sec-secret",
		"cookie_header": "header-cookie",
		"cookie_param":  "param-cookie",
		"uid":           "uid-secret",
		"cid":           "cid-secret",
		"seid":          "seid-secret",
		"kid":           "kid-secret",
	}

	message := fmt.Sprintf(
		"receive_code=%[1]s again receive_code=%[1]s security_code=%[2]s repeat security_code=%[2]s Cookie: %[3]s Cookie: %[3]s Cookie=%[4]s Cookie=%[4]s UID=%[5]s UID=%[5]s CID=%[6]s CID=%[6]s SEID=%[7]s SEID=%[7]s KID=%[8]s KID=%[8]s",
		secrets["receive_code"],
		secrets["security_code"],
		secrets["cookie_header"],
		secrets["cookie_param"],
		secrets["uid"],
		secrets["cid"],
		secrets["seid"],
		secrets["kid"],
	)

	got := (&BusinessError{
		Profile:  "default",
		Endpoint: "/api",
		Errno:    123,
		Message:  message,
	}).Error()

	for name, secret := range secrets {
		if strings.Contains(got, secret) {
			t.Fatalf("redacted error still contains %s secret %q: %s", name, secret, got)
		}
	}

	wantSuffix := "receive_code=[REDACTED] again receive_code=[REDACTED] security_code=[REDACTED] repeat security_code=[REDACTED] Cookie: [REDACTED] Cookie: [REDACTED] Cookie=[REDACTED] Cookie=[REDACTED] UID=[REDACTED] UID=[REDACTED] CID=[REDACTED] CID=[REDACTED] SEID=[REDACTED] SEID=[REDACTED] KID=[REDACTED] KID=[REDACTED]"
	want := "default request /api failed with errno 123: " + wantSuffix
	if got != want {
		t.Fatalf("sanitized error mismatch\n got: %q\nwant: %q", got, want)
	}

	if count := strings.Count(got, "[REDACTED]"); count != 16 {
		t.Fatalf("redaction count = %d, want 16: %s", count, got)
	}
}

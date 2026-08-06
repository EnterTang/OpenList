package _115_cd2

import (
	"context"
	"fmt"
	"time"
)

// The stripped CloudDrive2 binary proves an "about to expired" refresh path,
// but does not retain the numeric lead time. Keep this safety margin explicit
// rather than presenting it as a recovered CD2 constant.
const (
	cd2RefreshLead       = 5 * time.Minute
	cd2RefreshRetryDelay = time.Minute
)

func accessTokenRefreshDue(now time.Time, expiresAt int64) bool {
	if expiresAt <= 0 {
		return false
	}
	return !time.Unix(expiresAt, 0).Add(-cd2RefreshLead).After(now)
}

func accessTokenExpiryUnix(now time.Time, expiresIn int64) int64 {
	if expiresIn <= 0 {
		return 0
	}
	return now.Add(time.Duration(expiresIn) * time.Second).Unix()
}

func (d *CD2) startTokenRefreshTask() {
	d.apiMu.Lock()
	ready := d.delegate != nil && d.Addition.RefreshToken != "" && d.Addition.AccessTokenExpiresAt > 0
	d.refreshRetryAt = time.Time{}
	d.apiMu.Unlock()
	if !ready {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	d.refreshStateMu.Lock()
	d.refreshCancel = cancel
	d.refreshDone = done
	d.refreshStateMu.Unlock()

	go func() {
		defer close(done)
		d.tokenRefreshLoop(ctx)
	}()
}

func (d *CD2) stopTokenRefreshTask() {
	d.refreshStateMu.Lock()
	cancel := d.refreshCancel
	done := d.refreshDone
	d.refreshCancel = nil
	d.refreshDone = nil
	d.refreshStateMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (d *CD2) tokenRefreshLoop(ctx context.Context) {
	for {
		wait, ok := d.nextTokenRefreshWait()
		if !ok {
			return
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}

		d.apiMu.Lock()
		if d.delegate == nil {
			d.apiMu.Unlock()
			return
		}
		_ = d.refreshAccessTokenIfDueLocked(ctx)
		d.apiMu.Unlock()
	}
}

func (d *CD2) nextTokenRefreshWait() (time.Duration, bool) {
	d.apiMu.Lock()
	defer d.apiMu.Unlock()

	if d.delegate == nil || d.Addition.RefreshToken == "" || d.Addition.AccessTokenExpiresAt <= 0 {
		return 0, false
	}
	now := time.Now()
	if d.refreshRetryAt.After(now) {
		return d.refreshRetryAt.Sub(now), true
	}
	refreshAt := time.Unix(d.Addition.AccessTokenExpiresAt, 0).Add(-cd2RefreshLead)
	if !refreshAt.After(now) {
		return 0, true
	}
	return refreshAt.Sub(now), true
}

func (d *CD2) refreshAccessTokenIfDueLocked(ctx context.Context) error {
	now := time.Now()
	if !accessTokenRefreshDue(now, d.Addition.AccessTokenExpiresAt) {
		return nil
	}
	if d.refreshRetryAt.After(now) {
		return nil
	}
	if err := d.throttler.wait(ctx, requestRESTAPI); err != nil {
		return err
	}

	expiresIn, err := d.delegate.RefreshToken(ctx)
	if err != nil {
		d.refreshRetryAt = time.Now().Add(cd2RefreshRetryDelay)
		return fmt.Errorf("115 CD2 background token refresh: %w", err)
	}
	d.refreshRetryAt = time.Time{}
	d.Addition.AccessTokenExpiresAt = accessTokenExpiryUnix(time.Now(), expiresIn)
	d.persistAuthenticationState()
	return nil
}

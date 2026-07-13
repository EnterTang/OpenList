package embeddedredis

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestAcquireInstallLockHonorsContextDeadline(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".install.lock")
	owner, err := acquireInstallLock(path)
	if err != nil {
		t.Fatal(err)
	}
	ownerHeld := true
	defer func() {
		if ownerHeld {
			if err := owner.release(); err != nil {
				t.Errorf("release owner lock: %v", err)
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	contender, err := acquireInstallLockContext(ctx, path)
	if contender != nil {
		_ = contender.release()
		t.Fatal("acquired an install lock already held by another owner")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquireInstallLockContext error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("lock acquisition ignored context deadline: %v", elapsed)
	}

	if err := owner.release(); err != nil {
		t.Errorf("release owner lock: %v", err)
	}
	ownerHeld = false
	reacquired, err := acquireInstallLock(path)
	if err != nil {
		t.Fatalf("reacquire install lock after deadline: %v", err)
	}
	if err := reacquired.release(); err != nil {
		t.Fatalf("release reacquired install lock: %v", err)
	}
}

package embeddedredis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

type installLock struct{ file *os.File }

func acquireInstallLock(path string) (*installLock, error) {
	return acquireInstallLockContext(context.Background(), path)
}

func acquireInstallLockContext(ctx context.Context, path string) (*installLock, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("lock runtime installation: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open runtime install lock: %w", err)
	}
	for {
		locked, err := tryLockFile(file)
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("lock runtime installation: %w", err)
		}
		if locked {
			return &installLock{file: file}, nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			_ = file.Close()
			return nil, fmt.Errorf("lock runtime installation: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func (lock *installLock) release() error {
	return errors.Join(unlockFile(lock.file), lock.file.Close())
}

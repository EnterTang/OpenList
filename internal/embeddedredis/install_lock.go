package embeddedredis

import (
	"errors"
	"fmt"
	"os"
)

type installLock struct{ file *os.File }

func acquireInstallLock(path string) (*installLock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open runtime install lock: %w", err)
	}
	if err := lockFile(file); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock runtime installation: %w", err)
	}
	return &installLock{file: file}, nil
}

func (lock *installLock) release() error {
	return errors.Join(unlockFile(lock.file), lock.file.Close())
}

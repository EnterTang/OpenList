//go:build !windows && !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !solaris

package embeddedredis

import (
	"os"
	"sync"
)

// fallbackInstallMutex protects extraction within one process on platforms where
// x/sys does not expose a crash-safe cross-process file lock. Embedded payload
// management is Windows-only; this fallback keeps the package portable.
var fallbackInstallMutex sync.Mutex

func tryLockFile(_ *os.File) (bool, error) { return fallbackInstallMutex.TryLock(), nil }

func unlockFile(_ *os.File) error {
	fallbackInstallMutex.Unlock()
	return nil
}

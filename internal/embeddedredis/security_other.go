//go:build !windows

package embeddedredis

import "os"

func secureDirectory(path string) error { return os.Chmod(path, 0700) }

func secureFile(path string) error { return os.Chmod(path, 0600) }

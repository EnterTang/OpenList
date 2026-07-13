//go:build !windows

package embeddedredis

import (
	"os"
	"os/exec"
)

func configureManagedCommand(*exec.Cmd) {}

func replaceFileAtomic(from, to string) error { return os.Rename(from, to) }

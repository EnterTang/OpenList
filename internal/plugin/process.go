package plugin

import (
	"fmt"
	"os"
	"path/filepath"
)

type ProcessOptions struct {
	AntiHash  bool
	ISORename bool
	Whitelist string
}

func ProcessAbsolutePath(absPath string, opts ProcessOptions) (string, error) {
	if !opts.AntiHash && !opts.ISORename {
		return absPath, nil
	}
	info, err := os.Stat(absPath)
	if err != nil {
		return absPath, err
	}
	if info.IsDir() {
		return absPath, nil
	}
	base := filepath.Base(absPath)
	if IsTempIncompleteName(base) {
		return absPath, nil
	}
	wl := ParseWhitelist(opts.Whitelist)
	if !ExtensionAllowed(base, wl) {
		return absPath, nil
	}
	result := absPath
	if opts.AntiHash {
		if _, err := ModifyHash(result); err != nil {
			return result, fmt.Errorf("antihash: %w", err)
		}
	}
	if opts.ISORename {
		newPath, _, err := RenameToISO(result)
		if err != nil {
			return result, fmt.Errorf("iso rename: %w", err)
		}
		result = newPath
	}
	return result, nil
}

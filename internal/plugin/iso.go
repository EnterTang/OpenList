package plugin

import (
	"os"
	"path/filepath"
	"strings"
)

func ISOTargetName(name string) (string, bool) {
	if name == "" {
		return "", false
	}
	if strings.HasSuffix(strings.ToLower(name), ".iso") {
		return "", false
	}
	return name + ".iso", true
}

func RenameToISO(absPath string) (string, bool, error) {
	base := filepath.Base(absPath)
	targetName, ok := ISOTargetName(base)
	if !ok {
		return absPath, false, nil
	}
	dst := filepath.Join(filepath.Dir(absPath), targetName)
	if _, err := os.Stat(dst); err == nil {
		return absPath, false, nil // conflict: skip
	} else if !os.IsNotExist(err) {
		return absPath, false, err
	}
	if err := os.Rename(absPath, dst); err != nil {
		return absPath, false, err
	}
	return dst, true, nil
}

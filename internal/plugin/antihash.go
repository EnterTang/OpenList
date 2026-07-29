package plugin

import (
	"bytes"
	"os"
	"sync"
)

var (
	MagicTag = []byte("ANTIHASH") // 8 bytes
	Padding  = []byte("OpenList") // 8 bytes
)

const TrailerSize = 16

var fileLocks sync.Map // path -> *sync.Mutex

func lockPath(path string) func() {
	v, _ := fileLocks.LoadOrStore(path, &sync.Mutex{})
	m := v.(*sync.Mutex)
	m.Lock()
	return m.Unlock
}

func IsModified(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	if info.Size() < int64(len(MagicTag)) {
		return false, nil
	}
	buf := make([]byte, len(MagicTag))
	if _, err := f.ReadAt(buf, info.Size()-int64(len(MagicTag))); err != nil {
		return false, err
	}
	return bytes.Equal(buf, MagicTag), nil
}

func ModifyHash(path string) (bool, error) {
	unlock := lockPath(path)
	defer unlock()
	ok, err := IsModified(path)
	if err != nil {
		return false, err
	}
	if ok {
		return false, nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return false, err
	}
	defer f.Close()
	payload := append(append([]byte{}, Padding...), MagicTag...)
	if _, err := f.Write(payload); err != nil {
		return false, err
	}
	return true, nil
}

func RestoreHash(path string) (bool, error) {
	unlock := lockPath(path)
	defer unlock()
	ok, err := IsModified(path)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	if info.Size() < TrailerSize {
		return false, nil
	}
	if err := os.Truncate(path, info.Size()-TrailerSize); err != nil {
		return false, err
	}
	return true, nil
}

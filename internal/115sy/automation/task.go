package automation

import (
	"fmt"
	"strings"
	"sync"
)

type Task struct {
	mu       sync.RWMutex
	Status   string  `json:"status"`
	Progress float64 `json:"progress"`
	Error    string  `json:"error,omitempty"`
}

type TaskSnapshot struct {
	Status   string  `json:"status"`
	Progress float64 `json:"progress"`
	Error    string  `json:"error,omitempty"`
}

func (t *Task) Update(status string, progress float64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if progress < 0 {
		progress = 0
	}
	if progress > 100 {
		progress = 100
	}
	t.Status, t.Progress = status, progress
	if err != nil {
		t.Error = redact(err.Error())
	} else {
		t.Error = ""
	}
}

func (t *Task) Snapshot() TaskSnapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return TaskSnapshot{Status: t.Status, Progress: t.Progress, Error: t.Error}
}

func redact(value string) string {
	for _, key := range []string{"UID=", "CID=", "SEID=", "KID=", "password=", "security_code="} {
		if index := strings.Index(value, key); index >= 0 {
			value = value[:index] + key + "[REDACTED]"
		}
	}
	return fmt.Sprint(value)
}

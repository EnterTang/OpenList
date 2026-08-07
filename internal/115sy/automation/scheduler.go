package automation

import (
	"fmt"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/pkg/cron"
)

type Scheduler struct {
	mu      sync.Mutex
	cron    *cron.Cron
	run     func()
	started bool
}

func NewScheduler(interval time.Duration, run func()) (*Scheduler, error) {
	if interval <= 0 {
		return &Scheduler{}, nil
	}
	if run == nil {
		return nil, fmt.Errorf("scheduler callback is required")
	}
	return &Scheduler{cron: cron.NewCron(interval), run: run}, nil
}

func (s *Scheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cron == nil || s.run == nil || s.started {
		return
	}
	s.started = true
	s.cron.Do(s.run)
}

func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cron != nil {
		s.cron.Stop()
		s.cron = nil
	}
	s.started = false
}

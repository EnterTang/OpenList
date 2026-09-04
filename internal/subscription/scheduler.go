package subscription

import (
	"context"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	log "github.com/sirupsen/logrus"
)

const (
	defaultMaxConcurrentSubscriptionRuns = 2
	// A source/provider outage must not hold a scheduler slot forever. Cluster
	// jobs have their own durable recovery, while this bounds the discovery and
	// dispatch goroutine that owns the in-memory slot.
	defaultSubscriptionRunTimeout = 30 * time.Minute
)

var defaultScheduler = &scheduler{
	stop:              make(chan struct{}),
	running:           map[uint]struct{}{},
	maxConcurrentRuns: defaultMaxConcurrentSubscriptionRuns,
}

type scheduler struct {
	mu                sync.Mutex
	started           bool
	stop              chan struct{}
	running           map[uint]struct{}
	maxConcurrentRuns int
}

func StartScheduler() {
	defaultScheduler.start()
}

func StopScheduler() {
	defaultScheduler.stopLoop()
}

func (s *scheduler) start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return
	}
	s.started = true
	s.stop = make(chan struct{})
	go s.loop()
}

func (s *scheduler) stopLoop() {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return
	}
	close(s.stop)
	s.started = false
	s.mu.Unlock()
}

func (s *scheduler) loop() {
	if !s.waitForStoragesLoaded() {
		return
	}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	s.tick()
	for {
		select {
		case <-ticker.C:
			s.tick()
		case <-s.stop:
			return
		}
	}
}

func (s *scheduler) waitForStoragesLoaded() bool {
	select {
	case <-conf.StoragesLoadSignal():
		return true
	case <-s.stop:
		return false
	}
}

func (s *scheduler) tick() {
	items, err := db.ListActiveSubscriptions()
	if err != nil {
		log.Errorf("subscription scheduler list failed: %+v", err)
		return
	}
	now := time.Now()
	for _, item := range items {
		if _, err := ReconcileSubscriptionExecution(context.Background(), item.ID); err != nil {
			log.Errorf("subscription %d execution reconciliation failed: %+v", item.ID, err)
			continue
		}
		followupAction, err := subscriptionExecutionFollowupActionFor(context.Background(), item.ID)
		if err != nil {
			log.Errorf("subscription %d follow-up check failed: %+v", item.ID, err)
			continue
		}
		interval := item.CheckIntervalMinutes
		if interval <= 0 {
			interval = 60
		}
		if followupAction == subscriptionFollowupNone && item.LastCheckedAt != nil && now.Sub(*item.LastCheckedAt) < time.Duration(interval)*time.Minute {
			continue
		}
		if !s.markRunning(item.ID) {
			log.Debugf("subscription %d skipped: scheduler concurrency limit reached", item.ID)
			continue
		}
		go func(id uint, action subscriptionExecutionFollowupAction) {
			defer s.markDone(id)
			runCtx, cancel := context.WithTimeout(context.Background(), defaultSubscriptionRunTimeout)
			defer cancel()
			var err error
			if action == subscriptionFollowupClusterJob {
				_, err = RetryFailedForRole(runCtx, id, conf.Conf.Cluster.Role)
			} else {
				_, err = RunForRole(runCtx, id, true, conf.Conf.Cluster.Role)
			}
			if err != nil {
				log.Errorf("subscription %d run failed: %+v", id, err)
			}
		}(item.ID, followupAction)
	}
}

func (s *scheduler) markRunning(id uint) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.running[id]; ok {
		return false
	}
	limit := s.maxConcurrentRuns
	if limit <= 0 {
		limit = defaultMaxConcurrentSubscriptionRuns
	}
	if len(s.running) >= limit {
		return false
	}
	s.running[id] = struct{}{}
	return true
}

func (s *scheduler) markDone(id uint) {
	s.mu.Lock()
	delete(s.running, id)
	s.mu.Unlock()
}

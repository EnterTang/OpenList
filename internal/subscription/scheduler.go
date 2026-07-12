package subscription

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	log "github.com/sirupsen/logrus"
)

var defaultScheduler = &scheduler{
	stop:    make(chan struct{}),
	running: map[uint]struct{}{},
}

type scheduler struct {
	mu      sync.Mutex
	started bool
	stop    chan struct{}
	running map[uint]struct{}
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
		interval := item.CheckIntervalMinutes
		if interval <= 0 {
			interval = 60
		}
		if item.LastCheckedAt != nil && now.Sub(*item.LastCheckedAt) < time.Duration(interval)*time.Minute {
			continue
		}
		if !s.markRunning(item.ID) {
			continue
		}
		go func(id uint) {
			defer s.markDone(id)
			var err error
			if schedulerTransfersLocally(conf.Conf.Cluster.Role) {
				_, err = Run(context.Background(), id, true)
			} else {
				_, err = RunCluster(context.Background(), id)
			}
			if err != nil {
				log.Errorf("subscription %d run failed: %+v", id, err)
			}
		}(item.ID)
	}
}

func schedulerTransfersLocally(role string) bool {
	role = strings.ToLower(strings.TrimSpace(role))
	return role == "" || role == model.ClusterRoleStandalone
}

func (s *scheduler) markRunning(id uint) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.running[id]; ok {
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

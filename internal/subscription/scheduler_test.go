package subscription

import (
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func resetStoragesLoadSignalForTest(t *testing.T) {
	t.Helper()
	conf.SendStoragesLoadedSignal()
	conf.ResetStoragesLoadSignal()
	t.Cleanup(func() {
		conf.SendStoragesLoadedSignal()
	})
}

func TestSchedulerWaitsForStoragesLoadedSignal(t *testing.T) {
	resetStoragesLoadSignalForTest(t)

	s := &scheduler{stop: make(chan struct{})}
	done := make(chan bool, 1)
	go func() {
		done <- s.waitForStoragesLoaded()
	}()

	select {
	case <-done:
		t.Fatal("scheduler continued before storages finished loading")
	case <-time.After(50 * time.Millisecond):
	}

	conf.SendStoragesLoadedSignal()

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("scheduler wait returned false after storages loaded")
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler did not continue after storages loaded")
	}
}

func TestSchedulerStorageWaitStops(t *testing.T) {
	resetStoragesLoadSignalForTest(t)

	s := &scheduler{stop: make(chan struct{})}
	close(s.stop)

	if s.waitForStoragesLoaded() {
		t.Fatal("scheduler wait returned true after stop signal")
	}
}

func TestSchedulerMarkRunningUsesDefaultConcurrencyLimit(t *testing.T) {
	s := &scheduler{running: map[uint]struct{}{}}
	for id := uint(1); id <= defaultMaxConcurrentSubscriptionRuns; id++ {
		if !s.markRunning(id) {
			t.Fatalf("expected run %d to start under default limit", id)
		}
	}
	if s.markRunning(defaultMaxConcurrentSubscriptionRuns + 1) {
		t.Fatal("expected default concurrency limit to defer extra runs")
	}
}

func TestSubscriptionExecutionTransfersLocallyOnlyForStandaloneRole(t *testing.T) {
	for _, role := range []string{"", model.ClusterRoleStandalone, " STANDALONE "} {
		if !subscriptionTransfersLocally(role) {
			t.Fatalf("role %q should keep local transfer behavior", role)
		}
	}
	for _, role := range []string{model.ClusterRoleCoordinator, model.ClusterRoleWorker, model.ClusterRoleHybrid} {
		if subscriptionTransfersLocally(role) {
			t.Fatalf("%s scheduler must not bypass cluster dispatch with a local transfer", role)
		}
	}
}

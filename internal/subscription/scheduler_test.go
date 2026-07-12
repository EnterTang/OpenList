package subscription

import (
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
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

func TestClusterCoordinatorSchedulerDoesNotTransferLocally(t *testing.T) {
	if !schedulerTransfersLocally("standalone") {
		t.Fatal("standalone scheduler should keep local transfer behavior")
	}
	for _, role := range []string{"coordinator", "worker", "hybrid"} {
		if schedulerTransfersLocally(role) {
			t.Fatalf("%s scheduler must not bypass cluster dispatch with a local transfer", role)
		}
	}
}

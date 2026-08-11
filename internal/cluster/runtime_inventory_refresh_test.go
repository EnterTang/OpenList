package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/transport"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
)

func TestRefreshInventoryAfterStoragesLoadedWaitsForStorageInitialization(t *testing.T) {
	resetPendingStoragesLoadSignal(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	refreshed := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		refreshInventoryAfterStoragesLoaded(ctx, func(context.Context) error {
			refreshed <- struct{}{}
			return transport.ErrNotConnected
		})
	}()

	select {
	case <-refreshed:
		t.Fatal("inventory refreshed before storage initialization completed")
	case <-time.After(50 * time.Millisecond):
	}

	conf.SendStoragesLoadedSignal()
	select {
	case <-refreshed:
	case <-time.After(time.Second):
		t.Fatal("inventory was not refreshed after storage initialization completed")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("inventory refresh did not return")
	}
}

func TestRefreshInventoryAfterStoragesLoadedStopsWhenWorkerStops(t *testing.T) {
	resetPendingStoragesLoadSignal(t)
	ctx, cancel := context.WithCancel(context.Background())
	refreshed := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		refreshInventoryAfterStoragesLoaded(ctx, func(context.Context) error {
			refreshed <- struct{}{}
			return nil
		})
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("inventory refresh did not stop with worker context")
	}
	select {
	case <-refreshed:
		t.Fatal("inventory refreshed after worker stopped")
	default:
	}
}

func TestRunWorkerInventoryRefreshLoopRefreshesPeriodically(t *testing.T) {
	resetPendingStoragesLoadSignal(t)
	oldInterval := workerInventoryRefreshInterval
	workerInventoryRefreshInterval = 10 * time.Millisecond
	t.Cleanup(func() { workerInventoryRefreshInterval = oldInterval })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	refreshed := make(chan struct{}, 4)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runWorkerInventoryRefreshLoop(ctx, func(context.Context) error {
			refreshed <- struct{}{}
			return nil
		})
	}()
	conf.SendStoragesLoadedSignal()
	for i := 0; i < 2; i++ {
		select {
		case <-refreshed:
		case <-time.After(time.Second):
			t.Fatalf("inventory refresh %d did not occur", i+1)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("inventory refresh loop did not stop")
	}
}

func resetPendingStoragesLoadSignal(t *testing.T) {
	t.Helper()
	conf.SendStoragesLoadedSignal()
	conf.ResetStoragesLoadSignal()
	t.Cleanup(conf.SendStoragesLoadedSignal)
}

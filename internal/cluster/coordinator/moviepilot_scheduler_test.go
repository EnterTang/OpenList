package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/moviepilotbridge"
	"gorm.io/gorm"
)

func seedMoviePilotAdmissionRoute(t *testing.T, database *gorm.DB, workerID, sessionID string, route protocol.MoviePilotRouteInventory) {
	t.Helper()
	capabilities, err := json.Marshal(protocol.NodeCapabilities{MoviePilotRoutes: []protocol.MoviePilotRouteInventory{route}})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{
		&model.ClusterNode{ID: workerID, Status: model.ClusterNodeStatusOnline, LastSessionID: sessionID},
		&model.ClusterNodeSession{ID: sessionID, NodeID: workerID, Status: model.ClusterSessionStatusConnected},
		&model.ClusterNodeInventory{
			ID: "inventory-" + workerID, NodeID: workerID, Revision: 1,
			CollectedAt: time.Now().UTC(), CapabilitiesJSON: string(capabilities),
		},
	} {
		if err := database.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
}

func TestChooseMoviePilotAdmissionCandidatePrefersPostReservationFreeSpace(t *testing.T) {
	candidates := []moviePilotAdmissionCandidate{
		{
			routeID:        "route-a",
			availableBytes: 120,
			route: protocol.MoviePilotRouteInventory{
				DownloadFreeBytes: 120, DownloadLowWatermarkBytes: 10,
				DownloadSafetyReserveBytes: 10, DownloadConcurrency: 4,
				DownloadActiveCount: 0, DownloadLoadKnown: true,
			},
		},
		{
			routeID:        "route-b",
			availableBytes: 220,
			route: protocol.MoviePilotRouteInventory{
				DownloadFreeBytes: 220, DownloadLowWatermarkBytes: 10,
				DownloadSafetyReserveBytes: 10, DownloadConcurrency: 4,
				DownloadActiveCount: 1, DownloadLoadKnown: true,
			},
		},
	}

	selected, reason := chooseMoviePilotAdmissionCandidate(candidates, 50)
	if reason != "" || selected == nil || selected.routeID != "route-b" {
		t.Fatalf("selected = %#v, reason = %q", selected, reason)
	}
}

func TestSelectMoviePilotDownloaderPolicyReservesAndIsIdempotent(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	seedMoviePilotAdmissionRoute(t, database, "worker-a", "session-a", protocol.MoviePilotRouteInventory{
		BridgeInstanceID: "bridge-main", Downloader: "qb-a", QBClientID: "qb-a", QBHealth: "healthy",
		DownloadCapacityKnown: true, DownloadFreeBytes: 100 << 30, DownloadLowWatermarkBytes: 1 << 30,
		DownloadSafetyReserveBytes: 1 << 30, DownloadConcurrency: 2, DownloadLoadKnown: true,
	})
	seedMoviePilotAdmissionRoute(t, database, "worker-b", "session-b", protocol.MoviePilotRouteInventory{
		BridgeInstanceID: "bridge-main", Downloader: "qb-b", QBClientID: "qb-b", QBHealth: "healthy",
		DownloadCapacityKnown: true, DownloadFreeBytes: 200 << 30, DownloadLowWatermarkBytes: 1 << 30,
		DownloadSafetyReserveBytes: 1 << 30, DownloadConcurrency: 2, DownloadLoadKnown: true,
	})

	service := New(database, "")
	service.SetMoviePilotDownloaderPolicyMode(moviepilotbridge.DownloaderPolicyCoordinatorSelect)
	policy, err := service.SelectMoviePilotDownloaderPolicy(context.Background(), "bridge-main", "request-1", 10<<30)
	if err != nil {
		t.Fatalf("select downloader policy: %v", err)
	}
	if policy.Mode != moviepilotbridge.DownloaderPolicyCoordinatorSelect || policy.Downloader != "qb-b" || policy.RouteID == "" || policy.ReservationID == "" {
		t.Fatalf("policy = %#v", policy)
	}
	var reservation model.MoviePilotDownloaderReservation
	if err := database.First(&reservation, "request_id = ?", "request-1").Error; err != nil {
		t.Fatal(err)
	}
	if reservation.Status != model.MoviePilotReservationStatusReserved || reservation.WorkerNodeID != "worker-b" || reservation.ExpectedBytes != 10<<30 {
		t.Fatalf("reservation = %#v", reservation)
	}

	repeated, err := service.SelectMoviePilotDownloaderPolicy(context.Background(), "bridge-main", "request-1", 10<<30)
	if err != nil {
		t.Fatalf("repeat selection: %v", err)
	}
	if repeated.ReservationID != policy.ReservationID || repeated.Downloader != policy.Downloader {
		t.Fatalf("repeat policy = %#v, initial = %#v", repeated, policy)
	}
}

func TestSelectMoviePilotDownloaderPolicyHonorsReservationsAndStrictCapacity(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	seedMoviePilotAdmissionRoute(t, database, "worker-a", "session-a", protocol.MoviePilotRouteInventory{
		BridgeInstanceID: "bridge-capacity", Downloader: "qb-a", QBClientID: "qb-a", QBHealth: "healthy",
		DownloadCapacityKnown: true, DownloadFreeBytes: 100, DownloadConcurrency: 3, DownloadLoadKnown: true,
	})
	service := New(database, "")
	service.SetMoviePilotDownloaderPolicyMode(moviepilotbridge.DownloaderPolicyCoordinatorSelect)
	first, err := service.SelectMoviePilotDownloaderPolicy(context.Background(), "bridge-capacity", "request-large", 80)
	if err != nil {
		t.Fatalf("first reservation: %v", err)
	}
	if _, err := service.SelectMoviePilotDownloaderPolicy(context.Background(), "bridge-capacity", "request-too-large", 30); !errors.Is(err, errMoviePilotCapacityUnavailable) {
		t.Fatalf("second reservation error = %v, want capacity error", err)
	}
	if err := service.ReleaseMoviePilotDownloaderReservation(context.Background(), "request-large"); err != nil {
		t.Fatalf("release reservation: %v", err)
	}
	retry, err := service.SelectMoviePilotDownloaderPolicy(context.Background(), "bridge-capacity", "request-large", 80)
	if err != nil {
		t.Fatalf("reuse released reservation: %v", err)
	}
	if retry.ReservationID != first.ReservationID || retry.Downloader != first.Downloader {
		t.Fatalf("reused policy = %#v, first = %#v", retry, first)
	}
	var reservation model.MoviePilotDownloaderReservation
	if err := database.First(&reservation, "request_id = ?", "request-large").Error; err != nil {
		t.Fatal(err)
	}
	if reservation.Status != model.MoviePilotReservationStatusReserved {
		t.Fatalf("reused reservation status = %q", reservation.Status)
	}
}

func TestSelectMoviePilotDownloaderPolicyPreferredFallsBackWithoutFreshTelemetry(t *testing.T) {
	database := openTorrentTransferTestDB(t)
	seedMoviePilotAdmissionRoute(t, database, "worker-no-telemetry", "session-no-telemetry", protocol.MoviePilotRouteInventory{
		BridgeInstanceID: "bridge-preferred", Downloader: "qb-a", QBClientID: "qb-a", QBHealth: "healthy",
		DownloadCapacityKnown: true, DownloadFreeBytes: 100 << 30,
	})
	service := New(database, "")
	service.SetMoviePilotDownloaderPolicyMode(moviepilotbridge.DownloaderPolicyCoordinatorPreferred)

	policy, err := service.SelectMoviePilotDownloaderPolicy(context.Background(), "bridge-preferred", "request-preferred", 10<<30)
	if err != nil {
		t.Fatalf("preferred selection: %v", err)
	}
	if policy.Mode != moviepilotbridge.DownloaderPolicyMoviePilotSelect || !strings.Contains(policy.FallbackReason, "load telemetry") {
		t.Fatalf("fallback policy = %#v", policy)
	}
	var count int64
	if err := database.Model(&model.MoviePilotDownloaderReservation{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("fallback unexpectedly created %d reservations", count)
	}
}

func TestMoviePilotRouteIDIsStableAndScoped(t *testing.T) {
	left := moviePilotRouteID("bridge", "worker", "qb", "client")
	right := moviePilotRouteID("bridge", "worker", "qb", "client")
	other := moviePilotRouteID("bridge", "worker-other", "qb", "client")
	if left == "" || left != right || left == other || !strings.HasPrefix(left, "route-") {
		t.Fatalf("route IDs = %q, %q, %q", left, right, other)
	}
	if len(left) != len("route-")+24 {
		t.Fatalf("route ID length = %d", len(left))
	}
}

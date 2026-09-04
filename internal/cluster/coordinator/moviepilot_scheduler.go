package coordinator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/cluster/protocol"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/moviepilotbridge"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	moviePilotDownloaderReservationTTL = 15 * time.Minute
	// Selection persists a reservation before the Bridge service persists its
	// intent. Keep a short handoff window so the reaper cannot reclaim a valid
	// reservation between those two writes.
	moviePilotReservationOrphanGrace = time.Minute
	// A missing resource size must still consume a meaningful admission
	// budget. The selected MoviePilot search result normally supplies a size.
	moviePilotUnknownReservationBytes int64 = 1 << 30
	moviePilotInventoryMaxAge               = 10 * time.Minute
	moviePilotMaxInt64                int64 = int64(^uint64(0) >> 1)
)

var errMoviePilotCapacityUnavailable = moviepilotbridge.ErrDownloaderCapacityUnavailable

type moviePilotAdmissionCandidate struct {
	route          protocol.MoviePilotRouteInventory
	workerID       string
	routeID        string
	reservedBytes  int64
	reservedCount  int
	availableBytes int64
}

// SelectMoviePilotDownloaderPolicy is called immediately before a Bridge
// intent is persisted. It selects and reserves one fresh Worker/qB route, so
// MoviePilot receives a downloader that the Coordinator can later bind back
// to the same Worker.
func (s *Service) SelectMoviePilotDownloaderPolicy(ctx context.Context, bridgeID, requestID string, expectedBytes int64) (moviepilotbridge.DownloaderPolicy, error) {
	return s.selectMoviePilotDownloaderPolicy(ctx, bridgeID, requestID, "", expectedBytes, "")
}

// SelectMoviePilotDownloaderPolicyAutomatically selects a qB route using the
// Coordinator's admission factors and never falls back to MoviePilot's native
// downloader selection.
func (s *Service) SelectMoviePilotDownloaderPolicyAutomatically(ctx context.Context, bridgeID, requestID string, expectedBytes int64) (moviepilotbridge.DownloaderPolicy, error) {
	return s.selectMoviePilotDownloaderPolicy(ctx, bridgeID, requestID, "", expectedBytes, moviepilotbridge.DownloaderPolicyCoordinatorSelect)
}

// SelectMoviePilotDownloaderPolicyForDownloader reserves the requested qB
// route without allowing the global policy to fall back to MoviePilot's native
// downloader selection.
func (s *Service) SelectMoviePilotDownloaderPolicyForDownloader(ctx context.Context, bridgeID, requestID, requestedDownloader string, expectedBytes int64) (moviepilotbridge.DownloaderPolicy, error) {
	requestedDownloader = strings.TrimSpace(requestedDownloader)
	if requestedDownloader == "" {
		return moviepilotbridge.DownloaderPolicy{}, errors.New("downloader is required for manual MoviePilot selection")
	}
	return s.selectMoviePilotDownloaderPolicy(ctx, bridgeID, requestID, requestedDownloader, expectedBytes, moviepilotbridge.DownloaderPolicyCoordinatorSelect)
}

// ListMoviePilotDownloaderBridgeInstanceIDs returns the MoviePilot Bridges
// that currently advertise a downloader alias in Worker inventory. Subscription
// binding uses this before searching resources so a manually selected qB route
// cannot be paired with a resource from another Bridge instance.
func (s *Service) ListMoviePilotDownloaderBridgeInstanceIDs(ctx context.Context, requestedDownloader string) ([]string, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("cluster database is unavailable")
	}
	requestedDownloader = strings.TrimSpace(requestedDownloader)
	if requestedDownloader == "" {
		return nil, errors.New("downloader is required for MoviePilot Bridge lookup")
	}

	var inventories []model.ClusterNodeInventory
	if err := s.db.WithContext(ctx).Order("node_id ASC, revision DESC").Find(&inventories).Error; err != nil {
		return nil, err
	}
	seenNodes := make(map[string]struct{})
	bridgeIDs := make(map[string]struct{})
	for _, inventory := range inventories {
		if _, seen := seenNodes[inventory.NodeID]; seen {
			continue
		}
		seenNodes[inventory.NodeID] = struct{}{}
		var capabilities protocol.NodeCapabilities
		if err := jsonUnmarshalCapabilities(inventory.CapabilitiesJSON, &capabilities); err != nil {
			continue
		}
		for _, route := range capabilities.MoviePilotRoutes {
			if strings.EqualFold(strings.TrimSpace(route.Downloader), requestedDownloader) && strings.TrimSpace(route.BridgeInstanceID) != "" {
				bridgeIDs[strings.TrimSpace(route.BridgeInstanceID)] = struct{}{}
			}
		}
	}
	result := make([]string, 0, len(bridgeIDs))
	for bridgeID := range bridgeIDs {
		result = append(result, bridgeID)
	}
	sort.Strings(result)
	return result, nil
}

func (s *Service) selectMoviePilotDownloaderPolicy(ctx context.Context, bridgeID, requestID, requestedDownloader string, expectedBytes int64, modeOverride string) (moviepilotbridge.DownloaderPolicy, error) {
	if s == nil || s.db == nil {
		return moviepilotbridge.DownloaderPolicy{}, errors.New("cluster database is unavailable")
	}
	bridgeID = strings.TrimSpace(bridgeID)
	requestID = strings.TrimSpace(requestID)
	if bridgeID == "" || requestID == "" {
		return moviepilotbridge.DownloaderPolicy{}, errors.New("bridge_id and request_id are required for downloader selection")
	}
	mode := strings.TrimSpace(modeOverride)
	if mode == "" {
		mode = s.moviePilotDownloaderPolicyMode()
	}
	if requestedDownloader != "" {
		mode = moviepilotbridge.DownloaderPolicyCoordinatorSelect
	} else if mode == moviepilotbridge.DownloaderPolicyMoviePilotSelect {
		return moviepilotbridge.DownloaderPolicy{Mode: moviepilotbridge.DownloaderPolicyMoviePilotSelect}, nil
	}
	if expectedBytes <= 0 {
		expectedBytes = moviePilotUnknownReservationBytes
	}

	s.moviePilotReservationMu.Lock()
	defer s.moviePilotReservationMu.Unlock()
	if err := s.reapMoviePilotDownloaderReservationsLocked(ctx, time.Now().UTC()); err != nil {
		return moviepilotbridge.DownloaderPolicy{}, err
	}
	var existing model.MoviePilotDownloaderReservation
	existingID := ""
	if err := s.db.WithContext(ctx).Where("request_id = ?", requestID).First(&existing).Error; err == nil {
		if existing.Status == model.MoviePilotReservationStatusReserved && existing.ExpiresAt.After(time.Now().UTC()) {
			return moviePilotPolicyFromReservation(existing), nil
		}
		if existing.Status == model.MoviePilotReservationStatusBound {
			return moviePilotPolicyFromReservation(existing), nil
		}
		if !strings.EqualFold(strings.TrimSpace(existing.BridgeInstanceID), bridgeID) {
			return moviepilotbridge.DownloaderPolicy{}, fmt.Errorf("request id %q already belongs to another MoviePilot Bridge", requestID)
		}
		existingID = existing.ID
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return moviepilotbridge.DownloaderPolicy{}, err
	}

	candidates, reason, err := s.moviePilotAdmissionCandidates(ctx, bridgeID, time.Now().UTC())
	if err != nil {
		return moviepilotbridge.DownloaderPolicy{}, err
	}
	if requestedDownloader != "" {
		filtered := candidates[:0]
		for _, candidate := range candidates {
			if strings.EqualFold(strings.TrimSpace(candidate.route.Downloader), requestedDownloader) {
				filtered = append(filtered, candidate)
			}
		}
		candidates = filtered
		if len(candidates) == 0 {
			return moviepilotbridge.DownloaderPolicy{}, fmt.Errorf("%w: requested downloader %q is not available for MoviePilot Bridge %q", errMoviePilotCapacityUnavailable, requestedDownloader, bridgeID)
		}
	}
	if len(candidates) == 0 {
		if mode == moviepilotbridge.DownloaderPolicyCoordinatorPreferred {
			return moviepilotbridge.DownloaderPolicy{
				Mode:           moviepilotbridge.DownloaderPolicyMoviePilotSelect,
				FallbackReason: reason,
			}, nil
		}
		if reason == "" {
			reason = errMoviePilotCapacityUnavailable.Error()
		}
		return moviepilotbridge.DownloaderPolicy{}, fmt.Errorf("%w: %s", errMoviePilotCapacityUnavailable, reason)
	}

	reservations, err := s.activeMoviePilotReservations(ctx, time.Now().UTC())
	if err != nil {
		return moviepilotbridge.DownloaderPolicy{}, err
	}
	for i := range candidates {
		candidate := &candidates[i]
		reserved := reservations[candidate.routeID]
		candidate.reservedBytes, candidate.reservedCount = reserved.bytes, reserved.count
		candidate.availableBytes = candidate.route.DownloadFreeBytes - candidate.reservedBytes
	}
	selected, reason := chooseMoviePilotAdmissionCandidate(candidates, expectedBytes)
	if selected == nil {
		if mode == moviepilotbridge.DownloaderPolicyCoordinatorPreferred {
			return moviepilotbridge.DownloaderPolicy{Mode: moviepilotbridge.DownloaderPolicyMoviePilotSelect, FallbackReason: reason}, nil
		}
		return moviepilotbridge.DownloaderPolicy{}, fmt.Errorf("%w: %s", errMoviePilotCapacityUnavailable, reason)
	}

	now := time.Now().UTC()
	expiresAt := now.Add(moviePilotDownloaderReservationTTL)
	reservation := model.MoviePilotDownloaderReservation{
		ID: uuid.NewString(), RequestID: requestID, BridgeInstanceID: bridgeID, RouteID: selected.routeID,
		PolicyMode:   mode,
		WorkerNodeID: selected.workerID, Downloader: strings.TrimSpace(selected.route.Downloader), QBClientID: strings.TrimSpace(selected.route.QBClientID),
		ExpectedBytes: expectedBytes, Status: model.MoviePilotReservationStatusReserved, ExpiresAt: expiresAt,
	}
	if existingID != "" {
		reservation.ID = existingID
	}
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var locked []model.MoviePilotDownloaderReservation
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("status = ? AND expires_at > ?", model.MoviePilotReservationStatusReserved, now).Find(&locked).Error; err != nil {
			return err
		}
		reservedBytes, reservedCount := int64(0), 0
		for _, item := range locked {
			if item.RouteID != reservation.RouteID {
				continue
			}
			reservedCount++
			if item.ExpectedBytes > 0 && reservedBytes <= moviePilotMaxInt64-item.ExpectedBytes {
				reservedBytes += item.ExpectedBytes
			}
		}
		if !moviePilotCandidateAdmitted(*selected, expectedBytes, reservedBytes, reservedCount) {
			return fmt.Errorf("%w: route capacity changed while reserving", errMoviePilotCapacityUnavailable)
		}
		updateReservation := func(current *model.MoviePilotDownloaderReservation) error {
			if current == nil {
				return errors.New("MoviePilot downloader reservation row is required")
			}
			planned := reservation
			result := tx.Model(current).Updates(map[string]any{
				"bridge_instance_id": bridgeID, "policy_mode": mode, "route_id": planned.RouteID,
				"worker_node_id": planned.WorkerNodeID, "downloader": planned.Downloader,
				"qb_client_id": planned.QBClientID, "expected_bytes": planned.ExpectedBytes,
				"status": model.MoviePilotReservationStatusReserved, "expires_at": planned.ExpiresAt,
				"bound_at": nil, "last_error": "", "updated_at": now,
			})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return errors.New("MoviePilot downloader reservation was changed concurrently")
			}
			planned.ID = current.ID
			planned.CreatedAt = current.CreatedAt
			planned.UpdatedAt = now
			planned.BridgeInstanceID = bridgeID
			planned.PolicyMode = mode
			planned.RouteID = strings.TrimSpace(planned.RouteID)
			planned.WorkerNodeID = strings.TrimSpace(planned.WorkerNodeID)
			planned.Downloader = strings.TrimSpace(planned.Downloader)
			planned.QBClientID = strings.TrimSpace(planned.QBClientID)
			planned.ExpectedBytes = expectedBytes
			planned.Status = model.MoviePilotReservationStatusReserved
			planned.ExpiresAt = expiresAt
			planned.BoundAt = nil
			planned.LastError = ""
			reservation = planned
			return nil
		}
		if existingID != "" {
			var current model.MoviePilotDownloaderReservation
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, "id = ? AND request_id = ?", existingID, requestID).Error; err != nil {
				return err
			}
			if current.Status == model.MoviePilotReservationStatusReserved && current.ExpiresAt.After(now) || current.Status == model.MoviePilotReservationStatusBound {
				reservation = current
				return nil
			}
			if current.Status != model.MoviePilotReservationStatusReleased && current.Status != model.MoviePilotReservationStatusExpired {
				return fmt.Errorf("MoviePilot downloader reservation %q is %s and cannot be reused", current.ID, current.Status)
			}
			return updateReservation(&current)
		}
		result := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "request_id"}}, DoNothing: true}).Create(&reservation)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 1 {
			return nil
		}
		// A second Coordinator process may have won the request-id race. Treat
		// an active row as the idempotent answer; reuse an old released row in
		// the same transaction so retries cannot return an unpersisted ID.
		var current model.MoviePilotDownloaderReservation
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("request_id = ?", requestID).First(&current).Error; err != nil {
			return err
		}
		if !strings.EqualFold(strings.TrimSpace(current.BridgeInstanceID), bridgeID) {
			return fmt.Errorf("request id %q already belongs to another MoviePilot Bridge", requestID)
		}
		if current.Status == model.MoviePilotReservationStatusReserved && current.ExpiresAt.After(now) || current.Status == model.MoviePilotReservationStatusBound {
			reservation = current
			return nil
		}
		if current.Status != model.MoviePilotReservationStatusReleased && current.Status != model.MoviePilotReservationStatusExpired {
			return fmt.Errorf("MoviePilot downloader reservation %q is %s and cannot be reused", current.ID, current.Status)
		}
		return updateReservation(&current)
	}); err != nil {
		return moviepilotbridge.DownloaderPolicy{}, err
	}
	return moviePilotPolicyFromReservation(reservation), nil
}

func moviePilotPolicyFromReservation(reservation model.MoviePilotDownloaderReservation) moviepilotbridge.DownloaderPolicy {
	mode := strings.TrimSpace(reservation.PolicyMode)
	if mode == "" {
		// Reservations written before policy_mode was introduced were only
		// created for an explicitly selected downloader, so strict is the safe
		// compatibility interpretation.
		mode = moviepilotbridge.DownloaderPolicyCoordinatorSelect
	}
	return moviepilotbridge.DownloaderPolicy{
		Mode:       mode,
		Downloader: strings.TrimSpace(reservation.Downloader), RouteID: strings.TrimSpace(reservation.RouteID),
		ReservationID: strings.TrimSpace(reservation.ID),
	}
}

func (s *Service) moviePilotAdmissionCandidates(ctx context.Context, bridgeID string, now time.Time) ([]moviePilotAdmissionCandidate, string, error) {
	var inventories []model.ClusterNodeInventory
	if err := s.db.WithContext(ctx).Order("node_id ASC, revision DESC").Find(&inventories).Error; err != nil {
		return nil, "", err
	}
	seenNodes := make(map[string]struct{})
	candidates := make([]moviePilotAdmissionCandidate, 0)
	aliasCounts := make(map[string]int)
	rejection := ""
	for _, inventory := range inventories {
		if _, seen := seenNodes[inventory.NodeID]; seen {
			continue
		}
		seenNodes[inventory.NodeID] = struct{}{}
		if !inventory.CollectedAt.IsZero() && inventory.CollectedAt.UTC().Before(now.Add(-moviePilotInventoryMaxAge)) {
			rejection = "all Worker route telemetry is stale"
			continue
		}
		var node model.ClusterNode
		if err := s.db.WithContext(ctx).First(&node, "id = ?", inventory.NodeID).Error; err != nil {
			continue
		}
		if node.Status != model.ClusterNodeStatusOnline || node.Disabled || node.Drain || strings.TrimSpace(node.LastSessionID) == "" {
			rejection = "no online, non-draining Worker route is available"
			continue
		}
		var activeSession int64
		if err := s.db.WithContext(ctx).Model(&model.ClusterNodeSession{}).Where("id = ? AND node_id = ? AND status = ?", node.LastSessionID, node.ID, model.ClusterSessionStatusConnected).Count(&activeSession).Error; err != nil || activeSession != 1 {
			rejection = "no connected Worker route is available"
			continue
		}
		var capabilities protocol.NodeCapabilities
		if err := jsonUnmarshalCapabilities(inventory.CapabilitiesJSON, &capabilities); err != nil {
			continue
		}
		for _, route := range capabilities.MoviePilotRoutes {
			if !strings.EqualFold(strings.TrimSpace(route.BridgeInstanceID), bridgeID) {
				continue
			}
			if !route.DownloadCapacityKnown || !route.DownloadLoadKnown {
				rejection = "qB capacity or load telemetry is not ready"
				continue
			}
			health := strings.ToLower(strings.TrimSpace(route.QBHealth))
			if health != "ready" && health != "healthy" {
				rejection = "qB route is unhealthy"
				continue
			}
			routeID := moviePilotRouteID(bridgeID, inventory.NodeID, route.Downloader, route.QBClientID)
			aliasKey := strings.ToLower(strings.TrimSpace(route.BridgeInstanceID)) + "\x00" + strings.ToLower(strings.TrimSpace(route.Downloader))
			aliasCounts[aliasKey]++
			candidates = append(candidates, moviePilotAdmissionCandidate{route: route, workerID: inventory.NodeID, routeID: routeID})
		}
	}
	if len(candidates) == 0 {
		if rejection == "" {
			rejection = "no route is configured for the selected MoviePilot Bridge"
		}
		return nil, rejection, nil
	}
	filtered := candidates[:0]
	for _, candidate := range candidates {
		aliasKey := strings.ToLower(strings.TrimSpace(candidate.route.BridgeInstanceID)) + "\x00" + strings.ToLower(strings.TrimSpace(candidate.route.Downloader))
		if aliasCounts[aliasKey] > 1 {
			rejection = fmt.Sprintf("MoviePilot downloader %q is advertised by multiple Workers; use unique downloader aliases", candidate.route.Downloader)
			continue
		}
		filtered = append(filtered, candidate)
	}
	if len(filtered) == 0 {
		return nil, rejection, nil
	}
	return filtered, "", nil
}

// jsonUnmarshalCapabilities is kept as a tiny seam for scheduler tests and
// makes the inventory decode error explicit at this boundary.
func jsonUnmarshalCapabilities(raw string, capabilities *protocol.NodeCapabilities) error {
	return json.Unmarshal([]byte(raw), capabilities)
}

type moviePilotReservationLoad struct {
	bytes int64
	count int
}

func (s *Service) activeMoviePilotReservations(ctx context.Context, now time.Time) (map[string]moviePilotReservationLoad, error) {
	var rows []model.MoviePilotDownloaderReservation
	if err := s.db.WithContext(ctx).Where("status = ? AND expires_at > ?", model.MoviePilotReservationStatusReserved, now).Find(&rows).Error; err != nil {
		return nil, err
	}
	result := make(map[string]moviePilotReservationLoad)
	for _, row := range rows {
		value := result[row.RouteID]
		value.count++
		if row.ExpectedBytes > 0 && value.bytes <= moviePilotMaxInt64-row.ExpectedBytes {
			value.bytes += row.ExpectedBytes
		}
		result[row.RouteID] = value
	}
	return result, nil
}

func chooseMoviePilotAdmissionCandidate(candidates []moviePilotAdmissionCandidate, expectedBytes int64) (*moviePilotAdmissionCandidate, string) {
	if expectedBytes <= 0 {
		expectedBytes = moviePilotUnknownReservationBytes
	}
	eligible := make([]moviePilotAdmissionCandidate, 0, len(candidates))
	lastReason := "all qB routes are below the configured download safety margin or concurrency limit"
	rejections := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if reason := moviePilotCandidateRejectionReason(candidate, expectedBytes); reason != "" {
			label := strings.TrimSpace(candidate.route.Downloader)
			if label == "" {
				label = candidate.routeID
			}
			rejections = append(rejections, fmt.Sprintf("%s: %s", label, reason))
			continue
		}
		candidate.availableBytes -= expectedBytes
		eligible = append(eligible, candidate)
	}
	if len(eligible) == 0 {
		if len(rejections) > 0 {
			return nil, lastReason + ": " + strings.Join(rejections, "; ")
		}
		return nil, lastReason
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		left, right := eligible[i], eligible[j]
		if left.availableBytes != right.availableBytes {
			return left.availableBytes > right.availableBytes
		}
		if left.route.DownloadActiveCount != right.route.DownloadActiveCount {
			return left.route.DownloadActiveCount < right.route.DownloadActiveCount
		}
		if left.route.DownloadRemainingBytes != right.route.DownloadRemainingBytes {
			return left.route.DownloadRemainingBytes < right.route.DownloadRemainingBytes
		}
		if left.route.DownloadRateBytesPerSecond != right.route.DownloadRateBytesPerSecond {
			return left.route.DownloadRateBytesPerSecond < right.route.DownloadRateBytesPerSecond
		}
		if left.route.ActiveUploadSlots != right.route.ActiveUploadSlots {
			return left.route.ActiveUploadSlots < right.route.ActiveUploadSlots
		}
		return left.routeID < right.routeID
	})
	return &eligible[0], ""
}

func moviePilotCandidateAdmitted(candidate moviePilotAdmissionCandidate, expectedBytes, reservedBytes int64, reservedCount int) bool {
	if expectedBytes <= 0 {
		expectedBytes = moviePilotUnknownReservationBytes
	}
	if candidate.route.DownloadConcurrency > 0 && candidate.route.DownloadActiveCount+reservedCount >= candidate.route.DownloadConcurrency {
		return false
	}
	minimum := candidate.route.DownloadLowWatermarkBytes
	if candidate.route.DownloadSafetyReserveBytes > 0 && minimum <= moviePilotMaxInt64-candidate.route.DownloadSafetyReserveBytes {
		minimum += candidate.route.DownloadSafetyReserveBytes
	}
	if minimum > moviePilotMaxInt64-expectedBytes {
		return false
	}
	minimum += expectedBytes
	if reservedBytes < 0 || candidate.route.DownloadFreeBytes < reservedBytes {
		return false
	}
	return candidate.route.DownloadFreeBytes-reservedBytes >= minimum
}

func moviePilotCandidateRejectionReason(candidate moviePilotAdmissionCandidate, expectedBytes int64) string {
	if expectedBytes <= 0 {
		expectedBytes = moviePilotUnknownReservationBytes
	}
	if candidate.route.DownloadConcurrency > 0 && candidate.route.DownloadActiveCount+candidate.reservedCount >= candidate.route.DownloadConcurrency {
		return fmt.Sprintf("download concurrency reached (%d active + %d reserved >= %d)", candidate.route.DownloadActiveCount, candidate.reservedCount, candidate.route.DownloadConcurrency)
	}
	minimum := candidate.route.DownloadLowWatermarkBytes
	if candidate.route.DownloadSafetyReserveBytes > 0 && minimum <= moviePilotMaxInt64-candidate.route.DownloadSafetyReserveBytes {
		minimum += candidate.route.DownloadSafetyReserveBytes
	}
	if minimum > moviePilotMaxInt64-expectedBytes {
		return "download safety margin overflows the supported capacity range"
	}
	minimum += expectedBytes
	if candidate.reservedBytes < 0 || candidate.route.DownloadFreeBytes < candidate.reservedBytes {
		return "reserved download bytes exceed reported free space"
	}
	if candidate.route.DownloadFreeBytes-candidate.reservedBytes < minimum {
		return fmt.Sprintf("free space after reservations is below the required margin (%d < %d bytes)", candidate.route.DownloadFreeBytes-candidate.reservedBytes, minimum)
	}
	return ""
}

func moviePilotRouteID(bridgeID, workerID, downloader, qbClientID string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{strings.TrimSpace(bridgeID), strings.TrimSpace(workerID), strings.TrimSpace(downloader), strings.TrimSpace(qbClientID)}, "\x00")))
	return "route-" + hex.EncodeToString(sum[:12])
}

func (s *Service) ReleaseMoviePilotDownloaderReservation(ctx context.Context, requestID string) error {
	if s == nil || s.db == nil {
		return errors.New("cluster database is unavailable")
	}
	s.moviePilotReservationMu.Lock()
	defer s.moviePilotReservationMu.Unlock()
	return s.db.WithContext(ctx).Model(&model.MoviePilotDownloaderReservation{}).
		Where("request_id = ? AND status = ?", strings.TrimSpace(requestID), model.MoviePilotReservationStatusReserved).
		Updates(map[string]any{"status": model.MoviePilotReservationStatusReleased, "updated_at": time.Now().UTC()}).Error
}

func (s *Service) ReapMoviePilotDownloaderReservations(ctx context.Context) error {
	if s == nil || s.db == nil {
		return nil
	}
	s.moviePilotReservationMu.Lock()
	defer s.moviePilotReservationMu.Unlock()
	return s.reapMoviePilotDownloaderReservationsLocked(ctx, time.Now().UTC())
}

func (s *Service) reapMoviePilotDownloaderReservationsLocked(ctx context.Context, now time.Time) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&model.MoviePilotDownloaderReservation{}).
			Where("status = ? AND expires_at <= ?", model.MoviePilotReservationStatusReserved, now).
			Updates(map[string]any{"status": model.MoviePilotReservationStatusExpired, "updated_at": now}).Error; err != nil {
			return err
		}

		var rows []model.MoviePilotDownloaderReservation
		if err := tx.Where("status = ? AND expires_at > ?", model.MoviePilotReservationStatusReserved, now).Find(&rows).Error; err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		requestIDs := make([]string, 0, len(rows))
		for _, row := range rows {
			requestIDs = append(requestIDs, row.RequestID)
		}
		var intents []model.MoviePilotDownloadIntent
		if err := tx.Where("request_id IN ?", requestIDs).Find(&intents).Error; err != nil {
			return err
		}
		intentByRequestID := make(map[string]model.MoviePilotDownloadIntent, len(intents))
		for _, intent := range intents {
			intentByRequestID[intent.RequestID] = intent
		}
		orphanCutoff := now.Add(-moviePilotReservationOrphanGrace)
		for _, row := range rows {
			intent, found := intentByRequestID[row.RequestID]
			reason := ""
			switch {
			case !found:
				if !row.CreatedAt.IsZero() && row.CreatedAt.Before(orphanCutoff) {
					reason = "reservation has no persisted MoviePilot intent"
				}
			case strings.TrimSpace(intent.ReservationID) != row.ID:
				reason = "reservation is no longer referenced by its MoviePilot intent"
			case intent.Status == model.MoviePilotIntentStatusFailed || intent.Status == model.MoviePilotIntentStatusCancelled || intent.Status == model.MoviePilotIntentStatusCompleted:
				reason = fmt.Sprintf("MoviePilot intent is already %s", intent.Status)
			case intent.Status == model.MoviePilotIntentStatusWaitingCapacity:
				reason = "MoviePilot intent is waiting for capacity and must not hold a reservation"
			case intent.Status == model.MoviePilotIntentStatusPending &&
				(strings.TrimSpace(intent.LastErrorCode) != "" || strings.TrimSpace(intent.LastError) != "") &&
				!row.CreatedAt.IsZero() && row.CreatedAt.Before(orphanCutoff) &&
				!intent.UpdatedAt.IsZero() && intent.UpdatedAt.Before(orphanCutoff):
				// A retry can reuse a waiting-capacity intent whose previous
				// error is still visible while its new reservation is being
				// persisted. Require both records to be old before reclaiming
				// this state, so the selection-to-delivery handoff gets grace.
				reason = "pending MoviePilot intent already contains a terminal delivery error"
			}
			if reason == "" {
				continue
			}
			if err := tx.Model(&model.MoviePilotDownloaderReservation{}).
				Where("id = ? AND status = ?", row.ID, model.MoviePilotReservationStatusReserved).
				Updates(map[string]any{"status": model.MoviePilotReservationStatusReleased, "last_error": reason, "updated_at": now}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Service) bindMoviePilotDownloaderReservation(ctx context.Context, intent *model.MoviePilotDownloadIntent) error {
	if intent == nil || strings.TrimSpace(intent.ReservationID) == "" {
		return nil
	}
	now := time.Now().UTC()
	result := s.db.WithContext(ctx).Model(&model.MoviePilotDownloaderReservation{}).
		Where("id = ? AND request_id = ? AND status IN ?", strings.TrimSpace(intent.ReservationID), intent.RequestID, []string{model.MoviePilotReservationStatusReserved, model.MoviePilotReservationStatusBound}).
		Updates(map[string]any{"status": model.MoviePilotReservationStatusBound, "bound_at": now, "updated_at": now, "last_error": ""})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("MoviePilot downloader reservation %q was not found or already expired", intent.ReservationID)
	}
	return nil
}

func (s *Service) resolveReservedMoviePilotRoute(ctx context.Context, bridgeID string, intent *model.MoviePilotDownloadIntent, event moviepilotbridge.BridgeEvent) (string, string, error) {
	if intent == nil || strings.TrimSpace(intent.SelectedDownloader) == "" {
		return s.resolveMoviePilotWorkerRoute(ctx, bridgeID, event.Torrent.Downloader)
	}
	if !strings.EqualFold(strings.TrimSpace(intent.SelectedDownloader), strings.TrimSpace(event.Torrent.Downloader)) {
		return "", "", fmt.Errorf("selected downloader %q does not match MoviePilot bound downloader %q", intent.SelectedDownloader, event.Torrent.Downloader)
	}
	var reservation model.MoviePilotDownloaderReservation
	if err := s.db.WithContext(ctx).Where("id = ? AND request_id = ?", strings.TrimSpace(intent.ReservationID), intent.RequestID).First(&reservation).Error; err != nil {
		return "", "", err
	}
	if reservation.Status != model.MoviePilotReservationStatusReserved && reservation.Status != model.MoviePilotReservationStatusBound {
		return "", "", fmt.Errorf("MoviePilot downloader reservation %q is %s", reservation.ID, reservation.Status)
	}
	if reservation.ExpiresAt.Before(time.Now().UTC()) && reservation.Status == model.MoviePilotReservationStatusReserved {
		return "", "", fmt.Errorf("MoviePilot downloader reservation %q expired before torrent.bound", reservation.ID)
	}
	if !strings.EqualFold(reservation.Downloader, event.Torrent.Downloader) || !strings.EqualFold(reservation.BridgeInstanceID, intent.BridgeInstanceID) {
		return "", "", errors.New("MoviePilot downloader reservation does not match torrent.bound")
	}
	return reservation.WorkerNodeID, reservation.QBClientID, nil
}

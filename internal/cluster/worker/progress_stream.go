package worker

import (
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

const uploadProgressReportInterval = 2 * time.Second

type progressFileStreamer struct {
	model.FileStreamer
	mu           sync.Mutex
	completed    int64
	lastReported int64
	lastReportAt time.Time
	report       func(completed, total int64)
}

func newProgressFileStreamer(file model.FileStreamer, report func(completed, total int64)) model.FileStreamer {
	if file == nil || report == nil {
		return file
	}
	return &progressFileStreamer{FileStreamer: file, report: report}
}

func (s *progressFileStreamer) Read(buffer []byte) (int, error) {
	count, err := s.FileStreamer.Read(buffer)
	total := s.GetSize()
	now := time.Now()
	s.mu.Lock()
	s.completed += int64(count)
	if total > 0 && s.completed > total {
		s.completed = total
	}
	completed := s.completed
	shouldReport := completed > s.lastReported && (total > 0 && completed >= total || s.lastReportAt.IsZero() || now.Sub(s.lastReportAt) >= uploadProgressReportInterval)
	if shouldReport {
		s.lastReported = completed
		s.lastReportAt = now
	}
	s.mu.Unlock()
	if shouldReport {
		s.report(completed, total)
	}
	return count, err
}

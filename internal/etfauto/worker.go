package etfauto

import (
	"context"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

type ProcessResult struct {
	ClosedBatches int `json:"closed_batches"`
	ProcessedJobs int `json:"processed_jobs"`
}

var workerState struct {
	sync.Mutex
	cancel context.CancelFunc
}

func ProcessOnce(ctx context.Context, opts RunnerOptions) (*ProcessResult, error) {
	opts = normalizeRunnerOptions(opts)
	closed, err := CloseDueBatches(ctx, opts.Now)
	if err != nil {
		return nil, err
	}
	processed, err := RunPendingJobs(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &ProcessResult{ClosedBatches: closed, ProcessedJobs: processed}, nil
}

func StartWorker() {
	workerState.Lock()
	defer workerState.Unlock()
	if workerState.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	workerState.cancel = cancel
	go runWorker(ctx)
}

func StopWorker() {
	workerState.Lock()
	cancel := workerState.cancel
	workerState.cancel = nil
	workerState.Unlock()
	if cancel != nil {
		cancel()
	}
}

func runWorker(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := ProcessOnce(ctx, RunnerOptions{}); err != nil {
				utils.Log.Warnf("ETF auto subscription worker failed: %v", err)
			}
		}
	}
}

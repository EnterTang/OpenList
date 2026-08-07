package automation

import (
	"context"
	"fmt"
	"strings"
	"sync"

	sy "github.com/OpenListTeam/OpenList/v4/internal/115sy"
)

type FastDeleteRequest struct {
	IDs         []string
	BatchSize   int
	Concurrency int
	Retries     int
}

type FastDeleteResult struct {
	Deleted []string `json:"deleted"`
	Failed  []string `json:"failed"`
}

func FastDelete(ctx context.Context, client *sy.Client, req FastDeleteRequest) (FastDeleteResult, error) {
	if client == nil {
		return FastDeleteResult{}, fmt.Errorf("115-sy client is nil")
	}
	if req.BatchSize <= 0 {
		req.BatchSize = 100
	}
	if req.Concurrency <= 0 {
		req.Concurrency = 4
	}
	if req.Concurrency > 16 {
		req.Concurrency = 16
	}
	if req.Retries < 0 {
		req.Retries = 0
	}
	if req.Retries > 3 {
		req.Retries = 3
	}
	ids := make([]string, 0, len(req.IDs))
	seen := make(map[string]struct{}, len(req.IDs))
	for _, id := range req.IDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if id == "0" {
			return FastDeleteResult{}, fmt.Errorf("refusing to delete root cid")
		}
		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	result := FastDeleteResult{}
	var mu sync.Mutex
	jobs := make(chan []string)
	var wg sync.WaitGroup
	for worker := 0; worker < req.Concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				if err := ctx.Err(); err != nil {
					mu.Lock()
					result.Failed = append(result.Failed, batch...)
					mu.Unlock()
					continue
				}
				var err error
				for attempt := 0; attempt <= req.Retries; attempt++ {
					err = client.RemoveMany(ctx, batch)
					if err == nil || ctx.Err() != nil {
						break
					}
				}
				mu.Lock()
				if err != nil {
					result.Failed = append(result.Failed, batch...)
				} else {
					result.Deleted = append(result.Deleted, batch...)
				}
				mu.Unlock()
			}
		}()
	}
	for start := 0; start < len(ids); start += req.BatchSize {
		end := start + req.BatchSize
		if end > len(ids) {
			end = len(ids)
		}
		batch := append([]string(nil), ids[start:end]...)
		select {
		case jobs <- batch:
		case <-ctx.Done():
			mu.Lock()
			result.Failed = append(result.Failed, ids[start:]...)
			mu.Unlock()
			close(jobs)
			wg.Wait()
			return result, ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()
	if len(result.Failed) > 0 {
		return result, fmt.Errorf("fast delete failed for %d items", len(result.Failed))
	}
	return result, nil
}

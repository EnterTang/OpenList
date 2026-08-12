package _115_cd2

import (
	"context"
	"io"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
)

type throttledRangeReader struct {
	upstream model.RangeReaderIF
	waiter   requestWaiter
}

func (r *throttledRangeReader) RangeRead(ctx context.Context, httpRange http_range.Range) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.waiter != nil {
		if err := r.waiter.Wait(ctx); err != nil {
			return nil, err
		}
	}
	return r.upstream.RangeRead(ctx, httpRange)
}

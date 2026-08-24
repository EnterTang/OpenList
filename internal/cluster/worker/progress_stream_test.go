package worker

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
)

func TestProgressFileStreamerReportsFinalReadPosition(t *testing.T) {
	source := []byte("0123456789")
	file := &stream.FileStream{
		Ctx: context.Background(), Obj: &model.Object{Name: "episode.mkv", Size: int64(len(source))},
		Reader: bytes.NewReader(source),
	}
	var completed, total int64
	wrapped := newProgressFileStreamer(file, func(current, size int64) {
		completed, total = current, size
	})
	content, err := io.ReadAll(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != string(source) || completed != int64(len(source)) || total != int64(len(source)) {
		t.Fatalf("content=%q completed=%d total=%d", content, completed, total)
	}
}

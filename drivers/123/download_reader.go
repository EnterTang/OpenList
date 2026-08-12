package _123

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	netpkg "github.com/OpenListTeam/OpenList/v4/internal/net"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	log "github.com/sirupsen/logrus"
)

const (
	pan123FallbackChunkSize  = int64(64 * utils.MB)
	pan123FallbackMaxRetries = 3
)

type pan123ResolvedDownload struct {
	URL    string
	Header http.Header
}

type pan123FallbackConfig struct {
	ChunkSize  int64
	MaxRetries int
}

type pan123DownloadResolver func(context.Context) (pan123ResolvedDownload, error)

type pan123DownloadReader struct {
	ctx       context.Context
	totalSize int64
	endOffset int64
	offset    int64
	remaining int64

	current          io.ReadCloser
	currentRemaining int64
	resolved         pan123ResolvedDownload
	refresh          pan123DownloadResolver
	config           pan123FallbackConfig
	fallback         bool
	forceRefresh     bool
	fallbackBytes    int64
	fallbackFailures int
	lastErr          error
	fatalErr         error
}

type pan123SourceReadError struct {
	Offset   int64
	Expected int64
	Attempts int
	Err      error
}

func (e *pan123SourceReadError) Error() string {
	return fmt.Sprintf("123pan source read failed after range fallback: offset=%d expected=%d attempts=%d: %s", e.Offset, e.Expected, e.Attempts, safePan123Error(e.Err))
}

func (e *pan123SourceReadError) Unwrap() error {
	return e.Err
}

func (e *pan123SourceReadError) ClusterErrorCode() string {
	if errors.Is(e.Err, io.EOF) || errors.Is(e.Err, io.ErrUnexpectedEOF) {
		return "source_unexpected_eof"
	}
	return "source_range_failed"
}

func newPan123DownloadReader(
	ctx context.Context,
	totalSize int64,
	httpRange http_range.Range,
	initial pan123ResolvedDownload,
	refresh pan123DownloadResolver,
	config pan123FallbackConfig,
) (io.ReadCloser, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if totalSize < 0 {
		return nil, errors.New("123pan source size is unknown")
	}
	if httpRange.Start < 0 || httpRange.Start > totalSize {
		return nil, errors.New("123pan source range start is invalid")
	}
	if httpRange.Length < 0 || httpRange.Start+httpRange.Length > totalSize {
		httpRange.Length = totalSize - httpRange.Start
	}
	if config.ChunkSize <= 0 {
		config.ChunkSize = pan123FallbackChunkSize
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = pan123FallbackMaxRetries
	}
	reader := &pan123DownloadReader{
		ctx: ctx, totalSize: totalSize, endOffset: httpRange.Start + httpRange.Length,
		offset: httpRange.Start, remaining: httpRange.Length,
		resolved: initial, refresh: refresh, config: config,
	}
	if httpRange.Length == 0 {
		return reader, nil
	}
	current, err := openPan123DownloadRange(ctx, initial, totalSize, httpRange.Start, httpRange.Length)
	if err == nil {
		reader.current = current
		reader.currentRemaining = httpRange.Length
		return reader, nil
	}
	if ctxErr := context.Cause(ctx); ctxErr != nil {
		return nil, ctxErr
	}
	reader.fallback = true
	reader.forceRefresh = true
	reader.lastErr = err
	if err := reader.openFallbackChunk(); err != nil {
		return nil, err
	}
	return reader, nil
}

func (r *pan123DownloadReader) Read(p []byte) (int, error) {
	for {
		if r.fatalErr != nil {
			return 0, r.fatalErr
		}
		if err := context.Cause(r.ctx); err != nil {
			return 0, err
		}
		if r.remaining == 0 {
			return 0, io.EOF
		}
		if r.current == nil {
			if err := r.openFallbackChunk(); err != nil {
				return 0, err
			}
		}
		limit := min(int64(len(p)), r.currentRemaining, r.remaining)
		if limit <= 0 {
			_ = r.current.Close()
			r.current = nil
			continue
		}
		n, err := r.current.Read(p[:limit])
		if n > 0 {
			r.offset += int64(n)
			r.remaining -= int64(n)
			r.currentRemaining -= int64(n)
			if r.fallback {
				r.fallbackBytes += int64(n)
			}
		}

		if r.currentRemaining == 0 {
			_ = r.current.Close()
			r.current = nil
			if r.remaining == 0 {
				if n > 0 {
					return n, nil
				}
				return 0, io.EOF
			}
			if n > 0 {
				return n, nil
			}
			continue
		}

		if err == nil {
			if n == 0 {
				return 0, nil
			}
			return n, nil
		}
		if ctxErr := context.Cause(r.ctx); ctxErr != nil {
			return n, ctxErr
		}

		recoveryErr := err
		if errors.Is(err, io.EOF) {
			recoveryErr = io.ErrUnexpectedEOF
		}
		r.beginFallback(recoveryErr)
		if n > 0 {
			return n, nil
		}
		if r.fatalErr != nil {
			return 0, r.fatalErr
		}
	}
}

func (r *pan123DownloadReader) beginFallback(err error) {
	if r.current != nil {
		_ = r.current.Close()
		r.current = nil
	}
	if !r.fallback {
		log.Warnf("123pan source stream ended early; switching to range fallback: offset=%d expected=%d err_type=%T", r.offset, r.endOffset, err)
	} else {
		r.fallbackFailures++
		log.Warnf("123pan range response ended early; refreshing download link: offset=%d expected=%d err_type=%T", r.offset, r.endOffset, err)
		if r.fallbackFailures >= r.config.MaxRetries {
			r.fatalErr = &pan123SourceReadError{
				Offset: r.offset, Expected: r.endOffset, Attempts: r.fallbackFailures, Err: err,
			}
		}
	}
	r.fallback = true
	r.forceRefresh = true
	r.lastErr = err
}

func (r *pan123DownloadReader) openFallbackChunk() error {
	if !r.fallback {
		return errors.New("123pan source reader has no active response")
	}
	length := min(r.config.ChunkSize, r.remaining)
	var lastErr = r.lastErr
	for attempt := 1; attempt <= r.config.MaxRetries; attempt++ {
		if err := context.Cause(r.ctx); err != nil {
			return err
		}
		if r.forceRefresh || r.resolved.URL == "" {
			if r.refresh == nil {
				lastErr = errors.New("123pan download link refresher is unavailable")
			} else {
				resolved, err := r.refresh(r.ctx)
				if err == nil {
					r.resolved = resolved
					r.forceRefresh = false
				} else {
					lastErr = err
				}
			}
		}
		if !r.forceRefresh && r.resolved.URL != "" {
			current, err := openPan123DownloadRange(r.ctx, r.resolved, r.totalSize, r.offset, length)
			if err == nil {
				r.current = current
				r.currentRemaining = length
				r.lastErr = nil
				return nil
			}
			lastErr = err
			r.forceRefresh = true
		}
		if attempt < r.config.MaxRetries {
			delay := time.NewTimer(time.Duration(attempt) * 200 * time.Millisecond)
			select {
			case <-r.ctx.Done():
				delay.Stop()
				return context.Cause(r.ctx)
			case <-delay.C:
			}
		}
	}
	if lastErr == nil {
		lastErr = io.ErrUnexpectedEOF
	}
	return &pan123SourceReadError{
		Offset: r.offset, Expected: r.endOffset, Attempts: r.config.MaxRetries, Err: lastErr,
	}
}

func (r *pan123DownloadReader) Close() error {
	if r.current == nil {
		return nil
	}
	err := r.current.Close()
	r.current = nil
	return err
}

func openPan123DownloadRange(ctx context.Context, resolved pan123ResolvedDownload, totalSize, start, length int64) (io.ReadCloser, error) {
	if resolved.URL == "" {
		return nil, errors.New("123pan download URL is empty")
	}
	header := resolved.Header.Clone()
	if start == 0 && length == totalSize {
		header.Del("Range")
	} else {
		header = http_range.ApplyRangeToHttpHeader(http_range.Range{Start: start, Length: length}, header)
	}
	response, err := netpkg.RequestHttp(ctx, http.MethodGet, header, resolved.URL)
	if err != nil {
		return nil, err
	}
	valid := false
	if start == 0 && length == totalSize && response.StatusCode == http.StatusOK {
		valid = true
	}
	if response.StatusCode == http.StatusPartialContent {
		contentStart, contentEnd, parseErr := http_range.ParseContentRange(response.Header.Get("Content-Range"))
		valid = parseErr == nil && contentStart == start && contentEnd == start+length-1
	}
	if !valid {
		_ = response.Body.Close()
		return nil, fmt.Errorf("123pan range response is invalid: status=%d start=%d length=%d content_range=%q", response.StatusCode, start, length, response.Header.Get("Content-Range"))
	}
	if response.ContentLength >= 0 && response.ContentLength != length {
		_ = response.Body.Close()
		return nil, fmt.Errorf("123pan range response length mismatch: expect=%d actual=%d", length, response.ContentLength)
	}
	return response.Body, nil
}

func safePan123Error(err error) string {
	if err == nil {
		return ""
	}
	parts := strings.Fields(err.Error())
	for i := range parts {
		if strings.Contains(parts[i], "://") {
			parts[i] = "<redacted-url>"
		}
	}
	return strings.Join(parts, " ")
}

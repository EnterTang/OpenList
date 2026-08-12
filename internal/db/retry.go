package db

import (
	"context"
	stderrors "errors"
	"strings"
	"time"

	sqlite3 "modernc.org/sqlite/lib"
)

const (
	sqliteBusyRetryMaxAttempts = 3
	sqliteBusyRetryBaseBackoff = 10 * time.Millisecond
)

func withSQLiteBusyRetry(ctx context.Context, operation func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for attempt := 1; attempt <= sqliteBusyRetryMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := operation(ctx)
		if err == nil {
			return nil
		}
		if !isSQLiteBusyOrLockedError(err) || attempt == sqliteBusyRetryMaxAttempts {
			return err
		}
		if err := waitSQLiteBusyRetryBackoff(ctx, attempt); err != nil {
			return err
		}
	}
	return nil
}

func isSQLiteBusyOrLockedError(err error) bool {
	var sqliteErr interface {
		error
		Code() int
	}
	if !stderrors.As(err, &sqliteErr) {
		return false
	}
	switch sqliteErr.Code() & 0xff {
	case sqlite3.SQLITE_BUSY:
		return strings.Contains(strings.ToUpper(sqliteErr.Error()), "SQLITE_BUSY") ||
			strings.Contains(strings.ToLower(sqliteErr.Error()), "database is locked")
	case sqlite3.SQLITE_LOCKED:
		return strings.Contains(strings.ToUpper(sqliteErr.Error()), "SQLITE_LOCKED") ||
			strings.Contains(strings.ToLower(sqliteErr.Error()), "database table is locked")
	default:
		return false
	}
}

func waitSQLiteBusyRetryBackoff(ctx context.Context, attempt int) error {
	delay := sqliteBusyRetryBaseBackoff << (attempt - 1)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

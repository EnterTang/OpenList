package db

import (
	"context"
	stderrors "errors"
	"testing"

	sqlite3 "modernc.org/sqlite/lib"
)

func TestWithSQLiteBusyRetrySucceedsAfterTransientBusy(t *testing.T) {
	attempts := 0
	err := withSQLiteBusyRetry(context.Background(), func(ctx context.Context) error {
		attempts++
		if attempts < 3 {
			return newTestSQLiteError(sqlite3.SQLITE_BUSY)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("withSQLiteBusyRetry error = %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestWithSQLiteBusyRetryStopsAtMaxAttempts(t *testing.T) {
	attempts := 0
	err := withSQLiteBusyRetry(context.Background(), func(ctx context.Context) error {
		attempts++
		return newTestSQLiteError(sqlite3.SQLITE_BUSY)
	})
	if err == nil {
		t.Fatal("withSQLiteBusyRetry unexpectedly succeeded")
	}
	var sqliteErr interface {
		error
		Code() int
	}
	if !stderrors.As(err, &sqliteErr) {
		t.Fatalf("error = %T %v, want sqlite-coded error", err, err)
	}
	if sqliteErr.Code() != sqlite3.SQLITE_BUSY {
		t.Fatalf("sqlite error code = %d, want %d", sqliteErr.Code(), sqlite3.SQLITE_BUSY)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestWithSQLiteBusyRetryStopsWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	err := withSQLiteBusyRetry(ctx, func(ctx context.Context) error {
		attempts++
		cancel()
		return newTestSQLiteError(sqlite3.SQLITE_BUSY)
	})
	if !stderrors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestWithSQLiteBusyRetryReturnsNonSQLiteErrorsImmediately(t *testing.T) {
	attempts := 0
	expected := stderrors.New("boom")
	err := withSQLiteBusyRetry(context.Background(), func(ctx context.Context) error {
		attempts++
		return expected
	})
	if !stderrors.Is(err, expected) {
		t.Fatalf("error = %v, want %v", err, expected)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func newTestSQLiteError(code int) error {
	switch code & 0xff {
	case sqlite3.SQLITE_BUSY:
		return testSQLiteCodeError{code: code, msg: "database is locked (5) (SQLITE_BUSY)"}
	case sqlite3.SQLITE_LOCKED:
		return testSQLiteCodeError{code: code, msg: "database table is locked (6) (SQLITE_LOCKED)"}
	default:
		return testSQLiteCodeError{code: code, msg: "sqlite error"}
	}
}

type testSQLiteCodeError struct {
	code int
	msg  string
}

func (e testSQLiteCodeError) Error() string {
	return e.msg
}

func (e testSQLiteCodeError) Code() int {
	return e.code
}

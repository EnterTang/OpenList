//go:build !sqlite_cgo_compat && !(linux && (mips || mips64 || mips64le || mipsle || loong64)) && !(windows && 386)

package bootstrap

import (
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func sqliteDSN(path string) string {
	return path + "?_txlock=immediate&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=auto_vacuum(incremental)"
}

func openSQLite(dsn string) gorm.Dialector {
	return sqlite.Open(dsn)
}

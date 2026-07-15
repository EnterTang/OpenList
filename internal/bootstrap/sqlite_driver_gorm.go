//go:build sqlite_cgo_compat || (linux && (mips || mips64 || mips64le || mipsle || loong64)) || (windows && 386)

package bootstrap

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func sqliteDSN(path string) string {
	return path + "?_txlock=immediate&_journal=WAL&_busy_timeout=5000&_vacuum=incremental"
}

func openSQLite(dsn string) gorm.Dialector {
	return sqlite.Open(dsn)
}

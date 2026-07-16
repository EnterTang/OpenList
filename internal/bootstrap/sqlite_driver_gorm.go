//go:build sqlite_cgo_compat || (linux && (mips || mips64 || mips64le || mipsle || loong64)) || (windows && 386)

package bootstrap

import (
	"net/url"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func openSQLite(target string) gorm.Dialector {
	return sqlite.Open(sqliteDSN(target, url.Values{
		"_journal":      {"WAL"},
		"_busy_timeout": {"5000"},
		"_txlock":       {"immediate"},
		"_vacuum":       {"incremental"},
	}))
}
